#!/usr/bin/env bash
#
# 1) Resolve cluster name from Ambari (configs/ambari.env or env).
# 2) GET .../clusters/<cluster>/services/KUDU/components/KUDU_MASTER for
#    kudu.master_addresses (comma-separated if several masters).
# 3) kinit as impala/<FQDN>, run impala-shell SQL: Kudu table create + insert + select.
# 4) kinit as kudu/<FQDN> (kudu service keytab on this host), then Kudu CLI against the
#    first master host from step 2 (same host Ambari reports for KUDU_MASTER), port 7051.
#
#    List tables (namespaced tables created via Impala show as impala::<db>.<table>):
#      kudu table list  <kudu-master-host>:7051
#    Scan rows for the Impala-managed Kudu table:
#      kudu table scan <kudu-master-host>:7051 impala::kudu_db.test_kudu
#    (From a Kudu install tree this is often: ./bin/kudu table list ... / table scan ...)
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   KUDU_MASTER_ADDRESSES   comma-separated hosts; if set, skips Ambari KUDU_MASTER fetch
#   IMPALA_KEYTAB           default /etc/security/keytabs/impala.service.keytab
#   IMPALA_PRINCIPAL_HOST   default hostname -f / hostname (must match impala service keytab)
#   IMPALAD                 default $(hostname):21050
#   IMPALA_SHELL            default impala-shell
#   KUDU_NUM_TABLET_REPLICAS default 1
#   KUDU_HASH_PARTITIONS     default 3
#   KUDU_KEYTAB              default /etc/security/keytabs/kudu.keytab (for kudu CLI)
#   KUDU_PRINCIPAL_HOST      host in kudu/<host> (default: same as IMPALA principal host)
#   KUDU_CLI                 default /usr/odp/current/kudu/bin/kudu
#   KUDU_MASTER_RPC_PORT     default 7051 (kudu CLI master address port)
#   KUDU_NATIVE_TABLE        default impala::kudu_db.test_kudu (kudu table scan name)
#   KUDU_CLI_SKIP            if "1", skip kudu binary list/scan after Impala
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
IMPALA_KEYTAB="${IMPALA_KEYTAB:-/etc/security/keytabs/impala.service.keytab}"
IMPALA_SHELL="${IMPALA_SHELL:-impala-shell}"
KUDU_NUM_TABLET_REPLICAS="${KUDU_NUM_TABLET_REPLICAS:-1}"
KUDU_HASH_PARTITIONS="${KUDU_HASH_PARTITIONS:-3}"
KUDU_KEYTAB="${KUDU_KEYTAB:-/etc/security/keytabs/kudu.keytab}"
KUDU_CLI="${KUDU_CLI:-/usr/odp/current/kudu/bin/kudu}"
KUDU_MASTER_RPC_PORT="${KUDU_MASTER_RPC_PORT:-7051}"
KUDU_NATIVE_TABLE="${KUDU_NATIVE_TABLE:-impala::kudu_db.test_kudu}"
KUDU_CLI_SKIP="${KUDU_CLI_SKIP:-0}"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

strip_quotes() {
  local v="$1"
  if [[ ${#v} -ge 2 && ${v:0:1} == '"' && ${v: -1} == '"' ]]; then
    printf '%s' "${v:1:${#v}-2}"
  elif [[ ${#v} -ge 2 && ${v:0:1} == "'" && ${v: -1} == "'" ]]; then
    printf '%s' "${v:1:${#v}-2}"
  else
    printf '%s' "$v"
  fi
}

load_ambari_env_file() {
  local f="$1" key val line
  [[ -f "$f" ]] || return 1
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ -z "${line//[[:space:]]/}" ]] && continue
    line="${line#export[[:space:]]}"
    [[ "$line" != *=* ]] && continue
    key="${line%%=*}"
    val="${line#*=}"
    key="${key%"${key##*[![:space:]]}"}"
    key="${key#"${key%%[![:space:]]*}"}"
    val="${val%"${val##*[![:space:]]}"}"
    val="${val#"${val%%[![:space:]]*}"}"
    val="$(strip_quotes "$val")"
    case "$key" in
      AMBARI_BASE_URL) _cfg_AMBARI_BASE_URL="$val" ;;
      AMBARI_USER) _cfg_AMBARI_USER="$val" ;;
      AMBARI_PASSWORD) _cfg_AMBARI_PASSWORD="$val" ;;
    esac
  done <"$f"
  return 0
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

resolve_impala_host() {
  if [[ -n "${IMPALA_PRINCIPAL_HOST:-}" ]]; then
    printf '%s' "$IMPALA_PRINCIPAL_HOST"
    return
  fi
  local h
  h="$(hostname -f 2>/dev/null || true)"
  if [[ -z "$h" ]]; then
    h="$(hostname)"
  fi
  [[ -n "$h" ]] || die "could not determine FQDN for impala principal; set IMPALA_PRINCIPAL_HOST"
  printf '%s' "$h"
}

resolve_impalad() {
  if [[ -n "${IMPALAD:-}" ]]; then
    printf '%s' "$IMPALAD"
    return
  fi
  local h
  h="$(hostname)"
  [[ -n "$h" ]] || die "hostname returned empty string"
  printf '%s' "${h}:21050"
}

resolve_kudu_principal_host() {
  if [[ -n "${KUDU_PRINCIPAL_HOST:-}" ]]; then
    printf '%s' "$KUDU_PRINCIPAL_HOST"
    return
  fi
  resolve_impala_host
}

kudu_master_rpc_addr() {
  local first="${kudu_masters%%,*}"
  first="${first#"${first%%[![:space:]]*}"}"
  first="${first%"${first##*[![:space:]]}"}"
  [[ -n "$first" ]] || die "empty Kudu master host"
  printf '%s' "${first}:${KUDU_MASTER_RPC_PORT}"
}

need_cmd curl
need_cmd kinit
need_cmd python3
if ! command -v "$IMPALA_SHELL" >/dev/null 2>&1 && [[ ! -x "$IMPALA_SHELL" ]]; then
  die "impala-shell not found or not executable: $IMPALA_SHELL"
fi

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""

if [[ -n "${CLUSTER_NAME:-}" ]]; then
  :
elif [[ -f "$AMBARI_CONFIG_FILE" ]]; then
  load_ambari_env_file "$AMBARI_CONFIG_FILE" || die "failed to read $AMBARI_CONFIG_FILE"
elif [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]]; then
  :
else
  die "Missing Ambari credentials. Create ${AMBARI_CONFIG_FILE} (copy from ${SCRIPT_DIR}/configs/ambari.env.example) or set AMBARI_USER and AMBARI_PASSWORD. To skip the clusters API, set CLUSTER_NAME (and set KUDU_MASTER_ADDRESSES or keep Ambari creds for the KUDU_MASTER API)."
fi

AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://10.101.11.22:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"

if [[ -z "${CLUSTER_NAME:-}" ]]; then
  [[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]] || die "AMBARI_USER and AMBARI_PASSWORD must be set (or set CLUSTER_NAME with KUDU_MASTER_ADDRESSES)."
fi

if [[ ! -r "$IMPALA_KEYTAB" ]]; then
  die "keytab not readable: $IMPALA_KEYTAB"
fi

if [[ "$KUDU_CLI_SKIP" != "1" && ! -x "$KUDU_CLI" ]]; then
  die "Kudu CLI not executable: $KUDU_CLI (set KUDU_CLI or KUDU_CLI_SKIP=1)"
fi

if [[ "$KUDU_CLI_SKIP" != "1" && ! -r "$KUDU_KEYTAB" ]]; then
  die "keytab not readable: $KUDU_KEYTAB (set KUDU_KEYTAB or KUDU_CLI_SKIP=1)"
fi

if [[ -n "${CLUSTER_NAME:-}" ]]; then
  cluster="$CLUSTER_NAME"
else
  clusters_url="${AMBARI_BASE_URL%/}/api/v1/clusters/"
  curl_opts=(
    -sS -f
    -u "${AMBARI_USER}:${AMBARI_PASSWORD}"
    -H "X-Requested-By: ambari"
  )
  json="$(curl "${curl_opts[@]}" "$clusters_url")" || die "failed to GET $clusters_url"
  cluster="$(
    printf '%s\n' "$json" | python3 -c "
import json, sys
data = json.load(sys.stdin)
items = data.get('items') or []
if not items:
    sys.exit('no clusters in Ambari response')
name = (items[0].get('Clusters') or {}).get('cluster_name')
if not name:
    sys.exit('could not parse cluster_name from Ambari response')
print(name)
"
  )" || die "could not parse cluster name from Ambari JSON"
fi

if [[ -n "${KUDU_MASTER_ADDRESSES:-}" ]]; then
  kudu_masters="$KUDU_MASTER_ADDRESSES"
else
  [[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]] || die "Set KUDU_MASTER_ADDRESSES or provide Ambari credentials to discover Kudu masters."
  kudu_master_url="${AMBARI_BASE_URL%/}/api/v1/clusters/${cluster}/services/KUDU/components/KUDU_MASTER"
  curl_opts=(
    -sS -f
    -u "${AMBARI_USER}:${AMBARI_PASSWORD}"
    -H "X-Requested-By: ambari"
  )
  kudu_json="$(curl "${curl_opts[@]}" "$kudu_master_url")" || die "failed to GET $kudu_master_url (is KUDU installed?)"
  kudu_masters="$(
    printf '%s\n' "$kudu_json" | python3 -c "
import json, sys
data = json.load(sys.stdin)
hcs = data.get('host_components') or []
hosts = []
for hc in hcs:
    hn = (hc.get('HostRoles') or {}).get('host_name')
    if hn:
        hosts.append(hn)
if not hosts:
    sys.exit('no KUDU_MASTER host_name in Ambari response')
print(','.join(hosts))
"
  )" || die "could not parse Kudu master host(s) from Ambari JSON"
fi

impala_host="$(resolve_impala_host)"
principal="impala/${impala_host}"
impalad="$(resolve_impalad)"

sql_tmp="$(mktemp)"
cleanup() { rm -f "$sql_tmp"; }
trap cleanup EXIT

cat >"$sql_tmp" <<EOF
CREATE DATABASE IF NOT EXISTS kudu_db;
USE kudu_db;
DROP TABLE IF EXISTS test_kudu;
CREATE TABLE test_kudu (
  id INT PRIMARY KEY,
  name STRING
)
PARTITION BY HASH(id) PARTITIONS ${KUDU_HASH_PARTITIONS}
STORED AS KUDU
TBLPROPERTIES (
  'kudu.master_addresses' = '${kudu_masters}',
  'kudu.num_tablet_replicas' = '${KUDU_NUM_TABLET_REPLICAS}'
);
INSERT INTO test_kudu VALUES
(1, 'test kudu'),
(2, 'smoke_evt_8k2m'),
(3, 'row_q7p9x4'),
(4, 'sample_nm_41'),
(5, 'kudu_rand_a3'),
(6, 'data_blob_9z'),
(7, 'tmp_rec_6hf'),
(8, 'ingest_x1w'),
(9, 'batch_y4v2'),
(10, 'test kudu-10');
SELECT * FROM test_kudu ORDER BY id;
EOF

echo "Cluster:              ${cluster}"
echo "Kudu master_addresses: ${kudu_masters}"
echo "Impala principal:     ${principal}"
echo "impala-shell -i:      ${impalad}"

kinit -kt "$IMPALA_KEYTAB" "$principal" || die "kinit failed"

echo "---- ${IMPALA_SHELL} -i ${impalad} -f ${sql_tmp} ----"
"${IMPALA_SHELL}" -i "$impalad" -f "$sql_tmp"

echo "OK: Impala + Kudu table load finished."

if [[ "$KUDU_CLI_SKIP" == "1" ]]; then
  echo "KUDU_CLI_SKIP=1: skipping kudu CLI list/scan."
  exit 0
fi

kudu_host="$(resolve_kudu_principal_host)"
kudu_principal="kudu/${kudu_host}"
kudu_rpc="$(kudu_master_rpc_addr)"

echo "---- kinit (Kudu CLI) ${kudu_principal} ----"
kinit -kt "$KUDU_KEYTAB" "$kudu_principal" || die "kinit failed for Kudu CLI"

echo "---- Kudu CLI: list tables on ${kudu_rpc} ----"
echo "    ${KUDU_CLI} table list ${kudu_rpc}"
"$KUDU_CLI" table list "$kudu_rpc"

echo "---- Kudu CLI: scan table ${KUDU_NATIVE_TABLE} on ${kudu_rpc} ----"
echo "    ${KUDU_CLI} table scan ${kudu_rpc} ${KUDU_NATIVE_TABLE}"
if ! "$KUDU_CLI" table scan "$kudu_rpc" "$KUDU_NATIVE_TABLE"; then
  echo "WARN: kudu table scan failed. List tables above and set KUDU_NATIVE_TABLE if the Impala table id differs." >&2
fi

echo "OK: Kudu smoke test (Impala + kudu CLI) finished."
