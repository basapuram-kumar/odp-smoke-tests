#!/usr/bin/env bash
#
# Smoke: ClickHouse HTTP (and optional native client) create/insert/select/drop.
#
# Steps:
#   1) Resolve ClickHouse HTTP URL (Ambari CLICKHOUSE_SERVER host + http_port, or env)
#   2) Optional kinit as clickhouse-<cluster> (headless keytab)
#   3) SELECT version()
#   4) CREATE DATABASE + MergeTree table, INSERT rows, SELECT count/data
#   5) Optional: list cluster topology from system.clusters
#   6) DROP DATABASE (unless CLICKHOUSE_KEEP_DB=1)
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   CLICKHOUSE_ENV_FILE / CLICKHOUSE_CONFIG_FILE  default <script-dir>/configs/clickhouse.env
#   CLICKHOUSE_HTTP_URL     e.g. http://host:8123 (skip Ambari host discovery when set)
#   CLICKHOUSE_HOST         used with CLICKHOUSE_HTTP_PORT if URL unset
#   CLICKHOUSE_HTTP_PORT    default 8123
#   CLICKHOUSE_TCP_PORT     default 9001 (native client)
#   CLICKHOUSE_USER         default default
#   CLICKHOUSE_PASSWORD     default empty
#   CLICKHOUSE_DATABASE     default odp_ch_smoke
#   CLICKHOUSE_TABLE        default smoke_t
#   CLICKHOUSE_KEEP_DB      default 0 - drop smoke database at end
#   CLICKHOUSE_SKIP_KINIT   default 1 (HTTP default user usually needs no Kerberos)
#   CLICKHOUSE_KEYTAB       default /etc/security/keytabs/clickhouse.headless.keytab
#   CLICKHOUSE_CLIENT       optional path to clickhouse binary (multi-tool) or clickhouse-client
#   CLICKHOUSE_USE_CLIENT   if 1, also run a native-client SELECT after HTTP checks
#   CURL_EXTRA_OPTS         e.g. -k
#
# Usage:
#   ./clickhouse-sample-smoke.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
CLICKHOUSE_ENV_FILE="${CLICKHOUSE_ENV_FILE:-${CLICKHOUSE_CONFIG_FILE:-${SCRIPT_DIR}/configs/clickhouse.env}}"
CLICKHOUSE_KEYTAB="${CLICKHOUSE_KEYTAB:-/etc/security/keytabs/clickhouse.headless.keytab}"

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

_cfg_CLICKHOUSE_HTTP_URL=""
_cfg_CLICKHOUSE_HOST=""
_cfg_CLICKHOUSE_USER=""
_cfg_CLICKHOUSE_PASSWORD=""

load_clickhouse_env_file() {
  local f="$1" key val line
  [[ -f "$f" ]] || return 0
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
      CLICKHOUSE_HTTP_URL) _cfg_CLICKHOUSE_HTTP_URL="$val" ;;
      CLICKHOUSE_HOST) _cfg_CLICKHOUSE_HOST="$val" ;;
      CLICKHOUSE_HTTP_PORT) [[ "${CLICKHOUSE_HTTP_PORT+set}" == "set" ]] || CLICKHOUSE_HTTP_PORT="$val" ;;
      CLICKHOUSE_TCP_PORT) [[ "${CLICKHOUSE_TCP_PORT+set}" == "set" ]] || CLICKHOUSE_TCP_PORT="$val" ;;
      CLICKHOUSE_USER) _cfg_CLICKHOUSE_USER="$val" ;;
      CLICKHOUSE_PASSWORD) _cfg_CLICKHOUSE_PASSWORD="$val" ;;
      CLICKHOUSE_DATABASE) [[ "${CLICKHOUSE_DATABASE+set}" == "set" ]] || CLICKHOUSE_DATABASE="$val" ;;
      CLICKHOUSE_TABLE) [[ "${CLICKHOUSE_TABLE+set}" == "set" ]] || CLICKHOUSE_TABLE="$val" ;;
      CLICKHOUSE_SKIP_KINIT) [[ "${CLICKHOUSE_SKIP_KINIT+set}" == "set" ]] || CLICKHOUSE_SKIP_KINIT="$val" ;;
      CLICKHOUSE_USE_CLIENT) [[ "${CLICKHOUSE_USE_CLIENT+set}" == "set" ]] || CLICKHOUSE_USE_CLIENT="$val" ;;
      CLICKHOUSE_KEEP_DB) [[ "${CLICKHOUSE_KEEP_DB+set}" == "set" ]] || CLICKHOUSE_KEEP_DB="$val" ;;
      CLUSTER_NAME) [[ "${CLUSTER_NAME+set}" == "set" ]] || CLUSTER_NAME="$val" ;;
    esac
  done <"$f"
  return 0
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

ambari_curl() {
  # shellcheck disable=SC2086
  curl -sS -f ${CURL_EXTRA_OPTS:-} -u "${AMBARI_USER}:${AMBARI_PASSWORD}" -H "X-Requested-By: ambari" "$@"
}

need_cmd curl
need_cmd python3

load_clickhouse_env_file "$CLICKHOUSE_ENV_FILE" || die "failed to read $CLICKHOUSE_ENV_FILE"
CLICKHOUSE_HTTP_URL="${CLICKHOUSE_HTTP_URL:-${_cfg_CLICKHOUSE_HTTP_URL:-}}"
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-${_cfg_CLICKHOUSE_HOST:-}}"
CLICKHOUSE_HTTP_PORT="${CLICKHOUSE_HTTP_PORT:-8123}"
CLICKHOUSE_TCP_PORT="${CLICKHOUSE_TCP_PORT:-9001}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-${_cfg_CLICKHOUSE_USER:-default}}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-${_cfg_CLICKHOUSE_PASSWORD:-}}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-odp_ch_smoke}"
CLICKHOUSE_TABLE="${CLICKHOUSE_TABLE:-smoke_t}"
CLICKHOUSE_SKIP_KINIT="${CLICKHOUSE_SKIP_KINIT:-1}"
CLICKHOUSE_USE_CLIENT="${CLICKHOUSE_USE_CLIENT:-0}"
CLICKHOUSE_KEEP_DB="${CLICKHOUSE_KEEP_DB:-0}"

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""

need_ambari=0
if [[ -z "${CLICKHOUSE_HTTP_URL:-}" && -z "${CLICKHOUSE_HOST:-}" ]]; then
  need_ambari=1
fi
if [[ "${CLICKHOUSE_SKIP_KINIT}" != "1" && -z "${CLUSTER_NAME:-}" ]]; then
  need_ambari=1
fi

if [[ "$need_ambari" == "1" ]]; then
  if [[ -f "$AMBARI_CONFIG_FILE" ]]; then
    load_ambari_env_file "$AMBARI_CONFIG_FILE" || die "failed to read $AMBARI_CONFIG_FILE"
  elif [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]]; then
    :
  else
    die "Missing Ambari context or CLICKHOUSE_HTTP_URL/CLICKHOUSE_HOST. Create ${AMBARI_CONFIG_FILE} or set CLICKHOUSE_HTTP_URL."
  fi
fi

AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://127.0.0.1:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"

cluster="${CLUSTER_NAME:-}"
if [[ -z "$cluster" && "$need_ambari" == "1" ]]; then
  [[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]] || die "Ambari credentials required"
  clusters_url="${AMBARI_BASE_URL%/}/api/v1/clusters/"
  json="$(ambari_curl "$clusters_url")" || die "failed to GET $clusters_url"
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

discover_clickhouse_http_url() {
  CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}" \
    AMBARI_USER="$AMBARI_USER" AMBARI_PASSWORD="$AMBARI_PASSWORD" \
    python3 - "$AMBARI_BASE_URL" "$cluster" <<'PY'
import json, os, shlex, subprocess, sys, urllib.parse

def curl_extra():
    raw = os.environ.get("CURL_EXTRA_OPTS", "").strip()
    return shlex.split(raw) if raw else []

def curl_json(url):
    user, pw = os.environ["AMBARI_USER"], os.environ["AMBARI_PASSWORD"]
    cmd = ["curl", "-sS", "-f", "-u", f"{user}:{pw}", "-H", "X-Requested-By: ambari"] + curl_extra() + [url]
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if r.returncode != 0:
        sys.stderr.write(r.stderr or r.stdout or "curl failed\n")
        sys.exit(r.returncode)
    return json.loads(r.stdout)

ambari, cluster = sys.argv[1].rstrip("/"), sys.argv[2]
qc = urllib.parse.quote(cluster, safe="")
j = curl_json(f"{ambari}/api/v1/clusters/{qc}?fields=Clusters/desired_configs")
dc = (j.get("Clusters") or {}).get("desired_configs") or {}
# Prefer clickhouse-server-config / network config for http_port
port = "8123"
for ctype in ("clickhouse-server-config", "clickhouse-network-and-logging", "clickhouse-application"):
    meta = dc.get(ctype) or {}
    tag = meta.get("tag")
    if not tag:
        continue
    j2 = curl_json(
        f"{ambari}/api/v1/clusters/{qc}/configurations?type={urllib.parse.quote(ctype)}&tag={urllib.parse.quote(tag)}"
    )
    items = j2.get("items") or []
    if not items:
        continue
    props = items[0].get("properties") or {}
    for key in ("http_port", "clickhouse.http_port", "http_port_secure"):
        if props.get(key):
            port = str(props.get(key)).strip()
            break

hc = curl_json(
    f"{ambari}/api/v1/clusters/{qc}/host_components"
    f"?HostRoles/component_name=CLICKHOUSE_SERVER"
    f"&fields=HostRoles/host_name,HostRoles/public_host_name,HostRoles/state"
)
host = None
for it in hc.get("items") or []:
    hr = it.get("HostRoles") or {}
    cand = hr.get("public_host_name") or hr.get("host_name")
    if (hr.get("state") or "").upper() == "STARTED" and cand:
        host = cand
        break
if not host:
    for it in hc.get("items") or []:
        hr = it.get("HostRoles") or {}
        host = hr.get("public_host_name") or hr.get("host_name")
        if host:
            break
if not host:
    sys.stderr.write("No CLICKHOUSE_SERVER host in Ambari; set CLICKHOUSE_HTTP_URL\n")
    sys.exit(2)
print(f"http://{host}:{port}".rstrip("/"))
PY
}

if [[ -n "${CLICKHOUSE_HTTP_URL:-}" ]]; then
  http_base="${CLICKHOUSE_HTTP_URL%/}"
elif [[ -n "${CLICKHOUSE_HOST:-}" ]]; then
  http_base="http://${CLICKHOUSE_HOST}:${CLICKHOUSE_HTTP_PORT}"
else
  [[ -n "$cluster" ]] || die "cluster name required for Ambari ClickHouse discovery"
  http_base="$(discover_clickhouse_http_url)" || die "Could not discover ClickHouse HTTP URL; set CLICKHOUSE_HTTP_URL."
fi

# Derive host for native client from URL if unset
if [[ -z "${CLICKHOUSE_HOST:-}" ]]; then
  CLICKHOUSE_HOST="$(python3 - "$http_base" <<'PY'
import sys
from urllib.parse import urlparse
u=urlparse(sys.argv[1])
print(u.hostname or "")
PY
)"
fi

echo "---- ClickHouse sample smoke ----"
echo "    HTTP URL: $http_base"
echo "    user: $CLICKHOUSE_USER"
echo "    database: $CLICKHOUSE_DATABASE  table: $CLICKHOUSE_TABLE"
[[ -n "$cluster" ]] && echo "    cluster: $cluster"

if [[ "$CLICKHOUSE_SKIP_KINIT" == "1" ]]; then
  echo "---- CLICKHOUSE_SKIP_KINIT=1 - skipping kinit ----"
else
  need_cmd kinit
  [[ -n "$cluster" ]] || die "CLUSTER_NAME required for kinit (or set CLICKHOUSE_SKIP_KINIT=1)"
  [[ -r "$CLICKHOUSE_KEYTAB" ]] || die "keytab not readable: $CLICKHOUSE_KEYTAB"
  principal="clickhouse-${cluster}"
  echo "kinit principal: ${principal}"
  kinit -kt "$CLICKHOUSE_KEYTAB" "$principal" || die "kinit failed"
fi

ch_query() {
  local sql="$1"
  local out http_code
  # shellcheck disable=SC2086
  out="$(curl -sS ${CURL_EXTRA_OPTS:-} -w "\n%{http_code}" \
    -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
    "${http_base}/" \
    --data-binary "$sql")" || die "curl failed for query: $sql"
  http_code="${out##*$'\n'}"
  out="${out%$'\n'*}"
  if [[ "$http_code" != "200" ]]; then
    die "ClickHouse HTTP $http_code for query: $sql :: $out"
  fi
  # ClickHouse returns exception text with 200 in some setups; detect Code:
  if printf '%s' "$out" | grep -q '^Code:'; then
    die "ClickHouse error: $out"
  fi
  printf '%s' "$out"
}

echo "---- SELECT version() ----"
ver="$(ch_query "SELECT version()")"
echo "    version=$ver"
[[ -n "$ver" ]] || die "empty version()"

echo "---- CREATE DATABASE / TABLE ----"
ch_query "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DATABASE}" >/dev/null
ch_query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TABLE}" >/dev/null
ch_query "CREATE TABLE ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TABLE} (id UInt32, label String) ENGINE = MergeTree ORDER BY id" >/dev/null

echo "---- INSERT + SELECT ----"
ch_query "INSERT INTO ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TABLE} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')" >/dev/null
cnt="$(ch_query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TABLE}")"
echo "    count=$cnt"
[[ "$cnt" == "3" ]] || die "expected 3 rows, got: $cnt"
rows="$(ch_query "SELECT id, label FROM ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TABLE} ORDER BY id FORMAT TabSeparated")"
echo "    rows:"
printf '%s\n' "$rows" | sed 's/^/      /'
printf '%s\n' "$rows" | grep -q $'1\talpha' || die "missing row 1/alpha"
printf '%s\n' "$rows" | grep -q $'3\tgamma' || die "missing row 3/gamma"

echo "---- system.clusters (best-effort) ----"
clusters_out="$(ch_query "SELECT cluster, shard_num, replica_num, host_name FROM system.clusters WHERE cluster != 'default' OR 1 FORMAT TabSeparated" || true)"
if [[ -n "$clusters_out" ]]; then
  printf '%s\n' "$clusters_out" | head -20 | sed 's/^/      /'
else
  echo "    (no rows / skipped)"
fi

if [[ "$CLICKHOUSE_USE_CLIENT" == "1" ]]; then
  echo "---- native client SELECT ----"
  client_bin="${CLICKHOUSE_CLIENT:-}"
  if [[ -z "$client_bin" ]]; then
    if [[ -x /usr/odp/current/clickhouse/bin/clickhouse ]]; then
      client_bin=/usr/odp/current/clickhouse/bin/clickhouse
    elif command -v clickhouse-client >/dev/null 2>&1; then
      client_bin="$(command -v clickhouse-client)"
    elif command -v clickhouse >/dev/null 2>&1; then
      client_bin="$(command -v clickhouse)"
    else
      die "CLICKHOUSE_USE_CLIENT=1 but no clickhouse client found; set CLICKHOUSE_CLIENT"
    fi
  fi
  native_q="SELECT count() FROM ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TABLE}"
  if [[ "$(basename "$client_bin")" == "clickhouse-client" ]]; then
    native_cnt="$("$client_bin" -h "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_TCP_PORT" -u "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" -q "$native_q")"
  else
    native_cnt="$("$client_bin" client -h "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_TCP_PORT" -u "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" -q "$native_q")"
  fi
  echo "    native count=$native_cnt"
  [[ "$native_cnt" == "3" ]] || die "native client expected 3 rows, got: $native_cnt"
fi

if [[ "$CLICKHOUSE_KEEP_DB" == "1" ]]; then
  echo "---- CLICKHOUSE_KEEP_DB=1 - leaving ${CLICKHOUSE_DATABASE} ----"
else
  echo "---- DROP DATABASE ----"
  ch_query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}" >/dev/null
fi

echo "OK: ClickHouse sample smoke finished (version=$ver)."
