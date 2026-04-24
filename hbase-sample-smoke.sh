#!/usr/bin/env bash
#
# Resolve cluster name from Ambari (same pattern as hdfs/yarn smoke scripts),
# kinit as hbase-<cluster> with the HBase headless keytab, then run hbase shell
# commands from hbase/hbase-sample-smoke.hbase.
#
# Ambari credentials: configs/ambari.env (see configs/ambari.env.example), or env vars.
#
# Environment (optional):
#   AMBARI_CONFIG_FILE      default <script-dir>/configs/ambari.env
#   AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD
#   CLUSTER_NAME            If set, skip Ambari lookup
#   HBASE_KEYTAB            default /etc/security/keytabs/hbase.headless.keytab
#   HBASE_SMOKE_SCRIPT      default <script-dir>/hbase/hbase-sample-smoke.hbase
#   HBASE_SMOKE_DROP_FIRST  if "1" (default), best-effort disable/drop tables before create
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
HBASE_KEYTAB="${HBASE_KEYTAB:-/etc/security/keytabs/hbase.headless.keytab}"
HBASE_SMOKE_SCRIPT="${HBASE_SMOKE_SCRIPT:-${SCRIPT_DIR}/hbase/hbase-sample-smoke.hbase}"
HBASE_SMOKE_DROP_FIRST="${HBASE_SMOKE_DROP_FIRST:-1}"

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

need_cmd curl
need_cmd kinit
need_cmd hbase
need_cmd python3

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
  die "Missing Ambari credentials. Create ${AMBARI_CONFIG_FILE} (copy from ${SCRIPT_DIR}/configs/ambari.env.example) or set AMBARI_USER and AMBARI_PASSWORD in the environment."
fi

AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://10.101.11.22:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"

if [[ -z "${CLUSTER_NAME:-}" ]]; then
  [[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]] || die "AMBARI_USER and AMBARI_PASSWORD must be set in ${AMBARI_CONFIG_FILE} or in the environment."
fi

if [[ ! -r "$HBASE_KEYTAB" ]]; then
  die "keytab not readable: $HBASE_KEYTAB"
fi

if [[ ! -r "$HBASE_SMOKE_SCRIPT" ]]; then
  die "HBase shell script not readable: $HBASE_SMOKE_SCRIPT"
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

principal="hbase-${cluster}"
echo "Using cluster: ${cluster}"
echo "kinit principal: ${principal} (realm from krb5.conf / keytab)"

kinit -kt "$HBASE_KEYTAB" "$principal" || die "kinit failed"

if [[ "$HBASE_SMOKE_DROP_FIRST" == "1" ]]; then
  echo "---- hbase shell (best-effort disable/drop sample_table_1/2) ----"
  hbase shell <<'EOF' || true
disable 'sample_table_1'
drop 'sample_table_1'
disable 'sample_table_2'
drop 'sample_table_2'
EOF
fi

echo "---- hbase shell - ${HBASE_SMOKE_SCRIPT} ----"
hbase shell "$HBASE_SMOKE_SCRIPT"

echo "OK: HBase sample smoke finished."
