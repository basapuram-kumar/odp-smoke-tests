#!/usr/bin/env bash
#
# Fetch cluster name from Ambari, kinit as hdfs-<cluster>, run sample HDFS checks.
# Intended to run on a cluster node as root (or a user with the hdfs headless keytab).
#
# Ambari credentials: read from configs/ambari.env (see ambari.env.example), or override
# with environment variables (exported values win over the file).
#
# Environment (optional):
#   AMBARI_CONFIG_FILE  Path to env file (default: <script-dir>/configs/ambari.env)
#   AMBARI_BASE_URL
#   AMBARI_USER
#   AMBARI_PASSWORD
#   HDFS_KEYTAB         default /etc/security/keytabs/hdfs.headless.keytab
#   CLUSTER_NAME        If set, skip Ambari lookup and use this value only
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
HDFS_KEYTAB="${HDFS_KEYTAB:-/etc/security/keytabs/hdfs.headless.keytab}"

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

# Populate _cfg_AMBARI_* from KEY=value lines (comments and blank lines ignored).
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
need_cmd hdfs
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

if [[ ! -r "$HDFS_KEYTAB" ]]; then
  die "keytab not readable: $HDFS_KEYTAB"
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

principal="hdfs-${cluster}"
echo "Using cluster: ${cluster}"
echo "kinit principal: ${principal} (realm from krb5.conf / keytab)"

kinit -kt "$HDFS_KEYTAB" "$principal" || die "kinit failed"

echo "---- hdfs dfs -ls / ----"
hdfs dfs -ls /

echo "---- hdfs dfs -put /etc/hosts /tmp/ ----"
hdfs dfs -put /etc/hosts /tmp/

echo "---- hdfs dfs -ls /tmp/ ----"
hdfs dfs -ls /tmp/

echo "OK: HDFS smoke steps finished."
