#!/usr/bin/env bash
#
# Resolve cluster name from Ambari (same as hdfs-headless-smoke.sh), kinit as the
# smoke user, then run MapReduce pi via yarn jar.
#
# The job runs as ambari-qa rather than hdfs: the LinuxContainerExecutor lists
# hdfs/yarn/mapred/bin in banned.users, so containers launched as hdfs are
# rejected with "Requested user hdfs is banned".
#
# Ambari credentials: configs/ambari.env, or env vars.
#
# Environment (optional):
#   AMBARI_CONFIG_FILE   default <script-dir>/configs/ambari.env
#   AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD
#   CLUSTER_NAME         If set, skip Ambari lookup
#   SMOKE_USER           default ambari-qa
#   SMOKE_KEYTAB         default /etc/security/keytabs/smokeuser.headless.keytab
#   SMOKE_PRINCIPAL      default resolved from the keytab
#   MR_EXAMPLES_JAR      default /usr/odp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar
#   MR_PI_MAPS           default 1  (first arg to pi example)
#   MR_PI_SAMPLES        default 1  (second arg to pi example)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
SMOKE_USER="${SMOKE_USER:-ambari-qa}"
SMOKE_KEYTAB="${SMOKE_KEYTAB:-/etc/security/keytabs/smokeuser.headless.keytab}"
MR_EXAMPLES_JAR="${MR_EXAMPLES_JAR:-/usr/odp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar}"
MR_PI_MAPS="${MR_PI_MAPS:-1}"
MR_PI_SAMPLES="${MR_PI_SAMPLES:-1}"

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
need_cmd yarn
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
  die "Missing Ambari credentials. Edit ${AMBARI_CONFIG_FILE} or set AMBARI_USER and AMBARI_PASSWORD in the environment."
fi

AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://10.101.11.22:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"

if [[ -z "${CLUSTER_NAME:-}" ]]; then
  [[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]] || die "AMBARI_USER and AMBARI_PASSWORD must be set in ${AMBARI_CONFIG_FILE} or in the environment."
fi

if [[ ! -r "$SMOKE_KEYTAB" ]]; then
  die "keytab not readable: $SMOKE_KEYTAB"
fi

if [[ ! -r "$MR_EXAMPLES_JAR" ]]; then
  die "MapReduce examples jar not readable: $MR_EXAMPLES_JAR"
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

# Read the principal out of the keytab: the Ambari cluster name may differ in
# case from the principal (for example cluster Rol73upg vs ambari-qa-rol73upg).
principal="${SMOKE_PRINCIPAL:-}"
if [[ -z "$principal" ]]; then
  principal="$(klist -kt "$SMOKE_KEYTAB" 2>/dev/null |
    awk -v u="$SMOKE_USER" '$NF ~ "^" u "[-/@]" { print $NF; exit }')"
fi
[[ -n "$principal" ]] || principal="${SMOKE_USER}-${cluster}"

echo "Using cluster: ${cluster}"
echo "Smoke user: ${SMOKE_USER}"
echo "kinit principal: ${principal} (keytab ${SMOKE_KEYTAB})"

ccache="/tmp/krb5cc_yarn_smoke_$$"
cleanup() {
  rm -f "$ccache" 2>/dev/null || true
}
trap cleanup EXIT

echo "---- yarn jar ${MR_EXAMPLES_JAR} pi ${MR_PI_MAPS} ${MR_PI_SAMPLES} ----"

run_cmd="export KRB5CCNAME=$(printf '%q' "$ccache")
kinit -kt $(printf '%q' "$SMOKE_KEYTAB") $(printf '%q' "$principal")
yarn jar $(printf '%q' "$MR_EXAMPLES_JAR") pi $(printf '%q' "$MR_PI_MAPS") $(printf '%q' "$MR_PI_SAMPLES")"

if [[ "$(id -un)" != "$SMOKE_USER" ]] && id -u "$SMOKE_USER" >/dev/null 2>&1; then
  su -s /bin/bash "$SMOKE_USER" -c "set -e; $run_cmd" || die "YARN MapReduce sample failed"
else
  bash -c "set -e; $run_cmd" || die "YARN MapReduce sample failed"
fi

echo "OK: YARN MapReduce sample (pi) finished."
