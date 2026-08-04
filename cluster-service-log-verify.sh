#!/usr/bin/env bash
#
# Verify Ambari cluster service/component logs are present and generating.
#
# Discovers STARTED host-components from Ambari, SSHs to each host (using the
# Ambari-reported IP), optionally restarts the component via Ambari, then
# checks /var/log/<service> (with known path overrides) for log growth and
# ERROR/FATAL/Exception samples. Writes markdown + TSV reports.
#
# Config (optional):
#   configs/log-verify.env   SSH + Ambari + restart defaults for this utility
#   configs/ambari.env       fallback Ambari URL/user/password
#
# Usage:
#   ./cluster-service-log-verify.sh
#   ./cluster-service-log-verify.sh --dry-run
#   LOG_VERIFY_SERVICES=HDFS,ZOOKEEPER ./cluster-service-log-verify.sh
#   LOG_VERIFY_RESTART=0 ./cluster-service-log-verify.sh          # verify only
#   LOG_VERIFY_RESTART=1 ./cluster-service-log-verify.sh          # restart + verify
#   LOG_VERIFY_COMPONENTS=NAMENODE,ZOOKEEPER_SERVER ./cluster-service-log-verify.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY_SCRIPT="${SCRIPT_DIR}/lib/cluster_log_verify.py"

die() {
  echo "[ERROR] $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
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

# Load KEY=VALUE from env file into exported shell vars (does not override existing).
load_env_file_defaults() {
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
    # Only set if not already present in the environment
    if [[ -z "${!key+x}" ]]; then
      export "$key=$val"
    fi
  done <"$f"
}

need_cmd python3
need_cmd ssh
need_cmd curl

[[ -f "$PY_SCRIPT" ]] || die "missing $PY_SCRIPT"

# Prefer log-verify.env, then ambari.env for Ambari credentials
load_env_file_defaults "${LOG_VERIFY_CONFIG_FILE:-${SCRIPT_DIR}/configs/log-verify.env}"
load_env_file_defaults "${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"

# Expand leading ~ in SSH_KEY if present
if [[ -n "${SSH_KEY:-}" ]]; then
  SSH_KEY="${SSH_KEY/#\~/$HOME}"
  export SSH_KEY
fi

# Default report dir under this package
export LOG_VERIFY_REPORT_DIR="${LOG_VERIFY_REPORT_DIR:-${SCRIPT_DIR}/reports/log-verify}"

echo "[INFO] Cluster service log verification"
echo "[INFO] Ambari=${AMBARI_BASE_URL:-} user=${AMBARI_USER:-} restart=${LOG_VERIFY_RESTART:-0}"
echo "[INFO] SSH user=${SSH_USER:-acceldata} key=${SSH_KEY:-}"
echo "[INFO] Report dir=${LOG_VERIFY_REPORT_DIR}"

cd "$SCRIPT_DIR"
exec python3 "$PY_SCRIPT" "$@"
