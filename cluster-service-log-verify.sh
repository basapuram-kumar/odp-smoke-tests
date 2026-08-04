#!/usr/bin/env bash
#
# Verify Ambari cluster service/component logs are present and generating.
#
# Discovers STARTED host-components from Ambari, SSHs to each Ambari-reported
# host IP, optionally restarts the component via Ambari, then checks
# /var/log/<service> for log growth and ERROR/FATAL/Exception samples.
#
# Hosts are never hardcoded -- they come from the Ambari API.
# Ambari URL is read from configs/ambari.env (AMBARI_BASE_URL).
#
# Config (optional):
#   configs/ambari.env       Ambari URL/user/password (required source for Ambari host)
#   configs/log-verify.env   SSH + restart / filter defaults for this utility
#
# Usage:
#   ./cluster-service-log-verify.sh
#   ./cluster-service-log-verify.sh --dry-run
#   SSH_KEY=/path/to/key.pem ./cluster-service-log-verify.sh --no-restart
#   LOG_VERIFY_SERVICES=HDFS,ZOOKEEPER ./cluster-service-log-verify.sh
#   LOG_VERIFY_RESTART=1 ./cluster-service-log-verify.sh
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

# Load KEY=VALUE from env file into exported shell vars.
# If fill_empty=1, also replace existing empty values.
load_env_file_defaults() {
  local f="$1" fill_empty="${2:-0}" key val line
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
    if [[ -z "${!key+x}" ]]; then
      export "$key=$val"
    elif [[ "$fill_empty" == "1" && -z "${!key}" && -n "$val" ]]; then
      export "$key=$val"
    fi
  done <"$f"
}

resolve_ambari_config_file() {
  local f
  if [[ -n "${AMBARI_CONFIG_FILE:-}" ]]; then
    [[ -f "$AMBARI_CONFIG_FILE" ]] || die "AMBARI_CONFIG_FILE not found: $AMBARI_CONFIG_FILE"
    printf '%s' "$AMBARI_CONFIG_FILE"
    return 0
  fi
  for f in \
    "${SCRIPT_DIR}/configs/ambari.env" \
    "${SCRIPT_DIR}/configs/ambari.config"; do
    if [[ -f "$f" ]]; then
      printf '%s' "$f"
      return 0
    fi
  done
  return 1
}

ambari_reachable() {
  local url="$1"
  local user="${AMBARI_USER:-admin}"
  local pass="${AMBARI_PASSWORD:-admin}"
  curl -sS -f -m 5 -u "${user}:${pass}" -H "X-Requested-By: ambari" \
    "${url%/}/api/v1/clusters" >/dev/null 2>&1
}

resolve_ambari_base_url() {
  if [[ -n "${AMBARI_BASE_URL:-}" ]]; then
    echo "[INFO] Ambari URL from config/env: ${AMBARI_BASE_URL}"
    return 0
  fi
  local candidate
  for candidate in \
    "http://127.0.0.1:8080" \
    "http://localhost:8080"; do
    if ambari_reachable "$candidate"; then
      AMBARI_BASE_URL="$candidate"
      export AMBARI_BASE_URL
      echo "[INFO] Auto-detected Ambari at ${AMBARI_BASE_URL}"
      return 0
    fi
  done
  die "AMBARI_BASE_URL missing. Set it in configs/ambari.env (e.g. AMBARI_BASE_URL=http://<ambari-host>:8080)"
}

# Tighten private key perms if ssh would reject them (e.g. 0644).
ensure_ssh_key_perms() {
  local key="$1"
  [[ -n "$key" && -f "$key" ]] || return 0
  local mode
  mode="$(stat -c '%a' "$key" 2>/dev/null || stat -f '%OLp' "$key" 2>/dev/null || echo "")"
  [[ -n "$mode" ]] || return 0
  # Reject if group/other have any access bits
  if [[ "$mode" != "600" && "$mode" != "400" ]]; then
    echo "[WARN] SSH key permissions are ${mode}; setting to 600: ${key}"
    chmod 600 "$key" || die "failed to chmod 600 ${key} (ssh requires a private key)"
  fi
}

need_cmd python3
need_cmd ssh
need_cmd curl

[[ -f "$PY_SCRIPT" ]] || die "missing $PY_SCRIPT"

# SSH / verify options from log-verify.env (does not own Ambari host URL)
load_env_file_defaults "${LOG_VERIFY_CONFIG_FILE:-${SCRIPT_DIR}/configs/log-verify.env}"

# Ambari host/URL/user/password from configs/ambari.env (required for Ambari target)
AMBARI_CFG="$(resolve_ambari_config_file)" || \
  die "Missing Ambari config. Create configs/ambari.env with AMBARI_BASE_URL=http://<host>:8080"
load_env_file_defaults "$AMBARI_CFG" 1
echo "[INFO] Loaded Ambari config: ${AMBARI_CFG}"

# Expand leading ~ in SSH_KEY if present
if [[ -n "${SSH_KEY:-}" ]]; then
  SSH_KEY="${SSH_KEY/#\~/$HOME}"
  export SSH_KEY
fi

# Common local PEM fallbacks when SSH_KEY unset (no cluster host IPs hardcoded)
if [[ -z "${SSH_KEY:-}" ]]; then
  for candidate in \
    "${HOME}/Downloads/usdc.pem" \
    "${HOME}/usdc.pem" \
    "/root/Downloads/usdc.pem" \
    "${HOME}/.ssh/id_rsa" \
    "${HOME}/.ssh/id_ed25519"; do
    if [[ -f "$candidate" ]]; then
      SSH_KEY="$candidate"
      export SSH_KEY
      echo "[INFO] Using SSH key ${SSH_KEY}"
      break
    fi
  done
fi

if [[ -n "${SSH_KEY:-}" ]]; then
  [[ -f "$SSH_KEY" ]] || die "SSH_KEY not found: ${SSH_KEY}"
  ensure_ssh_key_perms "$SSH_KEY"
fi

resolve_ambari_base_url

# Default report dir under this package
export LOG_VERIFY_REPORT_DIR="${LOG_VERIFY_REPORT_DIR:-${SCRIPT_DIR}/reports/log-verify}"
export AMBARI_BASE_URL AMBARI_USER AMBARI_PASSWORD
export SSH_USER="${SSH_USER:-acceldata}"
export SSH_KEY="${SSH_KEY:-}"
export SSH_OPTS="${SSH_OPTS:--o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=15}"

echo "[INFO] Cluster service log verification"
echo "[INFO] Ambari=${AMBARI_BASE_URL} user=${AMBARI_USER:-} restart=${LOG_VERIFY_RESTART:-0}"
echo "[INFO] SSH user=${SSH_USER} key=${SSH_KEY:-"(ssh default identity)"}"
echo "[INFO] Report dir=${LOG_VERIFY_REPORT_DIR}"

cd "$SCRIPT_DIR"
exec python3 "$PY_SCRIPT" "$@"
