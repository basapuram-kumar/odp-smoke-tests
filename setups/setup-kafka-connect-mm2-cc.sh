#!/usr/bin/env bash
#
# Setups: install/configure Kafka (2 or 3) Connect, MirrorMaker2, and Cruise Control
# via Ambari. Wraps setups/setup_kafka_connect_mm2_cc.py
#
# Loads:
#   configs/ambari.env                         Ambari URL / user / password / CLUSTER_NAME
#   configs/setup-kafka-connect-mm2-cc.env     MM2 dest, SSH host prep, RF, flavor
#
# Environment / CLI (optional):
#   --ambari-url URL
#   --mm2-dest HOST:PORT          e.g. rl8kmm2n1:6667
#   --flavor auto|kafka|kafka3
#   --config PATH.json            use an explicit JSON config (skips generated merge)
#   --dry-run
#   --skip-host-setup
#   AMBARI_BASE_URL / AMBARI_USER / AMBARI_PASSWORD / CLUSTER_NAME
#   MM2_DEST_BOOTSTRAP_SERVERS / KAFKA_FLAVOR / SETUP_SSH_KEY / ...
#
# Usage:
#   ./setups/setup-kafka-connect-mm2-cc.sh
#   ./setups/setup-kafka-connect-mm2-cc.sh --ambari-url http://10.101.11.23:8080 --mm2-dest rl8kmm2n1:6667
#   ./setups/setup-kafka-connect-mm2-cc.sh --flavor kafka3 --dry-run
#   ./setups/setup-kafka-connect-mm2-cc.sh --config setups/examples/setup_kafka3_connect_mm2_cc.json
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PY_SCRIPT="${SCRIPT_DIR}/setup_kafka_connect_mm2_cc.py"

die() { echo "[ERROR] $*" >&2; exit 1; }
need_cmd() { command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"; }

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

load_env_file() {
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
    # Do not override values already present in the environment.
    if [[ -z "${!key+x}" ]]; then
      printf -v "$key" '%s' "$val"
      export "$key"
    fi
  done <"$f"
}

need_cmd python3
[[ -f "$PY_SCRIPT" ]] || die "missing $PY_SCRIPT"

# Load repo configs (Ambari first, then setup defaults).
if [[ -n "${AMBARI_CONFIG_FILE:-}" ]]; then
  load_env_file "$AMBARI_CONFIG_FILE"
else
  if [[ -f "${REPO_DIR}/configs/ambari.env" ]]; then
    load_env_file "${REPO_DIR}/configs/ambari.env"
  elif [[ -f "${REPO_DIR}/configs/ambari.config" ]]; then
    load_env_file "${REPO_DIR}/configs/ambari.config"
  fi
fi
load_env_file "${SETUP_KAFKA_CC_ENV_FILE:-${REPO_DIR}/configs/setup-kafka-connect-mm2-cc.env}"

CLI_AMBARI_URL=""
CLI_CLUSTER_NAME=""
CLI_MM2_DEST=""
CLI_FLAVOR=""
CLI_SSH_HOST=""
CLI_SSH_KEY=""
CLI_CONFIG=""
DRY_RUN=0
SKIP_HOST_SETUP=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ambari-url) CLI_AMBARI_URL="${2:-}"; shift 2 ;;
    --cluster-name) CLI_CLUSTER_NAME="${2:-}"; shift 2 ;;
    --mm2-dest) CLI_MM2_DEST="${2:-}"; shift 2 ;;
    --flavor) CLI_FLAVOR="${2:-}"; shift 2 ;;
    --ssh-host) CLI_SSH_HOST="${2:-}"; shift 2 ;;
    --ssh-key) CLI_SSH_KEY="${2:-}"; shift 2 ;;
    --config) CLI_CONFIG="${2:-}"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --skip-host-setup) SKIP_HOST_SETUP=1; shift ;;
    -h|--help)
      sed -n '2,30p' "$0"
      exit 0
      ;;
    *)
      die "unknown argument: $1 (try --help)"
      ;;
  esac
done

AMBARI_URL="${CLI_AMBARI_URL:-${AMBARI_BASE_URL:-}}"
[[ -n "$AMBARI_URL" ]] || die "AMBARI_BASE_URL missing. Set it in configs/ambari.env or pass --ambari-url"

MM2_DEST="${CLI_MM2_DEST:-${MM2_DEST_BOOTSTRAP_SERVERS:-}}"
[[ -n "$MM2_DEST" || -n "$CLI_CONFIG" ]] || die "MM2 dest missing. Set MM2_DEST_BOOTSTRAP_SERVERS in configs/setup-kafka-connect-mm2-cc.env or pass --mm2-dest"

# Exports used by JSON merge and CLI overrides below.
export AMBARI_URL
export AMBARI_USER="${AMBARI_USER:-admin}"
export AMBARI_PASSWORD="${AMBARI_PASSWORD:-admin}"
export CLUSTER_NAME="${CLUSTER_NAME:-}"
export MM2_DEST
export KAFKA_FLAVOR="${CLI_FLAVOR:-${KAFKA_FLAVOR:-auto}}"
export SETUP_SSH_HOST="${CLI_SSH_HOST:-${SETUP_SSH_HOST:-}}"
export SETUP_SSH_KEY="${CLI_SSH_KEY:-${SETUP_SSH_KEY:-}}"

TMP_JSON=""
cleanup() {
  [[ -n "$TMP_JSON" && -f "$TMP_JSON" ]] && rm -f "$TMP_JSON"
}
trap cleanup EXIT

if [[ -n "$CLI_CONFIG" ]]; then
  [[ -f "$CLI_CONFIG" ]] || die "config file not found: $CLI_CONFIG"
  CONFIG_PATH="$CLI_CONFIG"
else
  TMP_JSON="$(mktemp -t setup-kafka-cc.XXXXXX.json)"
  CONFIG_PATH="$TMP_JSON"
  python3 - "$CONFIG_PATH" <<'PY'
import json, os, sys
path = sys.argv[1]
cfg = {
  "ambari": {
    "url": os.environ.get("AMBARI_URL", ""),
    "username": os.environ.get("AMBARI_USER", "admin"),
    "password": os.environ.get("AMBARI_PASSWORD", "admin"),
    "cluster_name": os.environ.get("CLUSTER_NAME", ""),
    "verify_ssl": False,
  },
  "kafka": {
    "flavor": os.environ.get("KAFKA_FLAVOR", "auto"),
    "broker_port": None,
    "replication_factor": int(os.environ.get("KAFKA_REPLICATION_FACTOR", "1")),
    "min_insync_replicas": int(os.environ.get("KAFKA_MIN_INSYNC_REPLICAS", "1")),
    "disable_ranger_plugin": os.environ.get("KAFKA_DISABLE_RANGER_PLUGIN", "true").lower() in ("1", "true", "yes"),
    "authorizer_class": os.environ.get("KAFKA_AUTHORIZER_CLASS", ""),
    "kerberos_realm": os.environ.get("KAFKA_KERBEROS_REALM", "ADSRE.COM"),
  },
  "mm2": {
    "source_cluster_alias": os.environ.get("MM2_SOURCE_CLUSTER_ALIAS", ""),
    "dest_bootstrap_servers": os.environ.get("MM2_DEST", ""),
    "dest_security_protocol": os.environ.get("MM2_DEST_SECURITY_PROTOCOL", "PLAINTEXT"),
    "dest_cluster_alias": os.environ.get("MM2_DEST_CLUSTER_ALIAS", "dest"),
    "topics": os.environ.get("MM2_TOPICS", ".*"),
    "source_security_protocol": os.environ.get("MM2_SOURCE_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
    "enable_source_to_dest": True,
    "enable_dest_to_source": False,
  },
  "connect": {"rest_port": None},
  "cruise_control": {"webserver_http_port": None},
  "host_setup": {
    "enabled": os.environ.get("SETUP_HOST_ENABLED", "true").lower() in ("1", "true", "yes"),
    "ssh_user": os.environ.get("SETUP_SSH_USER", "acceldata"),
    "ssh_key": os.environ.get("SETUP_SSH_KEY", ""),
    "ssh_host": os.environ.get("SETUP_SSH_HOST", ""),
    "dest_hosts_entry": os.environ.get("SETUP_DEST_HOSTS_ENTRY", ""),
    "copy_credential_util_from": os.environ.get("SETUP_COPY_CREDENTIAL_UTIL_FROM", ""),
  },
  "operations": {
    "install_components": os.environ.get("SETUP_INSTALL_COMPONENTS", "true").lower() in ("1", "true", "yes"),
    "apply_configs": os.environ.get("SETUP_APPLY_CONFIGS", "true").lower() in ("1", "true", "yes"),
    "restart_broker": os.environ.get("SETUP_RESTART_BROKER", "true").lower() in ("1", "true", "yes"),
    "start_components": os.environ.get("SETUP_START_COMPONENTS", "true").lower() in ("1", "true", "yes"),
    "wait_timeout_sec": int(os.environ.get("SETUP_WAIT_TIMEOUT_SEC", "900")),
  },
}
with open(path, "w") as f:
  json.dump(cfg, f, indent=2)
PY
fi

CMD=(python3 "$PY_SCRIPT" --config "$CONFIG_PATH")
[[ -n "$CLI_AMBARI_URL" ]] && CMD+=(--ambari-url "$CLI_AMBARI_URL")
[[ -n "$CLI_CLUSTER_NAME" ]] && CMD+=(--cluster-name "$CLI_CLUSTER_NAME")
[[ -n "$CLI_MM2_DEST" ]] && CMD+=(--mm2-dest "$CLI_MM2_DEST")
[[ -n "$CLI_FLAVOR" ]] && CMD+=(--flavor "$CLI_FLAVOR")
[[ -n "$CLI_SSH_HOST" ]] && CMD+=(--ssh-host "$CLI_SSH_HOST")
[[ -n "$CLI_SSH_KEY" ]] && CMD+=(--ssh-key "$CLI_SSH_KEY")
[[ "$DRY_RUN" -eq 1 ]] && CMD+=(--dry-run)
[[ "$SKIP_HOST_SETUP" -eq 1 ]] && CMD+=(--skip-host-setup)

echo "[INFO] Running: ${CMD[*]}"
exec "${CMD[@]}"
