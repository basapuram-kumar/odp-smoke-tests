#!/usr/bin/env bash
#
# Smoke: Trino coordinator/workers on Kerberized ODP.
#
# Steps:
#   0) Optional setups/setup-trino.sh (TRINO_RUN_SETUP=1)
#   1) Ambari discovery of TRINO_COORDINATOR + workers + http_server_port
#   2) Ambari TRINO service STARTED (coordinator + workers)
#   3) GET /v1/info on coordinator (HTTP 200, coordinator=true)
#   4) GET /v1/info on each worker (HTTP 200, coordinator=false)
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   TRINO_ENV_FILE / TRINO_CONFIG_FILE  default <script-dir>/configs/trino.env
#   TRINO_URL / TRINO_COORDINATOR_HOST / TRINO_HTTP_PORT
#   TRINO_RUN_SETUP  default 0
#   SSH_USER / SSH_KEY  required when TRINO_RUN_SETUP=1
#
# Usage:
#   ./trino-sample-smoke.sh
#   TRINO_RUN_SETUP=1 SSH_KEY=$HOME/Downloads/usdc.pem ./trino-sample-smoke.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
TRINO_ENV_FILE="${TRINO_ENV_FILE:-${TRINO_CONFIG_FILE:-${SCRIPT_DIR}/configs/trino.env}}"

die() {
  echo "ERROR: $*" >&2
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
    case "$key" in
      AMBARI_BASE_URL) _cfg_AMBARI_BASE_URL="$val" ;;
      AMBARI_USER) _cfg_AMBARI_USER="$val" ;;
      AMBARI_PASSWORD) _cfg_AMBARI_PASSWORD="$val" ;;
      TRINO_*|SSH_*|CURL_EXTRA_OPTS)
        [[ -n "${!key:-}" ]] || printf -v "$key" '%s' "$val"
        ;;
    esac
  done <"$f"
}

pass() { echo "[PASS] $*"; PASS_N=$((PASS_N + 1)); }
fail() { echo "[FAIL] $*"; FAIL_N=$((FAIL_N + 1)); }
info() { echo "[INFO] $*"; }

need_cmd curl
need_cmd python3

PASS_N=0
FAIL_N=0

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""
load_env_file "$TRINO_ENV_FILE"
[[ -f "$AMBARI_CONFIG_FILE" ]] && load_env_file "$AMBARI_CONFIG_FILE"

AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://127.0.0.1:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-admin}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-admin}}"
CLUSTER_NAME="${CLUSTER_NAME:-}"
TRINO_RUN_SETUP="${TRINO_RUN_SETUP:-0}"
CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}"

if [[ "$TRINO_RUN_SETUP" == "1" ]]; then
  info "running setups/setup-trino.sh"
  SSH_KEY="${SSH_KEY:-${HOME}/Downloads/usdc.pem}" \
    SSH_USER="${SSH_USER:-acceldata}" \
    AMBARI_BASE_URL="$AMBARI_BASE_URL" \
    AMBARI_USER="$AMBARI_USER" \
    AMBARI_PASSWORD="$AMBARI_PASSWORD" \
    CLUSTER_NAME="${CLUSTER_NAME:-}" \
    "${SCRIPT_DIR}/setups/setup-trino.sh" || die "setup-trino.sh failed"
fi

ambari_json() {
  local method="$1" path="$2"
  python3 - "$AMBARI_BASE_URL" "$AMBARI_USER" "$AMBARI_PASSWORD" "$method" "$path" <<'PY'
import sys, urllib.request, base64
base, user, pw, method, path = sys.argv[1:6]
url = base.rstrip("/") + path
req = urllib.request.Request(url, method=method)
req.add_header("Authorization", "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode())
req.add_header("X-Requested-By", "ambari")
with urllib.request.urlopen(req, timeout=90) as resp:
    print(resp.read().decode())
PY
}

if [[ -z "$CLUSTER_NAME" ]]; then
  CLUSTER_NAME="$(ambari_json GET "/api/v1/clusters/" | python3 -c '
import json,sys
items=json.load(sys.stdin).get("items") or []
print((items[0].get("Clusters") or {}).get("cluster_name") or "")
')"
fi
[[ -n "$CLUSTER_NAME" ]] || die "CLUSTER_NAME required"

DISC="$(python3 - "$AMBARI_BASE_URL" "$AMBARI_USER" "$AMBARI_PASSWORD" "$CLUSTER_NAME" <<'PY'
import json, sys, urllib.request, base64, urllib.parse
base, user, pw, cluster = sys.argv[1:5]
auth = base64.b64encode(f"{user}:{pw}".encode()).decode()

def get(path):
    r = urllib.request.Request(base.rstrip("/") + path)
    r.add_header("Authorization", "Basic " + auth)
    with urllib.request.urlopen(r, timeout=90) as resp:
        return json.loads(resp.read())

qc = urllib.parse.quote(cluster, safe="")
svc = get(f"/api/v1/clusters/{qc}/services/TRINO?fields=ServiceInfo/state")
print("service_state=%s" % ((svc.get("ServiceInfo") or {}).get("state") or ""))
dc = get(f"/api/v1/clusters/{qc}?fields=Clusters/desired_configs")["Clusters"]["desired_configs"]
tag = (dc.get("trino-env") or {}).get("tag")
port = "9097"
if tag:
    props = get(f"/api/v1/clusters/{qc}/configurations?type=trino-env&tag={urllib.parse.quote(tag)}")["items"][0].get("properties") or {}
    port = (props.get("http_server_port") or "9097").strip()
print("port=%s" % port)
hosts_info = {
    (h["Hosts"]["host_name"]): (h["Hosts"].get("ip") or h["Hosts"]["host_name"])
    for h in get(f"/api/v1/clusters/{qc}/hosts?fields=Hosts/host_name,Hosts/ip").get("items") or []
}

def emit(comp, role):
    hc = get(
        f"/api/v1/clusters/{qc}/host_components"
        f"?HostRoles/component_name={comp}"
        f"&fields=HostRoles/host_name,HostRoles/public_host_name,HostRoles/state"
    )
    for it in hc.get("items") or []:
        hr = it.get("HostRoles") or {}
        host = hr.get("public_host_name") or hr.get("host_name") or ""
        if not host:
            continue
        ip = hosts_info.get(hr.get("host_name") or host, host)
        print("host=%s|%s|%s|%s|%s" % (role, host, hr.get("state") or "", ip, port))

emit("TRINO_COORDINATOR", "COORD")
emit("TRINO_WORKER", "WORKER")
PY
)"

SERVICE_STATE=""
PORT="9097"
HOSTS=()
while IFS='=' read -r k v; do
  case "$k" in
    service_state) SERVICE_STATE="$v" ;;
    port) PORT="$v" ;;
    host) HOSTS+=("$v") ;;
  esac
done <<< "$DISC"

PORT="${TRINO_HTTP_PORT:-$PORT}"
COORD_HOST="${TRINO_COORDINATOR_HOST:-}"
COORD_IP=""
COORD_STATE=""

echo "---- Trino smoke ----"
echo "    Ambari: ${AMBARI_BASE_URL} cluster=${CLUSTER_NAME}"
echo "    service=${SERVICE_STATE} port=${PORT}"

if [[ "$SERVICE_STATE" == "STARTED" ]]; then
  pass "Ambari TRINO service STARTED"
else
  fail "Ambari TRINO service state=${SERVICE_STATE:-unknown} (want STARTED)"
fi

for row in "${HOSTS[@]}"; do
  IFS='|' read -r role host state ip port <<<"$row"
  echo "    ${role} ${host} state=${state} ip=${ip}"
  if [[ "$state" == "STARTED" ]]; then
    pass "${role} ${host} Ambari STARTED"
  else
    fail "${role} ${host} Ambari state=${state}"
  fi
  if [[ "$role" == "COORD" && -z "$COORD_HOST" ]]; then
    COORD_HOST="$host"
    COORD_IP="$ip"
    COORD_STATE="$state"
  fi
  url="http://${ip}:${PORT}/v1/info"
  if [[ -n "${TRINO_URL:-}" && "$role" == "COORD" ]]; then
    url="$TRINO_URL"
  fi
  body="$(mktemp)"
  if [[ -n "${CURL_EXTRA_OPTS}" ]]; then
    # shellcheck disable=SC2086
    code="$(curl -sS -o "$body" -w '%{http_code}' ${CURL_EXTRA_OPTS} --max-time 15 "$url" || true)"
  else
    code="$(curl -sS -o "$body" -w '%{http_code}' --max-time 15 "$url" || true)"
  fi
  info "${role} ${host} HTTP ${code} @ ${url}"
  if [[ "$code" != "200" ]]; then
    fail "${role} ${host} /v1/info HTTP ${code:-000}"
    rm -f "$body"
    continue
  fi
  if [[ "$role" == "COORD" ]]; then
    if grep -q '"coordinator":true' "$body"; then
      pass "coordinator /v1/info coordinator=true"
    else
      fail "coordinator /v1/info missing coordinator=true: $(head -c 160 "$body")"
    fi
  else
    if grep -q '"coordinator":false' "$body"; then
      pass "worker ${host} /v1/info coordinator=false"
    else
      fail "worker ${host} /v1/info unexpected: $(head -c 160 "$body")"
    fi
  fi
  if grep -q '"starting":false' "$body"; then
    pass "${role} ${host} starting=false"
  else
    fail "${role} ${host} still starting: $(head -c 160 "$body")"
  fi
  rm -f "$body"
done

echo "---- summary PASS=${PASS_N} FAIL=${FAIL_N} ----"
[[ "$FAIL_N" -eq 0 ]] || exit 1
echo "OK: Trino smoke passed"
