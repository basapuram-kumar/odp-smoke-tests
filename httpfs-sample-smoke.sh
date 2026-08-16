#!/usr/bin/env bash
#
# Smoke: Apache Hadoop HttpFS (WebHDFS over HTTP) on Kerberized ODP.
#
# Steps:
#   0) Optional setups/setup-httpfs.sh (HTTPFS_RUN_SETUP=1): create
#      httpfs/<host>@REALM keytab + Ambari START
#   1) Ambari discovery of HTTPFS_GATEWAY host + port (default 14000)
#   2) Ambari HTTPFS service STARTED
#   3) Unauthenticated GET /webhdfs/v1/?op=LISTSTATUS returns 401 (or 200)
#      with WWW-Authenticate: Negotiate on Kerberos clusters
#
# Note: Ambari has no SERVICE_CHECK for HTTPFS. Client-side curl --negotiate
# may fail on some nodes (SPNEGO mech); listener + 401 Negotiate is enough.
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   HTTPFS_ENV_FILE / HTTPFS_CONFIG_FILE  default <script-dir>/configs/httpfs.env
#   HTTPFS_URL / HTTPFS_HOST / HTTPFS_PORT
#   HTTPFS_RUN_SETUP  default 0
#   SSH_USER / SSH_KEY  required when HTTPFS_RUN_SETUP=1
#
# Usage:
#   ./httpfs-sample-smoke.sh
#   HTTPFS_RUN_SETUP=1 SSH_KEY=$HOME/Downloads/usdc.pem ./httpfs-sample-smoke.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
HTTPFS_ENV_FILE="${HTTPFS_ENV_FILE:-${HTTPFS_CONFIG_FILE:-${SCRIPT_DIR}/configs/httpfs.env}}"

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
      HTTPFS_*|SSH_*|CURL_EXTRA_OPTS)
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
load_env_file "$HTTPFS_ENV_FILE"
[[ -f "$AMBARI_CONFIG_FILE" ]] && load_env_file "$AMBARI_CONFIG_FILE"

AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://127.0.0.1:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-admin}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-admin}}"
CLUSTER_NAME="${CLUSTER_NAME:-}"
HTTPFS_PORT="${HTTPFS_PORT:-}"
HTTPFS_RUN_SETUP="${HTTPFS_RUN_SETUP:-0}"
CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}"

if [[ "$HTTPFS_RUN_SETUP" == "1" ]]; then
  info "running setups/setup-httpfs.sh"
  SSH_KEY="${SSH_KEY:-${HOME}/Downloads/usdc.pem}" \
    SSH_USER="${SSH_USER:-acceldata}" \
    AMBARI_BASE_URL="$AMBARI_BASE_URL" \
    AMBARI_USER="$AMBARI_USER" \
    AMBARI_PASSWORD="$AMBARI_PASSWORD" \
    CLUSTER_NAME="${CLUSTER_NAME:-}" \
    "${SCRIPT_DIR}/setups/setup-httpfs.sh" || die "setup-httpfs.sh failed"
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
svc = get(f"/api/v1/clusters/{qc}/services/HTTPFS?fields=ServiceInfo/state")
state = (svc.get("ServiceInfo") or {}).get("state") or ""
dc = get(f"/api/v1/clusters/{qc}?fields=Clusters/desired_configs")["Clusters"]["desired_configs"]

def props(ctype):
    meta = dc.get(ctype) or {}
    tag = meta.get("tag")
    if not tag:
        return {}
    items = get(
        f"/api/v1/clusters/{qc}/configurations?type={urllib.parse.quote(ctype)}&tag={urllib.parse.quote(tag)}"
    ).get("items") or []
    return (items[0].get("properties") or {}) if items else {}

port = (props("httpfs").get("port") or "14000").strip()
hc = get(
    f"/api/v1/clusters/{qc}/host_components"
    f"?HostRoles/component_name=HTTPFS_GATEWAY"
    f"&fields=HostRoles/host_name,HostRoles/public_host_name,HostRoles/state"
)
host = ""
hc_state = ""
for it in hc.get("items") or []:
    hr = it.get("HostRoles") or {}
    cand = hr.get("public_host_name") or hr.get("host_name") or ""
    st = (hr.get("state") or "").upper()
    if st == "STARTED" and cand:
        host, hc_state = cand, st
        break
if not host:
    for it in hc.get("items") or []:
        hr = it.get("HostRoles") or {}
        host = hr.get("public_host_name") or hr.get("host_name") or ""
        hc_state = (hr.get("state") or "").upper()
        if host:
            break
print("service_state=%s" % state)
print("host=%s" % host)
print("host_state=%s" % hc_state)
print("port=%s" % port)
PY
)"

DISC_SERVICE_STATE=""
DISC_HOST=""
DISC_HOST_STATE=""
DISC_PORT=""
while IFS='=' read -r k v; do
  case "$k" in
    service_state) DISC_SERVICE_STATE="$v" ;;
    host) DISC_HOST="$v" ;;
    host_state) DISC_HOST_STATE="$v" ;;
    port) DISC_PORT="$v" ;;
  esac
done <<< "$DISC"

HTTPFS_HOST="${HTTPFS_HOST:-$DISC_HOST}"
HTTPFS_PORT="${HTTPFS_PORT:-${DISC_PORT:-14000}}"
[[ -n "$HTTPFS_HOST" ]] || die "No HTTPFS_GATEWAY host; set HTTPFS_HOST"
HTTPFS_URL="${HTTPFS_URL:-http://${HTTPFS_HOST}:${HTTPFS_PORT}/webhdfs/v1/?op=LISTSTATUS}"

echo "---- HttpFS smoke ----"
echo "    Ambari: ${AMBARI_BASE_URL} cluster=${CLUSTER_NAME}"
echo "    service=${DISC_SERVICE_STATE} host=${HTTPFS_HOST}(${DISC_HOST_STATE}) url=${HTTPFS_URL}"

if [[ "${DISC_SERVICE_STATE}" == "STARTED" ]]; then
  pass "Ambari HTTPFS service STARTED"
else
  fail "Ambari HTTPFS service state=${DISC_SERVICE_STATE:-unknown} (want STARTED)"
fi

if [[ "${DISC_HOST_STATE}" == "STARTED" ]]; then
  pass "HTTPFS_GATEWAY on ${HTTPFS_HOST} STARTED"
else
  fail "HTTPFS_GATEWAY on ${HTTPFS_HOST} state=${DISC_HOST_STATE:-unknown}"
fi

# Probe listener. On Kerberos expect 401 + Negotiate; some installs allow 200.
HDRS="$(mktemp)"
if [[ -n "${CURL_EXTRA_OPTS}" ]]; then
  # shellcheck disable=SC2086
  CODE="$(curl -sS -o /dev/null -D "$HDRS" -w '%{http_code}' ${CURL_EXTRA_OPTS} --max-time 20 "$HTTPFS_URL" || true)"
else
  CODE="$(curl -sS -o /dev/null -D "$HDRS" -w '%{http_code}' --max-time 20 "$HTTPFS_URL" || true)"
fi
AUTH_HDR="$(grep -i '^WWW-Authenticate:' "$HDRS" | tr -d '\r' || true)"
rm -f "$HDRS"
info "HTTP ${CODE} ${AUTH_HDR}"

case "$CODE" in
  200)
    pass "HttpFS LISTSTATUS reachable (HTTP 200)"
    ;;
  401|403)
    if echo "$AUTH_HDR" | grep -qi 'Negotiate\|Kerberos'; then
      pass "HttpFS listener up (HTTP ${CODE} with Kerberos challenge)"
    else
      pass "HttpFS listener up (HTTP ${CODE})"
    fi
    ;;
  000|"")
    fail "HttpFS not reachable at ${HTTPFS_URL}"
    ;;
  *)
    fail "HttpFS unexpected HTTP ${CODE} at ${HTTPFS_URL}"
    ;;
esac

echo "---- summary PASS=${PASS_N} FAIL=${FAIL_N} ----"
[[ "$FAIL_N" -eq 0 ]] || exit 1
echo "OK: HttpFS smoke passed"
