#!/usr/bin/env bash
#
# Smoke: Apache NiFi REST API - status endpoints plus a real flow write/delete.
#
# Steps:
#   1) Ambari discovery of the NIFI_MASTER host, web port and TLS flag
#   2) Optional kinit (only needed when NiFi runs over HTTPS with SPNEGO)
#   3) Read-only checks: /flow/about, /system-diagnostics, /flow/status,
#      /controller/cluster (every node CONNECTED), /flow/processor-types
#   4) Write path: create a process group under root, add a GenerateFlowFile
#      processor, confirm both appear in the flow, then delete the group
#
# Deleting the process group also removes anything inside it, so the cleanup
# trap only needs the group id.
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   NIFI_ENV_FILE / NIFI_CONFIG_FILE  default <script-dir>/configs/nifi.env
#   NIFI_URL              default from Ambari, e.g. http://<nifi-host>:9090
#   NIFI_SKIP_KINIT       default 1 - set 0 to kinit before the REST calls
#   NIFI_KEYTAB           default /etc/security/keytabs/nifi.service.keytab
#   NIFI_PRINCIPAL        default nifi/<nifi-host>
#   NIFI_SKIP_WRITE       default 0 - set 1 for read-only checks
#   NIFI_PG_NAME          default odp_smoke_pg_<timestamp>
#   NIFI_PROCESSOR_TYPE   default org.apache.nifi.processors.standard.GenerateFlowFile
#   NIFI_KEEP_FLOW        default 0 - set 1 to leave the process group behind
#   CURL_EXTRA_OPTS       e.g. -k for a self-signed NiFi certificate
#
# Usage:
#   ./nifi-sample-smoke.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
NIFI_ENV_FILE="${NIFI_ENV_FILE:-${NIFI_CONFIG_FILE:-${SCRIPT_DIR}/configs/nifi.env}}"

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
  local f="$1" pattern="$2" key val line
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
    esac
    case "$key" in
      $pattern)
        # Environment always wins over the file.
        [[ -n "${!key:-}" ]] || printf -v "$key" '%s' "$val"
        ;;
    esac
  done <"$f"
  return 0
}

# NiFi embeds raw control characters in some component descriptions, so every
# parse needs strict=False.
jget() {
  python3 -c '
import json, sys
try:
    d = json.load(sys.stdin, strict=False)
    print(eval(sys.argv[1]))
except Exception as exc:
    sys.stderr.write("%s\n" % exc)
    sys.exit(1)
' "$1"
}

ambari_get() {
  curl -sS -f $CURL_EXTRA_OPTS -u "${AMBARI_USER}:${AMBARI_PASSWORD}" \
    -H "X-Requested-By: ambari" "$1" 2>/dev/null
}

ambari_component_hosts() {
  local service="$1" component="$2"
  [[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]] || return 1
  ambari_get "${AMBARI_BASE_URL%/}/api/v1/clusters/${cluster}/services/${service}/components/${component}" \
    | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(1)
hosts = [h.get('HostRoles', {}).get('host_name') for h in data.get('host_components', [])]
hosts = [h for h in hosts if h]
if not hosts:
    sys.exit(1)
print(' '.join(hosts))
"
}

# ambari_service_prop <service> <property> - searches every current config type.
ambari_service_prop() {
  local service="$1" prop="$2"
  [[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]] || return 1
  ambari_get "${AMBARI_BASE_URL%/}/api/v1/clusters/${cluster}/configurations/service_config_versions?service_name=${service}&is_current=true" \
    | python3 -c "
import json, sys
want = sys.argv[1]
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(1)
for item in data.get('items', []):
    for conf in item.get('configurations', []):
        props = conf.get('properties') or {}
        if want in props:
            print(str(props[want]).strip())
            sys.exit(0)
sys.exit(1)
" "$prop"
}

pass=0
fail=0
skip=0
declare -a results=()

record_pass() {
  pass=$((pass + 1))
  results+=("PASS    $1")
}

record_fail() {
  fail=$((fail + 1))
  results+=("FAIL    $1")
  echo "        FAIL: $1" >&2
}

record_skip() {
  skip=$((skip + 1))
  results+=("SKIPPED $1")
}

# rest <method> <path> [json-body] - sets resp_body / resp_code.
rest() {
  local method="$1" path="$2" body="${3:-}" raw
  local -a args=(-sS $CURL_EXTRA_OPTS $NIFI_CURL_AUTH -X "$method" -w $'\n%{http_code}')
  if [[ -n "$body" ]]; then
    args+=(-H 'Content-Type: application/json' -d "$body")
  fi
  raw="$(curl "${args[@]}" "${NIFI_URL}/nifi-api${path}" 2>/dev/null || true)"
  resp_code="${raw##*$'\n'}"
  resp_body="${raw%$'\n'*}"
  [[ "$resp_code" == 2* ]]
}

need_cmd curl
need_cmd python3

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""

load_env_file "$NIFI_ENV_FILE" 'NIFI_*|CURL_EXTRA_OPTS'
NIFI_SKIP_KINIT="${NIFI_SKIP_KINIT:-1}"
NIFI_SKIP_WRITE="${NIFI_SKIP_WRITE:-0}"
NIFI_KEEP_FLOW="${NIFI_KEEP_FLOW:-0}"
NIFI_PROCESSOR_TYPE="${NIFI_PROCESSOR_TYPE:-org.apache.nifi.processors.standard.GenerateFlowFile}"
NIFI_PG_NAME="${NIFI_PG_NAME:-odp_smoke_pg_$(date +%s)}"
CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}"

cluster="${CLUSTER_NAME:-}"
if [[ -z "${NIFI_URL:-}" || -z "$cluster" ]]; then
  if [[ -f "$AMBARI_CONFIG_FILE" ]]; then
    load_env_file "$AMBARI_CONFIG_FILE" 'AMBARI_*'
  fi
  AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://10.101.11.22:8080}}"
  AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
  AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"
fi

if [[ -z "$cluster" ]]; then
  [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]] \
    || die "Set NIFI_URL and CLUSTER_NAME, or provide Ambari credentials in ${AMBARI_CONFIG_FILE}."
  cluster="$(ambari_get "${AMBARI_BASE_URL%/}/api/v1/clusters/" | python3 -c "
import json, sys
data = json.load(sys.stdin)
items = data.get('items') or []
if not items:
    sys.exit('no clusters in Ambari response')
print((items[0].get('Clusters') or {}).get('cluster_name') or sys.exit('no cluster_name'))
")" || die "could not resolve cluster name from Ambari"
fi

nifi_hosts=""
if [[ -z "${NIFI_URL:-}" ]]; then
  nifi_hosts="$(ambari_component_hosts NIFI NIFI_MASTER 2>/dev/null || true)"
  [[ -n "$nifi_hosts" ]] || die "no NIFI_MASTER host in Ambari; set NIFI_URL"
  nifi_host="${nifi_hosts%% *}"
  ssl_enabled="$(ambari_service_prop NIFI "nifi.node.ssl.isenabled" 2>/dev/null || echo false)"
  if [[ "${ssl_enabled,,}" == "true" ]]; then
    nifi_port="$(ambari_service_prop NIFI "nifi.node.ssl.port" 2>/dev/null || echo 9091)"
    NIFI_URL="https://${nifi_host}:${nifi_port}"
  else
    nifi_port="$(ambari_service_prop NIFI "nifi.node.port" 2>/dev/null || echo 9090)"
    NIFI_URL="http://${nifi_host}:${nifi_port}"
  fi
fi
NIFI_URL="${NIFI_URL%/}"

# NiFi rejects user authentication over plain HTTP, so only negotiate on HTTPS.
NIFI_CURL_AUTH=""
if [[ "$NIFI_URL" == https://* ]]; then
  NIFI_CURL_AUTH="--negotiate -u :"
fi

echo "---- NiFi sample smoke ----"
echo "    cluster:    ${cluster}"
echo "    NiFi URL:   ${NIFI_URL}"
echo "    hosts:      ${nifi_hosts:-<from NIFI_URL>}"
echo "    auth:       ${NIFI_CURL_AUTH:-anonymous (plain HTTP)}"

if [[ "$NIFI_SKIP_KINIT" != "1" ]]; then
  keytab="${NIFI_KEYTAB:-/etc/security/keytabs/nifi.service.keytab}"
  [[ -r "$keytab" ]] || die "keytab not readable: $keytab"
  principal="${NIFI_PRINCIPAL:-nifi/${nifi_host:-$(hostname -f)}}"
  echo "    kinit:      ${principal}"
  need_cmd kinit
  kinit -kt "$keytab" "$principal" || die "kinit failed for ${principal}"
fi

echo ""
echo "---- status endpoints ----"

if rest GET /flow/about; then
  version="$(printf '%s' "$resp_body" | jget 'd["about"]["version"]' 2>/dev/null || echo unknown)"
  echo "        NiFi version ${version}"
  record_pass "flow/about"
else
  record_fail "flow/about (HTTP ${resp_code})"
fi

if rest GET /system-diagnostics; then
  heap="$(printf '%s' "$resp_body" | jget 'd["systemDiagnostics"]["aggregateSnapshot"]["usedHeap"] + " / " + d["systemDiagnostics"]["aggregateSnapshot"]["maxHeap"]' 2>/dev/null || echo unknown)"
  echo "        heap used/max ${heap}"
  record_pass "system-diagnostics"
else
  record_fail "system-diagnostics (HTTP ${resp_code})"
fi

if rest GET /flow/status; then
  threads="$(printf '%s' "$resp_body" | jget 'd["controllerStatus"]["activeThreadCount"]' 2>/dev/null || echo '?')"
  echo "        active threads ${threads}"
  record_pass "flow/status"
else
  record_fail "flow/status (HTTP ${resp_code})"
fi

if rest GET /controller/cluster; then
  nodes="$(printf '%s' "$resp_body" | jget '", ".join("%s=%s" % (n["address"], n["status"]) for n in d["cluster"]["nodes"])' 2>/dev/null || echo '')"
  disconnected="$(printf '%s' "$resp_body" | jget 'sum(1 for n in d["cluster"]["nodes"] if n["status"] != "CONNECTED")' 2>/dev/null || echo 1)"
  echo "        nodes: ${nodes:-<none>}"
  if [[ "$disconnected" == "0" ]]; then
    record_pass "controller/cluster"
  else
    record_fail "controller/cluster (${disconnected} node(s) not CONNECTED)"
  fi
else
  record_fail "controller/cluster (HTTP ${resp_code})"
fi

if rest GET /flow/processor-types; then
  types="$(printf '%s' "$resp_body" | jget 'len(d["processorTypes"])' 2>/dev/null || echo 0)"
  echo "        ${types} processor types available"
  if (( types > 0 )); then
    record_pass "flow/processor-types"
  else
    record_fail "flow/processor-types (empty)"
  fi
else
  record_fail "flow/processor-types (HTTP ${resp_code})"
fi

pg_id=""
client_id="odp-nifi-smoke-$(date +%s)-$$"

cleanup_done=0
cleanup() {
  [[ "$cleanup_done" == "1" ]] && return 0
  cleanup_done=1
  [[ -n "$pg_id" && "$NIFI_KEEP_FLOW" != "1" ]] || return 0
  echo ""
  echo "---- cleanup ----"
  local rev
  if rest GET "/process-groups/${pg_id}"; then
    rev="$(printf '%s' "$resp_body" | jget 'd["revision"]["version"]' 2>/dev/null || echo 0)"
    # Deleting the group removes the processors inside it.
    if rest DELETE "/process-groups/${pg_id}?version=${rev}&clientId=${client_id}"; then
      echo "        deleted process group ${NIFI_PG_NAME}"
    else
      echo "        WARN: could not delete process group ${pg_id} (HTTP ${resp_code})" >&2
    fi
  fi
}
trap cleanup EXIT

if [[ "$NIFI_SKIP_WRITE" == "1" ]]; then
  record_skip "flow write path"
else
  echo ""
  echo "---- flow write path ----"

  root_id=""
  if rest GET /flow/process-groups/root; then
    root_id="$(printf '%s' "$resp_body" | jget 'd["processGroupFlow"]["id"]' 2>/dev/null || echo '')"
  fi
  if [[ -n "$root_id" ]]; then
    echo "        root process group ${root_id}"
    record_pass "root process group"
  else
    record_fail "root process group (HTTP ${resp_code})"
  fi

  if [[ -n "$root_id" ]]; then
    body="$(python3 -c '
import json, sys
print(json.dumps({
    "revision": {"version": 0, "clientId": sys.argv[1]},
    "component": {"name": sys.argv[2], "position": {"x": 0.0, "y": 0.0}},
}))' "$client_id" "$NIFI_PG_NAME")"
    if rest POST "/process-groups/${root_id}/process-groups" "$body"; then
      pg_id="$(printf '%s' "$resp_body" | jget 'd["id"]' 2>/dev/null || echo '')"
    fi
    if [[ -n "$pg_id" ]]; then
      echo "        created process group ${NIFI_PG_NAME} (${pg_id})"
      record_pass "create process group"
    else
      record_fail "create process group (HTTP ${resp_code})"
    fi
  else
    record_skip "create process group"
  fi

  if [[ -n "$pg_id" ]]; then
    body="$(python3 -c '
import json, sys
print(json.dumps({
    "revision": {"version": 0, "clientId": sys.argv[1]},
    "component": {"type": sys.argv[2], "name": sys.argv[3], "position": {"x": 0.0, "y": 0.0}},
}))' "$client_id" "$NIFI_PROCESSOR_TYPE" "odp_smoke_processor")"
    if rest POST "/process-groups/${pg_id}/processors" "$body"; then
      proc_name="$(printf '%s' "$resp_body" | jget 'd["component"]["name"]' 2>/dev/null || echo '')"
      echo "        created processor ${proc_name:-?} (${NIFI_PROCESSOR_TYPE##*.})"
      record_pass "create processor"
    else
      record_fail "create processor (HTTP ${resp_code})"
    fi

    if rest GET "/flow/process-groups/${pg_id}"; then
      found="$(printf '%s' "$resp_body" | jget 'sum(1 for p in d["processGroupFlow"]["flow"]["processors"] if p["component"]["name"] == "odp_smoke_processor")' 2>/dev/null || echo 0)"
      if [[ "$found" == "1" ]]; then
        echo "        processor visible in process group flow"
        record_pass "verify processor in flow"
      else
        record_fail "verify processor in flow (found ${found})"
      fi
    else
      record_fail "verify processor in flow (HTTP ${resp_code})"
    fi
  else
    record_skip "create processor"
    record_skip "verify processor in flow"
  fi
fi

cleanup

echo ""
echo "---- summary ----"
for r in "${results[@]}"; do
  echo "    $r"
done
echo "    PASS=${pass} FAIL=${fail} SKIPPED=${skip}"

if (( fail > 0 )); then
  die "NiFi sample smoke had ${fail} failing check(s)."
fi
echo "OK: NiFi sample smoke finished (PASS=${pass} SKIPPED=${skip})."
