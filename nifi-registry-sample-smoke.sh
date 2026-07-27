#!/usr/bin/env bash
#
# Smoke: Apache NiFi Registry REST API - status endpoints plus a bucket/flow
# create-read-delete cycle.
#
# Steps:
#   1) Ambari discovery of the NIFI_REGISTRY_MASTER host, web port and TLS flag
#   2) Optional kinit (only needed when the Registry runs over HTTPS with SPNEGO)
#   3) Read-only checks: /about, /access, /buckets
#   4) Write path: create a bucket, read it back, create a flow in it, list the
#      flows, then delete the flow and the bucket
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   NIFI_REGISTRY_ENV_FILE  default <script-dir>/configs/nifi-registry.env
#   NIFI_REGISTRY_URL       default from Ambari, e.g. http://<host>:61080
#   NIFI_REGISTRY_SKIP_KINIT  default 1
#   NIFI_REGISTRY_KEYTAB    default /etc/security/keytabs/nifi-registry.service.keytab
#   NIFI_REGISTRY_PRINCIPAL default nifiregistry/<registry-host>
#   NIFI_REGISTRY_SKIP_WRITE  default 0 - set 1 for read-only checks
#   NIFI_REGISTRY_BUCKET    default odp_smoke_bucket_<timestamp>
#   NIFI_REGISTRY_FLOW      default odp_smoke_flow
#   NIFI_REGISTRY_KEEP_DATA default 0 - set 1 to leave the bucket behind
#   CURL_EXTRA_OPTS         e.g. -k for a self-signed certificate
#
# Usage:
#   ./nifi-registry-sample-smoke.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
NIFI_REGISTRY_ENV_FILE="${NIFI_REGISTRY_ENV_FILE:-${SCRIPT_DIR}/configs/nifi-registry.env}"

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
  local -a args=(-sS $CURL_EXTRA_OPTS $REGISTRY_CURL_AUTH -X "$method" -w $'\n%{http_code}')
  if [[ -n "$body" ]]; then
    args+=(-H 'Content-Type: application/json' -d "$body")
  fi
  raw="$(curl "${args[@]}" "${NIFI_REGISTRY_URL}/nifi-registry-api${path}" 2>/dev/null || true)"
  resp_code="${raw##*$'\n'}"
  resp_body="${raw%$'\n'*}"
  [[ "$resp_code" == 2* ]]
}

need_cmd curl
need_cmd python3

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""

load_env_file "$NIFI_REGISTRY_ENV_FILE" 'NIFI_REGISTRY_*|CURL_EXTRA_OPTS'
NIFI_REGISTRY_SKIP_KINIT="${NIFI_REGISTRY_SKIP_KINIT:-1}"
NIFI_REGISTRY_SKIP_WRITE="${NIFI_REGISTRY_SKIP_WRITE:-0}"
NIFI_REGISTRY_KEEP_DATA="${NIFI_REGISTRY_KEEP_DATA:-0}"
NIFI_REGISTRY_BUCKET="${NIFI_REGISTRY_BUCKET:-odp_smoke_bucket_$(date +%s)}"
NIFI_REGISTRY_FLOW="${NIFI_REGISTRY_FLOW:-odp_smoke_flow}"
CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}"

cluster="${CLUSTER_NAME:-}"
if [[ -z "${NIFI_REGISTRY_URL:-}" || -z "$cluster" ]]; then
  if [[ -f "$AMBARI_CONFIG_FILE" ]]; then
    load_env_file "$AMBARI_CONFIG_FILE" 'AMBARI_*'
  fi
  AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://10.101.11.22:8080}}"
  AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
  AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"
fi

if [[ -z "$cluster" ]]; then
  [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]] \
    || die "Set NIFI_REGISTRY_URL and CLUSTER_NAME, or provide Ambari credentials in ${AMBARI_CONFIG_FILE}."
  cluster="$(ambari_get "${AMBARI_BASE_URL%/}/api/v1/clusters/" | python3 -c "
import json, sys
data = json.load(sys.stdin)
items = data.get('items') or []
if not items:
    sys.exit('no clusters in Ambari response')
print((items[0].get('Clusters') or {}).get('cluster_name') or sys.exit('no cluster_name'))
")" || die "could not resolve cluster name from Ambari"
fi

registry_hosts=""
if [[ -z "${NIFI_REGISTRY_URL:-}" ]]; then
  registry_hosts="$(ambari_component_hosts NIFI_REGISTRY NIFI_REGISTRY_MASTER 2>/dev/null || true)"
  [[ -n "$registry_hosts" ]] || die "no NIFI_REGISTRY_MASTER host in Ambari; set NIFI_REGISTRY_URL"
  registry_host="${registry_hosts%% *}"
  ssl_enabled="$(ambari_service_prop NIFI_REGISTRY "nifi.registry.ssl.isenabled" 2>/dev/null || echo false)"
  if [[ "${ssl_enabled,,}" == "true" ]]; then
    registry_port="$(ambari_service_prop NIFI_REGISTRY "nifi.registry.port.ssl" 2>/dev/null || echo 61443)"
    NIFI_REGISTRY_URL="https://${registry_host}:${registry_port}"
  else
    registry_port="$(ambari_service_prop NIFI_REGISTRY "nifi.registry.port" 2>/dev/null || echo 61080)"
    NIFI_REGISTRY_URL="http://${registry_host}:${registry_port}"
  fi
fi
NIFI_REGISTRY_URL="${NIFI_REGISTRY_URL%/}"

REGISTRY_CURL_AUTH=""
if [[ "$NIFI_REGISTRY_URL" == https://* ]]; then
  REGISTRY_CURL_AUTH="--negotiate -u :"
fi

echo "---- NiFi Registry sample smoke ----"
echo "    cluster:      ${cluster}"
echo "    Registry URL: ${NIFI_REGISTRY_URL}"
echo "    hosts:        ${registry_hosts:-<from NIFI_REGISTRY_URL>}"
echo "    auth:         ${REGISTRY_CURL_AUTH:-anonymous (plain HTTP)}"
echo "    bucket/flow:  ${NIFI_REGISTRY_BUCKET}/${NIFI_REGISTRY_FLOW}"

if [[ "$NIFI_REGISTRY_SKIP_KINIT" != "1" ]]; then
  keytab="${NIFI_REGISTRY_KEYTAB:-/etc/security/keytabs/nifi-registry.service.keytab}"
  [[ -r "$keytab" ]] || die "keytab not readable: $keytab"
  principal="${NIFI_REGISTRY_PRINCIPAL:-nifiregistry/${registry_host:-$(hostname -f)}}"
  echo "    kinit:        ${principal}"
  need_cmd kinit
  kinit -kt "$keytab" "$principal" || die "kinit failed for ${principal}"
fi

echo ""
echo "---- status endpoints ----"

if rest GET /about; then
  version="$(printf '%s' "$resp_body" | jget 'd["registryAboutVersion"]' 2>/dev/null || echo unknown)"
  echo "        Registry version ${version}"
  record_pass "about"
else
  record_fail "about (HTTP ${resp_code})"
fi

if rest GET /access; then
  identity="$(printf '%s' "$resp_body" | jget 'd.get("identity", "?")' 2>/dev/null || echo '?')"
  echo "        identity ${identity}"
  record_pass "access"
else
  record_fail "access (HTTP ${resp_code})"
fi

if rest GET /buckets; then
  count="$(printf '%s' "$resp_body" | jget 'len(d)' 2>/dev/null || echo '?')"
  echo "        ${count} existing bucket(s)"
  record_pass "list buckets"
else
  record_fail "list buckets (HTTP ${resp_code})"
fi

bucket_id=""
flow_id=""

cleanup_done=0
cleanup() {
  [[ "$cleanup_done" == "1" ]] && return 0
  cleanup_done=1
  [[ -n "$bucket_id" && "$NIFI_REGISTRY_KEEP_DATA" != "1" ]] || return 0
  echo ""
  echo "---- cleanup ----"
  local rev
  if [[ -n "$flow_id" ]] && rest GET "/buckets/${bucket_id}/flows/${flow_id}"; then
    rev="$(printf '%s' "$resp_body" | jget 'd.get("revision", {}).get("version", 0)' 2>/dev/null || echo 0)"
    if rest DELETE "/buckets/${bucket_id}/flows/${flow_id}?version=${rev}"; then
      echo "        deleted flow ${NIFI_REGISTRY_FLOW}"
    else
      echo "        WARN: could not delete flow ${flow_id} (HTTP ${resp_code})" >&2
    fi
  fi
  if rest GET "/buckets/${bucket_id}"; then
    rev="$(printf '%s' "$resp_body" | jget 'd.get("revision", {}).get("version", 0)' 2>/dev/null || echo 0)"
    if rest DELETE "/buckets/${bucket_id}?version=${rev}"; then
      echo "        deleted bucket ${NIFI_REGISTRY_BUCKET}"
    else
      echo "        WARN: could not delete bucket ${bucket_id} (HTTP ${resp_code})" >&2
    fi
  fi
}
trap cleanup EXIT

if [[ "$NIFI_REGISTRY_SKIP_WRITE" == "1" ]]; then
  record_skip "bucket/flow write path"
else
  echo ""
  echo "---- bucket / flow write path ----"

  body="$(python3 -c '
import json, sys
print(json.dumps({"name": sys.argv[1], "description": "ODP smoke test bucket"}))' "$NIFI_REGISTRY_BUCKET")"
  if rest POST /buckets "$body"; then
    bucket_id="$(printf '%s' "$resp_body" | jget 'd["identifier"]' 2>/dev/null || echo '')"
  fi
  if [[ -n "$bucket_id" ]]; then
    echo "        created bucket ${NIFI_REGISTRY_BUCKET} (${bucket_id})"
    record_pass "create bucket"
  else
    record_fail "create bucket (HTTP ${resp_code})"
  fi

  if [[ -n "$bucket_id" ]]; then
    if rest GET "/buckets/${bucket_id}"; then
      name="$(printf '%s' "$resp_body" | jget 'd["name"]' 2>/dev/null || echo '')"
      if [[ "$name" == "$NIFI_REGISTRY_BUCKET" ]]; then
        record_pass "read bucket back"
      else
        record_fail "read bucket back (name '${name}')"
      fi
    else
      record_fail "read bucket back (HTTP ${resp_code})"
    fi

    body="$(python3 -c '
import json, sys
print(json.dumps({"name": sys.argv[1], "description": "ODP smoke test flow"}))' "$NIFI_REGISTRY_FLOW")"
    if rest POST "/buckets/${bucket_id}/flows" "$body"; then
      flow_id="$(printf '%s' "$resp_body" | jget 'd["identifier"]' 2>/dev/null || echo '')"
    fi
    if [[ -n "$flow_id" ]]; then
      echo "        created flow ${NIFI_REGISTRY_FLOW} (${flow_id})"
      record_pass "create flow"
    else
      record_fail "create flow (HTTP ${resp_code})"
    fi

    if rest GET "/buckets/${bucket_id}/flows"; then
      found="$(printf '%s' "$resp_body" | python3 -c "
import json, sys
d = json.load(sys.stdin, strict=False)
print(sum(1 for f in d if f.get('name') == sys.argv[1]))
" "$NIFI_REGISTRY_FLOW" 2>/dev/null || echo 0)"
      if [[ "$found" == "1" ]]; then
        echo "        flow visible in bucket listing"
        record_pass "list flows in bucket"
      else
        record_fail "list flows in bucket (found ${found})"
      fi
    else
      record_fail "list flows in bucket (HTTP ${resp_code})"
    fi
  else
    record_skip "read bucket back"
    record_skip "create flow"
    record_skip "list flows in bucket"
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
  die "NiFi Registry sample smoke had ${fail} failing check(s)."
fi
echo "OK: NiFi Registry sample smoke finished (PASS=${pass} SKIPPED=${skip})."
