#!/usr/bin/env bash
#
# Smoke: Apache Knox gateway - TLS, topology deployment, auth enforcement, and
# (when credentials work) the admin API plus a proxied backend call.
#
# Steps:
#   1) Ambari discovery of the KNOX_GATEWAY host, gateway.port and gateway.path
#   2) TLS handshake and certificate validity window
#   3) Each topology answers 200/302/401 on a known service path (deployed)
#   4) An unauthenticated admin API call is rejected with 401 + WWW-Authenticate
#   5) Authenticated: /admin/api/v1/version, /admin/api/v1/topologies, and a
#      WebHDFS LISTSTATUS through the gateway
#
# Step 5 needs the identity store behind the topology's authentication provider.
# On a stock ODP install that is the Knox demo LDAP on port 33389; if it is not
# running, every login fails with 401 and the authenticated checks are reported
# SKIPPED (with the reason) instead of FAIL, since that is a prerequisite rather
# than a gateway fault. Start it from Ambari: Knox > Actions > Start Demo LDAP.
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   KNOX_ENV_FILE         default <script-dir>/configs/knox.env
#   KNOX_URL              default https://<knox-host>:<gateway.port>/<gateway.path>
#   KNOX_TOPOLOGY_PROBES  space-separated <topology>=<service-path> pairs
#   KNOX_USER             default admin
#   KNOX_PASSWORD         default the admin entry from the Ambari users-ldif config
#   KNOX_LDAP_URL         default main.ldapRealm.contextFactory.url from the topology
#   KNOX_SKIP_AUTH        default 0 - set 1 for unauthenticated checks only
#   KNOX_WEBHDFS_TOPOLOGY default default
#   KNOX_SKIP_WEBHDFS     default 0
#   CURL_EXTRA_OPTS       default -k (Knox ships a self-signed certificate)
#
# Usage:
#   ./knox-sample-smoke.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
KNOX_ENV_FILE="${KNOX_ENV_FILE:-${SCRIPT_DIR}/configs/knox.env}"

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

ambari_get() {
  curl -sS -f -u "${AMBARI_USER}:${AMBARI_PASSWORD}" \
    -H "X-Requested-By: ambari" "$1" 2>/dev/null
}

ambari_component_hosts() {
  local service="$1" component="$2"
  [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]] || return 1
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

knox_config_json=""
load_knox_config() {
  [[ -n "$knox_config_json" ]] && return 0
  [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]] || return 1
  knox_config_json="$(ambari_get "${AMBARI_BASE_URL%/}/api/v1/clusters/${cluster}/configurations/service_config_versions?service_name=KNOX&is_current=true")" || return 1
  [[ -n "$knox_config_json" ]]
}

knox_prop() {
  load_knox_config || return 1
  printf '%s' "$knox_config_json" | python3 -c "
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
" "$1"
}

# Pulls a <param> value out of the rendered topology XML.
knox_topology_param() {
  load_knox_config || return 1
  printf '%s' "$knox_config_json" | python3 -c "
import json, re, sys
want = sys.argv[1]
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(1)
for item in data.get('items', []):
    for conf in item.get('configurations', []):
        if conf.get('type') != 'topology':
            continue
        content = (conf.get('properties') or {}).get('content') or ''
        m = re.search(r'<name>\s*' + re.escape(want) + r'\s*</name>\s*<value>([^<]*)</value>', content)
        if m:
            print(m.group(1).strip())
            sys.exit(0)
sys.exit(1)
" "$1"
}

# Password for a uid in the demo users-ldif config.
knox_ldif_password() {
  load_knox_config || return 1
  printf '%s' "$knox_config_json" | python3 -c "
import json, re, sys
want = sys.argv[1]
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(1)
for item in data.get('items', []):
    for conf in item.get('configurations', []):
        if conf.get('type') != 'users-ldif':
            continue
        content = (conf.get('properties') or {}).get('content') or ''
        for m in re.finditer(r'uid:\s*(\S+)[\s\S]{0,400}?userPassword:\s*(\S+)', content):
            if m.group(1) == want:
                print(m.group(2))
                sys.exit(0)
sys.exit(1)
" "$1"
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
  echo "        FAIL: $1"
}

record_skip() {
  skip=$((skip + 1))
  results+=("SKIPPED $1")
}

# gw <path> [curl args...] - sets resp_body / resp_code against the gateway.
gw() {
  local path="$1" raw
  shift
  raw="$(curl -sS $CURL_EXTRA_OPTS -w $'\n%{http_code}' "$@" "${KNOX_URL}${path}" 2>/dev/null || true)"
  resp_code="${raw##*$'\n'}"
  resp_body="${raw%$'\n'*}"
  [[ "$resp_code" == 2* ]]
}

# Reachable in the TCP sense, used to tell "identity store down" from "bad password".
tcp_open() {
  local host="$1" port="$2"
  timeout 5 bash -c "echo > /dev/tcp/${host}/${port}" 2>/dev/null
}

need_cmd curl
need_cmd python3

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""

load_env_file "$KNOX_ENV_FILE" 'KNOX_*|CURL_EXTRA_OPTS'
# Knox only routes paths a topology actually declares as a service, so an
# unmapped path returns the same 404 as an undeployed topology. Each entry is
# therefore <topology>=<path of a service that topology exposes>.
KNOX_TOPOLOGY_PROBES="${KNOX_TOPOLOGY_PROBES:-admin=/api/v1/version default=/webhdfs/v1/?op=LISTSTATUS knoxsso=/api/v1/websso manager=/admin-ui/ homepage=/home metadata=/api/v1/metadata}"
KNOX_USER="${KNOX_USER:-admin}"
KNOX_SKIP_AUTH="${KNOX_SKIP_AUTH:-0}"
KNOX_SKIP_WEBHDFS="${KNOX_SKIP_WEBHDFS:-0}"
KNOX_WEBHDFS_TOPOLOGY="${KNOX_WEBHDFS_TOPOLOGY:-default}"
# Knox ships a self-signed certificate, so -k is the working default.
CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:--k}"

cluster="${CLUSTER_NAME:-}"
if [[ -f "$AMBARI_CONFIG_FILE" ]]; then
  load_env_file "$AMBARI_CONFIG_FILE" 'AMBARI_*'
fi
AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://10.101.11.22:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"

if [[ -z "$cluster" && -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]]; then
  cluster="$(ambari_get "${AMBARI_BASE_URL%/}/api/v1/clusters/" | python3 -c "
import json, sys
data = json.load(sys.stdin)
items = data.get('items') or []
if not items:
    sys.exit('no clusters in Ambari response')
print((items[0].get('Clusters') or {}).get('cluster_name') or sys.exit('no cluster_name'))
" 2>/dev/null || true)"
fi

knox_hosts=""
if [[ -z "${KNOX_URL:-}" ]]; then
  [[ -n "$cluster" ]] || die "Set KNOX_URL, or provide Ambari credentials in ${AMBARI_CONFIG_FILE}."
  knox_hosts="$(ambari_component_hosts KNOX KNOX_GATEWAY 2>/dev/null || true)"
  [[ -n "$knox_hosts" ]] || die "no KNOX_GATEWAY host in Ambari; set KNOX_URL"
  knox_host="${knox_hosts%% *}"
  knox_port="$(knox_prop "gateway.port" 2>/dev/null || echo 8443)"
  knox_path="$(knox_prop "gateway.path" 2>/dev/null || echo gateway)"
  KNOX_URL="https://${knox_host}:${knox_port}/${knox_path#/}"
fi
KNOX_URL="${KNOX_URL%/}"

gw_hostport="${KNOX_URL#*://}"
gw_hostport="${gw_hostport%%/*}"
gw_host="${gw_hostport%%:*}"
gw_port="${gw_hostport##*:}"
if [[ "$gw_port" == "$gw_host" ]]; then
  gw_port=443
fi

if [[ -z "${KNOX_LDAP_URL:-}" && -n "$cluster" ]]; then
  KNOX_LDAP_URL="$(knox_topology_param "main.ldapRealm.contextFactory.url" 2>/dev/null || true)"
  # The topology stores an Ambari placeholder for the host.
  KNOX_LDAP_URL="${KNOX_LDAP_URL//\{\{knox_host_name\}\}/$gw_host}"
fi

if [[ -z "${KNOX_PASSWORD:-}" && -n "$cluster" ]]; then
  KNOX_PASSWORD="$(knox_ldif_password "$KNOX_USER" 2>/dev/null || true)"
fi

echo "---- Knox sample smoke ----"
echo "    cluster:     ${cluster:-<unknown>}"
echo "    gateway:     ${KNOX_URL}"
echo "    hosts:       ${knox_hosts:-<from KNOX_URL>}"
echo "    topologies:  ${KNOX_TOPOLOGY_PROBES}"
echo "    user:        ${KNOX_USER}"
echo "    identity:    ${KNOX_LDAP_URL:-<unknown>}"

echo ""
echo "---- gateway reachability ----"

if tcp_open "$gw_host" "$gw_port"; then
  echo "        ${gw_host}:${gw_port} accepting connections"
  record_pass "gateway port open"
else
  record_fail "gateway port open (${gw_host}:${gw_port})"
fi

if command -v openssl >/dev/null 2>&1; then
  cert="$(echo | openssl s_client -connect "${gw_host}:${gw_port}" 2>/dev/null \
    | openssl x509 -noout -subject -enddate 2>/dev/null || true)"
  if [[ -n "$cert" ]]; then
    printf '%s\n' "$cert" | sed 's/^/        /'
    if echo | openssl s_client -connect "${gw_host}:${gw_port}" 2>/dev/null \
      | openssl x509 -noout -checkend 0 >/dev/null 2>&1; then
      record_pass "TLS certificate valid"
    else
      record_fail "TLS certificate expired"
    fi
  else
    record_fail "TLS handshake"
  fi
else
  record_skip "TLS certificate (openssl not installed)"
fi

echo ""
echo "---- topology deployment ----"
# A deployed topology answers 200, 302 (redirect to SSO) or 401 (auth required).
# 404 means it was never deployed, 5xx means it failed to deploy.
# read -ra rather than unquoted word splitting: the probe paths contain "?".
IFS=' ' read -ra topology_probes <<<"$KNOX_TOPOLOGY_PROBES"
for entry in "${topology_probes[@]}"; do
  [[ -z "$entry" ]] && continue
  topo="${entry%%=*}"
  probe="${entry#*=}"
  if [[ "$probe" == "$entry" ]]; then
    probe="/"
  fi
  gw "/${topo}${probe}" >/dev/null || true
  case "$resp_code" in
    200|30[1237]|401|403)
      echo "        ${topo}${probe} -> HTTP ${resp_code}"
      record_pass "topology ${topo} deployed"
      ;;
    *)
      record_fail "topology ${topo} deployed (${probe} -> HTTP ${resp_code})"
      ;;
  esac
done

echo ""
echo "---- auth enforcement ----"
challenge="$(curl -sS $CURL_EXTRA_OPTS -D - -o /dev/null "${KNOX_URL}/admin/api/v1/version" 2>/dev/null || true)"
code="$(printf '%s' "$challenge" | awk 'NR==1 {print $2}')"
if [[ "$code" == "401" ]]; then
  realm="$(printf '%s' "$challenge" | awk -F': ' 'tolower($1) == "www-authenticate" {print $2}' | tr -d '\r')"
  echo "        unauthenticated admin API -> 401 ${realm:-}"
  record_pass "unauthenticated request rejected"
else
  record_fail "unauthenticated request rejected (HTTP ${code:-none})"
fi

auth_possible=1
auth_reason=""
if [[ "$KNOX_SKIP_AUTH" == "1" ]]; then
  auth_possible=0
  auth_reason="KNOX_SKIP_AUTH=1"
elif [[ -z "${KNOX_PASSWORD:-}" ]]; then
  auth_possible=0
  auth_reason="no KNOX_PASSWORD"
fi

if (( auth_possible == 1 )); then
  echo ""
  echo "---- authenticated checks ----"
  if gw "/admin/api/v1/version" -u "${KNOX_USER}:${KNOX_PASSWORD}" -H "Accept: application/json"; then
    ver="$(printf '%s' "$resp_body" | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
v = d.get('ServerVersion') or d
print(v.get('version', '?'))
" 2>/dev/null || echo '?')"
    echo "        Knox version ${ver}"
    record_pass "admin API version"
  elif [[ "$resp_code" == "401" ]]; then
    # Distinguish a wrong password from an identity store that is simply down.
    ldap_down=0
    if [[ -n "${KNOX_LDAP_URL:-}" ]]; then
      hostport="${KNOX_LDAP_URL#*://}"
      hostport="${hostport%%/*}"
      if ! tcp_open "${hostport%%:*}" "${hostport##*:}"; then
        ldap_down=1
      fi
    fi
    if (( ldap_down == 1 )); then
      echo "        WARN: identity store ${KNOX_LDAP_URL} is not accepting connections;"
      echo "              Knox cannot authenticate anyone. Start it from Ambari:"
      echo "              Knox > Actions > Start Demo LDAP."
      auth_possible=0
      auth_reason="identity store ${KNOX_LDAP_URL} down"
      record_skip "admin API version (${auth_reason})"
    else
      record_fail "admin API version (HTTP 401 - check KNOX_USER / KNOX_PASSWORD)"
    fi
  else
    record_fail "admin API version (HTTP ${resp_code})"
  fi
else
  record_skip "admin API version (${auth_reason})"
fi

if (( auth_possible == 1 )); then
  if gw "/admin/api/v1/topologies" -u "${KNOX_USER}:${KNOX_PASSWORD}" -H "Accept: application/json"; then
    names="$(printf '%s' "$resp_body" | python3 -c "
import json, sys
d = json.load(sys.stdin)
topos = (d.get('topologies') or {}).get('topology') or []
if isinstance(topos, dict):
    topos = [topos]
print(', '.join(sorted(t.get('name', '?') for t in topos)))
" 2>/dev/null || echo '')"
    echo "        deployed: ${names:-<none>}"
    record_pass "admin API topologies"
  else
    record_fail "admin API topologies (HTTP ${resp_code})"
  fi
else
  record_skip "admin API topologies (${auth_reason})"
fi

if (( auth_possible == 1 )) && [[ "$KNOX_SKIP_WEBHDFS" != "1" ]]; then
  if gw "/${KNOX_WEBHDFS_TOPOLOGY}/webhdfs/v1/?op=LISTSTATUS" -u "${KNOX_USER}:${KNOX_PASSWORD}"; then
    entries="$(printf '%s' "$resp_body" | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(len(d['FileStatuses']['FileStatus']))
" 2>/dev/null || echo '?')"
    echo "        webhdfs LISTSTATUS / -> ${entries} entries"
    record_pass "proxied WebHDFS LISTSTATUS"
  else
    record_fail "proxied WebHDFS LISTSTATUS (HTTP ${resp_code})"
  fi
elif [[ "$KNOX_SKIP_WEBHDFS" == "1" ]]; then
  record_skip "proxied WebHDFS LISTSTATUS (KNOX_SKIP_WEBHDFS=1)"
else
  record_skip "proxied WebHDFS LISTSTATUS (${auth_reason})"
fi

echo ""
echo "---- summary ----"
for r in "${results[@]}"; do
  echo "    $r"
done
echo "    PASS=${pass} FAIL=${fail} SKIPPED=${skip}"

if (( fail > 0 )); then
  die "Knox sample smoke had ${fail} failing check(s)."
fi
echo "OK: Knox sample smoke finished (PASS=${pass} SKIPPED=${skip})."
