#!/usr/bin/env bash
#
# Smoke: JupyterHub REST API - status, form login, and a single-user server
# spawn / query / stop cycle.
#
# Steps:
#   1) Ambari discovery of the JUPYTERHUB host, port, TLS flag, base_url and the
#      DummyAuthenticator credentials
#   2) Unauthenticated checks: <base>/hub/api (version), /hub/health, /hub/login
#   3) Form login, then authenticated /hub/api/user, /hub/api/info, /hub/api/users
#   4) Spawn the user's single-user server, wait until ready, query the notebook
#      server /api and /api/kernelspecs, then stop it again
#
# XSRF note: JupyterHub scopes the _xsrf cookie per path. Hub API calls need the
# token from <base>/hub/, while the single-user server needs the one from
# <base>/user/<name>/. Mixing them up yields 403.
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   JUPYTERHUB_ENV_FILE       default <script-dir>/configs/jupyterhub.env
#   JUPYTERHUB_URL            default from Ambari, e.g. http://<host>:8000
#   JUPYTERHUB_BASE_URL       default c.JupyterHub.base_url from Ambari, e.g. /lab
#   JUPYTERHUB_USER           default initial_admin from Ambari, else jupyterhub
#   JUPYTERHUB_PASSWORD       default dummy_password from Ambari
#   JUPYTERHUB_SKIP_SPAWN     default 0 - set 1 for status + login checks only
#   JUPYTERHUB_KEEP_SERVER    default 0 - set 1 to leave the spawned server running
#   JUPYTERHUB_SPAWN_TIMEOUT  default 120 seconds
#   JUPYTERHUB_POLL_SECONDS   default 5
#   CURL_EXTRA_OPTS           e.g. -k for a self-signed certificate
#
# Usage:
#   ./jupyterhub-sample-smoke.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
JUPYTERHUB_ENV_FILE="${JUPYTERHUB_ENV_FILE:-${SCRIPT_DIR}/configs/jupyterhub.env}"

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

# Caches the whole JUPYTER config payload; several values come out of it.
jupyter_config_json=""
load_jupyter_config() {
  [[ -n "$jupyter_config_json" ]] && return 0
  [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]] || return 1
  jupyter_config_json="$(ambari_get "${AMBARI_BASE_URL%/}/api/v1/clusters/${cluster}/configurations/service_config_versions?service_name=JUPYTER&is_current=true")" || return 1
  [[ -n "$jupyter_config_json" ]]
}

jupyter_prop() {
  load_jupyter_config || return 1
  printf '%s' "$jupyter_config_json" | python3 -c "
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

# base_url only exists inside the rendered jupyterhub_config.py content.
jupyter_base_url_from_config() {
  load_jupyter_config || return 1
  printf '%s' "$jupyter_config_json" | python3 -c "
import json, re, sys
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(1)
for item in data.get('items', []):
    for conf in item.get('configurations', []):
        content = (conf.get('properties') or {}).get('content')
        if not content:
            continue
        m = re.search(r'c\.JupyterHub\.base_url\s*=\s*[\'\"]([^\'\"]+)', content)
        if m:
            print(m.group(1))
            sys.exit(0)
sys.exit(1)
"
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

# Netscape cookie jar: domain, flag, path, secure, expiry, name, value.
cookie_value() {
  local name="$1" path="$2"
  awk -v n="$name" -v p="$path" '$6 == n && $3 == p { v = $7 } END { print v }' "$COOKIE_JAR"
}

# req <method> <url> <xsrf> [curl args...] - sets resp_body / resp_code.
req() {
  local method="$1" url="$2" xsrf="$3" raw
  shift 3
  local -a args=(-sS $CURL_EXTRA_OPTS -b "$COOKIE_JAR" -c "$COOKIE_JAR" -X "$method" -w $'\n%{http_code}')
  [[ -n "$xsrf" ]] && args+=(-H "X-XSRFToken: ${xsrf}")
  raw="$(curl "${args[@]}" "$@" "$url" 2>/dev/null || true)"
  resp_code="${raw##*$'\n'}"
  resp_body="${raw%$'\n'*}"
  [[ "$resp_code" == 2* ]]
}

hub_xsrf() { cookie_value _xsrf "${BASE}/hub/"; }
user_xsrf() { cookie_value _xsrf "${BASE}/user/${JUPYTERHUB_USER}/"; }

# A successful form login answers 302 to "next". Redirects are deliberately not
# followed: curl would replay the POST against the redirect target and get a 403.
login_request() {
  local xsrf raw
  xsrf="$(hub_xsrf)"
  raw="$(curl -sS $CURL_EXTRA_OPTS -b "$COOKIE_JAR" -c "$COOKIE_JAR" \
    -w $'\n%{http_code}' \
    --data-urlencode "username=${JUPYTERHUB_USER}" \
    --data-urlencode "password=${JUPYTERHUB_PASSWORD}" \
    --data-urlencode "_xsrf=${xsrf}" \
    "${HUB}/hub/login?next=" 2>/dev/null || true)"
  resp_code="${raw##*$'\n'}"
  resp_body="${raw%$'\n'*}"
  [[ "$resp_code" == 2* || "$resp_code" == 3* ]] || return 1
  # Logging in rotates _xsrf, but only an HTML handler reissues the cookie - the
  # JSON API never does. Without this fetch every later POST fails with
  # "XSRF cookie does not match POST argument".
  curl -sS $CURL_EXTRA_OPTS -b "$COOKIE_JAR" -c "$COOKIE_JAR" -L -o /dev/null \
    "${HUB}/hub/home" 2>/dev/null || true
  return 0
}

need_cmd curl
need_cmd python3

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""

load_env_file "$JUPYTERHUB_ENV_FILE" 'JUPYTERHUB_*|CURL_EXTRA_OPTS'
JUPYTERHUB_SKIP_SPAWN="${JUPYTERHUB_SKIP_SPAWN:-0}"
JUPYTERHUB_KEEP_SERVER="${JUPYTERHUB_KEEP_SERVER:-0}"
JUPYTERHUB_SPAWN_TIMEOUT="${JUPYTERHUB_SPAWN_TIMEOUT:-120}"
JUPYTERHUB_POLL_SECONDS="${JUPYTERHUB_POLL_SECONDS:-5}"
CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}"

cluster="${CLUSTER_NAME:-}"
if [[ -f "$AMBARI_CONFIG_FILE" ]]; then
  load_env_file "$AMBARI_CONFIG_FILE" 'AMBARI_*'
fi
AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://10.101.11.22:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"

if [[ -z "$cluster" ]]; then
  if [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]]; then
    cluster="$(ambari_get "${AMBARI_BASE_URL%/}/api/v1/clusters/" | python3 -c "
import json, sys
data = json.load(sys.stdin)
items = data.get('items') or []
if not items:
    sys.exit('no clusters in Ambari response')
print((items[0].get('Clusters') or {}).get('cluster_name') or sys.exit('no cluster_name'))
" 2>/dev/null || true)"
  fi
fi

jh_hosts=""
if [[ -z "${JUPYTERHUB_URL:-}" ]]; then
  [[ -n "$cluster" ]] || die "Set JUPYTERHUB_URL, or provide Ambari credentials in ${AMBARI_CONFIG_FILE}."
  jh_hosts="$(ambari_component_hosts JUPYTER JUPYTERHUB 2>/dev/null || true)"
  [[ -n "$jh_hosts" ]] || die "no JUPYTERHUB host in Ambari; set JUPYTERHUB_URL"
  jh_host="${jh_hosts%% *}"
  jh_port="$(jupyter_prop port 2>/dev/null || echo 8000)"
  if [[ "$(jupyter_prop enable_ssl 2>/dev/null || echo false)" == "true" ]]; then
    JUPYTERHUB_URL="https://${jh_host}:${jh_port}"
  else
    JUPYTERHUB_URL="http://${jh_host}:${jh_port}"
  fi
fi
JUPYTERHUB_URL="${JUPYTERHUB_URL%/}"

if [[ -z "${JUPYTERHUB_BASE_URL:-}" && -n "$cluster" ]]; then
  JUPYTERHUB_BASE_URL="$(jupyter_base_url_from_config 2>/dev/null || echo /)"
fi
# Normalise to either "" (root) or "/prefix" with no trailing slash.
BASE="/${JUPYTERHUB_BASE_URL:-/}"
BASE="/$(printf '%s' "$BASE" | sed 's#^/*##; s#/*$##')"
[[ "$BASE" == "/" ]] && BASE=""

if [[ -z "${JUPYTERHUB_USER:-}" && -n "$cluster" ]]; then
  JUPYTERHUB_USER="$(jupyter_prop initial_admin 2>/dev/null || true)"
fi
JUPYTERHUB_USER="${JUPYTERHUB_USER:-jupyterhub}"

if [[ -z "${JUPYTERHUB_PASSWORD:-}" && -n "$cluster" ]]; then
  JUPYTERHUB_PASSWORD="$(jupyter_prop dummy_password 2>/dev/null || true)"
fi

HUB="${JUPYTERHUB_URL}${BASE}"

echo "---- JupyterHub sample smoke ----"
echo "    cluster:   ${cluster:-<unknown>}"
echo "    hub URL:   ${HUB}"
echo "    hosts:     ${jh_hosts:-<from JUPYTERHUB_URL>}"
echo "    base_url:  ${BASE:-/}"
echo "    user:      ${JUPYTERHUB_USER}"

COOKIE_JAR="$(mktemp "${TMPDIR:-/tmp}/jupyterhub-smoke.XXXXXX")"

echo ""
echo "---- status endpoints ----"

if req GET "${HUB}/hub/api" ""; then
  version="$(printf '%s' "$resp_body" | jget 'd["version"]' 2>/dev/null || echo unknown)"
  echo "        JupyterHub version ${version}"
  record_pass "hub/api version"
else
  record_fail "hub/api version (HTTP ${resp_code})"
fi

if req GET "${HUB}/hub/health" ""; then
  record_pass "hub/health"
else
  record_fail "hub/health (HTTP ${resp_code})"
fi

if req GET "${HUB}/hub/login" ""; then
  record_pass "hub/login page"
else
  record_fail "hub/login page (HTTP ${resp_code})"
fi

logged_in=0
if [[ -z "${JUPYTERHUB_PASSWORD:-}" ]]; then
  record_skip "login (no JUPYTERHUB_PASSWORD)"
else
  echo ""
  echo "---- login ----"
  if login_request; then
    if req GET "${HUB}/hub/api/user" "$(hub_xsrf)"; then
      who="$(printf '%s' "$resp_body" | jget 'd["name"]' 2>/dev/null || echo '')"
      admin="$(printf '%s' "$resp_body" | jget 'd.get("admin")' 2>/dev/null || echo '?')"
      if [[ "$who" == "$JUPYTERHUB_USER" ]]; then
        echo "        authenticated as ${who} (admin=${admin})"
        record_pass "form login"
        logged_in=1
      else
        record_fail "form login (identity '${who}')"
      fi
    else
      record_fail "form login (hub/api/user HTTP ${resp_code})"
    fi
  else
    record_fail "form login (HTTP ${resp_code})"
  fi
fi

if (( logged_in == 1 )); then
  if req GET "${HUB}/hub/api/info" "$(hub_xsrf)"; then
    spawner="$(printf '%s' "$resp_body" | jget 'd["spawner"]["class"]' 2>/dev/null || echo '?')"
    authn="$(printf '%s' "$resp_body" | jget 'd["authenticator"]["class"]' 2>/dev/null || echo '?')"
    echo "        spawner ${spawner##*.}, authenticator ${authn##*.}"
    record_pass "hub/api/info"
  else
    record_fail "hub/api/info (HTTP ${resp_code})"
  fi

  if req GET "${HUB}/hub/api/users" "$(hub_xsrf)"; then
    users="$(printf '%s' "$resp_body" | jget 'len(d)' 2>/dev/null || echo '?')"
    echo "        ${users} user(s) known to the hub"
    record_pass "hub/api/users"
  else
    record_fail "hub/api/users (HTTP ${resp_code})"
  fi
else
  record_skip "hub/api/info"
  record_skip "hub/api/users"
fi

spawned=0
cleanup_done=0
cleanup() {
  [[ "$cleanup_done" == "1" ]] && return 0
  cleanup_done=1
  [[ "$spawned" == "1" && "$JUPYTERHUB_KEEP_SERVER" != "1" ]] || return 0
  echo ""
  echo "---- cleanup ----"
  # The hub API needs the hub-scoped token, not the single-user server one.
  if req DELETE "${HUB}/hub/api/users/${JUPYTERHUB_USER}/server" "$(hub_xsrf)"; then
    echo "        stopped single-user server for ${JUPYTERHUB_USER}"
  else
    echo "        WARN: could not stop single-user server (HTTP ${resp_code})" >&2
  fi
}
trap 'cleanup; rm -f "$COOKIE_JAR"' EXIT

if (( logged_in == 0 )) || [[ "$JUPYTERHUB_SKIP_SPAWN" == "1" ]]; then
  record_skip "spawn single-user server"
  record_skip "single-user server api"
  record_skip "single-user kernelspecs"
else
  echo ""
  echo "---- single-user server ----"
  if req POST "${HUB}/hub/api/users/${JUPYTERHUB_USER}/server" "$(hub_xsrf)" \
      -H 'Content-Type: application/json' -d '{}'; then
    spawned=1
  elif [[ "$resp_code" == "400" ]]; then
    # 400 means it is already running; treat that as spawned so cleanup stops it.
    spawned=1
  fi

  ready=0
  if (( spawned == 1 )); then
    deadline=$(( $(date +%s) + JUPYTERHUB_SPAWN_TIMEOUT ))
    while true; do
      if req GET "${HUB}/hub/api/users/${JUPYTERHUB_USER}" "$(hub_xsrf)"; then
        state="$(printf '%s' "$resp_body" | python3 -c "
import json, sys
d = json.load(sys.stdin, strict=False)
servers = d.get('servers') or {}
if any(s.get('ready') for s in servers.values()):
    print('ready')
elif servers:
    print('pending')
else:
    print('none')
" 2>/dev/null || echo unknown)"
      else
        state="unknown"
      fi
      echo "        state=${state}"
      [[ "$state" == "ready" ]] && { ready=1; break; }
      (( $(date +%s) >= deadline )) && break
      sleep "$JUPYTERHUB_POLL_SECONDS"
    done
  fi

  if (( ready == 1 )); then
    record_pass "spawn single-user server"
  else
    record_fail "spawn single-user server (HTTP ${resp_code})"
  fi

  if (( ready == 1 )); then
    if req GET "${HUB}/user/${JUPYTERHUB_USER}/api" ""; then
      nb="$(printf '%s' "$resp_body" | jget 'd["version"]' 2>/dev/null || echo unknown)"
      echo "        notebook server version ${nb}"
      record_pass "single-user server api"
    else
      record_fail "single-user server api (HTTP ${resp_code})"
    fi

    # Visiting the server root issues the _xsrf cookie scoped to its own path.
    req GET "${HUB}/user/${JUPYTERHUB_USER}/lab" "" -L >/dev/null || true
    if req GET "${HUB}/user/${JUPYTERHUB_USER}/api/kernelspecs" "$(user_xsrf)"; then
      kernels="$(printf '%s' "$resp_body" | jget '"default=" + str(d.get("default")) + " kernels=" + ",".join(sorted(d.get("kernelspecs", {})))' 2>/dev/null || echo '')"
      echo "        ${kernels:-<none>}"
      count="$(printf '%s' "$resp_body" | jget 'len(d.get("kernelspecs", {}))' 2>/dev/null || echo 0)"
      if (( count > 0 )); then
        record_pass "single-user kernelspecs"
      else
        record_fail "single-user kernelspecs (no kernels)"
      fi
    else
      record_fail "single-user kernelspecs (HTTP ${resp_code})"
    fi
  else
    record_skip "single-user server api"
    record_skip "single-user kernelspecs"
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
  die "JupyterHub sample smoke had ${fail} failing check(s)."
fi
echo "OK: JupyterHub sample smoke finished (PASS=${pass} SKIPPED=${skip})."
