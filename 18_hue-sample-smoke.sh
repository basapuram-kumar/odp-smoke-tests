#!/usr/bin/env bash
#
# Smoke: Apache Hue setup-ready checks, UI accessibility, form login, APIs.
#
# Steps:
#   0) Optional setups/setup-hue.sh (HUE_RUN_SETUP=1): MySQL metastore, keytab,
#      migrate, Ambari START
#   1) Ambari discovery of HUE_SERVER host, http_port, ssl_enable, auth backend
#   2) Ambari HUE service STARTED
#   3) Unauthenticated: /desktop/debug/is_alive, / (redirect to login),
#      login page + CSRF, static CSS linked from login page
#   4) Form login (Django CSRF + session cookies)
#   5) Authenticated: /hue/about/, /desktop/api2/get_config,
#      /useradmin/api/get_users, /notebook/api/get_history
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   HUE_ENV_FILE / HUE_CONFIG_FILE  default <script-dir>/configs/hue.env
#   HUE_URL               e.g. http://hue-host:8888 (skip Ambari host discovery when set)
#   HUE_HOST              used with HUE_PORT / HUE_SSL if URL unset
#   HUE_PORT              default Ambari hue-env http_port, else 8888
#   HUE_SSL               default Ambari hue-desktop-site ssl_enable, else 0
#   HUE_USER              default admin
#   HUE_PASSWORD          default admin (AllowFirstUserDjangoBackend / ModelBackend)
#   HUE_SKIP_AUTH         default 0 - set 1 for is_alive + login-page only
#   HUE_RUN_SETUP         default 0 - set 1 to run setups/setup-hue.sh first
#   CURL_EXTRA_OPTS       e.g. -k when ssl_enable is true with a self-signed cert
#
# Usage:
#   ./hue-sample-smoke.sh
#   HUE_RUN_SETUP=1 SSH_KEY=$HOME/Downloads/usdc.pem ./hue-sample-smoke.sh
#   HUE_SKIP_AUTH=1 ./hue-sample-smoke.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
HUE_ENV_FILE="${HUE_ENV_FILE:-${HUE_CONFIG_FILE:-${SCRIPT_DIR}/configs/hue.env}}"

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
        [[ -n "${!key:-}" ]] || printf -v "$key" '%s' "$val"
        ;;
    esac
  done <"$f"
  return 0
}

pass=0
fail=0
skip=0
declare -a results=()

record_pass() {
  results+=("PASS: $1")
  pass=$((pass + 1))
  echo "    PASS: $1"
}

record_fail() {
  results+=("FAIL: $1")
  fail=$((fail + 1))
  echo "    FAIL: $1"
}

record_skip() {
  results+=("SKIPPED: $1")
  skip=$((skip + 1))
  echo "    SKIPPED: $1"
}

ambari_get() {
  # shellcheck disable=SC2086
  curl -sS -f ${CURL_EXTRA_OPTS:-} -u "${AMBARI_USER}:${AMBARI_PASSWORD}" \
    -H "X-Requested-By: ambari" "$1" 2>/dev/null
}

need_cmd curl
need_cmd python3

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""

load_env_file "$HUE_ENV_FILE" 'HUE_*|SSH_*|CURL_EXTRA_OPTS'
HUE_SKIP_AUTH="${HUE_SKIP_AUTH:-0}"
HUE_RUN_SETUP="${HUE_RUN_SETUP:-0}"
HUE_USER="${HUE_USER:-admin}"
HUE_PASSWORD="${HUE_PASSWORD:-admin}"
HUE_PORT="${HUE_PORT:-}"
HUE_SSL="${HUE_SSL:-}"
CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}"

if [[ "$HUE_RUN_SETUP" == "1" ]]; then
  echo "---- Hue setup (HUE_RUN_SETUP=1) ----"
  # shellcheck disable=SC1091
  "${SCRIPT_DIR}/setups/setup-hue.sh" || die "setups/setup-hue.sh failed"
fi

cluster="${CLUSTER_NAME:-}"
if [[ -f "$AMBARI_CONFIG_FILE" ]]; then
  load_env_file "$AMBARI_CONFIG_FILE" 'AMBARI_*'
fi
AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://127.0.0.1:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"

if [[ -z "$cluster" && ( -z "${HUE_URL:-}" && -z "${HUE_HOST:-}" ) ]]; then
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

discover_hue() {
  CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}" \
    AMBARI_USER="$AMBARI_USER" AMBARI_PASSWORD="$AMBARI_PASSWORD" \
    python3 - "$AMBARI_BASE_URL" "$cluster" <<'PY'
import json, os, shlex, subprocess, sys, urllib.parse

def curl_extra():
    raw = os.environ.get("CURL_EXTRA_OPTS", "").strip()
    return shlex.split(raw) if raw else []

def curl_json(url):
    user, pw = os.environ["AMBARI_USER"], os.environ["AMBARI_PASSWORD"]
    cmd = ["curl", "-sS", "-f", "-u", f"{user}:{pw}", "-H", "X-Requested-By: ambari"] + curl_extra() + [url]
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if r.returncode != 0:
        sys.stderr.write(r.stderr or r.stdout or "curl failed\n")
        sys.exit(r.returncode)
    return json.loads(r.stdout)

ambari, cluster = sys.argv[1].rstrip("/"), sys.argv[2]
qc = urllib.parse.quote(cluster, safe="")
j = curl_json(f"{ambari}/api/v1/clusters/{qc}?fields=Clusters/desired_configs")
dc = (j.get("Clusters") or {}).get("desired_configs") or {}

def props_for(ctype):
    meta = dc.get(ctype) or {}
    tag = meta.get("tag")
    if not tag:
        return {}
    j2 = curl_json(
        f"{ambari}/api/v1/clusters/{qc}/configurations?type={urllib.parse.quote(ctype)}&tag={urllib.parse.quote(tag)}"
    )
    items = j2.get("items") or []
    if not items:
        return {}
    return items[0].get("properties") or {}

env_props = props_for("hue-env")
desktop_props = props_for("hue-desktop-site")
auth_props = props_for("hue-auth-site")

port = str(env_props.get("http_port") or "8888").strip()
ssl_raw = str(desktop_props.get("ssl_enable") or "false").strip().lower()
ssl = "1" if ssl_raw in ("true", "1", "yes") else "0"
backend = str(auth_props.get("backend") or "").strip()

hc = curl_json(
    f"{ambari}/api/v1/clusters/{qc}/host_components"
    f"?HostRoles/component_name=HUE_SERVER"
    f"&fields=HostRoles/host_name,HostRoles/public_host_name,HostRoles/state"
)
host = None
for it in hc.get("items") or []:
    hr = it.get("HostRoles") or {}
    cand = hr.get("public_host_name") or hr.get("host_name")
    if (hr.get("state") or "").upper() == "STARTED" and cand:
        host = cand
        break
if not host:
    for it in hc.get("items") or []:
        hr = it.get("HostRoles") or {}
        host = hr.get("public_host_name") or hr.get("host_name")
        if host:
            break
if not host:
    sys.stderr.write("No HUE_SERVER host in Ambari; set HUE_URL\n")
    sys.exit(2)

print("host=%s" % host)
print("port=%s" % port)
print("ssl=%s" % ssl)
print("backend=%s" % backend)
PY
}

auth_backend=""
if [[ -n "${HUE_URL:-}" ]]; then
  hue_base="${HUE_URL%/}"
elif [[ -n "${HUE_HOST:-}" ]]; then
  HUE_PORT="${HUE_PORT:-8888}"
  HUE_SSL="${HUE_SSL:-0}"
  scheme="http"
  [[ "$HUE_SSL" == "1" ]] && scheme="https"
  hue_base="${scheme}://${HUE_HOST}:${HUE_PORT}"
else
  [[ -n "$cluster" ]] || die "Set HUE_URL / HUE_HOST, or provide Ambari credentials in ${AMBARI_CONFIG_FILE}."
  [[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]] || die "Ambari credentials required for Hue discovery"
  while IFS='=' read -r key val; do
    case "$key" in
      host) HUE_HOST="${HUE_HOST:-$val}" ;;
      port) HUE_PORT="${HUE_PORT:-$val}" ;;
      ssl) HUE_SSL="${HUE_SSL:-$val}" ;;
      backend) auth_backend="$val" ;;
    esac
  done < <(discover_hue)
  HUE_PORT="${HUE_PORT:-8888}"
  HUE_SSL="${HUE_SSL:-0}"
  scheme="http"
  [[ "$HUE_SSL" == "1" ]] && scheme="https"
  hue_base="${scheme}://${HUE_HOST}:${HUE_PORT}"
fi

# Self-signed Hue TLS: default to -k when SSL is on and caller did not set opts.
if [[ "${HUE_SSL:-0}" == "1" && -z "${CURL_EXTRA_OPTS}" ]]; then
  CURL_EXTRA_OPTS="-k"
fi

COOKIE_JAR="$(mktemp "${TMPDIR:-/tmp}/hue-smoke-cookies.XXXXXX")"
trap 'rm -f "$COOKIE_JAR"' EXIT

echo "---- Hue sample smoke ----"
echo "    URL: ${hue_base}"
echo "    user: ${HUE_USER}"
[[ -n "$cluster" ]] && echo "    cluster: $cluster"
[[ -n "$auth_backend" ]] && echo "    auth backend: $auth_backend"

# Ambari service state when credentials/cluster are available.
if [[ -n "$cluster" && -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]]; then
  hue_svc_state="$(ambari_get "${AMBARI_BASE_URL%/}/api/v1/clusters/${cluster}/services/HUE?fields=ServiceInfo/state" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("ServiceInfo",{}).get("state",""))' 2>/dev/null || true)"
  echo "    Ambari HUE state: ${hue_svc_state:-?}"
  if [[ "$hue_svc_state" == "STARTED" ]]; then
    record_pass "Ambari HUE STARTED"
  elif [[ -n "$hue_svc_state" ]]; then
    record_fail "Ambari HUE state=${hue_svc_state} (expected STARTED)"
  else
    record_skip "Ambari HUE state (could not read)"
  fi
fi

# curl helpers: set resp_body / resp_code. Do not use a variable named "path"
# (zsh ties path <-> PATH).
resp_body=""
resp_code=""

hue_req() {
  local method="$1" epath="$2"
  shift 2
  local raw
  # shellcheck disable=SC2086
  raw="$(curl -sS ${CURL_EXTRA_OPTS:-} -b "$COOKIE_JAR" -c "$COOKIE_JAR" \
    -X "$method" -w $'\n%{http_code}' "$@" "${hue_base}${epath}" 2>/dev/null || true)"
  resp_code="${raw##*$'\n'}"
  resp_body="${raw%$'\n'*}"
}

hue_get() {
  hue_req GET "$1"
}

extract_csrf() {
  printf '%s' "$1" | python3 -c "
import re, sys
html = sys.stdin.read()
m = re.search(r'name=[\"\\']csrfmiddlewaretoken[\"\\']\\s+value=[\"\\']([^\"\\']+)', html)
print(m.group(1) if m else '')
"
}

echo "---- unauthenticated checks ----"
hue_get "/desktop/debug/is_alive"
if [[ "$resp_code" == "200" ]]; then
  record_pass "desktop/debug/is_alive"
else
  record_fail "desktop/debug/is_alive (HTTP ${resp_code})"
fi

# Root should bounce anonymous callers to the login form.
# shellcheck disable=SC2086
root_code="$(curl -sS ${CURL_EXTRA_OPTS:-} -o /dev/null -w '%{http_code}' \
  --connect-timeout 5 --max-time 20 "${hue_base}/" || true)"
# shellcheck disable=SC2086
root_loc="$(curl -sS ${CURL_EXTRA_OPTS:-} -o /dev/null -w '%{redirect_url}' \
  --connect-timeout 5 --max-time 20 "${hue_base}/" || true)"
echo "    GET / -> HTTP ${root_code} Location=${root_loc}"
if [[ "$root_code" =~ ^(302|301|303|307|308)$ ]] && [[ "$root_loc" == *login* ]]; then
  record_pass "GET / redirects to login"
elif [[ "$root_code" == "200" ]]; then
  # AllowAllBackend (or already-open session) may serve / directly.
  record_pass "GET / (HTTP 200)"
else
  record_fail "GET / unexpected HTTP ${root_code}"
fi

hue_get "/hue/accounts/login/?next=/"
csrf="$(extract_csrf "$resp_body")"
if [[ "$resp_code" == "200" && -n "$csrf" ]]; then
  echo "    login page CSRF ok (len=${#csrf})"
  record_pass "login page + CSRF"
else
  record_fail "login page (HTTP ${resp_code}, csrf_len=${#csrf})"
fi

# Static CSS linked from the login page (UI accessibility).
css_path="$(printf '%s' "$resp_body" | python3 -c "
import re, sys
html = sys.stdin.read()
m = re.search(r'href=[\"\\'](/static/[^\"\\']+\\.css)[\"\\']', html)
print(m.group(1) if m else '')
")"
if [[ -n "$css_path" ]]; then
  hue_get "$css_path"
  if [[ "$resp_code" == "200" && ${#resp_body} -gt 500 ]]; then
    record_pass "static CSS (${css_path}, bytes=${#resp_body})"
  else
    record_fail "static CSS ${css_path} (HTTP ${resp_code}, bytes=${#resp_body})"
  fi
else
  record_skip "static CSS (no link on login page)"
fi

if [[ "$HUE_SKIP_AUTH" == "1" ]]; then
  record_skip "form login (HUE_SKIP_AUTH=1)"
  record_skip "hue/about (HUE_SKIP_AUTH=1)"
  record_skip "api2/get_config (HUE_SKIP_AUTH=1)"
  record_skip "useradmin get_users (HUE_SKIP_AUTH=1)"
  record_skip "notebook get_history (HUE_SKIP_AUTH=1)"
else
  echo "---- form login (${HUE_USER}) ----"
  if [[ -z "$csrf" ]]; then
    record_fail "form login (no CSRF token)"
  else
    # Do not follow the 302: curl would replay POST onto / and fail CSRF.
    # shellcheck disable=SC2086
    raw="$(curl -sS ${CURL_EXTRA_OPTS:-} -b "$COOKIE_JAR" -c "$COOKIE_JAR" \
      -w $'\n%{http_code}|%{redirect_url}' \
      -X POST "${hue_base}/hue/accounts/login/" \
      -H "Referer: ${hue_base}/hue/accounts/login/" \
      --data-urlencode "username=${HUE_USER}" \
      --data-urlencode "password=${HUE_PASSWORD}" \
      --data-urlencode "csrfmiddlewaretoken=${csrf}" \
      --data-urlencode "next=/" 2>/dev/null || true)"
    login_meta="${raw##*$'\n'}"
    login_code="${login_meta%%|*}"
    login_loc="${login_meta#*|}"
    echo "    POST login -> HTTP ${login_code} Location=${login_loc}"
    if [[ "$login_code" =~ ^(302|301|303|307|308)$ ]]; then
      record_pass "form login"
      # Establish session cookies against an HTML page.
      # shellcheck disable=SC2086
      curl -sS ${CURL_EXTRA_OPTS:-} -b "$COOKIE_JAR" -c "$COOKIE_JAR" -L \
        -o /dev/null --max-redirs 5 "${hue_base}/" >/dev/null 2>&1 || true
    elif [[ "$login_code" == "200" ]]; then
      # Some Hue builds land on /hue with 200 after successful login.
      if printf '%s' "$raw" | grep -qiE 'Welcome to Hue|/hue/|logout'; then
        record_pass "form login (HTTP 200 app shell)"
      else
        record_fail "form login (HTTP 200 - check HUE_USER / HUE_PASSWORD)"
      fi
    else
      record_fail "form login (HTTP ${login_code})"
    fi
  fi

  echo "---- authenticated UI + APIs ----"
  hue_get "/hue/about/"
  if [[ "$resp_code" == "200" && ${#resp_body} -gt 1000 ]]; then
    record_pass "hue/about (bytes=${#resp_body})"
  else
    record_fail "hue/about (HTTP ${resp_code}, bytes=${#resp_body})"
  fi
  hue_get "/desktop/api2/get_config"
  if [[ "$resp_code" == "200" ]]; then
    apps="$(printf '%s' "$resp_body" | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
cfg = d.get('app_config') or {}
print(','.join(sorted(cfg.keys())))
" 2>/dev/null || true)"
    echo "    apps: ${apps:-?}"
    record_pass "desktop/api2/get_config"
  else
    record_fail "desktop/api2/get_config (HTTP ${resp_code})"
  fi

  hue_get "/useradmin/api/get_users"
  if [[ "$resp_code" == "200" ]]; then
    users="$(printf '%s' "$resp_body" | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
users = d.get('users') or []
print(','.join(u.get('username','?') for u in users[:10]))
print('count=%d' % len(users))
" 2>/dev/null || true)"
    echo "    users: ${users}"
    # Confirm the smoke user appears when the API returns JSON.
    if printf '%s' "$resp_body" | python3 -c "
import json, sys
d = json.load(sys.stdin)
users = [u.get('username') for u in (d.get('users') or [])]
sys.exit(0 if '${HUE_USER}' in users else 1)
" 2>/dev/null; then
      record_pass "useradmin/api/get_users (includes ${HUE_USER})"
    else
      record_pass "useradmin/api/get_users"
    fi
  else
    record_fail "useradmin/api/get_users (HTTP ${resp_code})"
  fi

  hue_get "/notebook/api/get_history"
  if [[ "$resp_code" == "200" ]]; then
    status="$(printf '%s' "$resp_body" | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
print(d.get('status'))
" 2>/dev/null || true)"
    if [[ "$status" == "0" ]]; then
      record_pass "notebook/api/get_history"
    else
      record_fail "notebook/api/get_history (status=${status:-?})"
    fi
  else
    record_fail "notebook/api/get_history (HTTP ${resp_code})"
  fi
fi

echo ""
echo "---- summary ----"
for r in "${results[@]}"; do
  echo "    $r"
done
echo "    PASS=${pass} FAIL=${fail} SKIPPED=${skip}"

if (( fail > 0 )); then
  die "Hue sample smoke had ${fail} failing check(s)."
fi
echo "OK: Hue sample smoke finished (PASS=${pass} SKIPPED=${skip})."
