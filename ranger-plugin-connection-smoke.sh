#!/usr/bin/env bash
#
# Smoke: Ranger Admin "Test Connection" for every enabled Ranger service (plugin repo).
# Mirrors the Ranger UI Test Connection button via:
#   POST /service/plugins/services/validateConfig
#
# Lists services from:
#   GET /service/public/v2/api/service
#
# Ranger base URL:
#   Set RANGER_BASE_URL to skip Ambari discovery, or reuse configs/ambari.env discovery
#   (same pattern as ranger-yarn-all-queue-users-add.sh).
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   RANGER_ENV_FILE / RANGER_CONFIG_FILE   default <script-dir>/configs/ranger.env
#   RANGER_PASSWORD_FILE                  if RANGER_PASSWORD unset, read first line
#   RANGER_BASE_URL                       skip Ambari when set (trailing slash ok)
#   RANGER_USER                           default admin
#   RANGER_PASSWORD                       required unless ranger.env / password file
#   RANGER_INCLUDE_DISABLED               if 1, also test isEnabled=false services
#   RANGER_SERVICE_TYPES                  comma list to include (e.g. hdfs,yarn,hive)
#   RANGER_SKIP_TYPES                     comma list to skip (e.g. tag,kms)
#   RANGER_SKIP_SERVICES                  comma list of service names to skip
#   RANGER_TIMEOUT_SECONDS                per-service HTTP timeout (default 180)
#   RANGER_FAIL_ON_ERROR                  default 1 - exit 1 if any tested service FAILs
#   CURL_EXTRA_OPTS                       e.g. -k for self-signed TLS
#
# Usage:
#   edit configs/ranger.env   # set RANGER_PASSWORD / URL
#   ./ranger-plugin-connection-smoke.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
RANGER_ENV_FILE="${RANGER_ENV_FILE:-${RANGER_CONFIG_FILE:-${SCRIPT_DIR}/configs/ranger.env}}"
RANGER_INCLUDE_DISABLED="${RANGER_INCLUDE_DISABLED:-0}"
RANGER_TIMEOUT_SECONDS="${RANGER_TIMEOUT_SECONDS:-180}"
RANGER_FAIL_ON_ERROR="${RANGER_FAIL_ON_ERROR:-1}"

die() {
  echo "ERROR: $*" >&2
  exit 1
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

load_ambari_env_file() {
  local f="$1" key val line
  [[ -f "$f" ]] || return 1
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
  done <"$f"
  return 0
}

_cfg_RANGER_USER=""
_cfg_RANGER_PASSWORD=""
_cfg_RANGER_BASE_URL=""

load_ranger_env_file() {
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
      RANGER_USER) _cfg_RANGER_USER="$val" ;;
      RANGER_PASSWORD) _cfg_RANGER_PASSWORD="$val" ;;
      RANGER_BASE_URL) _cfg_RANGER_BASE_URL="$val" ;;
    esac
  done <"$f"
  return 0
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

ambari_curl() {
  # shellcheck disable=SC2086
  curl -sS -f ${CURL_EXTRA_OPTS:-} -u "${AMBARI_USER}:${AMBARI_PASSWORD}" -H "X-Requested-By: ambari" "$@"
}

need_cmd curl
need_cmd python3

load_ranger_env_file "$RANGER_ENV_FILE" || die "failed to read $RANGER_ENV_FILE"
RANGER_USER="${RANGER_USER:-${_cfg_RANGER_USER:-admin}}"
RANGER_PASSWORD="${RANGER_PASSWORD:-${_cfg_RANGER_PASSWORD:-}}"
RANGER_BASE_URL="${RANGER_BASE_URL:-${_cfg_RANGER_BASE_URL:-}}"

if [[ -z "${RANGER_PASSWORD:-}" && -n "${RANGER_PASSWORD_FILE:-}" ]]; then
  [[ -r "$RANGER_PASSWORD_FILE" ]] || die "RANGER_PASSWORD_FILE not readable: $RANGER_PASSWORD_FILE"
  RANGER_PASSWORD="$(head -n 1 "$RANGER_PASSWORD_FILE" | tr -d '\r')"
fi

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""

if [[ -z "${RANGER_BASE_URL:-}" ]]; then
  if [[ -n "${CLUSTER_NAME:-}" ]]; then
    :
  elif [[ -f "$AMBARI_CONFIG_FILE" ]]; then
    load_ambari_env_file "$AMBARI_CONFIG_FILE" || die "failed to read $AMBARI_CONFIG_FILE"
  elif [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]]; then
    :
  else
    die "Missing Ambari context. Create ${AMBARI_CONFIG_FILE}, or set CLUSTER_NAME / AMBARI_USER+AMBARI_PASSWORD, or set RANGER_BASE_URL."
  fi
fi

AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://127.0.0.1:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"

if [[ -z "${RANGER_BASE_URL:-}" ]]; then
  [[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]] || die "Ambari credentials required when RANGER_BASE_URL is unset."
fi

[[ -n "${RANGER_PASSWORD:-}" ]] || die "Ranger password missing. Export RANGER_PASSWORD, set RANGER_PASSWORD_FILE, or edit ${RANGER_ENV_FILE}."

cluster=""
if [[ -z "${RANGER_BASE_URL:-}" ]]; then
  if [[ -z "${CLUSTER_NAME:-}" ]]; then
    clusters_url="${AMBARI_BASE_URL%/}/api/v1/clusters/"
    json="$(ambari_curl "$clusters_url")" || die "failed to GET $clusters_url"
    cluster="$(
      printf '%s\n' "$json" | python3 -c "
import json, sys
data = json.load(sys.stdin)
items = data.get('items') or []
if not items:
    sys.exit('no clusters in Ambari response')
name = (items[0].get('Clusters') or {}).get('cluster_name')
if not name:
    sys.exit('could not parse cluster_name from Ambari response')
print(name)
"
    )" || die "could not parse cluster name from Ambari JSON"
  else
    cluster="$CLUSTER_NAME"
  fi
fi

discover_ranger_base_url() {
  CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}" \
    AMBARI_USER="$AMBARI_USER" AMBARI_PASSWORD="$AMBARI_PASSWORD" python3 - "$AMBARI_BASE_URL" "$cluster" <<'PY'
import json, os, shlex, subprocess, sys, urllib.parse
from urllib.parse import urlparse

def curl_extra():
    raw = os.environ.get("CURL_EXTRA_OPTS", "").strip()
    return shlex.split(raw) if raw else []

def curl_json(url):
    user, pw = os.environ["AMBARI_USER"], os.environ["AMBARI_PASSWORD"]
    cmd = ["curl", "-sS", "-f", "-u", f"{user}:{pw}", "-H", "X-Requested-By: ambari"] + curl_extra() + [url]
    r = subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
    )
    if r.returncode != 0:
        sys.stderr.write(r.stderr or r.stdout or "curl failed\n")
        sys.exit(r.returncode)
    return json.loads(r.stdout)

ambari, cluster = sys.argv[1].rstrip("/"), sys.argv[2]
qc = urllib.parse.quote(cluster, safe="")
url = f"{ambari}/api/v1/clusters/{qc}?fields=Clusters/desired_configs"
j = curl_json(url)
dc = (j.get("Clusters") or {}).get("desired_configs") or {}
ra = dc.get("ranger-admin-site") or {}
tag = ra.get("tag")
props = {}
if tag:
    u2 = f"{ambari}/api/v1/clusters/{qc}/configurations?type=ranger-admin-site&tag={urllib.parse.quote(tag)}"
    j2 = curl_json(u2)
    items = j2.get("items") or []
    if items:
        props = items[0].get("properties") or {}
pm = (props.get("policymgr_external_url") or "").strip()
if pm:
    raw = pm if "://" in pm else "http://" + pm
    p = urlparse(raw)
    if p.scheme and p.netloc:
        print(f"{p.scheme}://{p.netloc}".rstrip("/"))
        sys.exit(0)
    hostport = pm.split("/")[0]
    if hostport and ":" in hostport:
        print(f"http://{hostport}".rstrip("/"))
        sys.exit(0)

port = (props.get("ranger.service.http.port") or "6080").strip()
hc_url = (
    f"{ambari}/api/v1/clusters/{qc}/host_components"
    f"?HostRoles/component_name=RANGER_ADMIN&fields=HostRoles/host_name,HostRoles/public_host_name"
)
hj = curl_json(hc_url)
items = hj.get("items") or []
host = None
for it in items:
    hr = it.get("HostRoles") or {}
    host = hr.get("public_host_name") or hr.get("host_name")
    if host:
        break
if not host:
    sys.stderr.write("No RANGER_ADMIN host in Ambari; set RANGER_BASE_URL\n")
    sys.exit(2)
print(f"http://{host}:{port}".rstrip("/"))
PY
}

if [[ -n "${RANGER_BASE_URL:-}" ]]; then
  ranger_base="${RANGER_BASE_URL%/}"
else
  ranger_base="$(discover_ranger_base_url)" || die "Could not discover Ranger from Ambari; set RANGER_BASE_URL."
fi

echo "---- Ranger plugin connection smoke ----"
echo "    Ranger base URL: ${ranger_base}"
echo "    Ranger user: ${RANGER_USER}"
echo "    Include disabled: ${RANGER_INCLUDE_DISABLED}"
[[ -n "${RANGER_SERVICE_TYPES:-}" ]] && echo "    Include types: ${RANGER_SERVICE_TYPES}"
[[ -n "${RANGER_SKIP_TYPES:-}" ]] && echo "    Skip types: ${RANGER_SKIP_TYPES}"
[[ -n "${RANGER_SKIP_SERVICES:-}" ]] && echo "    Skip services: ${RANGER_SKIP_SERVICES}"

export RANGER_USER RANGER_PASSWORD
export RANGER_INCLUDE_DISABLED RANGER_TIMEOUT_SECONDS RANGER_FAIL_ON_ERROR
export RANGER_SERVICE_TYPES="${RANGER_SERVICE_TYPES:-}"
export RANGER_SKIP_TYPES="${RANGER_SKIP_TYPES:-}"
export RANGER_SKIP_SERVICES="${RANGER_SKIP_SERVICES:-}"
export CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}"

python3 - "$ranger_base" <<'PY'
import json, os, shlex, subprocess, sys, time, base64
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

ranger_base = sys.argv[1].rstrip("/")
user = os.environ["RANGER_USER"]
password = os.environ["RANGER_PASSWORD"]
timeout = int(os.environ.get("RANGER_TIMEOUT_SECONDS", "180"))
include_disabled = os.environ.get("RANGER_INCLUDE_DISABLED", "0") == "1"
fail_on_error = os.environ.get("RANGER_FAIL_ON_ERROR", "1") == "1"
curl_extra = shlex.split(os.environ.get("CURL_EXTRA_OPTS", "").strip() or "")

def split_csv(name):
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return set()
    out = set()
    for part in raw.replace(" ", ",").split(","):
        p = part.strip()
        if p:
            out.add(p)
    return out

include_types = {t.lower() for t in split_csv("RANGER_SERVICE_TYPES")}
skip_types = {t.lower() for t in split_csv("RANGER_SKIP_TYPES")}
skip_services = split_csv("RANGER_SKIP_SERVICES")

auth = "Basic " + base64.b64encode(("%s:%s" % (user, password)).encode()).decode()

def http_json(method, path, data=None):
    headers = {
        "Accept": "application/json",
        "Authorization": auth,
    }
    body = None
    if data is not None:
        body = json.dumps(data).encode()
        headers["Content-Type"] = "application/json"
    url = ranger_base + path
    # Prefer urllib; fall back path unused unless needed for -k via curl
    if curl_extra:
        cmd = ["curl", "-sS", "-w", "\n%{http_code}", "-X", method, "-u", "%s:%s" % (user, password)]
        cmd += curl_extra
        cmd += ["-H", "Accept: application/json"]
        if body is not None:
            cmd += ["-H", "Content-Type: application/json", "--data-binary", body]
        cmd.append(url)
        r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out = r.stdout.decode("utf-8", "replace")
        if r.returncode != 0 and not out.strip():
            raise RuntimeError(r.stderr.decode("utf-8", "replace") or "curl failed")
        lines = out.rsplit("\n", 1)
        raw, code_s = (lines[0], lines[1]) if len(lines) == 2 else (out, "0")
        try:
            code = int(code_s.strip() or "0")
        except ValueError:
            code = 0
            raw = out
        try:
            j = json.loads(raw) if raw.strip() else {}
        except Exception:
            j = {"raw": raw[:800]}
        return code, j

    req = Request(url, data=body, headers=headers, method=method)
    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
            return resp.status, (json.loads(raw) if raw.strip() else {})
    except HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        try:
            j = json.loads(raw) if raw.strip() else {}
        except Exception:
            j = {"raw": raw[:800]}
        return e.code, j
    except URLError as e:
        return None, {"raw": str(e)}

code, svcs = http_json("GET", "/service/public/v2/api/service")
if code != 200 or not isinstance(svcs, list):
    sys.stderr.write("ERROR: failed to list Ranger services HTTP=%s body=%s\n" % (code, svcs))
    sys.exit(2)

print("Found %d Ranger service(s)" % len(svcs))
results = []

for svc in svcs:
    name = svc.get("name") or "?"
    typ = (svc.get("type") or "?").lower()
    enabled = bool(svc.get("isEnabled"))

    if include_types and typ not in include_types:
        results.append((name, typ, "SKIP", "type filter"))
        continue
    if typ in skip_types:
        results.append((name, typ, "SKIP", "skip type"))
        continue
    if name in skip_services:
        results.append((name, typ, "SKIP", "skip service"))
        continue
    if not enabled and not include_disabled:
        results.append((name, typ, "SKIP", "disabled"))
        continue

    print("---- test type=%s name=%s enabled=%s ----" % (typ, name, enabled))
    t0 = time.time()
    http_code, out = http_json("POST", "/service/plugins/services/validateConfig", svc)
    dt = time.time() - t0
    msg = out.get("msgDesc") if isinstance(out, dict) else ""
    if not msg and isinstance(out, dict):
        mlist = out.get("messageList") or []
        if isinstance(mlist, list) and mlist:
            first = mlist[0]
            if isinstance(first, dict):
                msg = (first.get("message") or "").strip()
            else:
                msg = str(first).strip()
    status_code = out.get("statusCode") if isinstance(out, dict) else None
    # statusCode 0 means Ranger accepted the connection (UI Test Connection success),
    # even when msgDesc / messageList are empty (seen for tag services).
    if http_code == 200 and status_code == 0:
        ok = True
        if not msg:
            msg = "ConnectionTest Successful"
    else:
        ok = isinstance(msg, str) and "Successful" in msg and status_code in (0, None)
        if not msg and isinstance(out, dict):
            msg = out.get("raw") or json.dumps(out)[:300]
    state = "PASS" if ok else "FAIL"
    one_line = (msg or "").replace("\n", " ").strip()
    if len(one_line) > 180:
        one_line = one_line[:177] + "..."
    print("    %s HTTP=%s statusCode=%s %.1fs %s" % (state, http_code, status_code, dt, one_line))
    results.append((name, typ, state, one_line))

print("==== SUMMARY ====")
width_name = max([len(r[0]) for r in results] + [4])
width_type = max([len(r[1]) for r in results] + [4])
pass_n = fail_n = skip_n = 0
for name, typ, state, msg in results:
    if state == "PASS":
        pass_n += 1
    elif state == "FAIL":
        fail_n += 1
    else:
        skip_n += 1
    print("%-6s  %-*s  %-*s  %s" % (state, width_type, typ, width_name, name, msg))

print("---- totals: PASS=%d FAIL=%d SKIP=%d ----" % (pass_n, fail_n, skip_n))
if fail_n and fail_on_error:
    print("ERROR: one or more Ranger plugin connection tests FAILED", file=sys.stderr)
    sys.exit(1)
if pass_n == 0 and fail_n == 0:
    print("ERROR: no services were tested (all skipped?)", file=sys.stderr)
    sys.exit(2)
print("OK: Ranger plugin connection smoke finished.")
PY
