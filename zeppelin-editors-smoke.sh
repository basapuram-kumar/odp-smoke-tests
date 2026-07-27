#!/usr/bin/env bash
#
# Smoke: Apache Zeppelin notebook editors / interpreters.
# Logs into Zeppelin (Shiro), discovers interpreter settings from REST, creates a
# temporary note, runs one sample paragraph per editor (e.g. %md.md, %livy.spark,
# %sh.sh, %jdbc.sql, %angular.angular), then deletes the note.
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   ZEPPELIN_ENV_FILE / ZEPPELIN_CONFIG_FILE  default <script-dir>/configs/zeppelin.env
#   ZEPPELIN_BASE_URL        e.g. http://zeppelin-host:9995 (skip Ambari when set)
#   ZEPPELIN_USER            default admin
#   ZEPPELIN_PASSWORD        required unless set in zeppelin.env
#   ZEPPELIN_PASSWORD_FILE   if password unset, read first line
#   ZEPPELIN_SKIP_INTERPS    comma list of setting.interp to skip (default: livy.shared)
#   ZEPPELIN_ONLY_INTERPS    comma list to include only those setting.interp names
#   ZEPPELIN_SKIP_GROUPS     comma list of interpreter setting names to skip
#   ZEPPELIN_ONLY_GROUPS     comma list of interpreter setting names to include
#   ZEPPELIN_TIMEOUT_SECONDS per-paragraph run timeout (default 300)
#   ZEPPELIN_FAIL_ON_ERROR   default 1 - exit 1 if any tested editor FAILs
#   ZEPPELIN_KEEP_NOTE       if 1, do not delete the smoke note
#   CURL_EXTRA_OPTS          e.g. -k for TLS
#
# Usage:
#   cp configs/zeppelin.env.example configs/zeppelin.env
#   ./zeppelin-editors-smoke.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
ZEPPELIN_ENV_FILE="${ZEPPELIN_ENV_FILE:-${ZEPPELIN_CONFIG_FILE:-${SCRIPT_DIR}/configs/zeppelin.env}}"
ZEPPELIN_TIMEOUT_SECONDS="${ZEPPELIN_TIMEOUT_SECONDS:-300}"
ZEPPELIN_FAIL_ON_ERROR="${ZEPPELIN_FAIL_ON_ERROR:-1}"
ZEPPELIN_KEEP_NOTE="${ZEPPELIN_KEEP_NOTE:-0}"
ZEPPELIN_SKIP_INTERPS="${ZEPPELIN_SKIP_INTERPS:-livy.shared}"

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

_cfg_ZEPPELIN_USER=""
_cfg_ZEPPELIN_PASSWORD=""
_cfg_ZEPPELIN_BASE_URL=""

load_zeppelin_env_file() {
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
      ZEPPELIN_USER) _cfg_ZEPPELIN_USER="$val" ;;
      ZEPPELIN_PASSWORD) _cfg_ZEPPELIN_PASSWORD="$val" ;;
      ZEPPELIN_BASE_URL) _cfg_ZEPPELIN_BASE_URL="$val" ;;
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

load_zeppelin_env_file "$ZEPPELIN_ENV_FILE" || die "failed to read $ZEPPELIN_ENV_FILE"
ZEPPELIN_USER="${ZEPPELIN_USER:-${_cfg_ZEPPELIN_USER:-admin}}"
ZEPPELIN_PASSWORD="${ZEPPELIN_PASSWORD:-${_cfg_ZEPPELIN_PASSWORD:-}}"
ZEPPELIN_BASE_URL="${ZEPPELIN_BASE_URL:-${_cfg_ZEPPELIN_BASE_URL:-}}"

if [[ -z "${ZEPPELIN_PASSWORD:-}" && -n "${ZEPPELIN_PASSWORD_FILE:-}" ]]; then
  [[ -r "$ZEPPELIN_PASSWORD_FILE" ]] || die "ZEPPELIN_PASSWORD_FILE not readable: $ZEPPELIN_PASSWORD_FILE"
  ZEPPELIN_PASSWORD="$(head -n 1 "$ZEPPELIN_PASSWORD_FILE" | tr -d '\r')"
fi

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""

if [[ -z "${ZEPPELIN_BASE_URL:-}" ]]; then
  if [[ -n "${CLUSTER_NAME:-}" ]]; then
    :
  elif [[ -f "$AMBARI_CONFIG_FILE" ]]; then
    load_ambari_env_file "$AMBARI_CONFIG_FILE" || die "failed to read $AMBARI_CONFIG_FILE"
  elif [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]]; then
    :
  else
    die "Missing Ambari context. Create ${AMBARI_CONFIG_FILE}, or set CLUSTER_NAME / AMBARI_USER+AMBARI_PASSWORD, or set ZEPPELIN_BASE_URL."
  fi
fi

AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://127.0.0.1:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"

if [[ -z "${ZEPPELIN_BASE_URL:-}" ]]; then
  [[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]] || die "Ambari credentials required when ZEPPELIN_BASE_URL is unset."
fi

[[ -n "${ZEPPELIN_PASSWORD:-}" ]] || die "Zeppelin password missing. Export ZEPPELIN_PASSWORD, set ZEPPELIN_PASSWORD_FILE, or create ${ZEPPELIN_ENV_FILE} (copy from ${SCRIPT_DIR}/configs/zeppelin.env.example)."

cluster=""
if [[ -z "${ZEPPELIN_BASE_URL:-}" ]]; then
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

discover_zeppelin_base_url() {
  CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}" \
    AMBARI_USER="$AMBARI_USER" AMBARI_PASSWORD="$AMBARI_PASSWORD" python3 - "$AMBARI_BASE_URL" "$cluster" <<'PY'
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
zs = dc.get("zeppelin-site") or {}
tag = zs.get("tag")
props = {}
if tag:
    j2 = curl_json(
        f"{ambari}/api/v1/clusters/{qc}/configurations?type=zeppelin-site&tag={urllib.parse.quote(tag)}"
    )
    items = j2.get("items") or []
    if items:
        props = items[0].get("properties") or {}
port = (props.get("zeppelin.server.port") or "9995").strip()
ssl = (props.get("zeppelin.ssl") or "false").strip().lower() in ("true", "1", "yes")
scheme = "https" if ssl else "http"
if ssl and props.get("zeppelin.server.ssl.port"):
    port = props.get("zeppelin.server.ssl.port").strip()

hc = curl_json(
    f"{ambari}/api/v1/clusters/{qc}/host_components"
    f"?HostRoles/component_name=ZEPPELIN_MASTER"
    f"&fields=HostRoles/host_name,HostRoles/public_host_name,HostRoles/state"
)
host = None
for it in hc.get("items") or []:
    hr = it.get("HostRoles") or {}
    if (hr.get("state") or "").upper() not in ("STARTED", "INSTALLED", ""):
        # still accept INSTALLED/STARTED; prefer STARTED
        pass
    host = hr.get("public_host_name") or hr.get("host_name")
    if (hr.get("state") or "").upper() == "STARTED" and host:
        break
if not host:
    sys.stderr.write("No ZEPPELIN_MASTER host in Ambari; set ZEPPELIN_BASE_URL\n")
    sys.exit(2)
print(f"{scheme}://{host}:{port}".rstrip("/"))
PY
}

if [[ -n "${ZEPPELIN_BASE_URL:-}" ]]; then
  zeppelin_base="${ZEPPELIN_BASE_URL%/}"
else
  zeppelin_base="$(discover_zeppelin_base_url)" || die "Could not discover Zeppelin from Ambari; set ZEPPELIN_BASE_URL."
fi

echo "---- Zeppelin editors smoke ----"
echo "    Zeppelin URL: ${zeppelin_base}"
echo "    User: ${ZEPPELIN_USER}"
[[ -n "${ZEPPELIN_ONLY_GROUPS:-}" ]] && echo "    Only groups: ${ZEPPELIN_ONLY_GROUPS}"
[[ -n "${ZEPPELIN_SKIP_GROUPS:-}" ]] && echo "    Skip groups: ${ZEPPELIN_SKIP_GROUPS}"
[[ -n "${ZEPPELIN_ONLY_INTERPS:-}" ]] && echo "    Only interps: ${ZEPPELIN_ONLY_INTERPS}"
[[ -n "${ZEPPELIN_SKIP_INTERPS:-}" ]] && echo "    Skip interps: ${ZEPPELIN_SKIP_INTERPS}"

export ZEPPELIN_USER ZEPPELIN_PASSWORD
export ZEPPELIN_TIMEOUT_SECONDS ZEPPELIN_FAIL_ON_ERROR ZEPPELIN_KEEP_NOTE
export ZEPPELIN_SKIP_INTERPS ZEPPELIN_ONLY_INTERPS="${ZEPPELIN_ONLY_INTERPS:-}"
export ZEPPELIN_SKIP_GROUPS="${ZEPPELIN_SKIP_GROUPS:-}"
export ZEPPELIN_ONLY_GROUPS="${ZEPPELIN_ONLY_GROUPS:-}"
export CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}"

python3 - "$zeppelin_base" <<'PY'
import json, os, sys, time, urllib.parse, http.cookiejar
from urllib.request import Request, build_opener, HTTPCookieProcessor
from urllib.error import HTTPError, URLError

base = sys.argv[1].rstrip("/")
user = os.environ["ZEPPELIN_USER"]
password = os.environ["ZEPPELIN_PASSWORD"]
timeout = int(os.environ.get("ZEPPELIN_TIMEOUT_SECONDS", "300"))
fail_on_error = os.environ.get("ZEPPELIN_FAIL_ON_ERROR", "1") == "1"
keep_note = os.environ.get("ZEPPELIN_KEEP_NOTE", "0") == "1"

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

only_groups = split_csv("ZEPPELIN_ONLY_GROUPS")
skip_groups = split_csv("ZEPPELIN_SKIP_GROUPS")
only_interps = split_csv("ZEPPELIN_ONLY_INTERPS")
skip_interps = split_csv("ZEPPELIN_SKIP_INTERPS")

cj = http.cookiejar.CookieJar()
opener = build_opener(HTTPCookieProcessor(cj))

def http_json(method, path, data=None, form=False, timeout_s=None):
    headers = {"Accept": "application/json"}
    body = None
    if data is not None:
        if form:
            body = urllib.parse.urlencode(data).encode()
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        else:
            body = json.dumps(data).encode()
            headers["Content-Type"] = "application/json"
    req = Request(base + path, data=body, headers=headers, method=method)
    try:
        with opener.open(req, timeout=timeout_s or timeout) as resp:
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

def one_line(s, n=160):
    s = (s or "").replace("\n", " ").strip()
    if len(s) > n:
        return s[: n - 3] + "..."
    return s

code, login = http_json("POST", "/api/login", {"userName": user, "password": password}, form=True, timeout_s=60)
if code != 200 or (login.get("status") or "").upper() != "OK":
    sys.stderr.write("ERROR: Zeppelin login failed HTTP=%s body=%s\n" % (code, login))
    sys.exit(2)
print("Logged in as %s" % ((login.get("body") or {}).get("principal") or user))

code, ver = http_json("GET", "/api/version", timeout_s=30)
print("Zeppelin version: %s" % ((ver.get("body") or {}).get("version") or "?"))

code, interps = http_json("GET", "/api/interpreter", timeout_s=60)
if code != 200 or (interps.get("status") or "").upper() != "OK":
    sys.stderr.write("ERROR: failed to list interpreters HTTP=%s body=%s\n" % (code, interps))
    sys.exit(2)
settings = interps.get("body") or {}
if not isinstance(settings, dict) or not settings:
    sys.stderr.write("ERROR: no interpreter settings returned\n")
    sys.exit(2)

print("Discovered interpreter groups: %s" % ", ".join(sorted(settings.keys())))

def sample_paragraph(setting_name, interp_name):
    """Return (text, skip_reason). skip_reason set means do not run."""
    key = ("%s.%s" % (setting_name, interp_name)).lower()
    magic = "%%%s.%s" % (setting_name, interp_name)
    if interp_name.lower() in ("shared", "dep"):
        return None, "infrastructure interpreter"
    if "terminal" in key:
        # Terminal opens a UI socket widget; still a supported editor - light smoke.
        return magic + "\necho ok-terminal", None
    if setting_name == "md" or interp_name == "md":
        return magic + "\n# Zeppelin smoke\nOK", None
    if "angular" in key or interp_name in ("angular", "ng"):
        return magic + "\n<div>ok-angular</div>", None
    if interp_name in ("sql",) or key.endswith(".sql"):
        return magic + "\nSELECT 1 AS n", None
    if "pyspark" in key:
        return magic + '\nprint("ok-pyspark")', None
    if interp_name in ("sparkr", "r") or key.endswith(".sparkr"):
        return magic + '\nprint("ok-sparkr")', None
    if "spark" in key:
        return magic + '\nprintln("ok-spark")', None
    if setting_name == "sh" or interp_name == "sh":
        return magic + "\necho ok-shell", None
    if "jdbc" in key:
        return magic + "\nSELECT 1", None
    # Generic fallback
    return magic + '\nprintln("ok")', None

# Build editor list
editors = []
for sid, setting in sorted(settings.items(), key=lambda kv: kv[0]):
    name = setting.get("name") or sid
    if only_groups and name not in only_groups and sid not in only_groups:
        continue
    if name in skip_groups or sid in skip_groups:
        continue
    igs = setting.get("interpreterGroup") or [{"name": name}]
    for ig in igs:
        iname = ig.get("name") or name
        full = "%s.%s" % (name, iname)
        if only_interps and full not in only_interps and iname not in only_interps:
            continue
        if full in skip_interps or iname in skip_interps:
            editors.append((name, iname, full, None, "skip list"))
            continue
        text, reason = sample_paragraph(name, iname)
        editors.append((name, iname, full, text, reason))

note_name = "odp-smoke-zeppelin-editors-%d" % int(time.time())
code, note = http_json("POST", "/api/notebook", {"name": note_name}, timeout_s=60)
if code != 200 or (note.get("status") or "").upper() != "OK":
    sys.stderr.write("ERROR: create note failed HTTP=%s body=%s\n" % (code, note))
    sys.exit(2)
note_id = note.get("body")
print("Created note: %s (%s)" % (note_name, note_id))

results = []
try:
    for name, iname, full, text, skip_reason in editors:
        if skip_reason:
            print("---- SKIP %s (%s) ----" % (full, skip_reason))
            results.append((full, "SKIP", skip_reason))
            continue
        print("---- test %%%s ----" % full)
        code, p = http_json(
            "POST",
            "/api/notebook/%s/paragraph" % note_id,
            {"title": full, "text": text},
            timeout_s=60,
        )
        pid = p.get("body") if isinstance(p, dict) else None
        if not pid:
            msg = one_line(str(p))
            print("    FAIL add paragraph: %s" % msg)
            results.append((full, "FAIL", "add paragraph: %s" % msg))
            continue
        t0 = time.time()
        rc, rout = http_json(
            "POST",
            "/api/notebook/run/%s/%s" % (note_id, pid),
            timeout_s=timeout,
        )
        gc, gout = http_json(
            "GET",
            "/api/notebook/%s/paragraph/%s" % (note_id, pid),
            timeout_s=60,
        )
        dt = time.time() - t0
        pbody = (gout.get("body") if isinstance(gout, dict) else {}) or {}
        status = (pbody.get("status") or "").upper()
        out = ""
        for res in (pbody.get("results") or {}).get("msg", [])[:1]:
            out = res.get("data") or ""
        out_l = one_line(out)
        ok = status == "FINISHED"
        state = "PASS" if ok else "FAIL"
        if not out_l and isinstance(rout, dict):
            out_l = one_line(rout.get("message") or json.dumps(rout)[:200])
        print("    %s status=%s %.1fs %s" % (state, status or "?", dt, out_l))
        results.append((full, state, out_l or status))
finally:
    if keep_note:
        print("Keeping note %s (ZEPPELIN_KEEP_NOTE=1)" % note_id)
    else:
        dc, dout = http_json("DELETE", "/api/notebook/%s" % note_id, timeout_s=60)
        if dc in (200, 204) or (isinstance(dout, dict) and (dout.get("status") or "").upper() == "OK"):
            print("Deleted note %s" % note_id)
        else:
            print("WARN: could not delete note %s HTTP=%s %s" % (note_id, dc, dout))

print("==== SUMMARY ====")
w = max([len(r[0]) for r in results] + [6])
pass_n = fail_n = skip_n = 0
for full, state, msg in results:
    if state == "PASS":
        pass_n += 1
    elif state == "FAIL":
        fail_n += 1
    else:
        skip_n += 1
    print("%-6s  %-*s  %s" % (state, w, full, msg))
print("---- totals: PASS=%d FAIL=%d SKIP=%d ----" % (pass_n, fail_n, skip_n))

if fail_n and fail_on_error:
    print("ERROR: one or more Zeppelin editor smokes FAILED", file=sys.stderr)
    sys.exit(1)
if pass_n == 0 and fail_n == 0:
    print("ERROR: no editors were tested (all skipped?)", file=sys.stderr)
    sys.exit(2)
print("OK: Zeppelin editors smoke finished.")
PY
