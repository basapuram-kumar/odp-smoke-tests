#!/usr/bin/env bash
#
# Merge extra UNIX users into Ranger YARN policy "all - queue" (default) without hardcoding
# policy id, guid, or service name: GET current policy from Ranger, merge users, PUT back.
#
# Ranger base URL:
#   Set RANGER_BASE_URL (e.g. http://ranger-admin-host:6080) to skip Ambari discovery.
#   Otherwise reads Ambari desired_configs for ranger-admin-site (policymgr_external_url), or
#   falls back to the first RANGER_ADMIN host + ranger.service.http.port (default 6080).
#
# Ambari: configs/ambari.env (same pattern as other sample-jobs).
# Ranger: optional configs/ranger.env (copy from ranger.env.example) for RANGER_PASSWORD, etc.
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   RANGER_ENV_FILE           default <script-dir>/configs/ranger.env (loaded if present)
#   RANGER_CONFIG_FILE        alias of RANGER_ENV_FILE (backward compatibility)
#   RANGER_PASSWORD_FILE      if RANGER_PASSWORD unset, read first line from this file
#   RANGER_BASE_URL           skip Ambari when set (no trailing slash)
#   RANGER_USER               default admin
#   RANGER_PASSWORD           required unless set via ranger.env or RANGER_PASSWORD_FILE
#   RANGER_YARN_SERVICE_NAME  e.g. ub20j11p3_yarn; if unset, first Ranger service with type yarn
#   RANGER_POLICY_NAME        default: all - queue
#   RANGER_POLICY_ID          if set, skip policy search
#   RANGER_ADD_USERS          comma- or space-separated users to add (deduped with existing)
#   RANGER_DRY_RUN            if 1, print merged JSON only, no PUT
#   CURL_EXTRA_OPTS           e.g. -k for self-signed TLS
#
# Usage:
#   cp configs/ranger.env.example configs/ranger.env && edit RANGER_PASSWORD
#   RANGER_ADD_USERS=registry,flink ./ranger-yarn-all-queue-users-add.sh
#   RANGER_PASSWORD=secret RANGER_ADD_USERS=registry,flink ./ranger-yarn-all-queue-users-add.sh
#   ./ranger-yarn-all-queue-users-add.sh druid nifi   # same as adding to RANGER_ADD_USERS
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
# Support both names; RANGER_ENV_FILE takes precedence when both are set.
RANGER_ENV_FILE="${RANGER_ENV_FILE:-${RANGER_CONFIG_FILE:-${SCRIPT_DIR}/configs/ranger.env}}"
RANGER_POLICY_NAME="${RANGER_POLICY_NAME:-all - queue}"
RANGER_DRY_RUN="${RANGER_DRY_RUN:-0}"

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
_cfg_RANGER_YARN_SERVICE_NAME=""
_cfg_RANGER_ADD_USERS=""

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
      RANGER_YARN_SERVICE_NAME) _cfg_RANGER_YARN_SERVICE_NAME="$val" ;;
      RANGER_ADD_USERS) _cfg_RANGER_ADD_USERS="$val" ;;
    esac
  done <"$f"
  return 0
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

ambari_curl() {
  curl -sS -f ${CURL_EXTRA_OPTS:-} -u "${AMBARI_USER}:${AMBARI_PASSWORD}" -H "X-Requested-By: ambari" "$@"
}

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""

need_cmd curl
need_cmd python3

load_ranger_env_file "$RANGER_ENV_FILE" || die "failed to read $RANGER_ENV_FILE"
RANGER_USER="${RANGER_USER:-${_cfg_RANGER_USER:-admin}}"
RANGER_PASSWORD="${RANGER_PASSWORD:-${_cfg_RANGER_PASSWORD:-}}"
RANGER_BASE_URL="${RANGER_BASE_URL:-${_cfg_RANGER_BASE_URL:-}}"
RANGER_YARN_SERVICE_NAME="${RANGER_YARN_SERVICE_NAME:-${_cfg_RANGER_YARN_SERVICE_NAME:-}}"
RANGER_ADD_USERS="${RANGER_ADD_USERS:-${_cfg_RANGER_ADD_USERS:-}}"

if [[ -z "${RANGER_PASSWORD:-}" && -n "${RANGER_PASSWORD_FILE:-}" ]]; then
  [[ -r "$RANGER_PASSWORD_FILE" ]] || die "RANGER_PASSWORD_FILE not readable: $RANGER_PASSWORD_FILE"
  RANGER_PASSWORD="$(head -n 1 "$RANGER_PASSWORD_FILE" | tr -d '\r')"
fi

if [[ -z "${RANGER_BASE_URL:-}" ]]; then
  if [[ -n "${CLUSTER_NAME:-}" ]]; then
    :
  elif [[ -f "$AMBARI_CONFIG_FILE" ]]; then
    load_ambari_env_file "$AMBARI_CONFIG_FILE" || die "failed to read $AMBARI_CONFIG_FILE"
  elif [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]]; then
    :
  else
    die "Missing Ambari context. Create ${AMBARI_CONFIG_FILE}, or set CLUSTER_NAME, or set AMBARI_USER and AMBARI_PASSWORD (or set RANGER_BASE_URL to skip Ambari)."
  fi
fi

AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://127.0.0.1:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"

if [[ -z "${RANGER_BASE_URL:-}" ]]; then
  [[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]] || die "Ambari credentials required when RANGER_BASE_URL is unset."
fi

[[ -n "${RANGER_PASSWORD:-}" ]] || die "Ranger password missing. Export RANGER_PASSWORD, set RANGER_PASSWORD_FILE, or create ${RANGER_ENV_FILE} (copy from ${SCRIPT_DIR}/configs/ranger.env.example). REST user is ${RANGER_USER}."

_merge="${RANGER_ADD_USERS:-}"
for a in "$@"; do
  [[ -n "$a" ]] && _merge+=" $a"
done
[[ -n "${_merge// }" ]] || die "No users to add. Set RANGER_ADD_USERS and/or pass usernames as arguments."
export _RANGER_USERS_MERGE_INPUT="$_merge"

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
hc_url = f"{ambari}/api/v1/clusters/{qc}/host_components?HostRoles/component_name=RANGER_ADMIN&fields=HostRoles/host_name,HostRoles/public_host_name"
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

echo "Ranger base URL: ${ranger_base}"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
merged_json="${work}/policy-merged.json"

export AMBARI_BASE_URL cluster
policy_id="$(
  CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}" \
  RANGER_USER="$RANGER_USER" RANGER_PASSWORD="$RANGER_PASSWORD" \
  RANGER_POLICY_NAME="${RANGER_POLICY_NAME}" \
  RANGER_YARN_SERVICE_NAME="${RANGER_YARN_SERVICE_NAME:-}" \
  RANGER_POLICY_ID="${RANGER_POLICY_ID:-}" \
  _RANGER_USERS_MERGE_INPUT="$_RANGER_USERS_MERGE_INPUT" \
  python3 - "$ranger_base" "$merged_json" <<'PY'
import json, os, re, shlex, subprocess, sys, urllib.parse

def curl_extra():
    raw = os.environ.get("CURL_EXTRA_OPTS", "").strip()
    return shlex.split(raw) if raw else []

def shcurl(argv):
    r = subprocess.run(
        argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
    )
    if r.returncode != 0:
        sys.stderr.write(r.stderr or r.stdout or "curl failed\n")
        sys.exit(r.returncode)
    return r.stdout

def ranger_get(ranger_base, path):
    ruser, rpw = os.environ["RANGER_USER"], os.environ["RANGER_PASSWORD"]
    url = ranger_base.rstrip("/") + path
    out = shcurl(
        ["curl", "-sS", "-f", "-u", f"{ruser}:{rpw}"] + curl_extra() + [url]
    )
    return json.loads(out) if out.strip() else {}

def ranger_get_maybe(ranger_base, path):
    ruser, rpw = os.environ["RANGER_USER"], os.environ["RANGER_PASSWORD"]
    url = ranger_base.rstrip("/") + path
    r = subprocess.run(
        ["curl", "-sS", "-f", "-u", f"{ruser}:{rpw}"] + curl_extra() + [url],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    if r.returncode != 0:
        return None
    if not (r.stdout or "").strip():
        return None
    return json.loads(r.stdout)

def parse_users(s):
    return [p for p in re.split(r"[\s,]+", s.strip()) if p]

ranger_base, out_path = sys.argv[1], sys.argv[2]
policy_name = os.environ.get("RANGER_POLICY_NAME", "all - queue")
yarn_svc = os.environ.get("RANGER_YARN_SERVICE_NAME", "").strip()
policy_id_env = os.environ.get("RANGER_POLICY_ID", "").strip()
add_users = parse_users(os.environ.get("_RANGER_USERS_MERGE_INPUT", ""))
ruser, rpw = os.environ["RANGER_USER"], os.environ["RANGER_PASSWORD"]
api = "/service/public/v2/api"

def services_list(rb):
    j = ranger_get(rb, f"{api}/service")
    if isinstance(j, list):
        return j
    svcs = j.get("services") or j.get("data") or []
    if isinstance(svcs, dict):
        svcs = svcs.get("services") or []
    if not isinstance(svcs, list):
        return []
    return svcs

def resolve_yarn_service(rb):
    svcs = services_list(rb)
    if yarn_svc:
        for s in svcs:
            if (s or {}).get("name") == yarn_svc:
                return yarn_svc
        sys.stderr.write(f"No Ranger service named {yarn_svc!r}\n")
        sys.exit(3)
    for s in svcs:
        if str((s or {}).get("type", "")).lower() == "yarn":
            return (s or {})["name"]
    sys.stderr.write("No Ranger service with type yarn; set RANGER_YARN_SERVICE_NAME\n")
    sys.exit(3)

def policies_from_page(j):
    if isinstance(j, list):
        return j
    if not isinstance(j, dict):
        return []
    for key in ("policies", "vXPolicies", "policy", "data"):
        v = j.get(key)
        if isinstance(v, list):
            return v
        if isinstance(v, dict):
            inner = v.get("policies") or v.get("vXPolicies")
            if isinstance(inner, list):
                return inner
    return []


def find_policy_id(rb, service_name):
    if policy_id_env:
        return int(policy_id_env)
    page, size = 0, 200
    while page <= 50:
        q1 = urllib.parse.urlencode(
            {"serviceName": service_name, "page": page, "size": size}
        )
        q2 = urllib.parse.urlencode({"page": page, "size": size})
        j = ranger_get_maybe(rb, f"{api}/policy?{q1}")
        if j is None:
            j = ranger_get_maybe(rb, f"{api}/policy?{q2}")
        if j is None:
            j = ranger_get(rb, f"{api}/policy?{q2}")
        pols = policies_from_page(j)
        for p in pols:
            if (p or {}).get("name") == policy_name and (p or {}).get("service") == service_name:
                return int(p["id"])
        if not pols:
            break
        if len(pols) < size:
            break
        page += 1
    sys.stderr.write(
        f"Policy {policy_name!r} for service {service_name!r} not found; set RANGER_POLICY_ID\n"
    )
    sys.exit(4)

def merge_item_index(policy):
    items = policy.get("policyItems") or []
    for i, it in enumerate(items):
        acc = [a.get("type") for a in (it.get("accesses") or [])]
        if "admin-queue" in acc:
            return i
    return 0

yarn_name = resolve_yarn_service(ranger_base)
pid = find_policy_id(ranger_base, yarn_name)
pol = ranger_get(ranger_base, f"{api}/policy/{pid}")
idx = merge_item_index(pol)
if "policyItems" not in pol or idx >= len(pol["policyItems"]):
    sys.stderr.write("Unexpected policy JSON (missing policyItems)\n")
    sys.exit(5)
users = list(pol["policyItems"][idx].get("users") or [])
before = set(users)
for u in add_users:
    if u not in users:
        users.append(u)
users.sort()
pol["policyItems"][idx]["users"] = users
added = sorted(set(users) - before)
sys.stderr.write(f"Ranger: service={yarn_name!r} policy id={pid} name={policy_name!r}\n")
sys.stderr.write(f"Users added ({len(added)}): {added}\n")
sys.stderr.write(f"Merged user count: {len(users)}\n")
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(pol, f, separators=(",", ":"))
print(pid)
PY
)"

if [[ "$RANGER_DRY_RUN" == "1" ]]; then
  echo "---- RANGER_DRY_RUN=1 merged policy JSON ----"
  cat "$merged_json"
  echo ""
  echo "OK: dry-run only (no PUT). Policy id would be ${policy_id}."
  exit 0
fi

put_url="${ranger_base}/service/public/v2/api/policy/${policy_id}"
echo "---- PUT ${put_url} ----"
curl -sS -f ${CURL_EXTRA_OPTS:-} -u "${RANGER_USER}:${RANGER_PASSWORD}" -X PUT \
  -H "Content-Type: application/json" --data-binary "@${merged_json}" "$put_url" \
  || die "Ranger PUT failed"

echo "OK: Ranger YARN policy users updated (policy id ${policy_id})."
