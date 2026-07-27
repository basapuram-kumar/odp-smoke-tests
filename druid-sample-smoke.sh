#!/usr/bin/env bash
#
# Smoke: Apache Druid health + Kerberos SQL + optional inline ingest/query.
#
# Steps:
#   1) Ambari discovery of Broker / Coordinator / Overlord / Router (+ Historical / MM)
#   2) kinit as druid-<cluster> (headless keytab)
#   3) GET /status/health on each discovered role
#   4) Authenticated status + leaders; SQL SELECT 1 via Broker (and Router if set)
#   5) Submit inline index_parallel task (druid/odp_druid_smoke_index.json), wait SUCCESS
#   6) SQL COUNT(*) on smoke datasource
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   DRUID_ENV_FILE / DRUID_CONFIG_FILE  default <script-dir>/configs/druid.env
#   DRUID_BROKER_URL, DRUID_COORDINATOR_URL, DRUID_OVERLORD_URL, DRUID_ROUTER_URL
#   DRUID_HISTORICAL_URL, DRUID_MIDDLEMANAGER_URL
#   DRUID_KEYTAB            default /etc/security/keytabs/druid.headless.keytab
#   DRUID_SKIP_KINIT        default 0
#   DRUID_SKIP_INGEST       default 0 - set 1 for health+SQL only
#   DRUID_INDEX_SPEC        default <script-dir>/druid/odp_druid_smoke_index.json
#   DRUID_DATASOURCE        default odp_druid_smoke (must match index spec)
#   DRUID_EXPECTED_COUNT    default 3
#   DRUID_TIMEOUT_SECONDS   default 600 (ingest + query wait)
#   DRUID_POLL_SECONDS      default 5
#   DRUID_KEEP_DATASOURCE   default 1 - leave smoke datasource after run
#   CURL_EXTRA_OPTS         e.g. -k
#
# Usage:
#   sudo ./druid-sample-smoke.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
DRUID_ENV_FILE="${DRUID_ENV_FILE:-${DRUID_CONFIG_FILE:-${SCRIPT_DIR}/configs/druid.env}}"
DRUID_KEYTAB="${DRUID_KEYTAB:-/etc/security/keytabs/druid.headless.keytab}"
DRUID_INDEX_SPEC="${DRUID_INDEX_SPEC:-${SCRIPT_DIR}/druid/odp_druid_smoke_index.json}"

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

load_druid_env_file() {
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
      DRUID_BROKER_URL) [[ "${DRUID_BROKER_URL+set}" == "set" ]] || DRUID_BROKER_URL="$val" ;;
      DRUID_COORDINATOR_URL) [[ "${DRUID_COORDINATOR_URL+set}" == "set" ]] || DRUID_COORDINATOR_URL="$val" ;;
      DRUID_OVERLORD_URL) [[ "${DRUID_OVERLORD_URL+set}" == "set" ]] || DRUID_OVERLORD_URL="$val" ;;
      DRUID_ROUTER_URL) [[ "${DRUID_ROUTER_URL+set}" == "set" ]] || DRUID_ROUTER_URL="$val" ;;
      DRUID_HISTORICAL_URL) [[ "${DRUID_HISTORICAL_URL+set}" == "set" ]] || DRUID_HISTORICAL_URL="$val" ;;
      DRUID_MIDDLEMANAGER_URL) [[ "${DRUID_MIDDLEMANAGER_URL+set}" == "set" ]] || DRUID_MIDDLEMANAGER_URL="$val" ;;
      DRUID_SKIP_KINIT) [[ "${DRUID_SKIP_KINIT+set}" == "set" ]] || DRUID_SKIP_KINIT="$val" ;;
      DRUID_SKIP_INGEST) [[ "${DRUID_SKIP_INGEST+set}" == "set" ]] || DRUID_SKIP_INGEST="$val" ;;
      DRUID_DATASOURCE) [[ "${DRUID_DATASOURCE+set}" == "set" ]] || DRUID_DATASOURCE="$val" ;;
      DRUID_KEEP_DATASOURCE) [[ "${DRUID_KEEP_DATASOURCE+set}" == "set" ]] || DRUID_KEEP_DATASOURCE="$val" ;;
      CLUSTER_NAME) [[ "${CLUSTER_NAME+set}" == "set" ]] || CLUSTER_NAME="$val" ;;
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
need_cmd kinit

load_druid_env_file "$DRUID_ENV_FILE" || die "failed to read $DRUID_ENV_FILE"
DRUID_SKIP_KINIT="${DRUID_SKIP_KINIT:-0}"
DRUID_SKIP_INGEST="${DRUID_SKIP_INGEST:-0}"
DRUID_DATASOURCE="${DRUID_DATASOURCE:-odp_druid_smoke}"
DRUID_EXPECTED_COUNT="${DRUID_EXPECTED_COUNT:-3}"
DRUID_TIMEOUT_SECONDS="${DRUID_TIMEOUT_SECONDS:-600}"
DRUID_POLL_SECONDS="${DRUID_POLL_SECONDS:-5}"
DRUID_KEEP_DATASOURCE="${DRUID_KEEP_DATASOURCE:-1}"

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""

urls_complete=0
if [[ -n "${DRUID_BROKER_URL:-}" && -n "${DRUID_COORDINATOR_URL:-}" && -n "${DRUID_OVERLORD_URL:-}" ]]; then
  urls_complete=1
fi

if [[ "$urls_complete" != "1" || ( -z "${CLUSTER_NAME:-}" && "$DRUID_SKIP_KINIT" != "1" ) ]]; then
  if [[ -f "$AMBARI_CONFIG_FILE" ]]; then
    load_ambari_env_file "$AMBARI_CONFIG_FILE" || die "failed to read $AMBARI_CONFIG_FILE"
  elif [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]]; then
    :
  elif [[ "$urls_complete" == "1" && "$DRUID_SKIP_KINIT" == "1" ]]; then
    :
  else
    die "Missing Ambari context or full DRUID_*_URL set. Create ${AMBARI_CONFIG_FILE} or set Broker/Coordinator/Overlord URLs."
  fi
fi

AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://127.0.0.1:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"

cluster="${CLUSTER_NAME:-}"
if [[ -z "$cluster" && ( "$urls_complete" != "1" || "$DRUID_SKIP_KINIT" != "1" ) ]]; then
  if [[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]]; then
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
  fi
fi

discover_druid_urls() {
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

defaults = {
    "druid-coordinator": 9081,
    "druid-broker": 8082,
    "druid-overlord": 8090,
    "druid-router": 9888,
    "druid-historical": 8083,
    "druid-middlemanager": 8091,
}
ports = dict(defaults)
for ctype, dport in defaults.items():
    meta = dc.get(ctype) or {}
    tag = meta.get("tag")
    if not tag:
        continue
    j2 = curl_json(
        f"{ambari}/api/v1/clusters/{qc}/configurations?type={urllib.parse.quote(ctype)}&tag={urllib.parse.quote(tag)}"
    )
    items = j2.get("items") or []
    if not items:
        continue
    props = items[0].get("properties") or {}
    if props.get("druid.port"):
        ports[ctype] = int(str(props["druid.port"]).strip())

comp_map = {
    "DRUID_COORDINATOR": ("coordinator", "druid-coordinator"),
    "DRUID_BROKER": ("broker", "druid-broker"),
    "DRUID_OVERLORD": ("overlord", "druid-overlord"),
    "DRUID_ROUTER": ("router", "druid-router"),
    "DRUID_HISTORICAL": ("historical", "druid-historical"),
    "DRUID_MIDDLEMANAGER": ("middlemanager", "druid-middlemanager"),
}

out = {}
for comp, (role, ctype) in comp_map.items():
    hc = curl_json(
        f"{ambari}/api/v1/clusters/{qc}/host_components"
        f"?HostRoles/component_name={comp}"
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
    if host:
        out[role] = f"http://{host}:{ports[ctype]}"

need = ("broker", "coordinator", "overlord")
missing = [n for n in need if n not in out]
if missing:
    sys.stderr.write("Missing Druid hosts for: %s; set DRUID_*_URL\n" % ",".join(missing))
    sys.exit(2)
for role, url in sorted(out.items()):
    print("%s=%s" % (role, url))
PY
}

if [[ "$urls_complete" != "1" ]]; then
  [[ -n "$cluster" ]] || die "cluster name required for Ambari Druid discovery"
  [[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]] || die "Ambari credentials required for discovery"
  while IFS='=' read -r role url; do
    case "$role" in
      broker) DRUID_BROKER_URL="${DRUID_BROKER_URL:-$url}" ;;
      coordinator) DRUID_COORDINATOR_URL="${DRUID_COORDINATOR_URL:-$url}" ;;
      overlord) DRUID_OVERLORD_URL="${DRUID_OVERLORD_URL:-$url}" ;;
      router) DRUID_ROUTER_URL="${DRUID_ROUTER_URL:-$url}" ;;
      historical) DRUID_HISTORICAL_URL="${DRUID_HISTORICAL_URL:-$url}" ;;
      middlemanager) DRUID_MIDDLEMANAGER_URL="${DRUID_MIDDLEMANAGER_URL:-$url}" ;;
    esac
  done < <(discover_druid_urls)
fi

[[ -n "${DRUID_BROKER_URL:-}" ]] || die "DRUID_BROKER_URL unset"
[[ -n "${DRUID_COORDINATOR_URL:-}" ]] || die "DRUID_COORDINATOR_URL unset"
[[ -n "${DRUID_OVERLORD_URL:-}" ]] || die "DRUID_OVERLORD_URL unset"

echo "---- Druid sample smoke ----"
echo "    broker:      ${DRUID_BROKER_URL}"
echo "    coordinator: ${DRUID_COORDINATOR_URL}"
echo "    overlord:    ${DRUID_OVERLORD_URL}"
[[ -n "${DRUID_ROUTER_URL:-}" ]] && echo "    router:      ${DRUID_ROUTER_URL}"
[[ -n "${DRUID_HISTORICAL_URL:-}" ]] && echo "    historical:  ${DRUID_HISTORICAL_URL}"
[[ -n "${DRUID_MIDDLEMANAGER_URL:-}" ]] && echo "    middleMgr:    ${DRUID_MIDDLEMANAGER_URL}"
[[ -n "$cluster" ]] && echo "    cluster: $cluster"

if [[ "$DRUID_SKIP_KINIT" == "1" ]]; then
  echo "---- DRUID_SKIP_KINIT=1 - skipping kinit ----"
else
  [[ -n "$cluster" ]] || die "CLUSTER_NAME required for kinit (or set DRUID_SKIP_KINIT=1)"
  [[ -r "$DRUID_KEYTAB" ]] || die "keytab not readable: $DRUID_KEYTAB"
  principal="druid-${cluster}"
  echo "kinit principal: ${principal}"
  kinit -kt "$DRUID_KEYTAB" "$principal" || die "kinit failed"
fi

druid_curl() {
  # Authenticated (Kerberos SPNEGO) request; prints body on stdout.
  # shellcheck disable=SC2086
  curl -sS -f ${CURL_EXTRA_OPTS:-} --negotiate -u : "$@"
}

druid_curl_nofail() {
  # shellcheck disable=SC2086
  curl -sS ${CURL_EXTRA_OPTS:-} --negotiate -u : "$@"
}

echo "---- health checks ----"
health_urls=(
  "${DRUID_BROKER_URL}/status/health"
  "${DRUID_COORDINATOR_URL}/status/health"
  "${DRUID_OVERLORD_URL}/status/health"
)
[[ -n "${DRUID_ROUTER_URL:-}" ]] && health_urls+=("${DRUID_ROUTER_URL}/status/health")
[[ -n "${DRUID_HISTORICAL_URL:-}" ]] && health_urls+=("${DRUID_HISTORICAL_URL}/status/health")
[[ -n "${DRUID_MIDDLEMANAGER_URL:-}" ]] && health_urls+=("${DRUID_MIDDLEMANAGER_URL}/status/health")

for u in "${health_urls[@]}"; do
  # health is typically anonymous
  # shellcheck disable=SC2086
  body="$(curl -sS -f ${CURL_EXTRA_OPTS:-} "$u")" || die "health failed: $u"
  echo "    OK $u -> $body"
  [[ "$body" == "true" ]] || die "unexpected health body from $u: $body"
done

echo "---- authenticated status / leaders ----"
coord_leader="$(druid_curl "${DRUID_COORDINATOR_URL}/druid/coordinator/v1/leader")" || die "coordinator leader failed"
overlord_leader="$(druid_curl "${DRUID_OVERLORD_URL}/druid/indexer/v1/leader")" || die "overlord leader failed"
echo "    coordinator leader: $coord_leader"
echo "    overlord leader:    $overlord_leader"

echo "---- SQL SELECT 1 (broker) ----"
sql_out="$(druid_curl -H "Content-Type: application/json" \
  -d '{"query":"SELECT 1 AS n"}' \
  "${DRUID_BROKER_URL}/druid/v2/sql")" || die "broker SQL failed"
echo "    $sql_out"
printf '%s' "$sql_out" | python3 -c "
import json,sys
d=json.load(sys.stdin)
assert isinstance(d,list) and d and d[0].get('n')==1, d
" || die "broker SQL unexpected: $sql_out"

if [[ -n "${DRUID_ROUTER_URL:-}" ]]; then
  echo "---- SQL SELECT 1 (router) ----"
  sql_r="$(druid_curl -H "Content-Type: application/json" \
    -d '{"query":"SELECT 1 AS n"}' \
    "${DRUID_ROUTER_URL}/druid/v2/sql")" || die "router SQL failed"
  echo "    $sql_r"
fi

if [[ "$DRUID_SKIP_INGEST" == "1" ]]; then
  echo "---- DRUID_SKIP_INGEST=1 - skipping ingest/query ----"
  echo "OK: Druid sample smoke finished (health + SQL only)."
  exit 0
fi

[[ -r "$DRUID_INDEX_SPEC" ]] || die "index spec not readable: $DRUID_INDEX_SPEC"

# Ensure datasource name in spec matches DRUID_DATASOURCE (rewrite via python)
work="$(mktemp -d "${TMPDIR:-/tmp}/druid-smoke.XXXXXX")"
trap 'rm -rf "$work"' EXIT
spec_file="${work}/index.json"
python3 - "$DRUID_INDEX_SPEC" "$spec_file" "$DRUID_DATASOURCE" <<'PY'
import json, sys
src, dst, ds = sys.argv[1], sys.argv[2], sys.argv[3]
spec = json.load(open(src))
# walk common locations
if isinstance(spec, dict):
    if "spec" in spec and isinstance(spec["spec"], dict):
        ds_obj = (spec["spec"].get("dataSchema") or {})
        if isinstance(ds_obj, dict):
            ds_obj["dataSource"] = ds
    elif "dataSchema" in spec and isinstance(spec["dataSchema"], dict):
        spec["dataSchema"]["dataSource"] = ds
json.dump(spec, open(dst, "w"), indent=2)
print(dst)
PY

echo "---- submit ingest task (datasource=$DRUID_DATASOURCE) ----"
task_json="$(druid_curl -H "Content-Type: application/json" \
  --data-binary @"$spec_file" \
  "${DRUID_OVERLORD_URL}/druid/indexer/v1/task")" || die "task submit failed"
echo "    $task_json"
task_id="$(printf '%s\n' "$task_json" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('task') or '')")"
[[ -n "$task_id" ]] || die "could not parse task id from: $task_json"
echo "    task_id=$task_id"

echo "---- wait for task SUCCESS ----"
deadline=$(( $(date +%s) + DRUID_TIMEOUT_SECONDS ))
status=""
while true; do
  st_json="$(druid_curl "${DRUID_OVERLORD_URL}/druid/indexer/v1/task/${task_id}/status")" || die "task status failed"
  status="$(printf '%s\n' "$st_json" | python3 -c "import json,sys; d=json.load(sys.stdin); print(((d.get('status') or {}).get('status')) or '')")"
  echo "    status=$status"
  case "$status" in
    SUCCESS) break ;;
    FAILED|CANCELED)
      echo "$st_json" >&2
      die "ingest task ended with status=$status"
      ;;
  esac
  if (( $(date +%s) >= deadline )); then
    echo "$st_json" >&2
    die "timed out waiting for ingest task $task_id"
  fi
  sleep "$DRUID_POLL_SECONDS"
done

echo "---- SQL COUNT(*) FROM ${DRUID_DATASOURCE} ----"
q_deadline=$(( $(date +%s) + DRUID_TIMEOUT_SECONDS ))
count=""
while true; do
  q_json="$(druid_curl_nofail -H "Content-Type: application/json" \
    -d "{\"query\":\"SELECT COUNT(*) AS c FROM ${DRUID_DATASOURCE}\"}" \
    "${DRUID_BROKER_URL}/druid/v2/sql" || true)"
  count="$(printf '%s\n' "$q_json" | python3 -c "
import json,sys
raw=sys.stdin.read().strip()
try:
  d=json.loads(raw)
except Exception:
  sys.exit(0)
if isinstance(d,list) and d and 'c' in d[0]:
  print(d[0]['c'])
" 2>/dev/null || true)"
  if [[ -n "$count" ]]; then
    echo "    count=$count"
    break
  fi
  echo "    waiting for datasource to become queryable..."
  if (( $(date +%s) >= q_deadline )); then
    echo "$q_json" >&2
    die "timed out waiting for SQL against $DRUID_DATASOURCE"
  fi
  sleep "$DRUID_POLL_SECONDS"
done
[[ "$count" == "$DRUID_EXPECTED_COUNT" ]] || die "expected count=$DRUID_EXPECTED_COUNT got=$count"

ds_list="$(druid_curl "${DRUID_COORDINATOR_URL}/druid/coordinator/v1/metadata/datasources")" || true
echo "    datasources: $ds_list"

if [[ "$DRUID_KEEP_DATASOURCE" != "1" ]]; then
  echo "---- mark datasource unused (best-effort) ----"
  druid_curl_nofail -X DELETE \
    "${DRUID_COORDINATOR_URL}/druid/coordinator/v1/datasources/${DRUID_DATASOURCE}?kill=true&interval=1000/3000" >/dev/null || true
fi

echo "OK: Druid sample smoke finished (task=$task_id count=$count)."
