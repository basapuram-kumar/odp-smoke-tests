#!/usr/bin/env bash
#
# Apply ODP cluster tuning helpers (Ambari configs + Ranger YARN policy).
#
# Changes applied (defaults; override via env):
#
# 1) YARN / container-executor (template content):
#      banned.users -> mapred,bin
#    UI: Services > YARN > Configs > Advanced > Advanced container-executor
#
# 2) MapReduce2 / mapred-site:
#      mapreduce.map.java.opts -> -Xmx3276m
#    UI: Services > MapReduce2 > Configs > Advanced > Advanced mapred-site
#        (MR Map Java Heap Size)
#
# 3) Tez / tez-site:
#      tez.am.resource.memory.mb -> 5120
#    UI: Services > Tez > Configs > General
#
# 4) Ranger YARN policy "all - queue":
#      merge users: hdfs,yarn,hive,spark,flink,pinot,kafka
#    (calls ./ranger-yarn-all-queue-users-add.sh)
#
# Ambari credentials: configs/ambari.env (see configs/ambari.env.example), or env.
# Ranger credentials: configs/ranger.env (see configs/ranger.env.example), or env.
#
# Environment (optional):
#   AMBARI_CONFIG_FILE        default <script-dir>/configs/ambari.env
#   AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD
#   CLUSTER_NAME              if set, skip Ambari cluster-name lookup
#   BANNED_USERS              default mapred,bin
#   MAPREDUCE_MAP_JAVA_OPTS   default -Xmx3276m
#   TEZ_AM_RESOURCE_MEMORY_MB default 5120
#   RANGER_ADD_USERS          default hdfs,yarn,hive,spark,flink,pinot,kafka
#   RANGER_ENV_FILE           default <script-dir>/configs/ranger.env
#   SKIP_BANNED_USERS         if 1, skip container-executor update
#   SKIP_MAP_JAVA_OPTS        if 1, skip mapred-site update
#   SKIP_TEZ_AM_MEMORY        if 1, skip tez-site update
#   SKIP_RANGER_YARN_USERS    if 1, skip Ranger policy user merge
#   DRY_RUN                   if 1, preview Ambari + Ranger changes (no PUT)
#   CURL_EXTRA_OPTS           e.g. -k for TLS
#
# Usage:
#   ./update-ambari-yarn-mapred-tez-configs.sh
#   DRY_RUN=1 ./update-ambari-yarn-mapred-tez-configs.sh
#
# After a successful Ambari PUT, restart / refresh affected services:
#   - YARN (NodeManagers) for container-executor.cfg
#   - MapReduce2 / clients for mapred-site
#   - Tez / Hive clients for tez-site
#   Ranger policy changes take effect without Ambari restart.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
RANGER_ENV_FILE="${RANGER_ENV_FILE:-${RANGER_CONFIG_FILE:-${SCRIPT_DIR}/configs/ranger.env}}"
RANGER_USERS_SCRIPT="${RANGER_USERS_SCRIPT:-${SCRIPT_DIR}/ranger-yarn-all-queue-users-add.sh}"
BANNED_USERS="${BANNED_USERS:-mapred,bin}"
MAPREDUCE_MAP_JAVA_OPTS="${MAPREDUCE_MAP_JAVA_OPTS:--Xmx3276m}"
TEZ_AM_RESOURCE_MEMORY_MB="${TEZ_AM_RESOURCE_MEMORY_MB:-5120}"
# Default matches: ./ranger-yarn-all-queue-users-add.sh hdfs,yarn,hive,spark,flink,pinot,kafka
RANGER_ADD_USERS="${RANGER_ADD_USERS:-hdfs,yarn,hive,spark,flink,pinot,kafka}"
SKIP_BANNED_USERS="${SKIP_BANNED_USERS:-0}"
SKIP_MAP_JAVA_OPTS="${SKIP_MAP_JAVA_OPTS:-0}"
SKIP_TEZ_AM_MEMORY="${SKIP_TEZ_AM_MEMORY:-0}"
SKIP_RANGER_YARN_USERS="${SKIP_RANGER_YARN_USERS:-0}"
DRY_RUN="${DRY_RUN:-0}"

die() { echo "[ERROR] $*" >&2; exit 1; }
log() { echo "[INFO] $*"; }

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

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

need_cmd curl
need_cmd python3

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""

if [[ -f "$AMBARI_CONFIG_FILE" ]]; then
  load_ambari_env_file "$AMBARI_CONFIG_FILE" || die "failed to read $AMBARI_CONFIG_FILE"
fi

AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"

[[ -n "$AMBARI_BASE_URL" ]] || die "AMBARI_BASE_URL is required (set in configs/ambari.env or env)."
[[ -n "$AMBARI_USER" ]] || die "AMBARI_USER is required."
[[ -n "$AMBARI_PASSWORD" ]] || die "AMBARI_PASSWORD is required."

AMBARI_BASE_URL="${AMBARI_BASE_URL%/}"

ambari_curl() {
  # shellcheck disable=SC2086
  curl -sS ${CURL_EXTRA_OPTS:-} \
    -u "${AMBARI_USER}:${AMBARI_PASSWORD}" \
    -H "X-Requested-By: ambari" \
    "$@"
}

if [[ -z "${CLUSTER_NAME:-}" ]]; then
  clusters_json="$(ambari_curl "${AMBARI_BASE_URL}/api/v1/clusters")" \
    || die "failed to list Ambari clusters"
  CLUSTER_NAME="$(
    python3 -c '
import json,sys
j=json.load(sys.stdin)
items=j.get("items") or []
if not items:
  raise SystemExit("no clusters found")
print(items[0]["Clusters"]["cluster_name"])
' <<<"$clusters_json"
  )" || die "failed to parse cluster name"
fi

log "Cluster: ${CLUSTER_NAME}"
[[ "${SKIP_BANNED_USERS}" == "1" ]] || log "Target banned.users=${BANNED_USERS}"
[[ "${SKIP_MAP_JAVA_OPTS}" == "1" ]] || log "Target mapreduce.map.java.opts=${MAPREDUCE_MAP_JAVA_OPTS}"
[[ "${SKIP_TEZ_AM_MEMORY}" == "1" ]] || log "Target tez.am.resource.memory.mb=${TEZ_AM_RESOURCE_MEMORY_MB}"
[[ "${SKIP_RANGER_YARN_USERS}" == "1" ]] || log "Target Ranger YARN all-queue users=${RANGER_ADD_USERS}"

export AMBARI_BASE_URL AMBARI_USER AMBARI_PASSWORD CLUSTER_NAME
export BANNED_USERS MAPREDUCE_MAP_JAVA_OPTS TEZ_AM_RESOURCE_MEMORY_MB
export SKIP_BANNED_USERS SKIP_MAP_JAVA_OPTS SKIP_TEZ_AM_MEMORY DRY_RUN
export CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}"

python3 <<'PY'
import json, os, re, sys, time, urllib.parse, urllib.request, base64

ambari = os.environ["AMBARI_BASE_URL"].rstrip("/")
user = os.environ["AMBARI_USER"]
password = os.environ["AMBARI_PASSWORD"]
cluster = os.environ["CLUSTER_NAME"]
banned = os.environ["BANNED_USERS"].strip()
map_opts = os.environ["MAPREDUCE_MAP_JAVA_OPTS"].strip()
tez_am_mb = os.environ["TEZ_AM_RESOURCE_MEMORY_MB"].strip()
skip_banned = os.environ.get("SKIP_BANNED_USERS", "0") == "1"
skip_map = os.environ.get("SKIP_MAP_JAVA_OPTS", "0") == "1"
skip_tez = os.environ.get("SKIP_TEZ_AM_MEMORY", "0") == "1"
dry_run = os.environ.get("DRY_RUN", "0") == "1"
curl_extra = os.environ.get("CURL_EXTRA_OPTS", "")

auth = base64.b64encode(f"{user}:{password}".encode()).decode()

def req(method, url, data=None):
    # Ambari REST resources declare @Produces("text/plain"), so an Accept header
    # limited to application/json makes Jersey answer 406 Not Acceptable.
    headers = {
        "Authorization": f"Basic {auth}",
        "X-Requested-By": "ambari",
        "Accept": "*/*",
    }
    body = None
    if data is not None:
        body = json.dumps(data).encode()
        headers["Content-Type"] = "application/json"
    r = urllib.request.Request(url, data=body, headers=headers, method=method)
    ctx = None
    if "-k" in curl_extra.split():
        import ssl
        ctx = ssl._create_unverified_context()
    try:
        with urllib.request.urlopen(r, context=ctx) as resp:
            raw = resp.read().decode()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        err = e.read().decode(errors="replace")
        raise SystemExit(f"[ERROR] Ambari {method} {url} -> HTTP {e.code}: {err}") from e

def get_config(qc, config_type):
    desired = req("GET", f"{ambari}/api/v1/clusters/{qc}?fields=Clusters/desired_configs")
    dc = (desired.get("Clusters") or {}).get("desired_configs") or {}
    if config_type not in dc:
        raise SystemExit(f"[ERROR] desired_configs has no type '{config_type}'")
    old_tag = dc[config_type]["tag"]
    cfg = req(
        "GET",
        f"{ambari}/api/v1/clusters/{qc}/configurations"
        f"?type={urllib.parse.quote(config_type)}&tag={urllib.parse.quote(old_tag)}",
    )
    items = cfg.get("items") or []
    if not items:
        raise SystemExit(f"[ERROR] no configuration items for {config_type} tag={old_tag}")
    props = dict(items[0].get("properties") or {})
    attrs = items[0].get("properties_attributes")
    return old_tag, props, attrs

def put_config(qc, config_type, props, attrs, note):
    new_tag = f"version{int(time.time() * 1000)}"
    payload = {
        "Clusters": {
            "desired_configs": {
                "type": config_type,
                "tag": new_tag,
                "service_config_version_note": note,
                "properties": props,
            }
        }
    }
    if attrs:
        payload["Clusters"]["desired_configs"]["properties_attributes"] = attrs
    if dry_run:
        print(f"[INFO] DRY_RUN=1; would PUT {config_type} tag={new_tag} ({note})")
        return new_tag
    req("PUT", f"{ambari}/api/v1/clusters/{qc}", payload)
    print(f"[INFO] Updated Ambari desired config {config_type} tag={new_tag}")
    return new_tag

qc = urllib.parse.quote(cluster, safe="")
changed = False

# ---------------------------------------------------------------------------
# 1) container-executor: banned.users
# ---------------------------------------------------------------------------
if not skip_banned:
    config_type = "container-executor"
    old_tag, props, attrs = get_config(qc, config_type)
    print(f"[INFO] Current {config_type} tag: {old_tag}")
    content = props.get("content")
    if content is None:
        raise SystemExit("[ERROR] property 'content' missing on container-executor")

    m = re.search(r"(?m)^banned\.users\s*=\s*(.*)$", content)
    if not m:
        raise SystemExit("[ERROR] banned.users line not found in container-executor content")

    old_value = m.group(1).strip()
    print(f"[INFO] Current banned.users={old_value}")
    if old_value == banned:
        print(f"[INFO] banned.users already set to {banned}; skipping.")
    else:
        new_content, n = re.subn(
            r"(?m)^(banned\.users\s*=\s*).*$",
            rf"\g<1>{banned}",
            content,
            count=1,
        )
        if n != 1:
            raise SystemExit("[ERROR] failed to rewrite banned.users line")
        props["content"] = new_content
        for line in new_content.splitlines():
            if line.strip().startswith("banned.users"):
                print(f"[INFO] New line: {line}")
        put_config(qc, config_type, props, attrs, f"Set banned.users={banned}")
        changed = True
else:
    print("[INFO] SKIP_BANNED_USERS=1; skipping container-executor update")

# ---------------------------------------------------------------------------
# 2) mapred-site: mapreduce.map.java.opts
# ---------------------------------------------------------------------------
if not skip_map:
    config_type = "mapred-site"
    prop_key = "mapreduce.map.java.opts"
    old_tag, props, attrs = get_config(qc, config_type)
    print(f"[INFO] Current {config_type} tag: {old_tag}")
    if prop_key not in props:
        raise SystemExit(f"[ERROR] property '{prop_key}' missing on {config_type}")

    old_value = props[prop_key]
    print(f"[INFO] Current {prop_key}={old_value}")
    if old_value == map_opts:
        print(f"[INFO] {prop_key} already set to {map_opts}; skipping.")
    else:
        # If caller passed only -XmxNNNm and current value has -Xmx..., replace in place
        # so any extra JVM opts are preserved. Otherwise set the property wholesale.
        if re.fullmatch(r"-Xmx\d+[mMgGkK]?", map_opts) and re.search(r"-Xmx\S+", old_value):
            new_value = re.sub(r"-Xmx\S+", map_opts, old_value, count=1)
        else:
            new_value = map_opts

        print(f"[INFO] New {prop_key}={new_value}")
        props[prop_key] = new_value
        put_config(qc, config_type, props, attrs, f"Set {prop_key}={new_value}")
        changed = True
else:
    print("[INFO] SKIP_MAP_JAVA_OPTS=1; skipping mapred-site update")

# ---------------------------------------------------------------------------
# 3) tez-site: tez.am.resource.memory.mb
# ---------------------------------------------------------------------------
if not skip_tez:
    config_type = "tez-site"
    prop_key = "tez.am.resource.memory.mb"
    old_tag, props, attrs = get_config(qc, config_type)
    print(f"[INFO] Current {config_type} tag: {old_tag}")
    if prop_key not in props:
        raise SystemExit(f"[ERROR] property '{prop_key}' missing on {config_type}")

    old_value = str(props[prop_key]).strip()
    print(f"[INFO] Current {prop_key}={old_value}")
    if old_value == tez_am_mb:
        print(f"[INFO] {prop_key} already set to {tez_am_mb}; skipping.")
    else:
        print(f"[INFO] New {prop_key}={tez_am_mb}")
        props[prop_key] = tez_am_mb
        put_config(qc, config_type, props, attrs, f"Set {prop_key}={tez_am_mb}")
        changed = True
else:
    print("[INFO] SKIP_TEZ_AM_MEMORY=1; skipping tez-site update")

if dry_run:
    print("[INFO] DRY_RUN complete; no Ambari changes applied.")
elif changed:
    print("[INFO] Restart YARN (NodeManagers) for container-executor.cfg.")
    print("[INFO] Restart MapReduce2 / refresh clients for mapred-site as needed.")
    print("[INFO] Restart Tez / refresh Hive clients for tez-site as needed.")
else:
    print("[INFO] No Ambari config changes were required.")
PY

# ---------------------------------------------------------------------------
# 4) Ranger YARN "all - queue" users
# ---------------------------------------------------------------------------
if [[ "${SKIP_RANGER_YARN_USERS}" == "1" ]]; then
  log "SKIP_RANGER_YARN_USERS=1; skipping Ranger YARN policy user merge"
else
  [[ -x "${RANGER_USERS_SCRIPT}" ]] || die "Ranger users script not executable: ${RANGER_USERS_SCRIPT}"
  log "Merging users into Ranger YARN policy via ${RANGER_USERS_SCRIPT}"
  # Propagate Ambari/Ranger context; DRY_RUN maps to RANGER_DRY_RUN.
  AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE}" \
  AMBARI_BASE_URL="${AMBARI_BASE_URL}" \
  AMBARI_USER="${AMBARI_USER}" \
  AMBARI_PASSWORD="${AMBARI_PASSWORD}" \
  CLUSTER_NAME="${CLUSTER_NAME}" \
  RANGER_ENV_FILE="${RANGER_ENV_FILE}" \
  RANGER_ADD_USERS="${RANGER_ADD_USERS}" \
  RANGER_DRY_RUN="${DRY_RUN}" \
  CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}" \
    "${RANGER_USERS_SCRIPT}" "${RANGER_ADD_USERS}"
fi

log "Done."
