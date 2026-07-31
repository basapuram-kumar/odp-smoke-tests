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
#      mapreduce.map.memory.mb + mapreduce.map.java.opts
#    UI: Services > MapReduce2 > Configs > Advanced > Advanced mapred-site
#        (MR Map Java Heap Size)
#
# 3) Tez / tez-site:
#      tez.am.resource.memory.mb + tez.am.java.opts
#    UI: Services > Tez > Configs > General
#
# Container sizes and heaps are derived from the cluster's actual NodeManager
# capacity (yarn.nodemanager.resource.memory-mb and the scheduler allocation
# limits) unless you override them. Two combinations are rejected outright,
# because both produce jobs that hang or die with no useful error:
#
#   * a container larger than yarn.scheduler.maximum-allocation-mb, or large
#     enough to need a mostly idle NodeManager. A Tez AM sized at the full
#     NodeManager memory only starts when a node is completely empty, so any
#     other container in the cluster leaves it pending in ACCEPTED forever.
#   * a -Xmx heap that does not fit inside its own container, which YARN kills
#     for exceeding physical memory.
#
# Set FORCE_UNSAFE_SIZING=1 to downgrade those refusals to warnings.
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
#   MAPREDUCE_MAP_MEMORY_MB   default derived from NodeManager capacity
#   MAPREDUCE_MAP_JAVA_OPTS   default derived (80% of the map container)
#   TEZ_AM_RESOURCE_MEMORY_MB default derived from NodeManager capacity
#   TEZ_AM_JAVA_OPTS          default derived (80% of the AM container)
#   FORCE_UNSAFE_SIZING       if 1, warn instead of refusing unsafe sizes
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
# Empty means "derive from the cluster's NodeManager capacity".
MAPREDUCE_MAP_MEMORY_MB="${MAPREDUCE_MAP_MEMORY_MB:-}"
MAPREDUCE_MAP_JAVA_OPTS="${MAPREDUCE_MAP_JAVA_OPTS:-}"
TEZ_AM_RESOURCE_MEMORY_MB="${TEZ_AM_RESOURCE_MEMORY_MB:-}"
TEZ_AM_JAVA_OPTS="${TEZ_AM_JAVA_OPTS:-}"
FORCE_UNSAFE_SIZING="${FORCE_UNSAFE_SIZING:-0}"
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
[[ "${SKIP_MAP_JAVA_OPTS}" == "1" ]] \
  || log "Target map container=${MAPREDUCE_MAP_MEMORY_MB:-derived} heap=${MAPREDUCE_MAP_JAVA_OPTS:-derived}"
[[ "${SKIP_TEZ_AM_MEMORY}" == "1" ]] \
  || log "Target Tez AM container=${TEZ_AM_RESOURCE_MEMORY_MB:-derived} heap=${TEZ_AM_JAVA_OPTS:-derived}"
[[ "${SKIP_RANGER_YARN_USERS}" == "1" ]] || log "Target Ranger YARN all-queue users=${RANGER_ADD_USERS}"

export AMBARI_BASE_URL AMBARI_USER AMBARI_PASSWORD CLUSTER_NAME
export BANNED_USERS MAPREDUCE_MAP_MEMORY_MB MAPREDUCE_MAP_JAVA_OPTS
export TEZ_AM_RESOURCE_MEMORY_MB TEZ_AM_JAVA_OPTS FORCE_UNSAFE_SIZING
export SKIP_BANNED_USERS SKIP_MAP_JAVA_OPTS SKIP_TEZ_AM_MEMORY DRY_RUN
export CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}"

python3 <<'PY'
import json, os, re, sys, time, urllib.parse, urllib.request, base64

ambari = os.environ["AMBARI_BASE_URL"].rstrip("/")
user = os.environ["AMBARI_USER"]
password = os.environ["AMBARI_PASSWORD"]
cluster = os.environ["CLUSTER_NAME"]
banned = os.environ["BANNED_USERS"].strip()
map_mb_override = os.environ.get("MAPREDUCE_MAP_MEMORY_MB", "").strip()
map_opts_override = os.environ.get("MAPREDUCE_MAP_JAVA_OPTS", "").strip()
tez_am_mb_override = os.environ.get("TEZ_AM_RESOURCE_MEMORY_MB", "").strip()
tez_am_opts_override = os.environ.get("TEZ_AM_JAVA_OPTS", "").strip()
force_unsafe = os.environ.get("FORCE_UNSAFE_SIZING", "0") == "1"
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
# Container sizing, derived from what the NodeManagers actually offer
# ---------------------------------------------------------------------------
HEAP_FRACTION = 0.8

def parse_xmx_mb(opts):
    m = re.search(r"-Xmx(\d+)([kKmMgG]?)", opts or "")
    if not m:
        return None
    size, unit = int(m.group(1)), m.group(2).lower()
    return {"k": size // 1024, "m": size, "g": size * 1024, "": size // (1024 * 1024)}[unit]

def as_int(value, default):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default

def unsafe(message):
    if force_unsafe:
        print(f"[WARN] {message}")
        print("[WARN] FORCE_UNSAFE_SIZING=1; applying anyway.")
        return
    raise SystemExit(
        f"[ERROR] {message}\n"
        "[ERROR] Refusing to apply. Pick sizes that fit a NodeManager, or set "
        "FORCE_UNSAFE_SIZING=1 to override."
    )

_, yarn_props, _ = get_config(qc, "yarn-site")
nm_mb = as_int(yarn_props.get("yarn.nodemanager.resource.memory-mb"), 0)
max_alloc_mb = as_int(yarn_props.get("yarn.scheduler.maximum-allocation-mb"), nm_mb)
min_alloc_mb = as_int(yarn_props.get("yarn.scheduler.minimum-allocation-mb"), 1024)
if nm_mb <= 0:
    raise SystemExit("[ERROR] could not read yarn.nodemanager.resource.memory-mb")
if min_alloc_mb <= 0:
    min_alloc_mb = 1024
print(
    f"[INFO] NodeManager memory={nm_mb}MB "
    f"(allocation limits {min_alloc_mb}MB..{max_alloc_mb}MB)"
)

# A quarter of a node keeps several containers schedulable per NodeManager, and
# the half-node ceiling is what stops an AM from needing an otherwise idle node.
def derive_container_mb():
    target = -(-(nm_mb // 4) // min_alloc_mb) * min_alloc_mb
    ceiling = max(min_alloc_mb, min(max_alloc_mb, nm_mb // 2))
    return max(min_alloc_mb, min(target, ceiling))

def check_container_mb(label, container_mb):
    if container_mb > max_alloc_mb:
        unsafe(
            f"{label} container {container_mb}MB exceeds "
            f"yarn.scheduler.maximum-allocation-mb ({max_alloc_mb}MB); YARN rejects the request"
        )
    elif container_mb > nm_mb // 2:
        unsafe(
            f"{label} container {container_mb}MB needs more than half of a "
            f"{nm_mb}MB NodeManager, so it stays pending unless a node is nearly idle"
        )

def check_heap(label, opts, container_mb):
    heap_mb = parse_xmx_mb(opts)
    if heap_mb is None:
        print(f"[WARN] no -Xmx found in {label} java opts; leaving heap untouched")
        return
    if heap_mb > int(container_mb * 0.9):
        unsafe(
            f"{label} heap {heap_mb}MB does not fit in its {container_mb}MB "
            "container; YARN kills the container for exceeding physical memory"
        )

def apply_heap(opts, container_mb):
    heap = f"-Xmx{int(container_mb * HEAP_FRACTION)}m"
    if re.search(r"-Xmx\S+", opts or ""):
        return re.sub(r"-Xmx\S+", heap, opts, count=1)
    return f"{opts} {heap}".strip() if opts else heap

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
# 2) mapred-site: map container size and matching heap
# ---------------------------------------------------------------------------
def update_sizing(config_type, mem_key, opts_key, label, mb_override, opts_override):
    old_tag, props, attrs = get_config(qc, config_type)
    print(f"[INFO] Current {config_type} tag: {old_tag}")
    for key in (mem_key, opts_key):
        if key not in props:
            raise SystemExit(f"[ERROR] property '{key}' missing on {config_type}")

    old_mb = str(props[mem_key]).strip()
    old_opts = props[opts_key]
    print(f"[INFO] Current {mem_key}={old_mb}")
    print(f"[INFO] Current {opts_key}={old_opts}")

    container_mb = as_int(mb_override, 0) if mb_override else derive_container_mb()
    if mb_override and container_mb <= 0:
        raise SystemExit(f"[ERROR] {mem_key} override is not a number: {mb_override!r}")
    check_container_mb(label, container_mb)

    if opts_override:
        new_opts = (
            re.sub(r"-Xmx\S+", opts_override, old_opts, count=1)
            if re.fullmatch(r"-Xmx\d+[kKmMgG]?", opts_override) and re.search(r"-Xmx\S+", old_opts)
            else opts_override
        )
        check_heap(label, new_opts, container_mb)
    else:
        new_opts = apply_heap(old_opts, container_mb)

    if old_mb == str(container_mb) and old_opts == new_opts:
        print(f"[INFO] {config_type} already sized for {label}; skipping.")
        return False

    print(f"[INFO] New {mem_key}={container_mb}")
    print(f"[INFO] New {opts_key}={new_opts}")
    props[mem_key] = str(container_mb)
    props[opts_key] = new_opts
    put_config(
        qc, config_type, props, attrs,
        f"Fit {label} within {nm_mb}MB NodeManagers ({mem_key}={container_mb})",
    )
    return True

if not skip_map:
    if update_sizing(
        "mapred-site", "mapreduce.map.memory.mb", "mapreduce.map.java.opts",
        "MR map", map_mb_override, map_opts_override,
    ):
        changed = True
else:
    print("[INFO] SKIP_MAP_JAVA_OPTS=1; skipping mapred-site update")

# ---------------------------------------------------------------------------
# 3) tez-site: Tez AM container size and matching heap
# ---------------------------------------------------------------------------
if not skip_tez:
    if update_sizing(
        "tez-site", "tez.am.resource.memory.mb", "tez.am.java.opts",
        "Tez AM", tez_am_mb_override, tez_am_opts_override,
    ):
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
