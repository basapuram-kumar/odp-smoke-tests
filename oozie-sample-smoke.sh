#!/usr/bin/env bash
#
# Resolve cluster name from Ambari (same pattern as hdfs/yarn smokes), kinit as
# hdfs-<cluster> with the HDFS headless keytab, stage one or more workflows on
# HDFS, submit them to Oozie, and wait until SUCCEEDED.
#
# Workflows (see oozie/<name>/workflow.xml):
#   shell  shell-action "Hello Oozie" with capture-output + decision node
#   hive   hive2-action (beeline to HiveServer2) create/insert/select/drop
#
# Uses hdfs (not the oozie service principal) so the YARN launcher can submit to
# the configured queue (oozie service user is often denied by Capacity Scheduler /
# Ranger ACLs).
#
# Environment (optional):
#   AMBARI_CONFIG_FILE       default <script-dir>/configs/ambari.env
#   AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD
#   CLUSTER_NAME             If set, skip Ambari lookup
#   HDFS_KEYTAB              default /etc/security/keytabs/hdfs.headless.keytab
#   OOZIE_URL                default from oozie-site.xml oozie.base.url, else http://$(hostname):11000/oozie
#   OOZIE_NAME_NODE          default from hdfs getconf fs.defaultFS
#   OOZIE_RESOURCE_MANAGER   default from yarn.resourcemanager.address (yarn-site / getconf)
#   OOZIE_QUEUE              default default
#   OOZIE_WORKFLOWS          comma/space list, default "shell,hive"
#   OOZIE_WORKFLOW_ROOT      default <script-dir>/oozie (holds <name>/workflow.xml)
#   OOZIE_WORKFLOW_DIR       legacy: run this single directory instead of OOZIE_WORKFLOWS
#   OOZIE_HDFS_APP_DIR       default /user/hdfs/oozie_smoke_<timestamp>
#   OOZIE_POLL_SECONDS       default 5
#   OOZIE_TIMEOUT_SECONDS    default 300
#   OOZIE_SITE_XML           default /etc/oozie/conf/oozie-site.xml
#   YARN_SITE_XML            default /etc/hadoop/conf/yarn-site.xml
#   HIVE_SITE_XML            default /etc/hive/conf/hive-site.xml
#   OOZIE_HIVE2_JDBC_URL     default built from hive-site + Ambari HIVE_SERVER host.
#                            Must NOT contain principal=; Oozie appends it from the credential.
#   OOZIE_HIVE2_PRINCIPAL    default hive.server2.authentication.kerberos.principal (_HOST resolved)
#   OOZIE_HIVE_DB            default odp_oozie_smoke_<timestamp>; created and dropped
#                            by the workflow, so the submitting user owns it
#   OOZIE_HIVE_TABLE         default smoke_events
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
HDFS_KEYTAB="${HDFS_KEYTAB:-/etc/security/keytabs/hdfs.headless.keytab}"
OOZIE_WORKFLOW_ROOT="${OOZIE_WORKFLOW_ROOT:-${SCRIPT_DIR}/oozie}"
OOZIE_WORKFLOWS="${OOZIE_WORKFLOWS:-shell,hive}"
OOZIE_QUEUE="${OOZIE_QUEUE:-default}"
OOZIE_POLL_SECONDS="${OOZIE_POLL_SECONDS:-5}"
OOZIE_TIMEOUT_SECONDS="${OOZIE_TIMEOUT_SECONDS:-300}"
OOZIE_SITE_XML="${OOZIE_SITE_XML:-/etc/oozie/conf/oozie-site.xml}"
YARN_SITE_XML="${YARN_SITE_XML:-/etc/hadoop/conf/yarn-site.xml}"
HIVE_SITE_XML="${HIVE_SITE_XML:-/etc/hive/conf/hive-site.xml}"
OOZIE_HIVE_TABLE="${OOZIE_HIVE_TABLE:-smoke_events}"

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

xml_prop_value() {
  local file="$1" name="$2"
  [[ -r "$file" ]] || return 1
  python3 - "$file" "$name" <<'PY'
import sys, xml.etree.ElementTree as ET
path, want = sys.argv[1], sys.argv[2]
root = ET.parse(path).getroot()
for prop in root.iter("property"):
    n = prop.find("name")
    v = prop.find("value")
    if n is not None and v is not None and (n.text or "").strip() == want:
        print((v.text or "").strip())
        sys.exit(0)
sys.exit(1)
PY
}

default_host() {
  local h
  h="$(hostname -f 2>/dev/null || true)"
  if [[ -z "$h" || "$h" == "(none)" ]]; then
    h="$(hostname 2>/dev/null || true)"
  fi
  [[ -n "$h" ]] || die "could not determine hostname"
  printf '%s' "$h"
}

resolve_oozie_url() {
  if [[ -n "${OOZIE_URL:-}" ]]; then
    printf '%s' "$OOZIE_URL"
    return
  fi
  local u
  u="$(xml_prop_value "$OOZIE_SITE_XML" "oozie.base.url" 2>/dev/null || true)"
  if [[ -n "$u" ]]; then
    printf '%s' "$u"
    return
  fi
  printf 'http://%s:11000/oozie' "$(default_host)"
}

resolve_name_node() {
  if [[ -n "${OOZIE_NAME_NODE:-}" ]]; then
    printf '%s' "$OOZIE_NAME_NODE"
    return
  fi
  local nn
  nn="$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || true)"
  [[ -n "$nn" ]] || die "could not resolve nameNode; set OOZIE_NAME_NODE"
  printf '%s' "$nn"
}

resolve_resource_manager() {
  if [[ -n "${OOZIE_RESOURCE_MANAGER:-}" ]]; then
    printf '%s' "$OOZIE_RESOURCE_MANAGER"
    return
  fi
  local rm
  rm="$(hdfs getconf -confKey yarn.resourcemanager.address 2>/dev/null || true)"
  if [[ -z "$rm" ]]; then
    rm="$(xml_prop_value "$YARN_SITE_XML" "yarn.resourcemanager.address" 2>/dev/null || true)"
  fi
  [[ -n "$rm" ]] || die "could not resolve resourceManager; set OOZIE_RESOURCE_MANAGER"
  printf '%s' "$rm"
}

ambari_component_host() {
  local service="$1" component="$2" url
  [[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]] || return 1
  url="${AMBARI_BASE_URL%/}/api/v1/clusters/${cluster}/services/${service}/components/${component}"
  curl -sS -f -u "${AMBARI_USER}:${AMBARI_PASSWORD}" -H "X-Requested-By: ambari" "$url" 2>/dev/null \
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
print(hosts[0])
"
}

# Sets hive2_jdbc_url and hive2_principal; returns non-zero when HS2 cannot be resolved.
resolve_hive2() {
  hive2_jdbc_url="${OOZIE_HIVE2_JDBC_URL:-}"
  hive2_principal="${OOZIE_HIVE2_PRINCIPAL:-}"
  if [[ -n "$hive2_jdbc_url" && -n "$hive2_principal" ]]; then
    return 0
  fi

  local hs2_host mode port principal use_ssl extra
  hs2_host="$(ambari_component_host HIVE HIVE_SERVER 2>/dev/null || true)"
  if [[ -z "$hs2_host" ]]; then
    echo "    WARN: could not resolve HIVE_SERVER host from Ambari" >&2
    return 1
  fi

  mode="$(xml_prop_value "$HIVE_SITE_XML" "hive.server2.transport.mode" 2>/dev/null || echo binary)"
  use_ssl="$(xml_prop_value "$HIVE_SITE_XML" "hive.server2.use.SSL" 2>/dev/null || echo false)"
  principal="${hive2_principal:-$(xml_prop_value "$HIVE_SITE_XML" "hive.server2.authentication.kerberos.principal" 2>/dev/null || true)}"
  [[ -n "$principal" ]] || principal="hive/_HOST@$(sed -n 's/^[[:space:]]*default_realm[[:space:]]*=[[:space:]]*//p' /etc/krb5.conf 2>/dev/null | head -1)"
  principal="${principal//_HOST/$hs2_host}"

  extra=""
  if [[ "$mode" == "http" ]]; then
    port="$(xml_prop_value "$HIVE_SITE_XML" "hive.server2.thrift.http.port" 2>/dev/null || echo 10001)"
    extra=";transportMode=http;httpPath=$(xml_prop_value "$HIVE_SITE_XML" "hive.server2.thrift.http.path" 2>/dev/null || echo cliservice)"
  else
    port="$(xml_prop_value "$HIVE_SITE_XML" "hive.server2.thrift.port" 2>/dev/null || echo 10000)"
  fi
  if [[ "$use_ssl" == "true" ]]; then
    extra="${extra};ssl=true"
  fi

  # Do not put principal= in the URL: Hive2Credentials appends hive2.server.principal
  # itself and HiveConnection rejects duplicate properties.
  hive2_jdbc_url="${hive2_jdbc_url:-jdbc:hive2://${hs2_host}:${port}/default${extra}}"
  hive2_principal="$principal"
  return 0
}

oozie_job_status() {
  local job_id="$1"
  oozie job -oozie "$OOZIE_URL" -info "$job_id" 2>/dev/null \
    | awk -F: '/^Status[[:space:]]*:/ { gsub(/^[[:space:]]+|[[:space:]]+$/, "", $2); print $2; exit }'
}

# A workflow left RUNNING keeps its launcher ApplicationMaster alive forever,
# and every leaked launcher holds a YARN container that later runs (and any
# other tenant) never get back. Always kill a job we are done waiting on.
RUNNING_JOB_ID=""

oozie_kill_job() {
  local job_id="$1"
  [[ -n "$job_id" ]] || return 0
  echo "    killing workflow ${job_id} to release its launcher container"
  oozie job -oozie "$OOZIE_URL" -kill "$job_id" >/dev/null 2>&1 || true
}

kill_running_job() {
  [[ -n "$RUNNING_JOB_ID" ]] || return 0
  oozie_kill_job "$RUNNING_JOB_ID"
  RUNNING_JOB_ID=""
}

# Prints why an action can be stuck: a container request larger than any single
# node's free memory stays pending forever while the cluster still reports
# plenty of memory available.
report_yarn_pressure() {
  local nm_mb max_mb tez_am_mb
  nm_mb="$(xml_prop_value "${HADOOP_CONF_DIR:-/etc/hadoop/conf}/yarn-site.xml" \
    yarn.nodemanager.resource.memory-mb 2>/dev/null || true)"
  max_mb="$(xml_prop_value "${HADOOP_CONF_DIR:-/etc/hadoop/conf}/yarn-site.xml" \
    yarn.scheduler.maximum-allocation-mb 2>/dev/null || true)"
  tez_am_mb="$(xml_prop_value "${TEZ_CONF_DIR:-/etc/tez/conf}/tez-site.xml" \
    tez.am.resource.memory.mb 2>/dev/null || true)"
  echo "---- YARN sizing (stuck actions are usually unschedulable requests) ----" >&2
  echo "    yarn.nodemanager.resource.memory-mb  = ${nm_mb:-<unknown>}" >&2
  echo "    yarn.scheduler.maximum-allocation-mb = ${max_mb:-<unknown>}" >&2
  echo "    tez.am.resource.memory.mb            = ${tez_am_mb:-<unknown>}" >&2
  if [[ "$nm_mb" =~ ^[0-9]+$ && "$tez_am_mb" =~ ^[0-9]+$ ]] && (( tez_am_mb >= nm_mb )); then
    echo "    WARN: a Tez AM needs ${tez_am_mb}MB but a NodeManager only offers ${nm_mb}MB," >&2
    echo "          so the AM only starts on a completely idle node. Lower" >&2
    echo "          tez.am.resource.memory.mb (Ambari > Tez > Configs) to well under" >&2
    echo "          the NodeManager size, or raise the NodeManager memory." >&2
  fi
  echo "    Pending applications hold no logs; check the RM UI for state=ACCEPTED." >&2
}

# run_workflow <label> <local-dir> [extra job.properties lines...]
run_workflow() {
  local label="$1" local_dir="$2"
  shift 2
  local app_dir="${app_base}/${label}"
  local job_props="${work_dir}/${label}.properties"

  echo ""
  echo "==== workflow: ${label} ===="
  echo "    local dir=$local_dir"
  echo "    HDFS app dir=$app_dir"

  hdfs dfs -mkdir -p "$app_dir" || { echo "ERROR: hdfs mkdir failed: $app_dir" >&2; return 1; }
  local f
  for f in "$local_dir"/*; do
    [[ -f "$f" ]] || continue
    hdfs dfs -put -f "$f" "${app_dir}/$(basename "$f")" \
      || { echo "ERROR: hdfs put failed: $f" >&2; return 1; }
  done

  {
    echo "nameNode=${name_node}"
    echo "resourceManager=${resource_manager}"
    echo "queueName=${OOZIE_QUEUE}"
    echo "oozie.wf.application.path=${app_dir}"
    echo "oozie.use.system.libpath=true"
    local line
    for line in "$@"; do
      echo "$line"
    done
  } >"$job_props"

  echo "---- job.properties ----"
  sed 's/^/    /' "$job_props"

  echo "---- oozie job -run ----"
  local run_out job_id
  if ! run_out="$(oozie job -oozie "$OOZIE_URL" -config "$job_props" -run 2>&1)"; then
    echo "$run_out" | sed 's/^/    /' >&2
    echo "ERROR: oozie job -run failed for workflow ${label}" >&2
    return 1
  fi
  echo "$run_out" | sed 's/^/    /'
  job_id="$(printf '%s\n' "$run_out" | awk '/^job:/ { print $NF; exit }')"
  [[ -n "$job_id" ]] || { echo "ERROR: could not parse Oozie job id for ${label}" >&2; return 1; }
  echo "    job id: $job_id"
  RUNNING_JOB_ID="$job_id"

  echo "---- wait for SUCCEEDED (timeout ${OOZIE_TIMEOUT_SECONDS}s) ----"
  local deadline status now
  deadline=$(( $(date +%s) + OOZIE_TIMEOUT_SECONDS ))
  while true; do
    status="$(oozie_job_status "$job_id" || true)"
    echo "    status=${status:-UNKNOWN}"
    case "${status}" in
      SUCCEEDED)
        RUNNING_JOB_ID=""
        echo "    OK: workflow ${label} SUCCEEDED (job $job_id)"
        return 0
        ;;
      KILLED|FAILED)
        RUNNING_JOB_ID=""
        echo "---- oozie job -info (${label}) ----" >&2
        oozie job -oozie "$OOZIE_URL" -info "$job_id" >&2 || true
        echo "---- oozie job -log tail (${label}) ----" >&2
        oozie job -oozie "$OOZIE_URL" -log "$job_id" 2>&1 | tail -40 >&2 || true
        echo "ERROR: workflow ${label} ended with status=$status (job $job_id)" >&2
        return 1
        ;;
      SUSPENDED)
        echo "---- oozie job -info (${label}) ----" >&2
        oozie job -oozie "$OOZIE_URL" -info "$job_id" >&2 || true
        echo "ERROR: workflow ${label} ended with status=$status (job $job_id)" >&2
        kill_running_job
        return 1
        ;;
    esac
    now="$(date +%s)"
    if (( now >= deadline )); then
      oozie job -oozie "$OOZIE_URL" -info "$job_id" >&2 || true
      echo "ERROR: timed out waiting for workflow ${label} (job $job_id, last status=${status:-UNKNOWN})" >&2
      report_yarn_pressure
      # Killing here matters: otherwise this run's launcher keeps a container
      # for good and every later attempt starts with less capacity.
      kill_running_job
      return 1
    fi
    sleep "$OOZIE_POLL_SECONDS"
  done
}

need_cmd curl
need_cmd kinit
need_cmd hdfs
need_cmd oozie
need_cmd python3

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""

if [[ -f "$AMBARI_CONFIG_FILE" ]]; then
  load_ambari_env_file "$AMBARI_CONFIG_FILE" || die "failed to read $AMBARI_CONFIG_FILE"
elif [[ -n "${CLUSTER_NAME:-}" ]]; then
  :
elif [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]]; then
  :
else
  die "Missing Ambari credentials. Create ${AMBARI_CONFIG_FILE} (copy from ${SCRIPT_DIR}/configs/ambari.env.example) or set AMBARI_USER and AMBARI_PASSWORD in the environment."
fi

AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://10.101.11.22:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"

if [[ -z "${CLUSTER_NAME:-}" ]]; then
  [[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]] || die "AMBARI_USER and AMBARI_PASSWORD must be set in ${AMBARI_CONFIG_FILE} or in the environment."
fi

[[ -r "$HDFS_KEYTAB" ]] || die "keytab not readable: $HDFS_KEYTAB"

if [[ -n "${CLUSTER_NAME:-}" ]]; then
  cluster="$CLUSTER_NAME"
else
  clusters_url="${AMBARI_BASE_URL%/}/api/v1/clusters/"
  json="$(curl -sS -f -u "${AMBARI_USER}:${AMBARI_PASSWORD}" -H "X-Requested-By: ambari" "$clusters_url")" \
    || die "failed to GET $clusters_url"
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

principal="hdfs-${cluster}"
echo "Using cluster: ${cluster}"
echo "kinit principal: ${principal} (realm from krb5.conf / keytab)"
kinit -kt "$HDFS_KEYTAB" "$principal" || die "kinit failed"

# Stale ~/.oozie-auth-token-* from a prior principal (e.g. oozie) makes the CLI
# submit as the wrong user and YARN then denies queue access (JA009).
rm -f "${HOME}/.oozie-auth-token-"* 2>/dev/null || true
export OOZIE_CLIENT_OPTS="${OOZIE_CLIENT_OPTS:-} -Doozie.auth.token.cache=false"

OOZIE_URL="$(resolve_oozie_url)"
name_node="$(resolve_name_node)"
resource_manager="$(resolve_resource_manager)"
app_base="${OOZIE_HDFS_APP_DIR:-/user/hdfs/oozie_smoke_$(date +%s)_$$}"

# Legacy single-directory mode.
if [[ -n "${OOZIE_WORKFLOW_DIR:-}" ]]; then
  workflows=("custom")
else
  IFS=', ' read -r -a workflows <<<"$OOZIE_WORKFLOWS"
fi

echo "---- Oozie smoke ----"
echo "    OOZIE_URL=$OOZIE_URL"
echo "    nameNode=$name_node"
echo "    resourceManager=$resource_manager"
echo "    HDFS app base=$app_base"
echo "    queue=$OOZIE_QUEUE"
echo "    workflows=${workflows[*]}"

echo "---- oozie admin -status ----"
oozie admin -oozie "$OOZIE_URL" -status || die "oozie admin -status failed"

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/oozie-smoke.XXXXXX")"
# Ctrl-C during the wait loop must not leave a launcher AM behind either.
trap 'kill_running_job; rm -rf "$work_dir"' EXIT INT TERM

pass=0
fail=0
skip=0
declare -a results=()

for wf in "${workflows[@]}"; do
  [[ -n "$wf" ]] || continue

  if [[ "$wf" == "custom" ]]; then
    wf_dir="$OOZIE_WORKFLOW_DIR"
  else
    wf_dir="${OOZIE_WORKFLOW_ROOT}/${wf}"
  fi

  if [[ ! -r "${wf_dir}/workflow.xml" ]]; then
    echo ""
    echo "==== workflow: ${wf} ===="
    echo "    SKIPPED: workflow.xml not readable under ${wf_dir}"
    skip=$((skip + 1))
    results+=("SKIPPED ${wf} (no workflow.xml)")
    continue
  fi

  extra_props=()
  if [[ "$wf" == "hive" ]]; then
    hive2_jdbc_url=""
    hive2_principal=""
    if ! resolve_hive2; then
      echo ""
      echo "==== workflow: ${wf} ===="
      echo "    SKIPPED: HiveServer2 not resolvable (set OOZIE_HIVE2_JDBC_URL / OOZIE_HIVE2_PRINCIPAL)"
      skip=$((skip + 1))
      results+=("SKIPPED ${wf} (no HiveServer2)")
      continue
    fi
    hive_db="${OOZIE_HIVE_DB:-odp_oozie_smoke_$(date +%s)}"
    extra_props+=("hive2JdbcUrl=${hive2_jdbc_url}")
    extra_props+=("hive2Principal=${hive2_principal}")
    extra_props+=("hiveDbName=${hive_db}")
    extra_props+=("hiveTableName=${OOZIE_HIVE_TABLE}")
  fi

  if run_workflow "$wf" "$wf_dir" ${extra_props[@]+"${extra_props[@]}"}; then
    pass=$((pass + 1))
    results+=("PASS    ${wf}")
  else
    fail=$((fail + 1))
    results+=("FAIL    ${wf}")
  fi
done

echo ""
echo "---- summary ----"
for r in "${results[@]}"; do
  echo "    $r"
done
echo "    PASS=${pass} FAIL=${fail} SKIPPED=${skip}"

if (( fail > 0 )); then
  die "Oozie sample smoke had ${fail} failing workflow(s)."
fi
if (( pass == 0 )); then
  die "Oozie sample smoke ran no workflows."
fi
echo "OK: Oozie sample smoke finished (PASS=${pass} SKIPPED=${skip})."
