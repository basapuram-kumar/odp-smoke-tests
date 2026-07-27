#!/usr/bin/env bash
#
# Smoke: Apache Airflow health + trigger a BashOperator DAG to success.
#
# Steps:
#   1) Optional Ambari discovery of AIRFLOW_WEBSERVER / base URL
#   2) Optional kinit as airflow-<cluster> (headless keytab)
#   3) GET /api/v1/health (metadatabase + scheduler healthy)
#   4) Install sample DAG into AIRFLOW_HOME/dags (from airflow/odp_airflow_smoke_dag.py)
#   5) airflow dags unpause + trigger, wait until dag run state=success
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   AIRFLOW_ENV_FILE / AIRFLOW_CONFIG_FILE  default <script-dir>/configs/airflow.env
#   AIRFLOW_BASE_URL         e.g. http://host:8889 (health check; skip Ambari when set)
#   AIRFLOW_HOME             default /usr/odp/current/airflow or discovered from systemd
#   AIRFLOW_CONFIG           default $AIRFLOW_HOME/airflow.cfg
#   AIRFLOW_BIN              default $AIRFLOW_HOME/bin/airflow
#   AIRFLOW_VENV_ACTIVATE    default $AIRFLOW_HOME/bin/activate
#   AIRFLOW_DAG_ID           default odp_airflow_smoke
#   AIRFLOW_DAG_FILE         default <script-dir>/airflow/odp_airflow_smoke_dag.py
#   AIRFLOW_TIMEOUT_SECONDS  default 300
#   AIRFLOW_POLL_SECONDS     default 3
#   AIRFLOW_SKIP_KINIT       default 0
#   AIRFLOW_KEYTAB           default /etc/security/keytabs/airflow.headless.keytab
#   AIRFLOW_KEEP_DAG_FILE    default 1 - leave DAG in dags/ after smoke
#   CURL_EXTRA_OPTS          e.g. -k
#
# Usage:
#   sudo ./airflow-sample-smoke.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
AIRFLOW_ENV_FILE="${AIRFLOW_ENV_FILE:-${AIRFLOW_CONFIG_FILE:-${SCRIPT_DIR}/configs/airflow.env}}"
AIRFLOW_DAG_FILE="${AIRFLOW_DAG_FILE:-${SCRIPT_DIR}/airflow/odp_airflow_smoke_dag.py}"
AIRFLOW_KEYTAB="${AIRFLOW_KEYTAB:-/etc/security/keytabs/airflow.headless.keytab}"

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

_cfg_AIRFLOW_BASE_URL=""
_cfg_AIRFLOW_HOME=""

load_airflow_env_file() {
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
      AIRFLOW_BASE_URL) _cfg_AIRFLOW_BASE_URL="$val" ;;
      AIRFLOW_HOME) _cfg_AIRFLOW_HOME="$val" ;;
      AIRFLOW_DAG_ID) [[ "${AIRFLOW_DAG_ID+set}" == "set" ]] || AIRFLOW_DAG_ID="$val" ;;
      AIRFLOW_SKIP_KINIT) [[ "${AIRFLOW_SKIP_KINIT+set}" == "set" ]] || AIRFLOW_SKIP_KINIT="$val" ;;
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

load_airflow_env_file "$AIRFLOW_ENV_FILE" || die "failed to read $AIRFLOW_ENV_FILE"
AIRFLOW_BASE_URL="${AIRFLOW_BASE_URL:-${_cfg_AIRFLOW_BASE_URL:-}}"
AIRFLOW_HOME="${AIRFLOW_HOME:-${_cfg_AIRFLOW_HOME:-}}"
AIRFLOW_DAG_ID="${AIRFLOW_DAG_ID:-odp_airflow_smoke}"
AIRFLOW_TIMEOUT_SECONDS="${AIRFLOW_TIMEOUT_SECONDS:-300}"
AIRFLOW_POLL_SECONDS="${AIRFLOW_POLL_SECONDS:-3}"
AIRFLOW_SKIP_KINIT="${AIRFLOW_SKIP_KINIT:-0}"
AIRFLOW_KEEP_DAG_FILE="${AIRFLOW_KEEP_DAG_FILE:-1}"

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""

if [[ -z "${AIRFLOW_BASE_URL:-}" || -z "${CLUSTER_NAME:-}" ]]; then
  if [[ -n "${CLUSTER_NAME:-}" && -n "${AIRFLOW_BASE_URL:-}" ]]; then
    :
  elif [[ -f "$AMBARI_CONFIG_FILE" ]]; then
    load_ambari_env_file "$AMBARI_CONFIG_FILE" || die "failed to read $AMBARI_CONFIG_FILE"
  elif [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]]; then
    :
  elif [[ -n "${AIRFLOW_BASE_URL:-}" && "${AIRFLOW_SKIP_KINIT}" == "1" ]]; then
    :
  else
    die "Missing Ambari context. Create ${AMBARI_CONFIG_FILE}, or set AMBARI_USER+AMBARI_PASSWORD, or set AIRFLOW_BASE_URL and CLUSTER_NAME / AIRFLOW_SKIP_KINIT=1."
  fi
fi

AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://127.0.0.1:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"

if [[ -z "${AIRFLOW_BASE_URL:-}" || ( -z "${CLUSTER_NAME:-}" && "${AIRFLOW_SKIP_KINIT}" != "1" ) ]]; then
  if [[ -z "${AIRFLOW_BASE_URL:-}" || -z "${CLUSTER_NAME:-}" ]]; then
    [[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]] || die "Ambari credentials required when AIRFLOW_BASE_URL / CLUSTER_NAME are incomplete."
  fi
fi

[[ -r "$AIRFLOW_DAG_FILE" ]] || die "DAG file not readable: $AIRFLOW_DAG_FILE"

resolve_airflow_home() {
  if [[ -n "${AIRFLOW_HOME:-}" ]]; then
    printf '%s' "$AIRFLOW_HOME"
    return
  fi
  if [[ -d /usr/odp/current/airflow && -x /usr/odp/current/airflow/bin/airflow ]]; then
    printf '%s' /usr/odp/current/airflow
    return
  fi
  local env_line home
  env_line="$(systemctl show -p Environment airflow-webserver.service 2>/dev/null || true)"
  home="$(printf '%s\n' "$env_line" | python3 -c "
import sys,re
s=sys.stdin.read()
m=re.search(r'AIRFLOW_HOME=([^\\s]+)', s)
print(m.group(1) if m else '')
")"
  if [[ -n "$home" && -d "$home" ]]; then
    printf '%s' "$home"
    return
  fi
  # Fallback: newest /usr/odp/*/airflow with bin/airflow
  home="$(ls -1d /usr/odp/*/airflow 2>/dev/null | tail -1 || true)"
  if [[ -n "$home" && -x "${home}/bin/airflow" ]]; then
    printf '%s' "$home"
    return
  fi
  die "Could not resolve AIRFLOW_HOME; set AIRFLOW_HOME"
}

AIRFLOW_HOME="$(resolve_airflow_home)"
AIRFLOW_CONFIG="${AIRFLOW_CONFIG:-${AIRFLOW_HOME}/airflow.cfg}"
AIRFLOW_BIN="${AIRFLOW_BIN:-${AIRFLOW_HOME}/bin/airflow}"
AIRFLOW_VENV_ACTIVATE="${AIRFLOW_VENV_ACTIVATE:-${AIRFLOW_HOME}/bin/activate}"

[[ -x "$AIRFLOW_BIN" ]] || die "airflow binary not executable: $AIRFLOW_BIN"
[[ -r "$AIRFLOW_CONFIG" ]] || die "airflow.cfg not readable: $AIRFLOW_CONFIG"

cluster=""
if [[ -n "${CLUSTER_NAME:-}" ]]; then
  cluster="$CLUSTER_NAME"
elif [[ "${AIRFLOW_SKIP_KINIT}" != "1" ]] || [[ -z "${AIRFLOW_BASE_URL:-}" ]]; then
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

discover_airflow_base_url() {
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
ws = dc.get("airflow-webserver-site") or {}
tag = ws.get("tag")
props = {}
if tag:
    j2 = curl_json(
        f"{ambari}/api/v1/clusters/{qc}/configurations?type=airflow-webserver-site&tag={urllib.parse.quote(tag)}"
    )
    items = j2.get("items") or []
    if items:
        props = items[0].get("properties") or {}
port = (props.get("web_server_port") or props.get("airflow.webserver.web_server_port") or "8889").strip()
base = (props.get("base_url") or "").strip()
if base:
    print(base.rstrip("/"))
    sys.exit(0)

hc = curl_json(
    f"{ambari}/api/v1/clusters/{qc}/host_components"
    f"?HostRoles/component_name=AIRFLOW_WEBSERVER"
    f"&fields=HostRoles/host_name,HostRoles/public_host_name,HostRoles/state"
)
host = None
for it in hc.get("items") or []:
    hr = it.get("HostRoles") or {}
    host = hr.get("public_host_name") or hr.get("host_name")
    if (hr.get("state") or "").upper() == "STARTED" and host:
        break
if not host:
    sys.stderr.write("No AIRFLOW_WEBSERVER host in Ambari; set AIRFLOW_BASE_URL\n")
    sys.exit(2)
print(f"http://{host}:{port}".rstrip("/"))
PY
}

if [[ -n "${AIRFLOW_BASE_URL:-}" ]]; then
  airflow_base="${AIRFLOW_BASE_URL%/}"
else
  [[ -n "$cluster" ]] || die "cluster name required for Ambari Airflow URL discovery"
  airflow_base="$(discover_airflow_base_url)" || die "Could not discover Airflow URL; set AIRFLOW_BASE_URL."
fi

echo "---- Airflow sample smoke ----"
echo "    AIRFLOW_HOME=$AIRFLOW_HOME"
echo "    Airflow URL: $airflow_base"
echo "    DAG id: $AIRFLOW_DAG_ID"
[[ -n "$cluster" ]] && echo "    cluster: $cluster"

if [[ "$AIRFLOW_SKIP_KINIT" == "1" ]]; then
  echo "---- AIRFLOW_SKIP_KINIT=1 - skipping kinit ----"
else
  need_cmd kinit
  [[ -n "$cluster" ]] || die "CLUSTER_NAME required for kinit (or set AIRFLOW_SKIP_KINIT=1)"
  [[ -r "$AIRFLOW_KEYTAB" ]] || die "keytab not readable: $AIRFLOW_KEYTAB"
  principal="airflow-${cluster}"
  echo "kinit principal: ${principal}"
  kinit -kt "$AIRFLOW_KEYTAB" "$principal" || die "kinit failed"
fi

echo "---- health ----"
# shellcheck disable=SC2086
health_json="$(curl -sS -f ${CURL_EXTRA_OPTS:-} "${airflow_base}/api/v1/health")" || die "health endpoint failed: ${airflow_base}/api/v1/health"
printf '%s\n' "$health_json" | python3 -c "
import json, sys
d=json.load(sys.stdin)
md=(d.get('metadatabase') or {}).get('status')
sch=(d.get('scheduler') or {}).get('status')
print('metadatabase=%s scheduler=%s' % (md, sch))
if md != 'healthy' or sch != 'healthy':
    sys.exit('Airflow health not OK: metadatabase=%s scheduler=%s' % (md, sch))
" || die "Airflow health check failed"

echo "---- stage DAG ----"
dags_dir="${AIRFLOW_HOME}/dags"
mkdir -p "$dags_dir"
target_dag="${dags_dir}/$(basename "$AIRFLOW_DAG_FILE")"
cp -f "$AIRFLOW_DAG_FILE" "$target_dag"
chmod 644 "$target_dag"
# Prefer airflow ownership when running as root
if id airflow >/dev/null 2>&1; then
  chown airflow:airflow "$target_dag" 2>/dev/null || true
fi
echo "Installed $target_dag"

run_airflow() {
  # Activate venv without breaking nounset (PS1 unbound)
  set +u
  # shellcheck disable=SC1090
  source "$AIRFLOW_VENV_ACTIVATE"
  set -u
  export AIRFLOW_HOME AIRFLOW_CONFIG
  "$AIRFLOW_BIN" "$@"
}

echo "---- wait for DAG in dag bag ----"
deadline=$(( $(date +%s) + AIRFLOW_TIMEOUT_SECONDS ))
while true; do
  if run_airflow dags list -o plain 2>/dev/null | awk '{print $1}' | grep -qx "$AIRFLOW_DAG_ID"; then
    echo "DAG visible: $AIRFLOW_DAG_ID"
    break
  fi
  if (( $(date +%s) >= deadline )); then
    run_airflow dags list -o plain 2>&1 | tail -40 || true
    die "timed out waiting for DAG $AIRFLOW_DAG_ID to appear"
  fi
  sleep "$AIRFLOW_POLL_SECONDS"
done

# Best-effort reserialize / unpause
run_airflow dags reserialize >/dev/null 2>&1 || true
run_airflow dags unpause "$AIRFLOW_DAG_ID" || die "failed to unpause $AIRFLOW_DAG_ID"

run_id="manual__smoke_$(date +%s)"
echo "---- trigger $AIRFLOW_DAG_ID run_id=$run_id ----"
run_airflow dags trigger "$AIRFLOW_DAG_ID" --run-id "$run_id" || die "trigger failed"

echo "---- wait for success (timeout ${AIRFLOW_TIMEOUT_SECONDS}s) ----"
deadline=$(( $(date +%s) + AIRFLOW_TIMEOUT_SECONDS ))
while true; do
  line="$(run_airflow dags list-runs -d "$AIRFLOW_DAG_ID" -o plain 2>/dev/null | grep -F "$run_id" | head -1 || true)"
  echo "    ${line:-"(no row yet)"}"
  state="$(printf '%s\n' "$line" | awk '{print $3}')"
  case "${state}" in
    success)
      echo "OK: Airflow sample smoke finished (dag=$AIRFLOW_DAG_ID run_id=$run_id)."
      if [[ "$AIRFLOW_KEEP_DAG_FILE" != "1" ]]; then
        rm -f "$target_dag"
        echo "Removed $target_dag"
      fi
      exit 0
      ;;
    failed|upstream_failed)
      run_airflow tasks states-for-dag-run "$AIRFLOW_DAG_ID" "$run_id" -o plain 2>&1 || true
      die "Airflow dag run failed (dag=$AIRFLOW_DAG_ID run_id=$run_id state=$state)"
      ;;
  esac
  if (( $(date +%s) >= deadline )); then
    run_airflow tasks states-for-dag-run "$AIRFLOW_DAG_ID" "$run_id" -o plain 2>&1 || true
    die "timed out waiting for Airflow dag run $run_id"
  fi
  sleep "$AIRFLOW_POLL_SECONDS"
done
