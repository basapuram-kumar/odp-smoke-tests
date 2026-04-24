#!/usr/bin/env bash
#
# Flink on YARN smoke: HADOOP_CLASSPATH, detached yarn-session, then TopSpeedWindowing.
#
#   export HADOOP_CLASSPATH=`hadoop classpath`
#   .../flink/bin/yarn-session.sh --detached
#   .../flink/bin/flink run .../examples/streaming/TopSpeedWindowing.jar
#
# Flink home defaults to the first match of /usr/odp/3.*/flink (override with FLINK_HOME).
# kinit as flink/<FQDN> with the Flink service keytab before YARN/HDFS access so checkpoints
# and state dirs use the Flink principal (not a stale spark or other ticket in the cache).
#
# Environment (optional):
#   FLINK_KEYTAB           default /etc/security/keytabs/flink.service.keytab
#   FLINK_PRINCIPAL_HOST   host part of flink/<host> (default: hostname -f, else hostname)
#   FLINK_KINIT_SKIP       if "1", do not kinit (use only existing ccache)
#   FLINK_HOME              default: first /usr/odp/3.*/flink
#   FLINK_SMOKE_JAR         default ${FLINK_HOME}/examples/streaming/TopSpeedWindowing.jar
#   FLINK_YARN_SESSION_ARGS extra args to yarn-session.sh (default: larger TM so Flink 2.x
#                         memory model fits; override if your queue is small — see README)
#   FLINK_RUN_ARGS          extra args between "flink run" and the jar
#   FLINK_CLEANUP_SESSION     if "1" (default), yarn application -kill after run (needs app id)
#   FLINK_SESSION_START_WAIT  seconds to sleep after starting session (default 15)
#
set -euo pipefail

FLINK_CLEANUP_SESSION="${FLINK_CLEANUP_SESSION:-1}"
FLINK_SESSION_START_WAIT="${FLINK_SESSION_START_WAIT:-15}"
FLINK_KEYTAB="${FLINK_KEYTAB:-/etc/security/keytabs/flink.service.keytab}"
FLINK_KINIT_SKIP="${FLINK_KINIT_SKIP:-0}"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

resolve_flink_home() {
  if [[ -n "${FLINK_HOME:-}" ]]; then
    printf '%s' "$FLINK_HOME"
    return
  fi
  shopt -s nullglob
  local -a homes=(/usr/odp/3.*/flink)
  shopt -u nullglob
  if [[ ${#homes[@]} -eq 0 ]]; then
    die "no directory matched /usr/odp/3.*/flink; set FLINK_HOME"
  fi
  if [[ ${#homes[@]} -gt 1 ]]; then
    echo "WARN: multiple Flink installs under /usr/odp/3.*/flink; using: ${homes[0]}" >&2
  fi
  printf '%s' "${homes[0]}"
}

resolve_flink_host() {
  if [[ -n "${FLINK_PRINCIPAL_HOST:-}" ]]; then
    printf '%s' "$FLINK_PRINCIPAL_HOST"
    return
  fi
  local h
  h="$(hostname -f 2>/dev/null || true)"
  if [[ -z "$h" ]]; then
    h="$(hostname)"
  fi
  [[ -n "$h" ]] || die "could not determine FQDN for flink principal; set FLINK_PRINCIPAL_HOST"
  printf '%s' "$h"
}

need_cmd hadoop
need_cmd yarn
need_cmd kinit

FLINK_HOME="$(resolve_flink_home)"
_odp_base="$(dirname "$FLINK_HOME")"
if [[ -z "${HADOOP_CONF_DIR:-}" && -d "${_odp_base}/hadoop/conf" ]]; then
  export HADOOP_CONF_DIR="${_odp_base}/hadoop/conf"
fi

# Flink 1.17+ / 2.x: small -tm yields "Total Flink Memory" too low vs framework/network/managed mins.
_default_yarn_session_args="-s 1 -jm 1024m -tm 4096m"
FLINK_YARN_SESSION_ARGS="${FLINK_YARN_SESSION_ARGS:-$_default_yarn_session_args}"

yarn_session="${FLINK_HOME}/bin/yarn-session.sh"
flink_bin="${FLINK_HOME}/bin/flink"
FLINK_SMOKE_JAR="${FLINK_SMOKE_JAR:-${FLINK_HOME}/examples/streaming/TopSpeedWindowing.jar}"

[[ -x "$yarn_session" ]] || die "not executable: $yarn_session"
[[ -x "$flink_bin" ]] || die "not executable: $flink_bin"
[[ -r "$FLINK_SMOKE_JAR" ]] || die "smoke jar not readable: $FLINK_SMOKE_JAR"

session_log="$(mktemp)"
app_id=""
cleanup() {
  local id="$app_id"
  if [[ "${FLINK_CLEANUP_SESSION}" == "1" && -n "$id" ]]; then
    echo "---- yarn application -kill ${id} ----"
    yarn application -kill "$id" 2>/dev/null || true
  fi
  rm -f "$session_log"
}
trap cleanup EXIT

echo "FLINK_HOME=${FLINK_HOME}"
if [[ -n "${HADOOP_CONF_DIR:-}" ]]; then
  echo "HADOOP_CONF_DIR=${HADOOP_CONF_DIR}"
fi

if [[ "$FLINK_KINIT_SKIP" != "1" ]]; then
  if [[ ! -r "$FLINK_KEYTAB" ]]; then
    die "keytab not readable: $FLINK_KEYTAB (set FLINK_KEYTAB or FLINK_KINIT_SKIP=1)"
  fi
  flink_host="$(resolve_flink_host)"
  flink_principal="flink/${flink_host}"
  echo "---- kinit ${flink_principal} (Flink service keytab) ----"
  kinit -kt "$FLINK_KEYTAB" "$flink_principal" || die "kinit failed"
else
  echo "FLINK_KINIT_SKIP=1: skipping kinit (using existing credentials cache)"
fi

echo "---- export HADOOP_CLASSPATH=\$(hadoop classpath) ----"
export HADOOP_CLASSPATH
HADOOP_CLASSPATH="$(hadoop classpath)" || die "hadoop classpath failed"

echo "---- ${yarn_session} --detached ${FLINK_YARN_SESSION_ARGS} ----"
# shellcheck disable=SC2086
"$yarn_session" --detached ${FLINK_YARN_SESSION_ARGS} 2>&1 | tee "$session_log"

app_id="$(grep -oE 'application_[0-9]+_[0-9]+' "$session_log" | tail -1 || true)"

if [[ -n "$app_id" ]]; then
  echo "Detected YARN application id: ${app_id}"
else
  echo "WARN: could not parse YARN application id from yarn-session output; flink run may need a valid default session." >&2
fi

echo "---- waiting ${FLINK_SESSION_START_WAIT}s for Flink JobManager on YARN ----"
sleep "$FLINK_SESSION_START_WAIT"

echo "---- ${flink_bin} run ... TopSpeedWindowing.jar ----"
# shellcheck disable=SC2086
if [[ -n "$app_id" ]]; then
  "$flink_bin" run -t yarn-session -Dyarn.application.id="$app_id" ${FLINK_RUN_ARGS:-} "$FLINK_SMOKE_JAR"
else
  "$flink_bin" run ${FLINK_RUN_ARGS:-} "$FLINK_SMOKE_JAR"
fi

echo "OK: Flink smoke test finished."
