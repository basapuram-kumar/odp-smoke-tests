#!/usr/bin/env bash
#
# Run every top-level ODP smoke test and print one consolidated summary.
#
# Run this script as root on a cluster node. Child scripts load their own
# configs and environment; this runner deliberately does not duplicate that.
#
# Environment:
#   SMOKE_SCRIPTS       comma/space list to run (alias for SMOKE_ONLY)
#   SMOKE_ONLY          comma/space list to run, with or without .sh
#   SMOKE_SKIP          comma/space list to exclude, with or without .sh
#   SMOKE_CONTINUE      1 (default) keeps running after failures; 0 stops early
#   SMOKE_REPORT_DIR    report directory (default reports/smoke-YYYYMMDD-HHMMSS)
#   SMOKE_TIMEOUT_SECONDS
#                       optional per-script timeout when timeout(1) is available
#
# Short names are accepted. For example, "knox" selects
# knox-sample-smoke.sh and "spark-3.5.5" selects its Pi smoke script.
#
# Usage:
#   ./run-all-smoke.sh
#   SMOKE_ONLY=knox,ozone,zookeeper ./run-all-smoke.sh
#   SMOKE_SKIP="flink spark-3.3.3" ./run-all-smoke.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
START_EPOCH="$(date +%s)"
STARTED="$(date '+%Y-%m-%d %H:%M:%S %z')"
RUN_STAMP="$(date '+%Y%m%d-%H%M%S')"

SMOKE_CONTINUE="${SMOKE_CONTINUE:-1}"
SMOKE_REPORT_DIR="${SMOKE_REPORT_DIR:-reports/smoke-${RUN_STAMP}}"
SMOKE_TIMEOUT_SECONDS="${SMOKE_TIMEOUT_SECONDS:-}"
ONLY_FILTER="${SMOKE_ONLY:-${SMOKE_SCRIPTS:-}}"
SKIP_FILTER="${SMOKE_SKIP:-}"

case "$SMOKE_CONTINUE" in
  0|1) ;;
  *)
    echo "ERROR: SMOKE_CONTINUE must be 0 or 1" >&2
    exit 2
    ;;
esac

if [[ -n "$SMOKE_TIMEOUT_SECONDS" && ! "$SMOKE_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  echo "ERROR: SMOKE_TIMEOUT_SECONDS must be a positive integer" >&2
  exit 2
fi

if [[ "$SMOKE_REPORT_DIR" = /* ]]; then
  REPORT_DIR="$SMOKE_REPORT_DIR"
  REPORT_DISPLAY="$SMOKE_REPORT_DIR"
else
  REPORT_DIR="${SCRIPT_DIR}/${SMOKE_REPORT_DIR#./}"
  REPORT_DISPLAY="${SMOKE_REPORT_DIR#./}"
fi
mkdir -p "$REPORT_DIR"

# Keep this list explicit: order is stable and helpers/manual scripts cannot
# accidentally enter a run merely because their filename ends in .sh.
SMOKE_TESTS=(
  # Hadoop storage, scheduling, SQL, and coordination.
  hdfs-headless-smoke.sh
  yarn-sample-smoke.sh
  hive-sample-smoke.sh
  hive-spark2-compat-smoke.sh
  hbase-sample-smoke.sh
  oozie-sample-smoke.sh
  zookeeper-sample-smoke.sh

  # Streaming, registry, and compute engines.
  kafka-sample-smoke.sh
  kafka3-sample-smoke.sh
  schema-registry-sample-smoke.sh
  spark-sample-smoke.sh
  spark2-pi-sample-smoke.sh
  spark-3.3.3-pi-sample-smoke.sh
  spark-3.5.1-pi-sample-smoke.sh
  spark-3.5.5-pi-sample-smoke.sh
  flink-sample-smoke.sh

  # Data stores and query engines.
  kudu-sample-smoke.sh
  impala-sample-smoke.sh
  sqoop-smoke-test.sh
  clickhouse-sample-smoke.sh
  druid-sample-smoke.sh
  ozone-sample-smoke.sh

  # Web services, gateways, and operational integrations.
  zeppelin-editors-smoke.sh
  airflow-sample-smoke.sh
  nifi-sample-smoke.sh
  nifi-registry-sample-smoke.sh
  jupyterhub-sample-smoke.sh
  knox-sample-smoke.sh
  infra-solr-sample-smoke.sh
  ranger-plugin-connection-smoke.sh
)

script_matches_token() {
  local script="$1" token="$2" base alias
  token="${token#./}"
  token="${token%.sh}"
  base="${script%.sh}"

  [[ "$token" == "$base" ]] && return 0

  for alias in \
    "${base%-pi-sample-smoke}" \
    "${base%-sample-smoke}" \
    "${base%-smoke-test}" \
    "${base%-smoke}"; do
    [[ "$token" == "$alias" ]] && return 0
  done
  return 1
}

script_matches_list() {
  local script="$1" list="$2" token
  [[ -n "$list" ]] || return 1
  while IFS= read -r token; do
    [[ -n "$token" ]] || continue
    script_matches_token "$script" "$token" && return 0
  done < <(printf '%s\n' "$list" | tr ',[:space:]' '\n')
  return 1
}

SELECTED_TESTS=()
for script in "${SMOKE_TESTS[@]}"; do
  if [[ -n "$ONLY_FILTER" ]] && ! script_matches_list "$script" "$ONLY_FILTER"; then
    continue
  fi
  if [[ -n "$SKIP_FILTER" ]] && script_matches_list "$script" "$SKIP_FILTER"; then
    continue
  fi
  SELECTED_TESTS+=("$script")
done

if (( ${#SELECTED_TESTS[@]} == 0 )); then
  echo "ERROR: no smoke scripts matched SMOKE_ONLY/SMOKE_SCRIPTS and SMOKE_SKIP" >&2
  exit 2
fi

if [[ -n "$SMOKE_TIMEOUT_SECONDS" ]] && ! command -v timeout >/dev/null 2>&1; then
  echo "WARN: timeout command not found; SMOKE_TIMEOUT_SECONDS is ignored" >&2
fi

declare -a RESULT_SCRIPTS=()
declare -a RESULT_EXITS=()
declare -a RESULT_PASSES=()
declare -a RESULT_FAILS=()
declare -a RESULT_SKIPS=()
declare -a RESULT_SECONDS=()

script_ok=0
script_fail=0
checks_pass=0
checks_fail=0
checks_skip=0
checks_parsed=0

parse_metric() {
  local line="$1" name="$2"
  printf '%s\n' "$line" | sed -nE "s/.*${name}=([0-9]+).*/\\1/p"
}

for script in "${SELECTED_TESTS[@]}"; do
  echo ""
  echo "==== ${script} ===="

  log_file="${REPORT_DIR}/${script}.log"
  child_start="$(date +%s)"
  rc=0

  if [[ ! -f "${SCRIPT_DIR}/${script}" ]]; then
    echo "ERROR: smoke script not found: ${SCRIPT_DIR}/${script}" | tee "$log_file"
    rc=127
  elif [[ ! -x "${SCRIPT_DIR}/${script}" ]]; then
    echo "ERROR: smoke script is not executable: ${SCRIPT_DIR}/${script}" | tee "$log_file"
    rc=126
  else
    command_prefix=()
    if [[ -n "$SMOKE_TIMEOUT_SECONDS" ]] && command -v timeout >/dev/null 2>&1; then
      command_prefix=(timeout "$SMOKE_TIMEOUT_SECONDS")
    fi

    set +e
    # Child stdin is /dev/null so teed runs never inherit a TTY/pipe that an
    # interactive tool (e.g. hbase shell without -n) would wait on. Smoke
    # scripts that need stdin must open their own file/heredoc explicitly.
    (
      cd "$SCRIPT_DIR"
      "${command_prefix[@]}" "./${script}" </dev/null
    ) 2>&1 | tee "$log_file"
    pipeline_status=("${PIPESTATUS[@]}")
    set -e
    rc="${pipeline_status[0]}"
  fi

  child_end="$(date +%s)"
  seconds=$((child_end - child_start))

  if (( rc == 124 )) && [[ -n "$SMOKE_TIMEOUT_SECONDS" ]]; then
    echo "ERROR: ${script} timed out after ${SMOKE_TIMEOUT_SECONDS} seconds" | tee -a "$log_file"
  fi

  summary_line="$(awk '/PASS=[0-9]+/ && /FAIL=[0-9]+/ && /(SKIPPED|SKIP)=[0-9]+/ { line=$0 } END { print line }' "$log_file")"
  parsed_pass="$(parse_metric "$summary_line" PASS)"
  parsed_fail="$(parse_metric "$summary_line" FAIL)"
  parsed_skip="$(parse_metric "$summary_line" SKIPPED)"
  if [[ -z "$parsed_skip" ]]; then
    parsed_skip="$(parse_metric "$summary_line" SKIP)"
  fi

  display_pass="${parsed_pass:--}"
  display_fail="${parsed_fail:--}"
  display_skip="${parsed_skip:--}"
  if [[ -n "$parsed_pass" && -n "$parsed_fail" && -n "$parsed_skip" ]]; then
    checks_pass=$((checks_pass + parsed_pass))
    checks_fail=$((checks_fail + parsed_fail))
    checks_skip=$((checks_skip + parsed_skip))
    checks_parsed=$((checks_parsed + 1))
  fi

  RESULT_SCRIPTS+=("$script")
  RESULT_EXITS+=("$rc")
  RESULT_PASSES+=("$display_pass")
  RESULT_FAILS+=("$display_fail")
  RESULT_SKIPS+=("$display_skip")
  RESULT_SECONDS+=("$seconds")

  if (( rc == 0 )); then
    script_ok=$((script_ok + 1))
  else
    script_fail=$((script_fail + 1))
    if [[ "$SMOKE_CONTINUE" == "0" ]]; then
      echo "WARN: stopping after ${script} because SMOKE_CONTINUE=0"
      break
    fi
  fi
done

FINISH_EPOCH="$(date +%s)"
FINISHED="$(date '+%Y-%m-%d %H:%M:%S %z')"
TOTAL_SECONDS=$((FINISH_EPOCH - START_EPOCH))

print_summary() {
  local i
  echo ""
  echo "==== smoke summary ===="
  echo "started:  ${STARTED}"
  echo "finished: ${FINISHED}"
  echo "elapsed:  ${TOTAL_SECONDS} seconds"
  echo ""
  printf '%-38s %4s %5s %4s %4s %8s\n' "SCRIPT" "EXIT" "PASS" "FAIL" "SKIP" "SECONDS"
  for ((i = 0; i < ${#RESULT_SCRIPTS[@]}; i++)); do
    printf '%-38s %4s %5s %4s %4s %8s\n' \
      "${RESULT_SCRIPTS[$i]}" \
      "${RESULT_EXITS[$i]}" \
      "${RESULT_PASSES[$i]}" \
      "${RESULT_FAILS[$i]}" \
      "${RESULT_SKIPS[$i]}" \
      "${RESULT_SECONDS[$i]}"
  done
  echo ""
  echo "TOTALS: scripts=${#RESULT_SCRIPTS[@]} ok=${script_ok} fail=${script_fail}  (checks PASS=${checks_pass} FAIL=${checks_fail} SKIPPED=${checks_skip} from ${checks_parsed} parsed scripts)"
  echo "logs: ${REPORT_DISPLAY%/}/"
}

print_summary | tee "${REPORT_DIR}/summary.txt"

if (( script_fail > 0 )); then
  exit 1
fi
exit 0
