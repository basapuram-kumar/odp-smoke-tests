#!/usr/bin/env bash
#
# Kafka 3 broker smoke (same pattern as kafka-sample-smoke.sh / Kafka 2):
# kinit kafka/<FQDN>, KAFKA_OPTS + client JAAS, console producer/consumer for several
# topics under /usr/odp/current/kafka3-broker/. Prefers conf/kafka3_client_jaas.conf,
# falls back to conf/kafka_client_jaas.conf (Kafka 2 name).
#
# Same keytab as Kafka 2: /etc/security/keytabs/kafka.service.keytab (override KAFKA_KEYTAB).
#
# Environment (optional):
#   KAFKA_HOME           default /usr/odp/current/kafka3-broker
#   KAFKA_JAAS_CONF      if unset: kafka3_client_jaas.conf if present, else kafka_client_jaas.conf
#   KAFKA_CLIENT_CONFIG  default <script-dir>/kafka/client-sasl.properties (shared with Kafka2)
#   KAFKA_BOOTSTRAP      default $(hostname -f):6667
#   KAFKA_TOPICS         space- or comma-separated list (default: three smoke topics below)
#   KAFKA_CREATE_TOPIC   if "true", --create --if-not-exists for each topic (default false)
#   KAFKA_REPLICATION_FACTOR (default 1)
#   KAFKA_KEYTAB         default /etc/security/keytabs/kafka.service.keytab
#   KAFKA_PRINCIPAL_HOST default FQDN from hostname -f / hostname
#   KAFKA_MSGS_PER_TOPIC lines to produce per topic (default 3)
#   KAFKA_MAX_MESSAGES   max messages consumer reads per topic
#                        (default KAFKA_MSGS_PER_TOPIC; must be <= messages available
#                        or the console consumer blocks waiting for more)
#   KAFKA_CONSUMER_TIMEOUT_MS  consumer idle timeout, default 15000
#   KAFKA_STEP_TIMEOUT   hard wall-clock seconds per produce/consume, default 120
#   KAFKA_REPORT_DIR     directory for the run report (default SMOKE_REPORT_DIR or /tmp)
#
# The consumer always terminates: --max-messages is capped at what was produced,
# --timeout-ms bounds idle waits, and timeout(1) is a hard stop. The script then
# prints a per-topic report and exits (0 all topics OK, 1 otherwise).
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_HOME="${KAFKA_HOME:-/usr/odp/current/kafka3-broker}"
KAFKA_CLIENT_CONFIG="${KAFKA_CLIENT_CONFIG:-${SCRIPT_DIR}/kafka/client-sasl.properties}"
KAFKA_KEYTAB="${KAFKA_KEYTAB:-/etc/security/keytabs/kafka.service.keytab}"
KAFKA_CREATE_TOPIC="${KAFKA_CREATE_TOPIC:-false}"
KAFKA_REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"
KAFKA_MSGS_PER_TOPIC="${KAFKA_MSGS_PER_TOPIC:-3}"
KAFKA_CONSUMER_TIMEOUT_MS="${KAFKA_CONSUMER_TIMEOUT_MS:-15000}"
KAFKA_STEP_TIMEOUT="${KAFKA_STEP_TIMEOUT:-120}"
KAFKA_REPORT_DIR="${KAFKA_REPORT_DIR:-${SMOKE_REPORT_DIR:-/tmp}}"

_default_topics="kafka3-smoke-1 kafka3-smoke-2 kafka3-smoke-3"
KAFKA_TOPICS="${KAFKA_TOPICS:-$_default_topics}"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

resolve_kafka_host() {
  if [[ -n "${KAFKA_PRINCIPAL_HOST:-}" ]]; then
    printf '%s' "$KAFKA_PRINCIPAL_HOST"
    return
  fi
  local h
  h="$(hostname -f 2>/dev/null || true)"
  if [[ -z "$h" ]]; then
    h="$(hostname)"
  fi
  [[ -n "$h" ]] || die "could not determine FQDN for kafka principal; set KAFKA_PRINCIPAL_HOST"
  printf '%s' "$h"
}

resolve_bootstrap() {
  if [[ -n "${KAFKA_BOOTSTRAP:-}" ]]; then
    printf '%s' "$KAFKA_BOOTSTRAP"
    return
  fi
  local h
  h="$(hostname -f 2>/dev/null || true)"
  if [[ -z "$h" ]]; then
    h="$(hostname)"
  fi
  printf '%s' "${h}:6667"
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

parse_topics() {
  local raw="$1"
  raw="${raw//,/ }"
  read -r -a PARSED_TOPICS <<< "$raw"
  if [[ ${#PARSED_TOPICS[@]} -eq 0 ]]; then
    die "no topics after parsing KAFKA_TOPICS"
  fi
}

payload_for_topic() {
  local topic="$1"
  local n="${KAFKA_MSGS_PER_TOPIC}"
  local i out=""
  for ((i = 1; i <= n; i++)); do
    out+="${topic}-line-${i}"$'\n'
  done
  printf '%s' "$out"
}

# Wrap a step so it can never hang forever. timeout(1) is optional: without it
# the consumer still ends via --max-messages / --timeout-ms.
run_step() {
  local rc=0
  if [[ -n "$TIMEOUT_BIN" ]]; then
    set +e
    "$TIMEOUT_BIN" -k 10 "$KAFKA_STEP_TIMEOUT" "$@"
    rc=$?
    set -e
  else
    set +e
    "$@"
    rc=$?
    set -e
  fi
  return "$rc"
}

need_cmd kinit

if [[ ! -d "$KAFKA_HOME" ]]; then
  die "KAFKA_HOME is not a directory: $KAFKA_HOME"
fi

if [[ -z "${KAFKA_JAAS_CONF:-}" ]]; then
  _jaas_k3="${KAFKA_HOME}/conf/kafka3_client_jaas.conf"
  _jaas_k2="${KAFKA_HOME}/conf/kafka_client_jaas.conf"
  if [[ -f "$_jaas_k3" ]]; then
    KAFKA_JAAS_CONF="$_jaas_k3"
  elif [[ -f "$_jaas_k2" ]]; then
    KAFKA_JAAS_CONF="$_jaas_k2"
  else
    die "No JAAS under ${KAFKA_HOME}/conf (tried kafka3_client_jaas.conf, kafka_client_jaas.conf). Set KAFKA_JAAS_CONF explicitly."
  fi
fi

if [[ ! -f "$KAFKA_JAAS_CONF" ]]; then
  die "JAAS config not found: $KAFKA_JAAS_CONF (set KAFKA_JAAS_CONF if installed elsewhere)"
fi

if [[ ! -r "$KAFKA_CLIENT_CONFIG" ]]; then
  die "client properties not readable: $KAFKA_CLIENT_CONFIG"
fi

if [[ ! -r "$KAFKA_KEYTAB" ]]; then
  die "keytab not readable: $KAFKA_KEYTAB"
fi

parse_topics "$KAFKA_TOPICS"

kafka_host="$(resolve_kafka_host)"
principal="kafka/${kafka_host}"
bootstrap="$(resolve_bootstrap)"

export KAFKA_OPTS="-Djava.security.auth.login.config=${KAFKA_JAAS_CONF}"

producer="${KAFKA_HOME}/bin/kafka-console-producer.sh"
consumer="${KAFKA_HOME}/bin/kafka-console-consumer.sh"
topics="${KAFKA_HOME}/bin/kafka-topics.sh"

[[ -x "$producer" ]] || die "not executable: $producer"
[[ -x "$consumer" ]] || die "not executable: $consumer"
[[ -x "$topics" ]] || die "not executable: $topics"

# Never ask for more messages than this run produces: the console consumer
# blocks until --max-messages is reached, so a larger value hangs on a topic
# that only holds what we just wrote.
max_consume="${KAFKA_MAX_MESSAGES:-$KAFKA_MSGS_PER_TOPIC}"
if (( max_consume > KAFKA_MSGS_PER_TOPIC )); then
  echo "WARN: KAFKA_MAX_MESSAGES=${max_consume} exceeds produced ${KAFKA_MSGS_PER_TOPIC}; capping to avoid a blocked consumer"
  max_consume="$KAFKA_MSGS_PER_TOPIC"
fi

TIMEOUT_BIN=""
if command -v timeout >/dev/null 2>&1; then
  TIMEOUT_BIN="$(command -v timeout)"
else
  echo "WARN: timeout(1) not found; relying on --max-messages / --timeout-ms only"
fi

mkdir -p "$KAFKA_REPORT_DIR" 2>/dev/null || KAFKA_REPORT_DIR="/tmp"
REPORT_FILE="${KAFKA_REPORT_DIR}/kafka3-smoke-report.txt"
WORK_DIR="$(mktemp -d 2>/dev/null || echo /tmp)"
cleanup() { [[ "$WORK_DIR" == /tmp ]] || rm -rf "$WORK_DIR" 2>/dev/null || true; }
trap cleanup EXIT

echo "Kafka 3 KAFKA_HOME:  ${KAFKA_HOME}"
echo "Kafka principal:     ${principal}"
echo "Bootstrap servers:   ${bootstrap}"
echo "Topics:              ${PARSED_TOPICS[*]}"
echo "Msgs per topic:      ${KAFKA_MSGS_PER_TOPIC} (consumer reads ${max_consume} per topic)"
echo "Consumer idle limit: ${KAFKA_CONSUMER_TIMEOUT_MS} ms"
echo "Step hard timeout:   ${KAFKA_STEP_TIMEOUT}s"
echo "KAFKA_OPTS:          ${KAFKA_OPTS}"
echo "Client config file:  ${KAFKA_CLIENT_CONFIG}"
echo "Report file:         ${REPORT_FILE}"

kinit -kt "$KAFKA_KEYTAB" "$principal" || die "kinit failed"

RESULT_LINES=()
FAIL_COUNT=0
PASS_COUNT=0

for topic in "${PARSED_TOPICS[@]}"; do
  [[ -z "$topic" ]] && continue

  topic_status="PASS"
  topic_note=""

  if [[ "${KAFKA_CREATE_TOPIC}" == "true" ]]; then
    echo "---- kafka-topics --create (if-not-exists) ${topic} ----"
    if ! run_step "$topics" --bootstrap-server "$bootstrap" \
      --command-config "$KAFKA_CLIENT_CONFIG" \
      --create --if-not-exists --topic "$topic" \
      --partitions 1 --replication-factor "$KAFKA_REPLICATION_FACTOR"; then
      topic_status="FAIL"
      topic_note="topic create failed"
    fi
  fi

  if [[ "$topic_status" == "PASS" ]]; then
    payload="$(payload_for_topic "$topic")"
    echo "---- kafka-console-producer topic=${topic} (${KAFKA_MSGS_PER_TOPIC} lines) ----"
    if ! printf '%s' "$payload" | run_step "$producer" \
      --topic "$topic" \
      --bootstrap-server "$bootstrap" \
      --producer.config "$KAFKA_CLIENT_CONFIG"; then
      topic_status="FAIL"
      topic_note="producer failed or timed out"
    fi
  fi

  if [[ "$topic_status" == "PASS" ]]; then
    echo "---- kafka-console-consumer topic=${topic} (--from-beginning, --max-messages ${max_consume}, --timeout-ms ${KAFKA_CONSUMER_TIMEOUT_MS}) ----"
    out_file="${WORK_DIR}/consume-${topic}.out"
    consume_rc=0
    run_step "$consumer" \
      --topic "$topic" \
      --bootstrap-server "$bootstrap" \
      --consumer.config "$KAFKA_CLIENT_CONFIG" \
      --from-beginning \
      --max-messages "$max_consume" \
      --timeout-ms "$KAFKA_CONSUMER_TIMEOUT_MS" >"$out_file" 2>&1 || consume_rc=$?
    cat "$out_file"

    got="$(grep -c -- "^${topic}-line-" "$out_file" 2>/dev/null || true)"
    got="${got:-0}"
    echo "[INFO] topic=${topic} consumed ${got}/${max_consume} message(s) rc=${consume_rc}"

    if (( got >= max_consume )); then
      # A non-zero rc here is only the idle timeout or the hard kill firing
      # after the messages were already read, which is not a failure.
      topic_status="PASS"
      if (( consume_rc != 0 )); then
        topic_note="consumed ${got}/${max_consume}; consumer exited rc=${consume_rc} after reading"
      else
        topic_note="consumed ${got}/${max_consume}"
      fi
    else
      topic_status="FAIL"
      if (( consume_rc == 124 )); then
        topic_note="consumer hard timeout after ${KAFKA_STEP_TIMEOUT}s; consumed ${got}/${max_consume}"
      else
        topic_note="consumed only ${got}/${max_consume} (rc=${consume_rc})"
      fi
    fi
  fi

  if [[ "$topic_status" == "PASS" ]]; then
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
  RESULT_LINES+=("${topic_status}  ${topic}  ${topic_note}")
  echo "--- done topic: ${topic} (${topic_status}) ---"
done

{
  echo "Kafka 3 smoke report"
  echo "generated: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "bootstrap: ${bootstrap}"
  echo "principal: ${principal}"
  echo "msgs per topic: ${KAFKA_MSGS_PER_TOPIC} (consumed ${max_consume})"
  echo ""
  printf '%s\n' "${RESULT_LINES[@]}"
  echo ""
  echo "PASS: ${PASS_COUNT}  FAIL: ${FAIL_COUNT}  TOTAL: $((PASS_COUNT + FAIL_COUNT))"
} >"$REPORT_FILE" 2>/dev/null || true

echo ""
echo "==== Kafka 3 smoke report ===="
printf '%s\n' "${RESULT_LINES[@]}"
echo "PASS: ${PASS_COUNT}  FAIL: ${FAIL_COUNT}  TOTAL: $((PASS_COUNT + FAIL_COUNT))"
echo "report: ${REPORT_FILE}"
echo "=============================="

if (( FAIL_COUNT > 0 )); then
  echo "FAIL: Kafka 3 smoke finished with ${FAIL_COUNT} failed topic(s)."
  exit 1
fi

echo "OK: Kafka 3 sample producer/consumer finished for ${PASS_COUNT} topic(s)."
exit 0
