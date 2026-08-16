#!/usr/bin/env bash
#
# kinit as kafka/<FQDN>, set KAFKA_OPTS for client JAAS, run a short console
# producer then consumer (same pattern as /usr/odp/current/kafka-broker/).
#
# Environment (optional):
#   KAFKA_HOME           default /usr/odp/current/kafka-broker
#   KAFKA_JAAS_CONF      default ${KAFKA_HOME}/conf/kafka_client_jaas.conf
#   KAFKA_CLIENT_CONFIG  default <script-dir>/kafka/client-sasl.properties
#   KAFKA_BOOTSTRAP      default $(hostname -f):6667 (override if brokers differ)
#   KAFKA_TOPIC          default test1
#   KAFKA_CREATE_TOPIC     if "true", create topic before produce (default false)
#   KAFKA_REPLICATION_FACTOR used with create (default 1)
#   KAFKA_KEYTAB         default /etc/security/keytabs/kafka.service.keytab
#   KAFKA_PRINCIPAL_HOST default FQDN from hostname -f / hostname
#   KAFKA_MAX_MESSAGES   messages the consumer reads (capped at what is produced,
#                        otherwise the console consumer blocks waiting for more)
#   KAFKA_CONSUMER_TIMEOUT_MS  consumer idle timeout, default 15000
#   KAFKA_STEP_TIMEOUT   hard wall-clock seconds per produce/consume, default 120
#
# The consumer always terminates, then the script prints a report and exits
# (0 when the expected messages were read, 1 otherwise).
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_HOME="${KAFKA_HOME:-/usr/odp/current/kafka-broker}"
KAFKA_JAAS_CONF="${KAFKA_JAAS_CONF:-${KAFKA_HOME}/conf/kafka_client_jaas.conf}"
KAFKA_CLIENT_CONFIG="${KAFKA_CLIENT_CONFIG:-${SCRIPT_DIR}/kafka/client-sasl.properties}"
KAFKA_KEYTAB="${KAFKA_KEYTAB:-/etc/security/keytabs/kafka.service.keytab}"
KAFKA_TOPIC="${KAFKA_TOPIC:-test1}"
KAFKA_CREATE_TOPIC="${KAFKA_CREATE_TOPIC:-false}"
KAFKA_REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"
KAFKA_CONSUMER_TIMEOUT_MS="${KAFKA_CONSUMER_TIMEOUT_MS:-15000}"
KAFKA_STEP_TIMEOUT="${KAFKA_STEP_TIMEOUT:-120}"

TIMEOUT_BIN=""
if command -v timeout >/dev/null 2>&1; then
  TIMEOUT_BIN="$(command -v timeout)"
fi

die() {
  echo "ERROR: $*" >&2
  exit 1
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

need_cmd kinit

if [[ ! -d "$KAFKA_HOME" ]]; then
  die "KAFKA_HOME is not a directory: $KAFKA_HOME"
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

echo "Kafka principal:     ${principal}"
echo "Bootstrap servers:   ${bootstrap}"
echo "Topic:               ${KAFKA_TOPIC}"
echo "KAFKA_OPTS:          ${KAFKA_OPTS}"
echo "Client config file:  ${KAFKA_CLIENT_CONFIG}"

kinit -kt "$KAFKA_KEYTAB" "$principal" || die "kinit failed"

if [[ "${KAFKA_CREATE_TOPIC}" == "true" ]]; then
  echo "---- kafka-topics --create (if-not-exists) ----"
  "$topics" --bootstrap-server "$bootstrap" --command-config "$KAFKA_CLIENT_CONFIG" \
    --create --if-not-exists --topic "$KAFKA_TOPIC" \
    --partitions 1 --replication-factor "$KAFKA_REPLICATION_FACTOR" || die "topic create failed (check ACLs / replication factor)"
fi

# Same payload shape as your manual test (6 messages).
PAYLOAD=$'1\n2\n3\n4\n4\n5\n'
MSG_COUNT=6

echo "---- kafka-console-producer (${MSG_COUNT} lines) ----"
printf '%s' "$PAYLOAD" | run_step "$producer" \
  --topic "$KAFKA_TOPIC" \
  --bootstrap-server "$bootstrap" \
  --producer.config "$KAFKA_CLIENT_CONFIG" \
  || die "producer failed or timed out"

# The console consumer blocks until --max-messages is reached, so asking for
# more than the topic holds hangs the run. Cap it and bound idle waits.
KAFKA_MAX_MESSAGES="${KAFKA_MAX_MESSAGES:-$MSG_COUNT}"
if (( KAFKA_MAX_MESSAGES > MSG_COUNT )); then
  echo "WARN: KAFKA_MAX_MESSAGES=${KAFKA_MAX_MESSAGES} exceeds produced ${MSG_COUNT}; capping to avoid a blocked consumer"
  KAFKA_MAX_MESSAGES="$MSG_COUNT"
fi

echo "---- kafka-console-consumer (--from-beginning, --max-messages ${KAFKA_MAX_MESSAGES}, --timeout-ms ${KAFKA_CONSUMER_TIMEOUT_MS}) ----"
echo "(If the topic already had data, you will see older messages first.)"

consume_out="$(mktemp 2>/dev/null || echo /tmp/kafka-smoke-consume.out)"
consume_rc=0
run_step "$consumer" \
  --topic "$KAFKA_TOPIC" \
  --bootstrap-server "$bootstrap" \
  --consumer.config "$KAFKA_CLIENT_CONFIG" \
  --from-beginning \
  --max-messages "$KAFKA_MAX_MESSAGES" \
  --timeout-ms "$KAFKA_CONSUMER_TIMEOUT_MS" >"$consume_out" 2>&1 || consume_rc=$?
cat "$consume_out"

got="$(grep -c -E '^[0-9]+$' "$consume_out" 2>/dev/null || true)"
got="${got:-0}"
rm -f "$consume_out" 2>/dev/null || true

echo ""
echo "==== Kafka smoke report ===="
echo "bootstrap: ${bootstrap}"
echo "topic:     ${KAFKA_TOPIC}"
echo "produced:  ${MSG_COUNT}"
echo "consumed:  ${got}/${KAFKA_MAX_MESSAGES} (consumer rc=${consume_rc})"

if (( got >= KAFKA_MAX_MESSAGES )); then
  echo "result:    PASS"
  echo "==========================="
  echo "OK: Kafka sample producer/consumer finished."
  exit 0
fi

echo "result:    FAIL"
echo "==========================="
if (( consume_rc == 124 )); then
  echo "FAIL: consumer hard timeout after ${KAFKA_STEP_TIMEOUT}s; consumed ${got}/${KAFKA_MAX_MESSAGES}."
else
  echo "FAIL: consumed only ${got}/${KAFKA_MAX_MESSAGES} message(s) (rc=${consume_rc})."
fi
exit 1
