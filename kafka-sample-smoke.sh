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
printf '%s' "$PAYLOAD" | "$producer" \
  --topic "$KAFKA_TOPIC" \
  --bootstrap-server "$bootstrap" \
  --producer.config "$KAFKA_CLIENT_CONFIG"

echo "---- kafka-console-consumer (--from-beginning, --max-messages ${MSG_COUNT}) ----"
echo "(If the topic already had data, you will see older messages first; increase KAFKA_MAX_MESSAGES to drain more.)"
KAFKA_MAX_MESSAGES="${KAFKA_MAX_MESSAGES:-$MSG_COUNT}"
"$consumer" \
  --topic "$KAFKA_TOPIC" \
  --bootstrap-server "$bootstrap" \
  --consumer.config "$KAFKA_CLIENT_CONFIG" \
  --from-beginning \
  --max-messages "$KAFKA_MAX_MESSAGES"

echo "OK: Kafka sample producer/consumer finished."
