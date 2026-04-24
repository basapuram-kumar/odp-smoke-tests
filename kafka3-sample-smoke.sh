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
#   KAFKA_MAX_MESSAGES   max messages consumer reads per topic (default: 2x KAFKA_MSGS_PER_TOPIC)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_HOME="${KAFKA_HOME:-/usr/odp/current/kafka3-broker}"
KAFKA_CLIENT_CONFIG="${KAFKA_CLIENT_CONFIG:-${SCRIPT_DIR}/kafka/client-sasl.properties}"
KAFKA_KEYTAB="${KAFKA_KEYTAB:-/etc/security/keytabs/kafka.service.keytab}"
KAFKA_CREATE_TOPIC="${KAFKA_CREATE_TOPIC:-false}"
KAFKA_REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"
KAFKA_MSGS_PER_TOPIC="${KAFKA_MSGS_PER_TOPIC:-3}"

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

max_consume="${KAFKA_MAX_MESSAGES:-$((KAFKA_MSGS_PER_TOPIC * 2))}"

echo "Kafka 3 KAFKA_HOME:  ${KAFKA_HOME}"
echo "Kafka principal:     ${principal}"
echo "Bootstrap servers:   ${bootstrap}"
echo "Topics:              ${PARSED_TOPICS[*]}"
echo "Msgs per topic:      ${KAFKA_MSGS_PER_TOPIC} (consumer max ${max_consume} per topic)"
echo "KAFKA_OPTS:          ${KAFKA_OPTS}"
echo "Client config file:  ${KAFKA_CLIENT_CONFIG}"

kinit -kt "$KAFKA_KEYTAB" "$principal" || die "kinit failed"

for topic in "${PARSED_TOPICS[@]}"; do
  [[ -z "$topic" ]] && continue

  if [[ "${KAFKA_CREATE_TOPIC}" == "true" ]]; then
    echo "---- kafka-topics --create (if-not-exists) ${topic} ----"
    "$topics" --bootstrap-server "$bootstrap" --command-config "$KAFKA_CLIENT_CONFIG" \
      --create --if-not-exists --topic "$topic" \
      --partitions 1 --replication-factor "$KAFKA_REPLICATION_FACTOR" || die "topic create failed: $topic"
  fi

  payload="$(payload_for_topic "$topic")"

  echo "---- kafka-console-producer topic=${topic} (${KAFKA_MSGS_PER_TOPIC} lines) ----"
  printf '%s' "$payload" | "$producer" \
    --topic "$topic" \
    --bootstrap-server "$bootstrap" \
    --producer.config "$KAFKA_CLIENT_CONFIG"

  echo "---- kafka-console-consumer topic=${topic} (--from-beginning, --max-messages ${max_consume}) ----"
  "$consumer" \
    --topic "$topic" \
    --bootstrap-server "$bootstrap" \
    --consumer.config "$KAFKA_CLIENT_CONFIG" \
    --from-beginning \
    --max-messages "$max_consume"

  echo "--- done topic: ${topic} ---"
done

echo "OK: Kafka 3 sample producer/consumer finished for ${#PARSED_TOPICS[@]} topic(s)."
