#!/usr/bin/env bash
#
# Schema Registry + Kafka Avro smoke:
#  1) kinit kafka/<FQDN> (kafka.service.keytab) → create topic on Kafka (SASL).
#  2) Save that ccache, kinit registry/<FQDN> (registry.service.keytab) for registry host identity.
#  3) Run KafkaAvroSerDesApp producer then consumer using the saved Kafka ccache (Kafka still needs
#     a kafka client ticket; registry ticket alone is usually not accepted as the Kafka client).
#
# Layout (ODP-style):
#   REGISTRY_HOME …/registry/examples/schema-registry/avro
#   Kafka tools from KAFKA_HOME, JAAS from KAFKA_HOME/conf/kafka_client_jaas.conf
#
# Environment (optional):
#   REGISTRY_HOME         default /usr/odp/current/registry
#   AVRO_EXAMPLES_DIR     default ${REGISTRY_HOME}/examples/schema-registry/avro
#   KAFKA_HOME            default /usr/odp/current/kafka-broker
#   KAFKA_BOOTSTRAP       default $(hostname -f):6667
#   KAFKA_CLIENT_CONFIG   for kafka-topics --command-config (default: REGISTRY_HOME/client-sasl.properties if present, else sample-jobs/kafka/client-sasl.properties)
#   KAFKA_KEYTAB          default /etc/security/keytabs/kafka.service.keytab
#   REGISTRY_KEYTAB       default /etc/security/keytabs/registry.service.keytab
#   KAFKA_PRINCIPAL_HOST, REGISTRY_PRINCIPAL_HOST (default: hostname -f / hostname)
#   SCHEMA_REGISTRY_URL   default http://$(hostname -f):7788/api/v1
#   SR_TOPIC              default truck_events_stream
#   KAFKA_JAAS_CONF       for java -Djava.security.auth.login.config (default ${KAFKA_HOME}/conf/kafka_client_jaas.conf)
#   SR_SKIP_TOPIC_CREATE  if "1", skip kafka-topics (kafka kinit + saved ccache still run)
#   SR_SKIP_PRODUCER      if "1", skip producer java
#   SR_SKIP_CONSUMER      if "1", skip consumer java
#   REGISTRY_JAVA_CP      if set, full -cp string for both producer and consumer (overrides auto)
#   REGISTRY_CP_DIRS      space-separated extra dirs to scan for *.jar (after defaults)
#   AVRO_EXAMPLES_JAR_GLOB  glob under AVRO_EXAMPLES_DIR (default: avro-examples-*.jar)
#   ODP_STACK_ROOT        e.g. /usr/odp/current or /usr/odp/3.3.6.2-1 (default: dirname(REGISTRY_HOME) when REGISTRY_HOME ends with /registry)
#   KAFKA3_LIBS_DIR       producer-only: dir of jars for Kafka 3 serializer (default: first of .../kafka3/libs, .../kafka3-broker/libs, KAFKA_HOME/libs)
#   KAFKA_LIBS_DIR        consumer-only: dir for Kafka client deserializer (default: first of .../kafka/libs, .../kafka-broker/libs, KAFKA_HOME/libs)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGISTRY_HOME="${REGISTRY_HOME:-/usr/odp/current/registry}"
AVRO_EXAMPLES_DIR="${AVRO_EXAMPLES_DIR:-${REGISTRY_HOME}/examples/schema-registry/avro}"
KAFKA_HOME="${KAFKA_HOME:-/usr/odp/current/kafka-broker}"
SR_TOPIC="${SR_TOPIC:-truck_events_stream}"
KAFKA_KEYTAB="${KAFKA_KEYTAB:-/etc/security/keytabs/kafka.service.keytab}"
REGISTRY_KEYTAB="${REGISTRY_KEYTAB:-/etc/security/keytabs/registry.service.keytab}"
SR_SKIP_TOPIC_CREATE="${SR_SKIP_TOPIC_CREATE:-0}"
SR_SKIP_PRODUCER="${SR_SKIP_PRODUCER:-0}"
SR_SKIP_CONSUMER="${SR_SKIP_CONSUMER:-0}"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

resolve_host() {
  local override="${1:-}"
  if [[ -n "$override" ]]; then
    printf '%s' "$override"
    return
  fi
  local h
  h="$(hostname -f 2>/dev/null || true)"
  if [[ -z "$h" ]]; then
    h="$(hostname)"
  fi
  [[ -n "$h" ]] || die "could not determine host FQDN; set KAFKA_PRINCIPAL_HOST / REGISTRY_PRINCIPAL_HOST"
  printf '%s' "$h"
}

resolve_bootstrap() {
  if [[ -n "${KAFKA_BOOTSTRAP:-}" ]]; then
    printf '%s' "$KAFKA_BOOTSTRAP"
    return
  fi
  local h
  h="$(resolve_host "${KAFKA_PRINCIPAL_HOST:-}")"
  printf '%s' "${h}:6667"
}

resolve_schema_registry_url() {
  if [[ -n "${SCHEMA_REGISTRY_URL:-}" ]]; then
    printf '%s' "$SCHEMA_REGISTRY_URL"
    return
  fi
  local h
  h="$(resolve_host "${REGISTRY_PRINCIPAL_HOST:-}")"
  printf '%s' "http://${h}:7788/api/v1"
}

resolve_kafka_client_config() {
  if [[ -n "${KAFKA_CLIENT_CONFIG:-}" ]]; then
    printf '%s' "$KAFKA_CLIENT_CONFIG"
    return
  fi
  local r="${REGISTRY_HOME}/client-sasl.properties"
  if [[ -f "$r" ]]; then
    printf '%s' "$r"
    return
  fi
  printf '%s' "${SCRIPT_DIR}/kafka/client-sasl.properties"
}

resolve_kafka_jaas() {
  if [[ -n "${KAFKA_JAAS_CONF:-}" ]]; then
    printf '%s' "$KAFKA_JAAS_CONF"
    return
  fi
  local k3="${KAFKA_HOME}/conf/kafka3_client_jaas.conf"
  local k2="${KAFKA_HOME}/conf/kafka_client_jaas.conf"
  if [[ -f "$k3" ]]; then
    printf '%s' "$k3"
  elif [[ -f "$k2" ]]; then
    printf '%s' "$k2"
  else
    printf '%s' "$k2"
  fi
}

# Appends *.jar from dir to cp_parts array (avro-examples alone misses SerDesException deps).
append_jars_from_dir() {
  local d="$1"
  [[ -d "$d" ]] || return 0
  shopt -s nullglob
  local f
  for f in "$d"/*.jar; do
    cp_parts+=( "$f" )
  done
  shopt -u nullglob
}

# First existing directory among arguments (for version-agnostic ODP layouts).
first_existing_dir() {
  local d
  for d in "$@"; do
    [[ -n "$d" && -d "$d" ]] || continue
    printf '%s' "$d"
    return 0
  done
  return 1
}

resolve_odp_stack_root() {
  if [[ -n "${ODP_STACK_ROOT:-}" ]]; then
    printf '%s' "$ODP_STACK_ROOT"
    return
  fi
  local base
  base="$(basename "$REGISTRY_HOME")"
  if [[ "$base" == "registry" ]]; then
    printf '%s' "$(cd "$(dirname "$REGISTRY_HOME")" && pwd)"
    return
  fi
  printf '%s' "$(cd "$(dirname "$KAFKA_HOME")" && pwd)"
}

# Pick highest-version-looking match when several jars match the glob (sort -V).
pick_jar_from_globs() {
  local dir="$1"
  local pattern="${2:-avro-examples-*.jar}"
  shopt -s nullglob
  local -a matches
  matches=( "$dir"/$pattern )
  shopt -u nullglob
  [[ ${#matches[@]} -ge 1 ]] || return 1
  if [[ ${#matches[@]} -eq 1 ]]; then
    printf '%s' "${matches[0]}"
    return 0
  fi
  local IFS=$'\n'
  printf '%s' "$(printf '%s\n' "${matches[@]}" | sort -V | tail -n 1)"
}

build_java_classpath() {
  local role="${1:-both}" # producer | consumer | both (single KAFKA_HOME/libs, backward compat)
  cp_parts=( "$avro_jar" )
  append_jars_from_dir "${AVRO_EXAMPLES_DIR}/lib"
  append_jars_from_dir "/tmp/libs"

  local stack kafka_lib_dir
  stack="$(resolve_odp_stack_root)"
  kafka_lib_dir=""
  if [[ "$role" == "producer" ]]; then
    if [[ -n "${KAFKA3_LIBS_DIR:-}" ]]; then
      kafka_lib_dir="$(first_existing_dir "${KAFKA3_LIBS_DIR}")" || true
    fi
    if [[ -z "$kafka_lib_dir" ]]; then
      kafka_lib_dir="$(first_existing_dir \
        "${stack}/kafka3/libs" \
        "${stack}/kafka3-broker/libs" )" || true
    fi
    if [[ -z "$kafka_lib_dir" && "$KAFKA_HOME" == *kafka3* ]]; then
      kafka_lib_dir="$(first_existing_dir "${KAFKA_HOME}/libs")" || true
    fi
    if [[ -z "$kafka_lib_dir" ]]; then
      kafka_lib_dir="$(first_existing_dir "${KAFKA_HOME}/libs")" || true
    fi
  elif [[ "$role" == "consumer" ]]; then
    if [[ -n "${KAFKA_LIBS_DIR:-}" ]]; then
      kafka_lib_dir="$(first_existing_dir "${KAFKA_LIBS_DIR}")" || true
    fi
    if [[ -z "$kafka_lib_dir" ]]; then
      kafka_lib_dir="$(first_existing_dir \
        "${stack}/kafka/libs" \
        "${stack}/kafka-broker/libs" )" || true
    fi
    if [[ -z "$kafka_lib_dir" ]]; then
      kafka_lib_dir="$(first_existing_dir "${KAFKA_HOME}/libs")" || true
    fi
  else
    kafka_lib_dir="$(first_existing_dir "${KAFKA_HOME}/libs")" || true
  fi
  [[ -n "$kafka_lib_dir" ]] || die "could not resolve Kafka libs dir for role=${role}; set KAFKA3_LIBS_DIR / KAFKA_LIBS_DIR / KAFKA_HOME"
  append_jars_from_dir "$kafka_lib_dir"

  # Registry / serde jars after Kafka (matches common ODP java -cp examples).
  local d
  for d in \
    "${REGISTRY_HOME}" \
    "${REGISTRY_HOME}/libs" \
    "${REGISTRY_HOME}/lib" \
    "${REGISTRY_HOME}/jars" \
    "${REGISTRY_HOME}/schema-registry/libs" \
    "${REGISTRY_HOME}/schema-registry/lib" \
    "${REGISTRY_HOME}/share/java/registry" \
    "${REGISTRY_HOME}/share/java/schema-registry" \
    "${REGISTRY_HOME}/share/schema-registry/lib"; do
    append_jars_from_dir "$d"
  done
  if [[ -n "${REGISTRY_CP_DIRS:-}" ]]; then
    local extra
    for extra in $REGISTRY_CP_DIRS; do
      [[ -n "$extra" ]] || continue
      append_jars_from_dir "$extra"
    done
  fi

  if [[ ${#cp_parts[@]} -eq 0 ]]; then
    die "internal: empty classpath"
  fi
  local IFS=:
  printf '%s' "${cp_parts[*]}"
}

need_cmd kinit
need_cmd java

kafka_host="$(resolve_host "${KAFKA_PRINCIPAL_HOST:-}")"
registry_host="$(resolve_host "${REGISTRY_PRINCIPAL_HOST:-}")"
kafka_principal="kafka/${kafka_host}"
registry_principal="registry/${registry_host}"
bootstrap="$(resolve_bootstrap)"
sr_url="$(resolve_schema_registry_url)"
cmd_cfg="$(resolve_kafka_client_config)"
jaas_conf="$(resolve_kafka_jaas)"
topics_sh="${KAFKA_HOME}/bin/kafka-topics.sh"

[[ -d "$REGISTRY_HOME" ]] || die "REGISTRY_HOME is not a directory: $REGISTRY_HOME"
[[ -d "$AVRO_EXAMPLES_DIR" ]] || die "AVRO_EXAMPLES_DIR is not a directory: $AVRO_EXAMPLES_DIR"
[[ -d "$KAFKA_HOME" ]] || die "KAFKA_HOME is not a directory: $KAFKA_HOME"
[[ -x "$topics_sh" ]] || die "not executable: $topics_sh"
[[ -f "$jaas_conf" ]] || die "JAAS config not found: $jaas_conf (set KAFKA_JAAS_CONF)"
[[ -r "$cmd_cfg" ]] || die "Kafka client config not readable: $cmd_cfg"
[[ -r "$KAFKA_KEYTAB" ]] || die "keytab not readable: $KAFKA_KEYTAB"
[[ -r "$REGISTRY_KEYTAB" ]] || die "keytab not readable: $REGISTRY_KEYTAB"

AVRO_EXAMPLES_JAR_GLOB="${AVRO_EXAMPLES_JAR_GLOB:-avro-examples-*.jar}"
avro_jar="$(pick_jar_from_globs "$AVRO_EXAMPLES_DIR" "$AVRO_EXAMPLES_JAR_GLOB")" \
  || die "no jar matched ${AVRO_EXAMPLES_DIR}/${AVRO_EXAMPLES_JAR_GLOB} (set AVRO_EXAMPLES_JAR_GLOB if the name differs)"

for path in "$AVRO_EXAMPLES_DIR/data/truck_events_json" "$AVRO_EXAMPLES_DIR/data/truck_events.avsc"; do
  [[ -e "$path" ]] || die "missing example data: $path"
done

work="$(mktemp -d)"
cleanup() { rm -rf "$work"; }
trap cleanup EXIT

kafka_cc_save="${work}/kafka.ccache"
prod_props="${work}/kafka-producer.props"
cons_props="${work}/kafka-consumer.props"

write_props() {
  local jaas_line='com.sun.security.auth.module.Krb5LoginModule required useTicketCache=true renewTicket=true serviceName="kafka";'
  cat >"$prod_props" <<EOF
topic=${SR_TOPIC}
bootstrap.servers=${bootstrap}
schema.registry.url=${sr_url}
security.protocol=SASL_PLAINTEXT
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer
ignoreInvalidMessages=true
sasl.jaas.config=${jaas_line}
EOF
  cat >"$cons_props" <<EOF
topic=${SR_TOPIC}
bootstrap.servers=${bootstrap}
schema.registry.url=${sr_url}
security.protocol=SASL_PLAINTEXT
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
value.deserializer=com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer
group.id=truck_group_smoke
auto.offset.reset=earliest
sasl.jaas.config=${jaas_line}
EOF
}

cc_path_from_env() {
  local raw="${KRB5CCNAME:-}"
  if [[ "$raw" == FILE:* ]]; then
    printf '%s' "${raw#FILE:}"
  else
    printf '%s' "/tmp/krb5cc_${UID:-0}"
  fi
}

write_props

echo "Kafka bootstrap:     ${bootstrap}"
echo "Schema registry URL: ${sr_url}"
echo "Topic:               ${SR_TOPIC}"
echo "Avro examples dir:   ${AVRO_EXAMPLES_DIR}"
echo "Avro jar glob:       ${AVRO_EXAMPLES_JAR_GLOB}"
echo "Avro jar:            ${avro_jar}"
echo "kafka-topics config: ${cmd_cfg}"
echo "Java JAAS:           ${jaas_conf}"

echo "---- kinit ${kafka_principal} (Kafka SASL + topic create) ----"
kinit -kt "$KAFKA_KEYTAB" "$kafka_principal" || die "kinit kafka failed"

if [[ "$SR_SKIP_TOPIC_CREATE" != "1" ]]; then
  echo "---- kafka-topics --create ${SR_TOPIC} ----"
  "$topics_sh" --bootstrap-server "$bootstrap" --command-config "$cmd_cfg" \
    --topic "$SR_TOPIC" --create --if-not-exists \
    --partitions 1 --replication-factor 1 || die "kafka-topics create failed"
fi

cc_path="$(cc_path_from_env)"
[[ -f "$cc_path" ]] || die "expected Kerberos ccache at $cc_path after kinit kafka"
cp "$cc_path" "$kafka_cc_save" || die "failed to save Kafka ccache copy"
echo "Saved Kafka credential cache to ${kafka_cc_save}"

echo "---- kinit ${registry_principal} (registry service keytab) ----"
kinit -kt "$REGISTRY_KEYTAB" "$registry_principal" || die "kinit registry failed"

export KRB5CCNAME="FILE:${kafka_cc_save}"
echo "KRB5CCNAME for Java:  ${KRB5CCNAME} (Kafka client ticket for producer/consumer)"

if [[ -n "${REGISTRY_JAVA_CP:-}" ]]; then
  _java_cp_prod="$REGISTRY_JAVA_CP"
  _java_cp_cons="$REGISTRY_JAVA_CP"
else
  _java_cp_prod="$(build_java_classpath producer)"
  _java_cp_cons="$(build_java_classpath consumer)"
fi
_preview="${_java_cp_prod:0:160}"
[[ ${#_java_cp_prod} -gt 160 ]] && _preview+="..."
echo "Java classpath producer (${#_java_cp_prod} chars): ${_preview}"
_preview="${_java_cp_cons:0:160}"
[[ ${#_java_cp_cons} -gt 160 ]] && _preview+="..."
echo "Java classpath consumer (${#_java_cp_cons} chars): ${_preview}"

if [[ "$SR_SKIP_PRODUCER" != "1" ]]; then
  echo "---- KafkaAvroSerDesApp producer (-sm) ----"
  ( cd "$AVRO_EXAMPLES_DIR" && java -Djava.security.auth.login.config="$jaas_conf" \
    -cp "${_java_cp_prod}" \
    com.hortonworks.registries.schemaregistry.examples.avro.KafkaAvroSerDesApp \
    -d data/truck_events_json -p "$prod_props" -sm -s data/truck_events.avsc ) || die "producer failed"
fi

if [[ "$SR_SKIP_CONSUMER" != "1" ]]; then
  echo "---- KafkaAvroSerDesApp consumer (-cm) ----"
  ( cd "$AVRO_EXAMPLES_DIR" && java -Djava.security.auth.login.config="$jaas_conf" \
    -cp "${_java_cp_cons}" \
    com.hortonworks.registries.schemaregistry.examples.avro.KafkaAvroSerDesApp \
    -c "$cons_props" -cm ) || die "consumer failed"
fi

echo "OK: Schema Registry smoke (topic + Avro producer/consumer) finished."
