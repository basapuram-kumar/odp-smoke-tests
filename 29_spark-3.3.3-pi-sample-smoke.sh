#!/usr/bin/env bash
#
# Spark 3.3.3 (ODP layout spark3_3_3_3-client): kinit, then SparkPi on YARN.
# JAR is resolved with a glob (no hard-coded full version in this script).
#
# Kerberos: Ambari SPARK3 ships spark.headless.keytab (spark-<cluster>@REALM),
# not spark.service.keytab / spark/<host>. Prefer service keytab when present;
# otherwise fall back to headless.
#
# Equivalent to:
#   SPARK_MAJOR_VERSION=3 SPARK_VERSION=3_3_3_3 spark-submit \
#     --class org.apache.spark.examples.SparkPi --master yarn \
#     /usr/odp/current/spark3_3_3_3-client/examples/jars/spark-examples_*.jar 10
#
# Environment (optional):
#   SPARK_KEYTAB, SPARK_PRINCIPAL, SPARK_PRINCIPAL_HOST
#   SPARK3_CLIENT_HOME (default .../spark3_3_3_3-client)
#   SPARK_SUBMIT, SPARK_EXAMPLES_JAR_GLOB, SPARK_PI_SLICES (default 10)
#   SPARK_MAJOR_VERSION (default 3), SPARK_VERSION (default 3_3_3_3)
#
set -euo pipefail

SPARK_SERVICE_KEYTAB_DEFAULT="/etc/security/keytabs/spark.service.keytab"
SPARK_HEADLESS_KEYTAB_DEFAULT="/etc/security/keytabs/spark.headless.keytab"
SPARK3_CLIENT_HOME="${SPARK3_CLIENT_HOME:-/usr/odp/current/spark3_3_3_3-client}"
SPARK_PI_SLICES="${SPARK_PI_SLICES:-10}"
SPARK_MAJOR_VERSION="${SPARK_MAJOR_VERSION:-3}"
SPARK_VERSION="${SPARK_VERSION:-3_3_3_3}"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

resolve_spark_host() {
  if [[ -n "${SPARK_PRINCIPAL_HOST:-}" ]]; then
    printf '%s' "$SPARK_PRINCIPAL_HOST"
    return
  fi
  local h
  h="$(hostname -f 2>/dev/null || true)"
  if [[ -z "$h" ]]; then
    h="$(hostname)"
  fi
  [[ -n "$h" ]] || die "could not determine FQDN for spark principal; set SPARK_PRINCIPAL_HOST"
  printf '%s' "$h"
}

first_keytab_principal() {
  local kt="$1"
  klist -kt "$kt" 2>/dev/null | awk 'NR > 3 && $4 != "" { print $4; exit }'
}

resolve_spark_identity() {
  # Sets SPARK_KEYTAB and principal (printed to stdout as "keytab|principal").
  local kt principal host

  if [[ -n "${SPARK_KEYTAB:-}" ]]; then
    [[ -r "$SPARK_KEYTAB" ]] || die "keytab not readable: $SPARK_KEYTAB"
    if [[ -n "${SPARK_PRINCIPAL:-}" ]]; then
      printf '%s|%s' "$SPARK_KEYTAB" "$SPARK_PRINCIPAL"
      return
    fi
    principal="$(first_keytab_principal "$SPARK_KEYTAB")"
    [[ -n "$principal" ]] || die "could not read principal from $SPARK_KEYTAB; set SPARK_PRINCIPAL"
    printf '%s|%s' "$SPARK_KEYTAB" "$principal"
    return
  fi

  if [[ -r "$SPARK_SERVICE_KEYTAB_DEFAULT" ]]; then
    kt="$SPARK_SERVICE_KEYTAB_DEFAULT"
    if [[ -n "${SPARK_PRINCIPAL:-}" ]]; then
      principal="$SPARK_PRINCIPAL"
    else
      host="$(resolve_spark_host)"
      principal="spark/${host}"
    fi
    printf '%s|%s' "$kt" "$principal"
    return
  fi

  if [[ -r "$SPARK_HEADLESS_KEYTAB_DEFAULT" ]]; then
    kt="$SPARK_HEADLESS_KEYTAB_DEFAULT"
    if [[ -n "${SPARK_PRINCIPAL:-}" ]]; then
      principal="$SPARK_PRINCIPAL"
    else
      principal="$(first_keytab_principal "$kt")"
      [[ -n "$principal" ]] || die "could not read principal from $kt; set SPARK_PRINCIPAL"
    fi
    printf '%s|%s' "$kt" "$principal"
    return
  fi

  die "no spark keytab found (tried $SPARK_SERVICE_KEYTAB_DEFAULT and $SPARK_HEADLESS_KEYTAB_DEFAULT); set SPARK_KEYTAB"
}

resolve_examples_jar() {
  local glob_pattern="${SPARK_EXAMPLES_JAR_GLOB:-${SPARK3_CLIENT_HOME}/examples/jars/spark-examples_*.jar}"
  shopt -s nullglob
  local -a candidates=()
  candidates=( $glob_pattern )
  shopt -u nullglob
  if [[ ${#candidates[@]} -eq 0 ]]; then
    die "no examples jar matched: ${glob_pattern}"
  fi
  if [[ ${#candidates[@]} -gt 1 ]]; then
    echo "WARN: multiple jars matched; using first: ${candidates[0]}" >&2
  fi
  printf '%s' "${candidates[0]}"
}

resolve_spark_submit() {
  if [[ -n "${SPARK_SUBMIT:-}" ]]; then
    printf '%s' "$SPARK_SUBMIT"
    return
  fi
  local p="${SPARK3_CLIENT_HOME}/bin/spark-submit"
  if [[ -x "$p" ]]; then
    printf '%s' "$p"
    return
  fi
  need_cmd spark-submit
  command -v spark-submit
}

need_cmd kinit
need_cmd klist

if [[ ! -d "$SPARK3_CLIENT_HOME" ]]; then
  die "SPARK3_CLIENT_HOME is not a directory: $SPARK3_CLIENT_HOME"
fi

identity="$(resolve_spark_identity)"
SPARK_KEYTAB="${identity%%|*}"
principal="${identity#*|}"
examples_jar="$(resolve_examples_jar)"
spark_submit="$(resolve_spark_submit)"

if [[ ! -x "$spark_submit" ]] && ! command -v "$spark_submit" >/dev/null 2>&1; then
  die "spark-submit not executable: $spark_submit"
fi

echo "Spark keytab:        ${SPARK_KEYTAB}"
echo "Spark principal:     ${principal}"
echo "SPARK_MAJOR_VERSION=${SPARK_MAJOR_VERSION} SPARK_VERSION=${SPARK_VERSION}"
echo "SPARK3_CLIENT_HOME:  ${SPARK3_CLIENT_HOME}"
echo "spark-submit:        ${spark_submit}"
echo "Examples jar:        ${examples_jar}"
echo "SparkPi slices:      ${SPARK_PI_SLICES}"

kinit -kt "$SPARK_KEYTAB" "$principal" || die "kinit failed"

export SPARK_MAJOR_VERSION
export SPARK_VERSION

echo "---- spark-submit SparkPi (YARN) Spark 3.3.3 ----"
# Timeline service init pulls Hadoop shaded JAXB that is missing from some
# spark3+hadoop-client-runtime classpaths; disable unless explicitly overridden.
"${spark_submit}" \
  --class org.apache.spark.examples.SparkPi \
  --master yarn \
  --conf spark.hadoop.yarn.timeline-service.enabled="${SPARK_YARN_TIMELINE_ENABLED:-false}" \
  "$examples_jar" \
  "$SPARK_PI_SLICES"

echo "OK: Spark 3.3.3 SparkPi smoke finished."
