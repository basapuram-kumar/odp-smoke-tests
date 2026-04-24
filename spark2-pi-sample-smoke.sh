#!/usr/bin/env bash
#
# kinit as spark/<FQDN> (Spark service keytab), then Spark 2 SparkPi on YARN via
# spark-submit. Resolves the examples JAR with a glob (no hard-coded version string).
#
# Equivalent to:
#   SPARK_MAJOR_VERSION=2 spark-submit --class org.apache.spark.examples.SparkPi \
#     --master yarn /usr/odp/current/spark2-client/examples/jars/spark-examples_*.jar 10
#
# Run on the host whose FQDN matches the keytab principal (e.g. spark/rl8j11p3-24.acceldata.ce).
#
# Environment (optional):
#   SPARK_KEYTAB           default /etc/security/keytabs/spark.service.keytab
#   SPARK_PRINCIPAL_HOST   default hostname -f / hostname
#   SPARK2_CLIENT_HOME     default /usr/odp/current/spark2-client
#   SPARK_SUBMIT           default ${SPARK2_CLIENT_HOME}/bin/spark-submit, else spark-submit on PATH
#   SPARK_EXAMPLES_JAR_GLOB  override full glob pattern (default: .../examples/jars/spark-examples_*.jar)
#   SPARK_PI_SLICES        default 10 (SparkPi final argument)
#   SPARK_MAJOR_VERSION     default 2 (exported for spark-submit)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_KEYTAB="${SPARK_KEYTAB:-/etc/security/keytabs/spark.service.keytab}"
SPARK2_CLIENT_HOME="${SPARK2_CLIENT_HOME:-/usr/odp/current/spark2-client}"
SPARK_PI_SLICES="${SPARK_PI_SLICES:-10}"
SPARK_MAJOR_VERSION="${SPARK_MAJOR_VERSION:-2}"

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

resolve_examples_jar() {
  local glob_pattern="${SPARK_EXAMPLES_JAR_GLOB:-${SPARK2_CLIENT_HOME}/examples/jars/spark-examples_*.jar}"
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
  local p="${SPARK2_CLIENT_HOME}/bin/spark-submit"
  if [[ -x "$p" ]]; then
    printf '%s' "$p"
    return
  fi
  need_cmd spark-submit
  command -v spark-submit
}

need_cmd kinit

if [[ ! -r "$SPARK_KEYTAB" ]]; then
  die "keytab not readable: $SPARK_KEYTAB"
fi

if [[ ! -d "$SPARK2_CLIENT_HOME" ]]; then
  die "SPARK2_CLIENT_HOME is not a directory: $SPARK2_CLIENT_HOME"
fi

spark_host="$(resolve_spark_host)"
principal="spark/${spark_host}"
examples_jar="$(resolve_examples_jar)"
spark_submit="$(resolve_spark_submit)"

if [[ ! -x "$spark_submit" ]] && ! command -v "$spark_submit" >/dev/null 2>&1; then
  die "spark-submit not executable: $spark_submit"
fi

echo "Spark principal:   ${principal}"
echo "SPARK_MAJOR_VERSION=${SPARK_MAJOR_VERSION}"
echo "spark-submit:      ${spark_submit}"
echo "Examples jar:      ${examples_jar}"
echo "SparkPi slices:    ${SPARK_PI_SLICES}"

kinit -kt "$SPARK_KEYTAB" "$principal" || die "kinit failed"

export SPARK_MAJOR_VERSION

echo "---- spark-submit SparkPi (YARN) ----"
"${spark_submit}" \
  --class org.apache.spark.examples.SparkPi \
  --master yarn \
  "$examples_jar" \
  "$SPARK_PI_SLICES"

echo "OK: Spark 2 SparkPi smoke finished."
