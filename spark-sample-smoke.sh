#!/usr/bin/env bash
#
# kinit as spark/<FQDN> with the Spark service keytab, then run sample Spark Shell
# snippets from scala/spark-sample-smoke.scala (non-interactive: spark-shell -i).
#
# Run on the host whose FQDN matches the keytab principal (e.g. spark/rl8j11p3-24.acceldata.ce).
#
# Environment (optional):
#   SPARK_KEYTAB           default /etc/security/keytabs/spark.service.keytab
#   SPARK_PRINCIPAL_HOST   default: hostname -f, else hostname
#   SPARK_SMOKE_SCALA      default <script-dir>/scala/spark-sample-smoke.scala
#   SPARK_SHELL            default spark-shell on PATH
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_KEYTAB="${SPARK_KEYTAB:-/etc/security/keytabs/spark.service.keytab}"
SCALA_FILE="${SPARK_SMOKE_SCALA:-${SCRIPT_DIR}/scala/spark-sample-smoke.scala}"
SPARK_SHELL="${SPARK_SHELL:-spark-shell}"

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

need_cmd kinit
if ! command -v "$SPARK_SHELL" >/dev/null 2>&1 && [[ ! -x "$SPARK_SHELL" ]]; then
  die "spark shell not found or not executable: $SPARK_SHELL"
fi

if [[ ! -r "$SPARK_KEYTAB" ]]; then
  die "keytab not readable: $SPARK_KEYTAB"
fi

if [[ ! -r "$SCALA_FILE" ]]; then
  die "Scala file not readable: $SCALA_FILE"
fi

spark_host="$(resolve_spark_host)"
principal="spark/${spark_host}"

echo "Spark principal: ${principal}"
echo "Spark shell:     ${SPARK_SHELL} -i ${SCALA_FILE}"

kinit -kt "$SPARK_KEYTAB" "$principal" || die "kinit failed"

echo "---- ${SPARK_SHELL} -i ${SCALA_FILE} ----"
"$SPARK_SHELL" -i "$SCALA_FILE"

echo "OK: Spark sample job finished."
