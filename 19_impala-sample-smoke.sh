#!/usr/bin/env bash
#
# kinit as impala/<FQDN> with the Impala service keytab, then run impala-shell -f
# against sql/impala-sample-smoke.sql (database test, table basa, 11 rows).
#
# Run on the host whose FQDN matches the keytab principal (e.g. impala/rl8j11p3-24.acceldata.ce).
#
# Environment (optional):
#   IMPALA_KEYTAB          default /etc/security/keytabs/impala.service.keytab
#   IMPALA_PRINCIPAL_HOST  default: hostname -f, else hostname
#   IMPALAD                coordinator host:port (default $(hostname):21050); passed as impala-shell -i
#   IMPALA_SHELL           default impala-shell on PATH
#   IMPALA_SMOKE_SQL       default <script-dir>/sql/impala-sample-smoke.sql
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMPALA_KEYTAB="${IMPALA_KEYTAB:-/etc/security/keytabs/impala.service.keytab}"
SQL_FILE="${IMPALA_SMOKE_SQL:-${SCRIPT_DIR}/sql/impala-sample-smoke.sql}"
IMPALA_SHELL="${IMPALA_SHELL:-impala-shell}"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

resolve_impala_host() {
  if [[ -n "${IMPALA_PRINCIPAL_HOST:-}" ]]; then
    printf '%s' "$IMPALA_PRINCIPAL_HOST"
    return
  fi
  local h
  h="$(hostname -f 2>/dev/null || true)"
  if [[ -z "$h" ]]; then
    h="$(hostname)"
  fi
  [[ -n "$h" ]] || die "could not determine FQDN for impala principal; set IMPALA_PRINCIPAL_HOST"
  printf '%s' "$h"
}

resolve_impalad() {
  if [[ -n "${IMPALAD:-}" ]]; then
    printf '%s' "$IMPALAD"
    return
  fi
  local h
  h="$(hostname)"
  [[ -n "$h" ]] || die "hostname returned empty string"
  printf '%s' "${h}:21050"
}

need_cmd kinit
if ! command -v "$IMPALA_SHELL" >/dev/null 2>&1 && [[ ! -x "$IMPALA_SHELL" ]]; then
  die "impala-shell not found or not executable: $IMPALA_SHELL"
fi

if [[ ! -r "$IMPALA_KEYTAB" ]]; then
  die "keytab not readable: $IMPALA_KEYTAB"
fi

if [[ ! -r "$SQL_FILE" ]]; then
  die "SQL file not readable: $SQL_FILE"
fi

impala_host="$(resolve_impala_host)"
principal="impala/${impala_host}"
impalad="$(resolve_impalad)"

echo "Impala principal: ${principal}"
echo "impala-shell -i:  ${impalad}"
echo "SQL file:         ${SQL_FILE}"

kinit -kt "$IMPALA_KEYTAB" "$principal" || die "kinit failed"

echo "---- ${IMPALA_SHELL} -i ${impalad} -f ${SQL_FILE} ----"
"${IMPALA_SHELL}" -i "$impalad" -f "$SQL_FILE"

echo "OK: Impala sample smoke finished."
