#!/usr/bin/env bash
#
# kinit as hive/<FQDN> using the Hive service keytab, then run sample Beeline SQL.
# Run on the host whose FQDN matches the keytab principal (e.g. hive/rl8j11p3-24.acceldata.ce).
#
# Beeline uses the default JDBC URL from hive-site.xml on the node when HIVE_JDBC_URL
# is unset. Set HIVE_JDBC_URL (or configs/hive.env) only if you need to override.
#
# Environment (optional):
#   HIVE_CONFIG_FILE     default <script-dir>/configs/hive.env
#   HIVE_KEYTAB          default /etc/security/keytabs/hive.service.keytab
#   HIVE_PRINCIPAL_HOST  host part of principal (default: hostname -f / hostname)
#   HIVE_JDBC_URL        optional; if unset, beeline is run without -u
#   HIVE_SMOKE_SQL       default <script-dir>/sql/hive-sample-smoke.sql
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HIVE_CONFIG_FILE="${HIVE_CONFIG_FILE:-${SCRIPT_DIR}/configs/hive.env}"
HIVE_KEYTAB="${HIVE_KEYTAB:-/etc/security/keytabs/hive.service.keytab}"
SQL_FILE="${HIVE_SMOKE_SQL:-${SCRIPT_DIR}/sql/hive-sample-smoke.sql}"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

strip_quotes() {
  local v="$1"
  if [[ ${#v} -ge 2 && ${v:0:1} == '"' && ${v: -1} == '"' ]]; then
    printf '%s' "${v:1:${#v}-2}"
  elif [[ ${#v} -ge 2 && ${v:0:1} == "'" && ${v: -1} == "'" ]]; then
    printf '%s' "${v:1:${#v}-2}"
  else
    printf '%s' "$v"
  fi
}

load_hive_env_file() {
  local f="$1" key val line
  [[ -f "$f" ]] || return 1
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ -z "${line//[[:space:]]/}" ]] && continue
    line="${line#export[[:space:]]}"
    [[ "$line" != *=* ]] && continue
    key="${line%%=*}"
    val="${line#*=}"
    key="${key%"${key##*[![:space:]]}"}"
    key="${key#"${key%%[![:space:]]*}"}"
    val="${val%"${val##*[![:space:]]}"}"
    val="${val#"${val%%[![:space:]]*}"}"
    val="$(strip_quotes "$val")"
    case "$key" in
      HIVE_JDBC_URL) _cfg_HIVE_JDBC_URL="$val" ;;
      HIVE_KEYTAB) _cfg_HIVE_KEYTAB="$val" ;;
      HIVE_PRINCIPAL_HOST) _cfg_HIVE_PRINCIPAL_HOST="$val" ;;
    esac
  done <"$f"
  return 0
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

resolve_hive_host() {
  if [[ -n "${HIVE_PRINCIPAL_HOST:-}" ]]; then
    printf '%s' "$HIVE_PRINCIPAL_HOST"
    return
  fi
  local h
  h="$(hostname -f 2>/dev/null || true)"
  if [[ -z "$h" ]]; then
    h="$(hostname)"
  fi
  [[ -n "$h" ]] || die "could not determine FQDN for hive principal; set HIVE_PRINCIPAL_HOST"
  printf '%s' "$h"
}

need_cmd kinit
need_cmd beeline

_cfg_HIVE_JDBC_URL=""
_cfg_HIVE_KEYTAB=""
_cfg_HIVE_PRINCIPAL_HOST=""

if [[ -f "$HIVE_CONFIG_FILE" ]]; then
  load_hive_env_file "$HIVE_CONFIG_FILE" || die "failed to read $HIVE_CONFIG_FILE"
fi

HIVE_KEYTAB="${HIVE_KEYTAB:-${_cfg_HIVE_KEYTAB:-/etc/security/keytabs/hive.service.keytab}}"
HIVE_PRINCIPAL_HOST="${HIVE_PRINCIPAL_HOST:-${_cfg_HIVE_PRINCIPAL_HOST:-}}"
HIVE_JDBC_URL="${HIVE_JDBC_URL:-${_cfg_HIVE_JDBC_URL:-}}"

if [[ ! -r "$HIVE_KEYTAB" ]]; then
  die "keytab not readable: $HIVE_KEYTAB"
fi

if [[ ! -r "$SQL_FILE" ]]; then
  die "SQL file not readable: $SQL_FILE"
fi

hive_host="$(resolve_hive_host)"
principal="hive/${hive_host}"

echo "Hive principal: ${principal}"
if [[ -n "$HIVE_JDBC_URL" ]]; then
  echo "Beeline URL:    ${HIVE_JDBC_URL} (-u override)"
else
  echo "Beeline URL:    (default from hive-site.xml / Beeline, no -u)"
fi

kinit -kt "$HIVE_KEYTAB" "$principal" || die "kinit failed"

echo "---- beeline -f ${SQL_FILE} ----"
if [[ -n "$HIVE_JDBC_URL" ]]; then
  beeline -u "$HIVE_JDBC_URL" -f "$SQL_FILE"
else
  beeline -f "$SQL_FILE"
fi

echo "OK: Hive sample SQL finished."
