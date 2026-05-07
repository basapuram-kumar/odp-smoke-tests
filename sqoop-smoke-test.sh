#!/usr/bin/env bash
#
# Sqoop smoke: MySQL → HDFS import (default), optional export + row-count verify.
# Run from a Hadoop gateway / edge node where `sqoop` and `hdfs` use the live cluster.
#
# Prerequisites (on MySQL, once as admin):
#   mysql -u root -p < sql/sqoop-smoke-mysql-setup.sql
#   That creates database sqoop_smoke, user sqoop_smoke / password sqoop_smoke, table smoke_import
#   with five rows (defaults in this script match that SQL).
#
# Environment:
#   SQOOP_CONFIG_FILE       default: <script-dir>/configs/sqoop.env — optional KEY=value file (gitignored
#                            copy: configs/sqoop.env.example → sqoop.env). Fills vars unset when the script
#                            started (before script defaults). Remove stale sqoop.env after upgrading.
#   SQOOP_MYSQL_HOST        default: this host's name (hostname -f, else hostname).
#   SQOOP_MYSQL_PORT        default: 3306
#   SQOOP_MYSQL_DATABASE    default: sqoop_smoke
#   SQOOP_MYSQL_USER        default: sqoop_smoke
#   SQOOP_MYSQL_PASSWORD    default: sqoop_smoke (same as sql/sqoop-smoke-mysql-setup.sql; override in prod).
#   SQOOP_MYSQL_PASSWORD_FILE  if set, passed as --password-file (overrides SQOOP_MYSQL_PASSWORD)
#   SQOOP_JDBC_EXTRA_PARAMS default: useSSL=false&allowPublicKeyRetrieval=true (append with &)
#   SQOOP_SOURCE_TABLE       default: smoke_import
#   SQOOP_EXPORT_TABLE         default: smoke_export (only if export enabled; create table yourself or extend SQL)
#   SQOOP_HDFS_BASE_DIR        default: /tmp/sqoop_smoke_<user>_<timestamp>
#   SQOOP_FIELDS_TERMINATED_BY default: ,
#   SQOOP_NUM_MAPPERS          default: 1
#   SQOOP_IMPORT_COLUMNS       optional: comma-separated for --columns on import
#   SQOOP_EXPORT_COLUMNS       optional: comma-separated for --columns on export
#   SQOOP_DELETE_TARGET_DIR   default: 1 (pass --delete-target-dir on import)
#   SQOOP_SKIP_EXPORT          default: 1 — import + HDFS checks only. Set 0 for export round-trip.
#   SQOOP_SKIP_IMPORT          if 1, only run export + optional verify (HDFS dir must exist)
#   SQOOP_EXPECTED_ROWS        if set, import line count must match (e.g. 5 after stock setup SQL)
#   SQOOP_MYSQL_VERIFY         default: 0. Set 1 with SQOOP_SKIP_EXPORT=0 to compare source vs export counts.
#   SQOOP_MYSQL_CLIENT         default: mysql
#   SQOOP_TRUNCATE_EXPORT      if 1, TRUNCATE export table before export (needs mysql client)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQOOP_CONFIG_FILE="${SQOOP_CONFIG_FILE:-$SCRIPT_DIR/configs/sqoop.env}"

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

# Apply KEY=value from sqoop.env only for variables not already set in the environment.
load_sqoop_env_file_if_present() {
  local f="$1" key val line
  [[ -f "$f" ]] || return 0
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
      SQOOP_MYSQL_HOST) [[ "${SQOOP_MYSQL_HOST+set}" == "set" ]] || SQOOP_MYSQL_HOST="$val" ;;
      SQOOP_MYSQL_PORT) [[ "${SQOOP_MYSQL_PORT+set}" == "set" ]] || SQOOP_MYSQL_PORT="$val" ;;
      SQOOP_MYSQL_DATABASE) [[ "${SQOOP_MYSQL_DATABASE+set}" == "set" ]] || SQOOP_MYSQL_DATABASE="$val" ;;
      SQOOP_MYSQL_USER) [[ "${SQOOP_MYSQL_USER+set}" == "set" ]] || SQOOP_MYSQL_USER="$val" ;;
      SQOOP_MYSQL_PASSWORD) [[ "${SQOOP_MYSQL_PASSWORD+set}" == "set" ]] || SQOOP_MYSQL_PASSWORD="$val" ;;
      SQOOP_MYSQL_PASSWORD_FILE) [[ "${SQOOP_MYSQL_PASSWORD_FILE+set}" == "set" ]] || SQOOP_MYSQL_PASSWORD_FILE="$val" ;;
      SQOOP_JDBC_EXTRA_PARAMS) [[ "${SQOOP_JDBC_EXTRA_PARAMS+set}" == "set" ]] || SQOOP_JDBC_EXTRA_PARAMS="$val" ;;
      SQOOP_SOURCE_TABLE) [[ "${SQOOP_SOURCE_TABLE+set}" == "set" ]] || SQOOP_SOURCE_TABLE="$val" ;;
      SQOOP_EXPORT_TABLE) [[ "${SQOOP_EXPORT_TABLE+set}" == "set" ]] || SQOOP_EXPORT_TABLE="$val" ;;
      SQOOP_HDFS_BASE_DIR) [[ "${SQOOP_HDFS_BASE_DIR+set}" == "set" ]] || SQOOP_HDFS_BASE_DIR="$val" ;;
      SQOOP_FIELDS_TERMINATED_BY) [[ "${SQOOP_FIELDS_TERMINATED_BY+set}" == "set" ]] || SQOOP_FIELDS_TERMINATED_BY="$val" ;;
      SQOOP_NUM_MAPPERS) [[ "${SQOOP_NUM_MAPPERS+set}" == "set" ]] || SQOOP_NUM_MAPPERS="$val" ;;
      SQOOP_DELETE_TARGET_DIR) [[ "${SQOOP_DELETE_TARGET_DIR+set}" == "set" ]] || SQOOP_DELETE_TARGET_DIR="$val" ;;
      SQOOP_SKIP_EXPORT) [[ "${SQOOP_SKIP_EXPORT+set}" == "set" ]] || SQOOP_SKIP_EXPORT="$val" ;;
      SQOOP_SKIP_IMPORT) [[ "${SQOOP_SKIP_IMPORT+set}" == "set" ]] || SQOOP_SKIP_IMPORT="$val" ;;
      SQOOP_EXPECTED_ROWS) [[ "${SQOOP_EXPECTED_ROWS+set}" == "set" ]] || SQOOP_EXPECTED_ROWS="$val" ;;
      SQOOP_MYSQL_VERIFY) [[ "${SQOOP_MYSQL_VERIFY+set}" == "set" ]] || SQOOP_MYSQL_VERIFY="$val" ;;
      SQOOP_MYSQL_CLIENT) [[ "${SQOOP_MYSQL_CLIENT+set}" == "set" ]] || SQOOP_MYSQL_CLIENT="$val" ;;
      SQOOP_TRUNCATE_EXPORT) [[ "${SQOOP_TRUNCATE_EXPORT+set}" == "set" ]] || SQOOP_TRUNCATE_EXPORT="$val" ;;
      SQOOP_IMPORT_COLUMNS) [[ "${SQOOP_IMPORT_COLUMNS+set}" == "set" ]] || SQOOP_IMPORT_COLUMNS="$val" ;;
      SQOOP_EXPORT_COLUMNS) [[ "${SQOOP_EXPORT_COLUMNS+set}" == "set" ]] || SQOOP_EXPORT_COLUMNS="$val" ;;
    esac
  done <"$f"
  return 0
}

load_sqoop_env_file_if_present "$SQOOP_CONFIG_FILE"
if [[ -f "$SQOOP_CONFIG_FILE" ]]; then
  echo "---- Loaded $SQOOP_CONFIG_FILE (only keys that were unset in the shell) ----" >&2
fi

SQOOP_MYSQL_PORT="${SQOOP_MYSQL_PORT:-3306}"
SQOOP_MYSQL_DATABASE="${SQOOP_MYSQL_DATABASE:-sqoop_smoke}"
SQOOP_MYSQL_USER="${SQOOP_MYSQL_USER:-sqoop_smoke}"
SQOOP_MYSQL_PASSWORD="${SQOOP_MYSQL_PASSWORD:-sqoop_smoke}"
SQOOP_SOURCE_TABLE="${SQOOP_SOURCE_TABLE:-smoke_import}"
SQOOP_EXPORT_TABLE="${SQOOP_EXPORT_TABLE:-smoke_export}"
SQOOP_FIELDS_TERMINATED_BY="${SQOOP_FIELDS_TERMINATED_BY:-,}"
SQOOP_NUM_MAPPERS="${SQOOP_NUM_MAPPERS:-1}"
SQOOP_JDBC_EXTRA_PARAMS="${SQOOP_JDBC_EXTRA_PARAMS:-useSSL=false&allowPublicKeyRetrieval=true}"
SQOOP_DELETE_TARGET_DIR="${SQOOP_DELETE_TARGET_DIR:-1}"
SQOOP_SKIP_EXPORT="${SQOOP_SKIP_EXPORT:-1}"
SQOOP_SKIP_IMPORT="${SQOOP_SKIP_IMPORT:-0}"
SQOOP_MYSQL_VERIFY="${SQOOP_MYSQL_VERIFY:-0}"
SQOOP_MYSQL_CLIENT="${SQOOP_MYSQL_CLIENT:-mysql}"
SQOOP_TRUNCATE_EXPORT="${SQOOP_TRUNCATE_EXPORT:-0}"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

# Prefer FQDN so JDBC and mysql -h match typical MySQL user@'host' grants (same as `mysql -h $(hostname)`).
default_sqoop_mysql_host() {
  local h
  h="$(hostname -f 2>/dev/null || true)"
  if [[ -z "$h" || "$h" == "(none)" ]]; then
    h="$(hostname 2>/dev/null || true)"
  fi
  [[ -n "$h" ]] || die "could not determine this host's name; set SQOOP_MYSQL_HOST"
  printf '%s' "$h"
}

jdbc_url() {
  local host="$1"
  local params="$SQOOP_JDBC_EXTRA_PARAMS"
  [[ -n "$params" ]] || params="useSSL=false&allowPublicKeyRetrieval=true"
  printf 'jdbc:mysql://%s:%s/%s?%s' "$host" "$SQOOP_MYSQL_PORT" "$SQOOP_MYSQL_DATABASE" "$params"
}

validate_mysql_password_file_if_set() {
  if [[ -n "${SQOOP_MYSQL_PASSWORD_FILE:-}" ]]; then
    [[ -f "$SQOOP_MYSQL_PASSWORD_FILE" ]] || die "SQOOP_MYSQL_PASSWORD_FILE not a file: $SQOOP_MYSQL_PASSWORD_FILE"
  fi
}

append_sqoop_mysql_auth() {
  local -n _sqoop_ref=$1
  if [[ -n "${SQOOP_MYSQL_PASSWORD_FILE:-}" ]]; then
    _sqoop_ref+=(--password-file "$SQOOP_MYSQL_PASSWORD_FILE")
  else
    _sqoop_ref+=(--password "${SQOOP_MYSQL_PASSWORD:-}")
  fi
}

mysql_batch() {
  local sql="$1"
  need_cmd "$SQOOP_MYSQL_CLIENT"
  local host="${SQOOP_MYSQL_HOST}"
  if [[ -n "${SQOOP_MYSQL_PASSWORD_FILE:-}" ]]; then
    "$SQOOP_MYSQL_CLIENT" -h "$host" -P"$SQOOP_MYSQL_PORT" -u"$SQOOP_MYSQL_USER" \
      --password="$(tr -d '\n\r' <"$SQOOP_MYSQL_PASSWORD_FILE")" \
      -N -e "$sql" "$SQOOP_MYSQL_DATABASE"
    return
  fi
  MYSQL_PWD="${SQOOP_MYSQL_PASSWORD:-}" "$SQOOP_MYSQL_CLIENT" -h "$host" -P"$SQOOP_MYSQL_PORT" -u"$SQOOP_MYSQL_USER" \
    -N -e "$sql" "$SQOOP_MYSQL_DATABASE"
}

line_count_parts() {
  local dir="$1" total=0 n f
  while IFS= read -r f; do
    [[ -z "$f" ]] && continue
    [[ "$f" == *part-m-* ]] || continue
    n="$(hdfs dfs -cat "$f" 2>/dev/null | wc -l | tr -d ' ')"
    total=$((total + n))
  done < <(hdfs dfs -ls "$dir" 2>/dev/null | awk '{print $NF}' | grep 'part-m-' || true)
  printf '%s' "$total"
}

need_cmd sqoop
need_cmd hdfs

# Old sqoop.env.example used hive + sqoop_test; smoke_import only exists in sqoop_smoke per setup SQL.
if [[ "$SQOOP_MYSQL_USER" == "hive" && "$SQOOP_MYSQL_DATABASE" == "sqoop_test" && "$SQOOP_SOURCE_TABLE" == "smoke_import" ]]; then
  die "MySQL settings look stale: user=hive database=sqoop_test but sql/sqoop-smoke-mysql-setup.sql creates sqoop_smoke.smoke_import for user sqoop_smoke. Remove or edit ${SQOOP_CONFIG_FILE} (re-copy configs/sqoop.env.example), or export SQOOP_MYSQL_DATABASE=sqoop_smoke SQOOP_MYSQL_USER=sqoop_smoke SQOOP_MYSQL_PASSWORD=sqoop_smoke"
fi

if [[ -z "${SQOOP_MYSQL_HOST:-}" ]]; then
  SQOOP_MYSQL_HOST="$(default_sqoop_mysql_host)"
  echo "---- SQOOP_MYSQL_HOST unset — using this host: $SQOOP_MYSQL_HOST ----" >&2
fi
validate_mysql_password_file_if_set

CONNECT_URL="$(jdbc_url "$SQOOP_MYSQL_HOST")"

TARGET_DIR="${SQOOP_HDFS_BASE_DIR:-}"
if [[ -z "$TARGET_DIR" ]]; then
  [[ "${SQOOP_SKIP_IMPORT}" == "1" ]] && die "Set SQOOP_HDFS_BASE_DIR when SQOOP_SKIP_IMPORT=1"
  TARGET_DIR="/tmp/sqoop_smoke_$(id -un)_$(date +%s)_$$"
fi

IMPORT_COL_ARGS=()
if [[ -n "${SQOOP_IMPORT_COLUMNS:-}" ]]; then
  IMPORT_COL_ARGS=(--columns "$SQOOP_IMPORT_COLUMNS")
fi
EXPORT_COL_ARGS=()
if [[ -n "${SQOOP_EXPORT_COLUMNS:-}" ]]; then
  EXPORT_COL_ARGS=(--columns "$SQOOP_EXPORT_COLUMNS")
fi

DELETE_ARG=()
if [[ "$SQOOP_DELETE_TARGET_DIR" == "1" && "${SQOOP_SKIP_IMPORT}" != "1" ]]; then
  DELETE_ARG=(--delete-target-dir)
fi

echo "---- Sqoop smoke: JDBC host=${SQOOP_MYSQL_HOST:-?} db=$SQOOP_MYSQL_DATABASE user=$SQOOP_MYSQL_USER ----"
echo "    HDFS dir: $TARGET_DIR"
echo "    import: ${SQOOP_MYSQL_DATABASE}.${SQOOP_SOURCE_TABLE}"
if [[ "${SQOOP_SKIP_EXPORT}" != "1" ]]; then
  echo "    export table: $SQOOP_EXPORT_TABLE (SQOOP_SKIP_EXPORT=0)"
fi

if [[ "$SQOOP_TRUNCATE_EXPORT" == "1" ]]; then
  echo "---- TRUNCATE $SQOOP_EXPORT_TABLE ----"
  mysql_batch "TRUNCATE TABLE \`$SQOOP_EXPORT_TABLE\`;"
fi

if [[ "${SQOOP_SKIP_IMPORT}" != "1" ]]; then
  echo "---- sqoop import ----"
  _imp=(sqoop import --connect "$CONNECT_URL" --username "$SQOOP_MYSQL_USER")
  append_sqoop_mysql_auth _imp
  _imp+=(--table "$SQOOP_SOURCE_TABLE")
  [[ ${#IMPORT_COL_ARGS[@]} -eq 0 ]] || _imp+=("${IMPORT_COL_ARGS[@]}")
  _imp+=(--target-dir "$TARGET_DIR")
  [[ ${#DELETE_ARG[@]} -eq 0 ]] || _imp+=("${DELETE_ARG[@]}")
  _imp+=(--fields-terminated-by "$SQOOP_FIELDS_TERMINATED_BY" --m "$SQOOP_NUM_MAPPERS")
  "${_imp[@]}"

  hdfs dfs -test -e "$TARGET_DIR/_SUCCESS" || die "HDFS missing _SUCCESS under $TARGET_DIR"
  part_line_total="$(line_count_parts "$TARGET_DIR")"
  [[ "$part_line_total" =~ ^[0-9]+$ ]] || die "could not count lines in $TARGET_DIR/part-m-*"
  [[ "$part_line_total" -ge 1 ]] || die "expected at least 1 data row in part files, got $part_line_total"
  if [[ -n "${SQOOP_EXPECTED_ROWS:-}" ]]; then
    [[ "$part_line_total" -eq "$SQOOP_EXPECTED_ROWS" ]] || \
      die "row count mismatch: HDFS lines=$part_line_total SQOOP_EXPECTED_ROWS=$SQOOP_EXPECTED_ROWS"
  fi
  echo "---- HDFS OK: $part_line_total data line(s) in part-m-* ----"
else
  hdfs dfs -test -d "$TARGET_DIR" || die "SQOOP_SKIP_IMPORT=1 but HDFS dir missing: $TARGET_DIR"
fi

if [[ "${SQOOP_SKIP_EXPORT}" == "1" ]]; then
  echo "---- SQOOP_SKIP_EXPORT=1 — done after import ----"
  exit 0
fi

echo "---- sqoop export ----"
_exp=(sqoop export --connect "$CONNECT_URL" --username "$SQOOP_MYSQL_USER")
append_sqoop_mysql_auth _exp
_exp+=(--table "$SQOOP_EXPORT_TABLE")
[[ ${#EXPORT_COL_ARGS[@]} -eq 0 ]] || _exp+=("${EXPORT_COL_ARGS[@]}")
_exp+=(--export-dir "$TARGET_DIR" --input-fields-terminated-by "$SQOOP_FIELDS_TERMINATED_BY" --m "$SQOOP_NUM_MAPPERS")
"${_exp[@]}"

if [[ "$SQOOP_MYSQL_VERIFY" != "1" ]]; then
  echo "---- SQOOP_MYSQL_VERIFY=0 — skip DB count check ----"
  echo "Sqoop smoke finished OK."
  exit 0
fi

need_cmd "$SQOOP_MYSQL_CLIENT"
echo "---- MySQL row counts ----"
SRC_N="$(mysql_batch "SELECT COUNT(*) FROM \`$SQOOP_SOURCE_TABLE\`;")"
DST_N="$(mysql_batch "SELECT COUNT(*) FROM \`$SQOOP_EXPORT_TABLE\`;")"
echo "    $SQOOP_SOURCE_TABLE: $SRC_N"
echo "    $SQOOP_EXPORT_TABLE: $DST_N"
[[ "$SRC_N" == "$DST_N" ]] || die "count mismatch: source=$SRC_N export_table=$DST_N (truncate export table or fix column mapping)"
echo "Sqoop smoke finished OK."
