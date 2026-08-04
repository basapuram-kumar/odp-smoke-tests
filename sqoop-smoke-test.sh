#!/usr/bin/env bash
#
# Sqoop smoke: MySQL -> HDFS import (default), optional export + row-count verify.
# Run from a Hadoop gateway / edge node where `sqoop` and `hdfs` use the live cluster.
#
# Auth (Kerberos, same pattern as hdfs/yarn smokes):
#   Resolve CLUSTER_NAME from Ambari (or env), then kinit as hdfs-<cluster> with the HDFS
#   headless keytab before import/export. Set SQOOP_SKIP_KINIT=1 to skip (non-Kerberos or
#   when a usable ticket is already present).
#
# MySQL fixture: created automatically (SQOOP_MYSQL_AUTO_SETUP=1, the default).
#   The script first probes whether SQOOP_MYSQL_USER can read the source table over TCP.
#   If not, it logs in as a local MySQL admin (SQOOP_MYSQL_ROOT_USER, optionally
#   SQOOP_MYSQL_ROOT_PASSWORD, else passwordless socket / ~/.my.cnf) and creates the
#   database, user, grants, and fixture tables from the effective SQOOP_MYSQL_* values.
#   Set SQOOP_MYSQL_AUTO_SETUP=0 to require the manual step instead:
#     mysql -u root -p < sql/sqoop-smoke-mysql-setup.sql
#
# Environment:
#   SQOOP_CONFIG_FILE       default: <script-dir>/configs/sqoop.env - optional KEY=value file ( configs/sqoop.env -> sqoop.env). Fills vars unset when the script
#                            started (before script defaults). Remove stale sqoop.env after upgrading.
#   AMBARI_CONFIG_FILE      default: <script-dir>/configs/ambari.env (cluster name for kinit)
#   AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD
#   CLUSTER_NAME            If set, skip Ambari lookup
#   HDFS_KEYTAB             default: /etc/security/keytabs/hdfs.headless.keytab
#   SQOOP_SKIP_KINIT        default: 0 - set 1 to skip Ambari + kinit
#   SQOOP_MYSQL_AUTO_SETUP  default: 1 - auto-create database/user/tables when the probe fails
#   SQOOP_MYSQL_SETUP       default: 0 - set 1 to always apply sql/sqoop-smoke-mysql-setup.sql as MySQL root
#   SQOOP_MYSQL_SETUP_SQL   default: <script-dir>/sql/sqoop-smoke-mysql-setup.sql
#   SQOOP_MYSQL_ROOT_USER   default: root (used only when SQOOP_MYSQL_SETUP=1)
#   SQOOP_MYSQL_ROOT_PASSWORD  optional; if unset, mysql root uses socket/auth plugin defaults
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
#   SQOOP_SKIP_EXPORT          default: 1 - import + HDFS checks only. Set 0 for export round-trip.
#   SQOOP_SKIP_IMPORT          if 1, only run export + optional verify (HDFS dir must exist)
#   SQOOP_EXPECTED_ROWS        if set, import line count must match (e.g. 5 after stock setup SQL)
#   SQOOP_MYSQL_VERIFY         default: 0. Set 1 with SQOOP_SKIP_EXPORT=0 to compare source vs export counts.
#   SQOOP_MYSQL_CLIENT         default: mysql
#   SQOOP_TRUNCATE_EXPORT      if 1, TRUNCATE export table before export (needs mysql client)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQOOP_CONFIG_FILE="${SQOOP_CONFIG_FILE:-$SCRIPT_DIR/configs/sqoop.env}"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"

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
      SQOOP_SKIP_KINIT) [[ "${SQOOP_SKIP_KINIT+set}" == "set" ]] || SQOOP_SKIP_KINIT="$val" ;;
      SQOOP_MYSQL_SETUP) [[ "${SQOOP_MYSQL_SETUP+set}" == "set" ]] || SQOOP_MYSQL_SETUP="$val" ;;
      SQOOP_MYSQL_AUTO_SETUP) [[ "${SQOOP_MYSQL_AUTO_SETUP+set}" == "set" ]] || SQOOP_MYSQL_AUTO_SETUP="$val" ;;
      SQOOP_MYSQL_SETUP_SQL) [[ "${SQOOP_MYSQL_SETUP_SQL+set}" == "set" ]] || SQOOP_MYSQL_SETUP_SQL="$val" ;;
      SQOOP_MYSQL_ROOT_USER) [[ "${SQOOP_MYSQL_ROOT_USER+set}" == "set" ]] || SQOOP_MYSQL_ROOT_USER="$val" ;;
      SQOOP_MYSQL_ROOT_PASSWORD) [[ "${SQOOP_MYSQL_ROOT_PASSWORD+set}" == "set" ]] || SQOOP_MYSQL_ROOT_PASSWORD="$val" ;;
      HDFS_KEYTAB) [[ "${HDFS_KEYTAB+set}" == "set" ]] || HDFS_KEYTAB="$val" ;;
      CLUSTER_NAME) [[ "${CLUSTER_NAME+set}" == "set" ]] || CLUSTER_NAME="$val" ;;
      AMBARI_BASE_URL) [[ "${AMBARI_BASE_URL+set}" == "set" ]] || AMBARI_BASE_URL="$val" ;;
      AMBARI_USER) [[ "${AMBARI_USER+set}" == "set" ]] || AMBARI_USER="$val" ;;
      AMBARI_PASSWORD) [[ "${AMBARI_PASSWORD+set}" == "set" ]] || AMBARI_PASSWORD="$val" ;;
    esac
  done <"$f"
  return 0
}

load_ambari_env_file() {
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
      AMBARI_BASE_URL) _cfg_AMBARI_BASE_URL="$val" ;;
      AMBARI_USER) _cfg_AMBARI_USER="$val" ;;
      AMBARI_PASSWORD) _cfg_AMBARI_PASSWORD="$val" ;;
    esac
  done <"$f"
  return 0
}

resolve_cluster_and_kinit_hdfs() {
  local cluster clusters_url json principal
  _cfg_AMBARI_BASE_URL=""
  _cfg_AMBARI_USER=""
  _cfg_AMBARI_PASSWORD=""

  need_cmd curl
  need_cmd kinit
  need_cmd python3

  if [[ -n "${CLUSTER_NAME:-}" ]]; then
    :
  elif [[ -f "$AMBARI_CONFIG_FILE" ]]; then
    load_ambari_env_file "$AMBARI_CONFIG_FILE" || die "failed to read $AMBARI_CONFIG_FILE"
  elif [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]]; then
    :
  else
    die "Missing Ambari credentials. Edit ${AMBARI_CONFIG_FILE} or set AMBARI_USER and AMBARI_PASSWORD, or set CLUSTER_NAME / SQOOP_SKIP_KINIT=1."
  fi

  AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://10.101.11.22:8080}}"
  AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
  AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"

  if [[ -z "${CLUSTER_NAME:-}" ]]; then
    [[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]] || die "AMBARI_USER and AMBARI_PASSWORD must be set in ${AMBARI_CONFIG_FILE} or in the environment."
  fi

  [[ -r "$HDFS_KEYTAB" ]] || die "keytab not readable: $HDFS_KEYTAB"

  if [[ -n "${CLUSTER_NAME:-}" ]]; then
    cluster="$CLUSTER_NAME"
  else
    clusters_url="${AMBARI_BASE_URL%/}/api/v1/clusters/"
    json="$(curl -sS -f -u "${AMBARI_USER}:${AMBARI_PASSWORD}" -H "X-Requested-By: ambari" "$clusters_url")" \
      || die "failed to GET $clusters_url"
    cluster="$(
      printf '%s\n' "$json" | python3 -c "
import json, sys
data = json.load(sys.stdin)
items = data.get('items') or []
if not items:
    sys.exit('no clusters in Ambari response')
name = (items[0].get('Clusters') or {}).get('cluster_name')
if not name:
    sys.exit('could not parse cluster_name from Ambari response')
print(name)
"
    )" || die "could not parse cluster name from Ambari JSON"
  fi

  principal="hdfs-${cluster}"
  echo "Using cluster: ${cluster}"
  echo "kinit principal: ${principal} (realm from krb5.conf / keytab)"
  kinit -kt "$HDFS_KEYTAB" "$principal" || die "kinit failed"
}

apply_mysql_setup_sql_if_requested() {
  [[ "$SQOOP_MYSQL_SETUP" == "1" ]] || return 0
  need_cmd "$SQOOP_MYSQL_CLIENT"
  [[ -r "$SQOOP_MYSQL_SETUP_SQL" ]] || die "SQOOP_MYSQL_SETUP_SQL not readable: $SQOOP_MYSQL_SETUP_SQL"
  echo "---- MySQL setup: $SQOOP_MYSQL_SETUP_SQL (as ${SQOOP_MYSQL_ROOT_USER}) ----"
  if [[ -n "${SQOOP_MYSQL_ROOT_PASSWORD+set}" ]]; then
    MYSQL_PWD="${SQOOP_MYSQL_ROOT_PASSWORD}" "$SQOOP_MYSQL_CLIENT" -u"$SQOOP_MYSQL_ROOT_USER" <"$SQOOP_MYSQL_SETUP_SQL" \
      || die "MySQL setup failed"
  else
    "$SQOOP_MYSQL_CLIENT" -u"$SQOOP_MYSQL_ROOT_USER" <"$SQOOP_MYSQL_SETUP_SQL" \
      || die "MySQL setup failed"
  fi
}

# ---------------------------------------------------------------------------
# Auto-provisioning of the MySQL fixture (no manual step needed).
#
# Probe as the smoke user first; only touch MySQL as admin when the probe fails.
# SQL is generated from the effective SQOOP_MYSQL_* values so overrides work too.
# ---------------------------------------------------------------------------

# Populates MYSQL_ADMIN_CMD with a working admin invocation, or returns 1.
# Order: explicit root password, then passwordless socket/defaults-file login.
declare -a MYSQL_ADMIN_CMD=()
resolve_mysql_admin_cmd() {
  command -v "$SQOOP_MYSQL_CLIENT" >/dev/null 2>&1 || return 1

  if [[ -n "${SQOOP_MYSQL_ROOT_PASSWORD:-}" ]]; then
    if MYSQL_PWD="$SQOOP_MYSQL_ROOT_PASSWORD" "$SQOOP_MYSQL_CLIENT" \
        -u"$SQOOP_MYSQL_ROOT_USER" -N -e 'SELECT 1;' >/dev/null 2>&1; then
      MYSQL_ADMIN_CMD=(env "MYSQL_PWD=$SQOOP_MYSQL_ROOT_PASSWORD" "$SQOOP_MYSQL_CLIENT" -u"$SQOOP_MYSQL_ROOT_USER")
      return 0
    fi
    return 1
  fi

  # Local socket as OS root, or credentials from ~/.my.cnf.
  if "$SQOOP_MYSQL_CLIENT" -u"$SQOOP_MYSQL_ROOT_USER" -N -e 'SELECT 1;' >/dev/null 2>&1; then
    MYSQL_ADMIN_CMD=("$SQOOP_MYSQL_CLIENT" -u"$SQOOP_MYSQL_ROOT_USER")
    return 0
  fi
  if "$SQOOP_MYSQL_CLIENT" -N -e 'SELECT 1;' >/dev/null 2>&1; then
    MYSQL_ADMIN_CMD=("$SQOOP_MYSQL_CLIENT")
    return 0
  fi
  return 1
}

# True when the smoke user can read the source table over TCP (what Sqoop does).
smoke_user_can_read_source() {
  command -v "$SQOOP_MYSQL_CLIENT" >/dev/null 2>&1 || return 1
  local pw="${SQOOP_MYSQL_PASSWORD:-}"
  if [[ -n "${SQOOP_MYSQL_PASSWORD_FILE:-}" && -f "${SQOOP_MYSQL_PASSWORD_FILE}" ]]; then
    pw="$(tr -d '\n\r' <"$SQOOP_MYSQL_PASSWORD_FILE")"
  fi
  MYSQL_PWD="$pw" "$SQOOP_MYSQL_CLIENT" \
    -h "$SQOOP_MYSQL_HOST" -P"$SQOOP_MYSQL_PORT" -u"$SQOOP_MYSQL_USER" \
    -N -e "SELECT COUNT(*) FROM \`${SQOOP_SOURCE_TABLE}\`;" "$SQOOP_MYSQL_DATABASE" \
    >/dev/null 2>&1
}

# Emit idempotent provisioning SQL for the effective database/user/tables.
# ALTER USER is included so an existing account with a stale password is repaired.
generate_mysql_setup_sql() {
  local db="$SQOOP_MYSQL_DATABASE"
  local user="$SQOOP_MYSQL_USER"
  local pw="${SQOOP_MYSQL_PASSWORD:-}"
  if [[ -n "${SQOOP_MYSQL_PASSWORD_FILE:-}" && -f "${SQOOP_MYSQL_PASSWORD_FILE}" ]]; then
    pw="$(tr -d '\n\r' <"$SQOOP_MYSQL_PASSWORD_FILE")"
  fi
  local src="$SQOOP_SOURCE_TABLE"
  local dst="$SQOOP_EXPORT_TABLE"

  cat <<SQL
CREATE DATABASE IF NOT EXISTS \`${db}\`;

CREATE USER IF NOT EXISTS '${user}'@'%' IDENTIFIED BY '${pw}';
CREATE USER IF NOT EXISTS '${user}'@'localhost' IDENTIFIED BY '${pw}';
ALTER USER '${user}'@'%' IDENTIFIED BY '${pw}';
ALTER USER '${user}'@'localhost' IDENTIFIED BY '${pw}';

GRANT ALL PRIVILEGES ON \`${db}\`.* TO '${user}'@'%';
GRANT ALL PRIVILEGES ON \`${db}\`.* TO '${user}'@'localhost';
FLUSH PRIVILEGES;

USE \`${db}\`;

CREATE TABLE IF NOT EXISTS \`${src}\` (
  id INT NOT NULL,
  label VARCHAR(64) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB;

INSERT INTO \`${src}\` (id, label) VALUES
  (1, 'alpha'),
  (2, 'beta'),
  (3, 'gamma'),
  (4, 'delta'),
  (5, 'epsilon')
ON DUPLICATE KEY UPDATE label = VALUES(label);

CREATE TABLE IF NOT EXISTS \`${dst}\` LIKE \`${src}\`;
SQL
}

# Create database/user/tables when the smoke user cannot read the fixture yet.
ensure_mysql_fixture() {
  [[ "$SQOOP_MYSQL_AUTO_SETUP" == "1" ]] || return 0

  if smoke_user_can_read_source; then
    echo "---- MySQL fixture OK: ${SQOOP_MYSQL_USER}@${SQOOP_MYSQL_HOST} can read ${SQOOP_MYSQL_DATABASE}.${SQOOP_SOURCE_TABLE} ----"
    return 0
  fi

  echo "---- MySQL fixture missing or unusable - provisioning automatically ----"
  if ! command -v "$SQOOP_MYSQL_CLIENT" >/dev/null 2>&1; then
    die "mysql client not found, so the fixture cannot be created automatically. Install it, or apply ${SQOOP_MYSQL_SETUP_SQL} on the MySQL host, or set SQOOP_MYSQL_AUTO_SETUP=0."
  fi

  if ! resolve_mysql_admin_cmd; then
    die "cannot log in to MySQL as an admin to create the fixture. Set SQOOP_MYSQL_ROOT_USER / SQOOP_MYSQL_ROOT_PASSWORD, or apply ${SQOOP_MYSQL_SETUP_SQL} manually, or set SQOOP_MYSQL_AUTO_SETUP=0."
  fi

  echo "    admin login: ${SQOOP_MYSQL_ROOT_USER} (local)"
  echo "    creating database=${SQOOP_MYSQL_DATABASE} user=${SQOOP_MYSQL_USER} tables=${SQOOP_SOURCE_TABLE},${SQOOP_EXPORT_TABLE}"
  if ! generate_mysql_setup_sql | "${MYSQL_ADMIN_CMD[@]}"; then
    die "automatic MySQL provisioning failed (admin user ${SQOOP_MYSQL_ROOT_USER}). Apply ${SQOOP_MYSQL_SETUP_SQL} manually or set SQOOP_MYSQL_AUTO_SETUP=0."
  fi

  if ! smoke_user_can_read_source; then
    die "provisioning ran but ${SQOOP_MYSQL_USER}@${SQOOP_MYSQL_HOST} still cannot read ${SQOOP_MYSQL_DATABASE}.${SQOOP_SOURCE_TABLE}. Check MySQL host-based grants (needs '${SQOOP_MYSQL_USER}'@'%') and the authentication plugin."
  fi
  echo "---- MySQL fixture ready ----"
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
SQOOP_SKIP_KINIT="${SQOOP_SKIP_KINIT:-0}"
SQOOP_MYSQL_SETUP="${SQOOP_MYSQL_SETUP:-0}"
SQOOP_MYSQL_AUTO_SETUP="${SQOOP_MYSQL_AUTO_SETUP:-1}"
SQOOP_MYSQL_SETUP_SQL="${SQOOP_MYSQL_SETUP_SQL:-${SCRIPT_DIR}/sql/sqoop-smoke-mysql-setup.sql}"
SQOOP_MYSQL_ROOT_USER="${SQOOP_MYSQL_ROOT_USER:-root}"
HDFS_KEYTAB="${HDFS_KEYTAB:-/etc/security/keytabs/hdfs.headless.keytab}"

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

# Old sqoop.env used hive + sqoop_test; smoke_import only exists in sqoop_smoke per setup SQL.
if [[ "$SQOOP_MYSQL_USER" == "hive" && "$SQOOP_MYSQL_DATABASE" == "sqoop_test" && "$SQOOP_SOURCE_TABLE" == "smoke_import" ]]; then
  die "MySQL settings look stale: user=hive database=sqoop_test but sql/sqoop-smoke-mysql-setup.sql creates sqoop_smoke.smoke_import for user sqoop_smoke. Remove or edit ${SQOOP_CONFIG_FILE}, or export SQOOP_MYSQL_DATABASE=sqoop_smoke SQOOP_MYSQL_USER=sqoop_smoke SQOOP_MYSQL_PASSWORD=sqoop_smoke"
fi

if [[ "$SQOOP_SKIP_KINIT" == "1" ]]; then
  echo "---- SQOOP_SKIP_KINIT=1 - skipping Ambari cluster lookup and hdfs kinit ----"
else
  resolve_cluster_and_kinit_hdfs
fi

apply_mysql_setup_sql_if_requested

if [[ -z "${SQOOP_MYSQL_HOST:-}" ]]; then
  SQOOP_MYSQL_HOST="$(default_sqoop_mysql_host)"
  echo "---- SQOOP_MYSQL_HOST unset - using this host: $SQOOP_MYSQL_HOST ----" >&2
fi
validate_mysql_password_file_if_set

# Needs SQOOP_MYSQL_HOST resolved, so it runs after the default above.
ensure_mysql_fixture

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
  echo "---- SQOOP_SKIP_EXPORT=1 - done after import ----"
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
  echo "---- SQOOP_MYSQL_VERIFY=0 - skip DB count check ----"
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
