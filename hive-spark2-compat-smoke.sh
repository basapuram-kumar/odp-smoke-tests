#!/usr/bin/env bash
#
# Hive <-> Spark2 compatibility smoke:
#   1) Hive creates an external ORC table under a shared HDFS path
#   2) Spark2 (spark-sql, YARN) reads that table and writes ORC + Parquet
#   3) Hive reads the Spark2 ORC table back
#
# Uses external tables under /tmp/... because the managed warehouse
# (/warehouse/tablespace/managed/hive) is typically hive-only (drwx------),
# so the spark user cannot traverse it.
#
# Run on a cluster node that has Hive + Spark2 clients and matching keytabs
# (hive/<host>, spark/<host>, hdfs-<cluster>). Prefer root/sudo for keytab access.
#
# Environment (optional):
#   HIVE_KEYTAB / HIVE_PRINCIPAL_HOST / HIVE_JDBC_URL / HIVE_CONFIG_FILE
#   SPARK_KEYTAB / SPARK_PRINCIPAL_HOST / SPARK2_CLIENT_HOME / SPARK_SQL
#   SPARK_MAJOR_VERSION          default 2
#   HDFS_KEYTAB                  default /etc/security/keytabs/hdfs.headless.keytab
#   HDFS_PRINCIPAL               e.g. hdfs-odp2007 (skips Ambari if set)
#   CLUSTER_NAME                 used as hdfs-${CLUSTER_NAME} when HDFS_PRINCIPAL unset
#   AMBARI_* / AMBARI_CONFIG_FILE  Ambari lookup for CLUSTER_NAME (same as hdfs-headless-smoke)
#   COMPAT_DB                    Hive database name (default hive_spark2_compat_<ts>)
#   COMPAT_LOC                   HDFS location (default /tmp/${COMPAT_DB})
#   SKIP_HDFS_SETUP=1            skip mkdir/chmod of COMPAT_LOC (must already be writable)
#   TEST_PARQUET=1               also probe Hive reading Spark Parquet (non-fatal by default)
#   FAIL_ON_PARQUET=1            with TEST_PARQUET=1, fail the script if Parquet probe fails
#   CLEANUP=1                    DROP DATABASE CASCADE at the end (and optionally remove HDFS loc)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HIVE_CONFIG_FILE="${HIVE_CONFIG_FILE:-${SCRIPT_DIR}/configs/hive.env}"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
HIVE_KEYTAB="${HIVE_KEYTAB:-/etc/security/keytabs/hive.service.keytab}"
SPARK_KEYTAB="${SPARK_KEYTAB:-/etc/security/keytabs/spark.service.keytab}"
HDFS_KEYTAB="${HDFS_KEYTAB:-/etc/security/keytabs/hdfs.headless.keytab}"
SPARK2_CLIENT_HOME="${SPARK2_CLIENT_HOME:-/usr/odp/current/spark2-client}"
SPARK_MAJOR_VERSION="${SPARK_MAJOR_VERSION:-2}"
HIVE_USE_CONFIG_PRINCIPAL_HOST="${HIVE_USE_CONFIG_PRINCIPAL_HOST:-0}"
SKIP_HDFS_SETUP="${SKIP_HDFS_SETUP:-0}"
TEST_PARQUET="${TEST_PARQUET:-0}"
FAIL_ON_PARQUET="${FAIL_ON_PARQUET:-0}"
CLEANUP="${CLEANUP:-0}"

SQL_HIVE_WRITE="${COMPAT_HIVE_WRITE_SQL:-${SCRIPT_DIR}/sql/hive-spark2-compat-hive-write.sql}"
SQL_SPARK="${COMPAT_SPARK_SQL:-${SCRIPT_DIR}/sql/hive-spark2-compat-spark.sql}"
SQL_HIVE_READ="${COMPAT_HIVE_READ_SQL:-${SCRIPT_DIR}/sql/hive-spark2-compat-hive-read-spark.sql}"
SQL_HIVE_PARQUET="${COMPAT_HIVE_PARQUET_SQL:-${SCRIPT_DIR}/sql/hive-spark2-compat-hive-read-parquet.sql}"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

log() {
  echo "$@"
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

load_kv_env_file() {
  # Load known KEY=value lines into _cfg_* globals (comments/blank lines ignored).
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
      AMBARI_BASE_URL) _cfg_AMBARI_BASE_URL="$val" ;;
      AMBARI_USER) _cfg_AMBARI_USER="$val" ;;
      AMBARI_PASSWORD) _cfg_AMBARI_PASSWORD="$val" ;;
    esac
  done <"$f"
  return 0
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

resolve_host() {
  # $1 = optional explicit host override env value already resolved by caller
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
  [[ -n "$h" ]] || die "could not determine hostname; set HIVE_PRINCIPAL_HOST / SPARK_PRINCIPAL_HOST"
  printf '%s' "$h"
}

write_embedded_sql() {
  # $1=kind (hive_write|spark|hive_read|hive_parquet)  $2=output path
  # Used when sql/*.sql templates are not present next to the script (e.g. only .sh copied).
  local kind="$1" out="$2" raw
  raw="${out}.raw"
  case "$kind" in
    hive_write)
      cat >"$raw" <<'EMBED_SQL'
CREATE DATABASE IF NOT EXISTS __DB__ LOCATION '__LOC__';
USE __DB__;

DROP TABLE IF EXISTS hive_written;
CREATE EXTERNAL TABLE hive_written (
  id INT,
  name STRING,
  amount DOUBLE
) STORED AS ORC
LOCATION '__LOC__/hive_written';

INSERT INTO hive_written VALUES
  (1, 'alice', 10.5),
  (2, 'bob', 20.0),
  (3, 'carol', 30.25);

SELECT 'HIVE_READ_OWN' AS src, * FROM hive_written ORDER BY id;
SELECT COUNT(*) AS hive_row_count FROM hive_written;
EMBED_SQL
      ;;
    spark)
      cat >"$raw" <<'EMBED_SQL'
USE __DB__;

SELECT 'SPARK2_READ_HIVE' AS src, * FROM hive_written ORDER BY id;
SELECT COUNT(*) AS spark_read_hive_count FROM hive_written;

DROP TABLE IF EXISTS spark_orc;
CREATE EXTERNAL TABLE spark_orc (
  id INT,
  name STRING,
  amount DOUBLE
) STORED AS ORC
LOCATION '__LOC__/spark_orc';

INSERT INTO TABLE spark_orc VALUES
  (201, 'gina', 70.0),
  (202, 'hank', 80.5);

SELECT 'SPARK2_READ_ORC' AS src, * FROM spark_orc ORDER BY id;
SELECT COUNT(*) AS spark_orc_count FROM spark_orc;

DROP TABLE IF EXISTS spark_parquet;
CREATE EXTERNAL TABLE spark_parquet (
  id INT,
  name STRING,
  amount DOUBLE
) STORED AS PARQUET
LOCATION '__LOC__/spark_parquet';

INSERT INTO TABLE spark_parquet VALUES
  (301, 'ivy', 90.0),
  (302, 'jade', 100.25);

SELECT 'SPARK2_READ_PARQUET' AS src, * FROM spark_parquet ORDER BY id;
SELECT COUNT(*) AS spark_parquet_count FROM spark_parquet;
EMBED_SQL
      ;;
    hive_read)
      cat >"$raw" <<'EMBED_SQL'
USE __DB__;

SELECT 'HIVE_READ_SPARK_ORC' AS src, * FROM spark_orc ORDER BY id;
SELECT COUNT(*) AS hive_read_spark_orc_count FROM spark_orc;

SELECT h.id AS hive_id, s.id AS spark_id, h.name AS hive_name, s.name AS spark_name
FROM hive_written h
CROSS JOIN spark_orc s
WHERE h.id = 1 AND s.id = 201;
EMBED_SQL
      ;;
    hive_parquet)
      cat >"$raw" <<'EMBED_SQL'
USE __DB__;
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;
SELECT 'HIVE_READ_SPARK_PARQUET' AS src, * FROM spark_parquet ORDER BY id;
SELECT COUNT(*) AS hive_read_spark_parquet_count FROM spark_parquet;
EMBED_SQL
      ;;
    *)
      die "unknown embedded SQL kind: $kind"
      ;;
  esac
  sed -e "s|__DB__|${COMPAT_DB}|g" -e "s|__LOC__|${COMPAT_LOC}|g" "$raw" >"$out"
  rm -f "$raw"
}

render_sql() {
  # $1=optional template path  $2=output  $3=embedded kind (hive_write|spark|hive_read|hive_parquet)
  local tmpl="$1" out="$2" kind="$3"
  if [[ -n "$tmpl" && -r "$tmpl" ]]; then
    sed -e "s|__DB__|${COMPAT_DB}|g" -e "s|__LOC__|${COMPAT_LOC}|g" "$tmpl" >"$out"
  else
    if [[ -n "$tmpl" ]]; then
      log "WARN: SQL template missing (${tmpl}); using embedded SQL (${kind})"
    fi
    write_embedded_sql "$kind" "$out"
  fi
}

run_beeline() {
  local sql_file="$1"
  if [[ -n "${HIVE_JDBC_URL:-}" ]]; then
    beeline --showHeader=true --outputformat=table -u "$HIVE_JDBC_URL" -f "$sql_file"
  else
    beeline --showHeader=true --outputformat=table -f "$sql_file"
  fi
}

resolve_spark_sql() {
  if [[ -n "${SPARK_SQL:-}" ]]; then
    printf '%s' "$SPARK_SQL"
    return
  fi
  local p="${SPARK2_CLIENT_HOME}/bin/spark-sql"
  if [[ -x "$p" ]]; then
    printf '%s' "$p"
    return
  fi
  need_cmd spark-sql
  command -v spark-sql
}

resolve_hdfs_principal() {
  if [[ -n "${HDFS_PRINCIPAL:-}" ]]; then
    printf '%s' "$HDFS_PRINCIPAL"
    return
  fi
  if [[ -n "${CLUSTER_NAME:-}" ]]; then
    printf 'hdfs-%s' "$CLUSTER_NAME"
    return
  fi

  # Ambari lookup (same pattern as hdfs-headless-smoke.sh)
  need_cmd curl
  need_cmd python3
  local base user pass clusters_url json cluster
  base="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://localhost:8080}}"
  user="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
  pass="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"
  [[ -n "$user" && -n "$pass" ]] || die "Set HDFS_PRINCIPAL, CLUSTER_NAME, or Ambari credentials (configs/ambari.env) for HDFS setup"
  clusters_url="${base%/}/api/v1/clusters/"
  json="$(curl -sS -f -u "${user}:${pass}" -H "X-Requested-By: ambari" "$clusters_url")" \
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
    sys.exit('could not parse cluster_name')
print(name)
"
  )" || die "could not parse cluster name from Ambari"
  CLUSTER_NAME="$cluster"
  printf 'hdfs-%s' "$cluster"
}

# --- load optional configs ---
_cfg_HIVE_JDBC_URL=""
_cfg_HIVE_KEYTAB=""
_cfg_HIVE_PRINCIPAL_HOST=""
_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""

[[ -f "$HIVE_CONFIG_FILE" ]] && load_kv_env_file "$HIVE_CONFIG_FILE" || true
[[ -f "$AMBARI_CONFIG_FILE" ]] && load_kv_env_file "$AMBARI_CONFIG_FILE" || true

HIVE_KEYTAB="${HIVE_KEYTAB:-${_cfg_HIVE_KEYTAB:-/etc/security/keytabs/hive.service.keytab}}"
HIVE_JDBC_URL="${HIVE_JDBC_URL:-${_cfg_HIVE_JDBC_URL:-}}"
if [[ -z "${HIVE_PRINCIPAL_HOST:-}" && "$HIVE_USE_CONFIG_PRINCIPAL_HOST" == "1" && -n "${_cfg_HIVE_PRINCIPAL_HOST:-}" ]]; then
  HIVE_PRINCIPAL_HOST="${_cfg_HIVE_PRINCIPAL_HOST}"
fi

need_cmd kinit
need_cmd beeline
need_cmd sed

[[ -r "$HIVE_KEYTAB" ]] || die "keytab not readable: $HIVE_KEYTAB"
[[ -r "$SPARK_KEYTAB" ]] || die "keytab not readable: $SPARK_KEYTAB"
[[ -d "$SPARK2_CLIENT_HOME" ]] || die "SPARK2_CLIENT_HOME is not a directory: $SPARK2_CLIENT_HOME"

hive_host="$(resolve_host "${HIVE_PRINCIPAL_HOST:-}")"
spark_host="$(resolve_host "${SPARK_PRINCIPAL_HOST:-}")"
hive_principal="hive/${hive_host}"
spark_principal="spark/${spark_host}"
spark_sql_bin="$(resolve_spark_sql)"

TS="$(date +%Y%m%d_%H%M%S)"
COMPAT_DB="${COMPAT_DB:-hive_spark2_compat_${TS}}"
COMPAT_LOC="${COMPAT_LOC:-/tmp/${COMPAT_DB}}"

WORKDIR="$(mktemp -d /tmp/hive-spark2-compat.XXXXXX)"
trap 'rm -rf "$WORKDIR"' EXIT

SQL1="${WORKDIR}/01-hive-write.sql"
SQL2="${WORKDIR}/02-spark.sql"
SQL3="${WORKDIR}/03-hive-read-spark.sql"
SQL4="${WORKDIR}/04-hive-read-parquet.sql"
render_sql "$SQL_HIVE_WRITE" "$SQL1" hive_write
render_sql "$SQL_SPARK" "$SQL2" spark
render_sql "$SQL_HIVE_READ" "$SQL3" hive_read
render_sql "$SQL_HIVE_PARQUET" "$SQL4" hive_parquet

log "=============================================="
log "Hive <-> Spark2 compatibility smoke"
log "Hive principal:  ${hive_principal}"
log "Spark principal: ${spark_principal}"
log "spark-sql:       ${spark_sql_bin}"
log "SPARK_MAJOR_VERSION=${SPARK_MAJOR_VERSION}"
log "Database:        ${COMPAT_DB}"
log "HDFS location:   ${COMPAT_LOC}"
if [[ -n "$HIVE_JDBC_URL" ]]; then
  log "Beeline URL:     ${HIVE_JDBC_URL}"
else
  log "Beeline URL:     (default from beeline-site.xml / hive-site.xml)"
fi
log "=============================================="

############################################
# HDFS shared location
############################################
if [[ "$SKIP_HDFS_SETUP" != "1" ]]; then
  need_cmd hdfs
  [[ -r "$HDFS_KEYTAB" ]] || die "HDFS keytab not readable: $HDFS_KEYTAB (or set SKIP_HDFS_SETUP=1)"
  hdfs_principal="$(resolve_hdfs_principal)"
  log ""
  log ">>> HDFS setup as ${hdfs_principal}"
  kinit -kt "$HDFS_KEYTAB" "$hdfs_principal" || die "hdfs kinit failed"
  hdfs dfs -rm -r -f "$COMPAT_LOC" >/dev/null 2>&1 || true
  hdfs dfs -mkdir -p "$COMPAT_LOC"
  hdfs dfs -chmod -R 777 "$COMPAT_LOC"
  hdfs dfs -ls -d "$COMPAT_LOC"
else
  log ">>> Skipping HDFS setup (SKIP_HDFS_SETUP=1)"
fi

############################################
# PART 1: Hive write ORC
############################################
log ""
log ">>> PART 1: Hive write external ORC"
kinit -kt "$HIVE_KEYTAB" "$hive_principal" || die "hive kinit failed"
run_beeline "$SQL1"
log "PASS: Hive write/read ORC"

############################################
# PART 2: Spark2 read Hive + write ORC/Parquet
############################################
log ""
log ">>> PART 2: Spark2 read Hive ORC; write ORC + Parquet"
kinit -kt "$SPARK_KEYTAB" "$spark_principal" || die "spark kinit failed"
export SPARK_MAJOR_VERSION
"$spark_sql_bin" \
  --master yarn \
  --deploy-mode client \
  --name hive-spark2-compat-smoke \
  -f "$SQL2"
log "PASS: Spark2 read Hive + write ORC/Parquet"

############################################
# PART 3: Hive read Spark ORC
############################################
log ""
log ">>> PART 3: Hive read Spark2 ORC"
kinit -kt "$HIVE_KEYTAB" "$hive_principal" || die "hive kinit failed"
run_beeline "$SQL3"
log "PASS: Hive read Spark2 ORC"

############################################
# Optional: Hive read Spark Parquet (often fails)
############################################
if [[ "$TEST_PARQUET" == "1" ]]; then
  log ""
  log ">>> PART 3b (optional): Hive read Spark2 Parquet"
  set +e
  run_beeline "$SQL4"
  parquet_rc=$?
  set -e
  if [[ "$parquet_rc" -eq 0 ]]; then
    log "PASS: Hive read Spark2 Parquet"
  else
    log "WARN: Hive read Spark2 Parquet failed (rc=${parquet_rc})"
    log "      Known on some ODP builds: missing JTS and/or Parquet/Hadoop API mismatch."
    if [[ "$FAIL_ON_PARQUET" == "1" ]]; then
      die "Parquet probe failed and FAIL_ON_PARQUET=1"
    fi
  fi
fi

############################################
# Optional cleanup
############################################
if [[ "$CLEANUP" == "1" ]]; then
  log ""
  log ">>> CLEANUP: DROP DATABASE CASCADE ${COMPAT_DB}"
  kinit -kt "$HIVE_KEYTAB" "$hive_principal" || die "hive kinit failed"
  cleanup_sql="${WORKDIR}/99-cleanup.sql"
  cat >"$cleanup_sql" <<SQL
DROP DATABASE IF EXISTS ${COMPAT_DB} CASCADE;
SQL
  run_beeline "$cleanup_sql" || log "WARN: DROP DATABASE failed"
  if [[ "$SKIP_HDFS_SETUP" != "1" ]]; then
    hdfs_principal="$(resolve_hdfs_principal)"
    kinit -kt "$HDFS_KEYTAB" "$hdfs_principal" || true
    hdfs dfs -rm -r -f "$COMPAT_LOC" >/dev/null 2>&1 || true
  fi
fi

log ""
log "=============================================="
log "RESULT: Hive <-> Spark2 ORC compatibility PASS"
log "Database: ${COMPAT_DB}"
log "Location: ${COMPAT_LOC}"
log "Tables:   hive_written (ORC), spark_orc (ORC), spark_parquet (PARQUET)"
log "=============================================="
