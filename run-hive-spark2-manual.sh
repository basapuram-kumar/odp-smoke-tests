#!/usr/bin/env bash
# Manual 3-step Hive <-> Spark2 ORC round-trip using concrete SQL files
# in this directory (hive-write.sql, spark.sql, hive-read-spark.sql).
#
# On cluster:
#   cd /tmp/basas/odp-smoke-tests && sudo ./run-hive-spark2-manual.sh
set -euo pipefail
cd "$(dirname "$0")"
HIVE_KT="${HIVE_KEYTAB:-/etc/security/keytabs/hive.service.keytab}"
SPARK_KT="${SPARK_KEYTAB:-/etc/security/keytabs/spark.service.keytab}"
HDFS_KT="${HDFS_KEYTAB:-/etc/security/keytabs/hdfs.headless.keytab}"
HDFS_PRINC="${HDFS_PRINCIPAL:-hdfs-odp2007}"
LOC="/tmp/hive_spark2_compat"
SPARK2="${SPARK2_CLIENT_HOME:-/usr/odp/current/spark2-client}"

echo "==> HDFS setup: $LOC"
kinit -kt "$HDFS_KT" "$HDFS_PRINC"
hdfs dfs -mkdir -p "$LOC"
hdfs dfs -chmod -R 777 "$LOC"
kdestroy 2>/dev/null || true

echo "==> PART 1: Hive write ORC"
PRINC=$(klist -kt "$HIVE_KT" | awk '/@/{print $NF; exit}')
kinit -kt "$HIVE_KT" "$PRINC"
beeline --showHeader=true --outputformat=table -f "$(pwd)/hive-write.sql"
kdestroy 2>/dev/null || true

echo "==> PART 2: Spark2 read Hive + write ORC"
PRINC=$(klist -kt "$SPARK_KT" | awk '/@/{print $NF; exit}')
kinit -kt "$SPARK_KT" "$PRINC"
export SPARK_MAJOR_VERSION=2
"$SPARK2/bin/spark-sql" --master yarn --deploy-mode client --name hive-spark2-manual \
  -f "$(pwd)/spark.sql"
kdestroy 2>/dev/null || true

echo "==> PART 3: Hive read Spark ORC"
PRINC=$(klist -kt "$HIVE_KT" | awk '/@/{print $NF; exit}')
kinit -kt "$HIVE_KT" "$PRINC"
beeline --showHeader=true --outputformat=table -f "$(pwd)/hive-read-spark.sql"
kdestroy 2>/dev/null || true

echo "==> RESULT: PASS"
