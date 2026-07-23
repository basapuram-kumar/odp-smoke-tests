-- PART 3b (optional probe): Hive read Spark Parquet — may fail on some ODP builds
-- Known issues seen on Hive 4 + Spark 2.4:
--   vectorized: NoClassDefFoundError org/locationtech/jts/io/ParseException
--   non-vectorized: NoSuchMethodError FutureDataInputStreamBuilder.withFileStatus
USE __DB__;
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;
SELECT 'HIVE_READ_SPARK_PARQUET' AS src, * FROM spark_parquet ORDER BY id;
SELECT COUNT(*) AS hive_read_spark_parquet_count FROM spark_parquet;
