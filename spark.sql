USE hive_spark2_compat;

SELECT 'SPARK2_READ_HIVE' AS src, * FROM hive_written ORDER BY id;
SELECT COUNT(*) AS spark_read_hive_count FROM hive_written;

DROP TABLE IF EXISTS spark_orc;
CREATE EXTERNAL TABLE spark_orc (
  id INT,
  name STRING,
  amount DOUBLE
) STORED AS ORC
LOCATION '/tmp/hive_spark2_compat/spark_orc';

INSERT INTO TABLE spark_orc VALUES
  (201, 'gina', 70.0),
  (202, 'hank', 80.5);

SELECT 'SPARK2_READ_ORC' AS src, * FROM spark_orc ORDER BY id;
SELECT COUNT(*) AS spark_orc_count FROM spark_orc;
