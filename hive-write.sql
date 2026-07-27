CREATE DATABASE IF NOT EXISTS hive_spark2_compat;
USE hive_spark2_compat;

DROP TABLE IF EXISTS hive_written;
CREATE EXTERNAL TABLE hive_written (
  id INT,
  name STRING,
  amount DOUBLE
) STORED AS ORC
LOCATION '/tmp/hive_spark2_compat/hive_written';

INSERT INTO hive_written VALUES
  (1, 'alice', 10.5),
  (2, 'bob', 20.0),
  (3, 'carol', 30.25);

SELECT 'HIVE_READ_OWN' AS src, * FROM hive_written ORDER BY id;
SELECT COUNT(*) AS hive_row_count FROM hive_written;
