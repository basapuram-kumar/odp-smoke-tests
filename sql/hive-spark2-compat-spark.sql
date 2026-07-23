-- PART 2: Spark2 reads Hive ORC and writes ORC (+ optional Parquet) (placeholders __DB__ / __LOC__)
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
