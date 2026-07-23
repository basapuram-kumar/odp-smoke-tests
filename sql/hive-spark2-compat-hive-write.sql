-- PART 1: Hive writes an external ORC table (placeholders __DB__ / __LOC__)
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
