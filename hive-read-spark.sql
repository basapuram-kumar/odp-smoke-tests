USE hive_spark2_compat;

SELECT 'HIVE_READ_SPARK_ORC' AS src, * FROM spark_orc ORDER BY id;
SELECT COUNT(*) AS hive_read_spark_orc_count FROM spark_orc;

SELECT h.id AS hive_id, s.id AS spark_id, h.name AS hive_name, s.name AS spark_name
FROM hive_written h
CROSS JOIN spark_orc s
WHERE h.id = 1 AND s.id = 201;
