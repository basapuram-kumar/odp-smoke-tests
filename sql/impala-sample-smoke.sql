CREATE DATABASE IF NOT EXISTS test;
USE test;
DROP TABLE IF EXISTS basa;
CREATE TABLE basa (id INT, name STRING);

INSERT INTO basa VALUES
(10, 'basa'),
(11, 'sample_1'),
(12, 'sample_2'),
(13, 'sample_3'),
(14, 'sample_4'),
(15, 'sample_5'),
(16, 'sample_6'),
(17, 'sample_7'),
(18, 'sample_8'),
(19, 'sample_9'),
(20, 'sample_10');

SELECT * FROM basa ORDER BY id;
