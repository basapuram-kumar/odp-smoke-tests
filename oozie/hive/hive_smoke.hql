-- Oozie hive2-action smoke: create database, create table, load, aggregate, clean up.
-- DB_NAME and TABLE_NAME are passed as Oozie <param> (beeline --hivevar).
--
-- Ranger note: the smoke uses a fresh per-run database instead of "default". The
-- submitting user only needs "create" on a database (granted to group public by
-- the stock policies); it then owns the new database, so the {OWNER} policy covers
-- insert, select and the drops. Reusing "default" fails on DROP, which Ranger
-- authorizes against the database rather than the table.

CREATE DATABASE IF NOT EXISTS ${hivevar:DB_NAME};

CREATE TABLE ${hivevar:DB_NAME}.${hivevar:TABLE_NAME} (
  id   INT,
  name STRING,
  qty  INT
) STORED AS ORC;

INSERT INTO ${hivevar:DB_NAME}.${hivevar:TABLE_NAME} VALUES
  (1, 'alpha', 10),
  (2, 'beta',  20),
  (3, 'gamma', 30);

SELECT COUNT(*) AS row_count FROM ${hivevar:DB_NAME}.${hivevar:TABLE_NAME};

SELECT name, qty FROM ${hivevar:DB_NAME}.${hivevar:TABLE_NAME} ORDER BY id;

SELECT SUM(qty) AS total_qty FROM ${hivevar:DB_NAME}.${hivevar:TABLE_NAME};

DROP TABLE ${hivevar:DB_NAME}.${hivevar:TABLE_NAME};

DROP DATABASE ${hivevar:DB_NAME};
