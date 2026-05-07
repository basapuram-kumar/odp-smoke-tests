-- One-time MySQL setup for sqoop-smoke-test.sh (dedicated user/DB, not hive).
-- Run as an admin, e.g.:
--   mysql -u root -p < sql/sqoop-smoke-mysql-setup.sql
-- Default password matches script defaults (change in both places if you rotate it).

CREATE DATABASE IF NOT EXISTS sqoop_smoke;

CREATE USER IF NOT EXISTS 'sqoop_smoke'@'%' IDENTIFIED BY 'sqoop_smoke';
CREATE USER IF NOT EXISTS 'sqoop_smoke'@'localhost' IDENTIFIED BY 'sqoop_smoke';

GRANT ALL PRIVILEGES ON sqoop_smoke.* TO 'sqoop_smoke'@'%';
GRANT ALL PRIVILEGES ON sqoop_smoke.* TO 'sqoop_smoke'@'localhost';

FLUSH PRIVILEGES;

USE sqoop_smoke;

DROP TABLE IF EXISTS smoke_import;
CREATE TABLE smoke_import (
  id INT NOT NULL,
  label VARCHAR(64) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB;

INSERT INTO smoke_import (id, label) VALUES
  (1, 'alpha'),
  (2, 'beta'),
  (3, 'gamma'),
  (4, 'delta'),
  (5, 'epsilon');

-- Optional round-trip target (sqoop export when SQOOP_SKIP_EXPORT=0).
DROP TABLE IF EXISTS smoke_export;
CREATE TABLE smoke_export LIKE smoke_import;
