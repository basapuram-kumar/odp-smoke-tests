-- Smoke checks for hive-sample-smoke.sh (Beeline -f)
DROP TABLE IF EXISTS bk1;
CREATE TABLE bk1 (id INT);
INSERT INTO bk1 VALUES (10);
SELECT * FROM bk1;
INSERT INTO bk1 VALUES (20), (30), (40);
SELECT * FROM bk1;
