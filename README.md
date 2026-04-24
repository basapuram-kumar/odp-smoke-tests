# Sample smoke jobs (ODP / Kerberos)

Shell helpers to run small end-to-end checks against Hadoop ecosystem components. They assume a Kerberos-enabled cluster, ODP-style paths under `/usr/odp/current/…`, and (where noted) Ambari for cluster metadata.

**Run these on the appropriate cluster nodes** (keytab principals must match the host or cluster naming on your environment).

---

## Configuration

### `configs/ambari.env` (create from example)

Used by scripts that call Ambari (cluster name, Kudu master discovery, etc.).

```bash
cp configs/ambari.env.example configs/ambari.env
# edit credentials and URL if needed
```

`configs/ambari.env` is gitignored. Variables: `AMBARI_BASE_URL`, `AMBARI_USER`, `AMBARI_PASSWORD`.

You can skip Ambari for some flows by exporting **`CLUSTER_NAME`** (and, for Kudu CLI master override, **`KUDU_MASTER_ADDRESSES`** when documented below).

### `configs/hive.env` (optional)

Only if you want to override Hive-related settings. See `configs/hive.env.example`. `hive.env` is gitignored.

---

## Scripts overview

| Script | Principal / auth | Typical host |
|--------|------------------|--------------|
| `hdfs-headless-smoke.sh` | `hdfs-<cluster>` + hdfs headless keytab | Node with keytab |
| `yarn-sample-smoke.sh` | Same as HDFS (`hdfs-<cluster>`) | YARN client / edge |
| `hive-sample-smoke.sh` | `hive/<FQDN>` + Hive service keytab | HiveServer2 host |
| `impala-sample-smoke.sh` | `impala/<FQDN>` + Impala service keytab | Impala / coordinator |
| `kudu-sample-smoke.sh` | Impala then `kudu/<FQDN>` for CLI | Impala + Kudu CLI keytab |
| `hbase-sample-smoke.sh` | `hbase-<cluster>` + HBase headless keytab | RegionServer / client |
| `kafka-sample-smoke.sh` | `kafka/<FQDN>` + Kafka service keytab | Broker / client (Kafka 2 path) |
| `kafka3-sample-smoke.sh` | Same keytab / principal | **Kafka 3** (`kafka3-broker`), multi-topic |
| `spark-sample-smoke.sh` | `spark/<FQDN>` + Spark service keytab | Spark gateway / HS host |
| `spark2-pi-sample-smoke.sh` | Same Spark service principal/keytab | Spark 2 client + YARN |
| `spark-3.3.3-pi-sample-smoke.sh` | Same | Spark **3.3.3** (`spark3_3_3_3-client`) + YARN |
| `spark-3.5.1-pi-sample-smoke.sh` | Same | Spark **3.5.1** (`spark3_3_5_1-client`) + YARN |
| `spark-3.5.5-pi-sample-smoke.sh` | Same | Spark **3.5.5** (`spark3-client`, glob `spark-examples_*.jar`) + YARN |
| `flink-sample-smoke.sh` | **`flink/<FQDN>`** + Flink service keytab | Flink on YARN (host with `flink.service.keytab`) |

> **Note:** `yarn-sample-smoke.sh` uses the **HDFS headless** keytab and **`hdfs-<cluster>`** principal (not `yarn-ats-`). Override in the script or with env vars if your site differs.

---

## `hdfs-headless-smoke.sh`

- Reads **`CLUSTER_NAME`** from Ambari (or env).
- `kinit` with `/etc/security/keytabs/hdfs.headless.keytab` as **`hdfs-<cluster>`**.
- Runs: `hdfs dfs -ls /`, `hdfs dfs -put /etc/hosts /tmp/`, `hdfs dfs -ls /tmp/`.

```bash
sudo ./hdfs-headless-smoke.sh
```

**Env:** `AMBARI_*`, `CLUSTER_NAME`, `HDFS_KEYTAB`, `AMBARI_CONFIG_FILE`.

---

## `yarn-sample-smoke.sh`

- Same Ambari + **`hdfs-<cluster>`** + hdfs headless keytab as above.
- Runs MapReduce pi: `yarn jar …/hadoop-mapreduce-examples.jar pi …`.

```bash
sudo ./yarn-sample-smoke.sh
```

**Env:** `MR_EXAMPLES_JAR`, `MR_PI_MAPS`, `MR_PI_SAMPLES`, `HDFS_KEYTAB`, plus Ambari vars.

---

## `hive-sample-smoke.sh`

- `kinit` **`hive/<FQDN>`** with `/etc/security/keytabs/hive.service.keytab`.
- **`beeline -f`** without **`-u`** by default (uses `hive-site.xml` on the node, e.g. ZooKeeper JDBC).
- SQL: `sample-jobs/sql/hive-sample-smoke.sql`.

```bash
sudo ./hive-sample-smoke.sh
```

**Env:** `HIVE_KEYTAB`, `HIVE_PRINCIPAL_HOST`, `HIVE_JDBC_URL` (optional override), `HIVE_SMOKE_SQL`, `HIVE_CONFIG_FILE`.

---

## `impala-sample-smoke.sh`

- `kinit` **`impala/<FQDN>`** with `/etc/security/keytabs/impala.service.keytab`.
- **`impala-shell -i $(hostname):21050 -f`** `sql/impala-sample-smoke.sql` by default.

```bash
sudo ./impala-sample-smoke.sh
```

**Env:** `IMPALAD`, `IMPALA_KEYTAB`, `IMPALA_PRINCIPAL_HOST`, `IMPALA_SHELL`, `IMPALA_SMOKE_SQL`.

---

## `kudu-sample-smoke.sh`

1. Ambari: cluster name + **KUDU_MASTER** host list (`…/services/KUDU/components/KUDU_MASTER`), or **`KUDU_MASTER_ADDRESSES`**.
2. **`impala/<FQDN>`** `kinit`, then **`impala-shell`**: create `kudu_db`, Kudu table `test_kudu`, inserts, Impala `SELECT`.
3. **`kudu/<FQDN>`** `kinit` (service keytab on the **current** host unless `KUDU_PRINCIPAL_HOST` is set), then Kudu CLI:
   - **`kudu table list <master>:7051`**
   - **`kudu table scan <master>:7051 impala::kudu_db.test_kudu`**

```bash
sudo ./kudu-sample-smoke.sh
```

**Env:** Ambari + `KUDU_MASTER_ADDRESSES`, `KUDU_KEYTAB`, `KUDU_CLI`, `KUDU_MASTER_RPC_PORT`, `KUDU_NATIVE_TABLE`, `KUDU_CLI_SKIP=1` to skip step 3, Impala vars as above.

---

## `hbase-sample-smoke.sh`

- Ambari + **`hbase-<cluster>`** + `/etc/security/keytabs/hbase.headless.keytab`.
- Optional best-effort drop of `sample_table_1` / `sample_table_2`, then **`hbase shell -f`** `hbase/hbase-sample-smoke.hbase`.

```bash
sudo ./hbase-sample-smoke.sh
```

**Env:** `HBASE_KEYTAB`, `HBASE_SMOKE_SCRIPT`, `HBASE_SMOKE_DROP_FIRST`, `CLUSTER_NAME`, Ambari vars.

---

## `kafka-sample-smoke.sh`

- **`kafka/<FQDN>`** + `/etc/security/keytabs/kafka.service.keytab`.
- **`KAFKA_OPTS`** points at **`$KAFKA_HOME/conf/kafka_client_jaas.conf`**.
- Producer pipes sample lines to **`test1`**; consumer **`--from-beginning --max-messages`**.

```bash
sudo ./kafka-sample-smoke.sh
```

**Env:** `KAFKA_HOME`, `KAFKA_CLIENT_CONFIG` (defaults to `kafka/client-sasl.properties`), `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC`, `KAFKA_MAX_MESSAGES`, `KAFKA_CREATE_TOPIC`.

---

## `kafka3-sample-smoke.sh`

- Same **`kafka/<FQDN>`** principal and **`kafka.service.keytab`** as Kafka 2 smoke; **`KAFKA_HOME`** defaults to **`/usr/odp/current/kafka3-broker`**. If **`KAFKA_JAAS_CONF`** is unset, uses **`conf/kafka3_client_jaas.conf`** when present, otherwise **`conf/kafka_client_jaas.conf`**.
- Reuses **`kafka/client-sasl.properties`** by default (same **`security.protocol` / SASL** pattern as Kafka 2).
- Loops **three** topics by default: **`kafka3-smoke-1`**, **`kafka3-smoke-2`**, **`kafka3-smoke-3`** — produce **`KAFKA_MSGS_PER_TOPIC`** lines per topic, then consume with **`--from-beginning`** and **`--max-messages`** (default **`2 * KAFKA_MSGS_PER_TOPIC`** per topic).

```bash
sudo ./kafka3-sample-smoke.sh
```

**Env:** `KAFKA_HOME`, `KAFKA_JAAS_CONF`, `KAFKA_CLIENT_CONFIG`, `KAFKA_BOOTSTRAP`, `KAFKA_TOPICS` (space or comma list), `KAFKA_MSGS_PER_TOPIC`, `KAFKA_MAX_MESSAGES`, `KAFKA_CREATE_TOPIC`, `KAFKA_REPLICATION_FACTOR`, `KAFKA_KEYTAB`, `KAFKA_PRINCIPAL_HOST`.

---

## `spark-sample-smoke.sh`

- No Ambari; **`kinit`** **`spark/<FQDN>`** with **`spark.service.keytab`**.
- **`spark-shell -i`** `scala/spark-sample-smoke.scala`.

```bash
sudo ./spark-sample-smoke.sh
```

**Env:** `SPARK_KEYTAB`, `SPARK_PRINCIPAL_HOST`, `SPARK_SHELL`, `SPARK_SMOKE_SCALA`.

---

## `spark2-pi-sample-smoke.sh`

- No Ambari; **`kinit`** **`spark/<FQDN>`** with **`spark.service.keytab`** (same as `spark-sample-smoke.sh`).
- Sets **`SPARK_MAJOR_VERSION=2`** (default) for the **`spark-submit`** process.
- Picks the Spark 2 examples JAR with a **glob** (no version in the script):  
  **`${SPARK2_CLIENT_HOME}/examples/jars/spark-examples_*.jar`** (first match if several exist).
- Runs **`SparkPi`** on **YARN**:

```text
spark-submit --class org.apache.spark.examples.SparkPi --master yarn <resolved-jar> <slices>
```

```bash
sudo ./spark2-pi-sample-smoke.sh
```

**Env:** `SPARK_KEYTAB`, `SPARK_PRINCIPAL_HOST`, `SPARK2_CLIENT_HOME`, `SPARK_SUBMIT`, `SPARK_EXAMPLES_JAR_GLOB`, `SPARK_PI_SLICES` (default `10`), `SPARK_MAJOR_VERSION` (default `2`).

---

## `spark-3.3.3-pi-sample-smoke.sh`

- Exports **`SPARK_MAJOR_VERSION=3`** and **`SPARK_VERSION=3_3_3_3`** (defaults).
- Client home: **`/usr/odp/current/spark3_3_3_3-client`** (`SPARK3_CLIENT_HOME`).
- Examples JAR: **`${SPARK3_CLIENT_HOME}/examples/jars/spark-examples_*.jar`** (first match).

```bash
sudo ./spark-3.3.3-pi-sample-smoke.sh
```

**Env:** `SPARK_KEYTAB`, `SPARK_PRINCIPAL_HOST`, `SPARK3_CLIENT_HOME`, `SPARK_SUBMIT`, `SPARK_EXAMPLES_JAR_GLOB`, `SPARK_PI_SLICES`, `SPARK_MAJOR_VERSION`, `SPARK_VERSION`.

---

## `spark-3.5.1-pi-sample-smoke.sh`

- Exports **`SPARK_MAJOR_VERSION=3`** and **`SPARK_VERSION=3_3_5_1`** (defaults).
- Client home: **`/usr/odp/current/spark3_3_5_1-client`** (`SPARK3_CLIENT_HOME`).
- Examples JAR: **`${SPARK3_CLIENT_HOME}/examples/jars/spark-examples_*.jar`** (first match).

```bash
sudo ./spark-3.5.1-pi-sample-smoke.sh
```

**Env:** same pattern as Spark 3.3.3 script.

---

## `spark-3.5.5-pi-sample-smoke.sh`

- Client home: **`/usr/odp/current/spark3-client`** (`SPARK3_CLIENT_HOME`).
- Examples JAR: **`${SPARK3_CLIENT_HOME}/examples/jars/spark-examples_*.jar`** (first match; e.g. `spark-examples_2.12-3.5.5.3.2.3.6-3.jar`).
- Exports **`SPARK_MAJOR_VERSION=3`** and **`SPARK_VERSION=3_5_5`** by default — override **`SPARK_VERSION`** if your ODP `spark3-client` expects a different value.

```bash
sudo ./spark-3.5.5-pi-sample-smoke.sh
```

**Env:** same pattern as Spark 3.3.3 / 3.5.1 Pi scripts.

---

## `flink-sample-smoke.sh`

- **`kinit`** with **`/etc/security/keytabs/flink.service.keytab`** as **`flink/<hostname>`** (FQDN from **`hostname -f`**, override **`FLINK_PRINCIPAL_HOST`**) so HDFS paths such as **`/apps/odp/flink`** are accessed as **Flink**, not a leftover **`spark`** (or other) ticket. Set **`FLINK_KINIT_SKIP=1`** only if you intentionally manage the cache yourself.
- Sets **`HADOOP_CLASSPATH=$(hadoop classpath)`**.
- If **`HADOOP_CONF_DIR`** is unset, sets it to **`$(dirname $FLINK_HOME)/hadoop/conf`** when that directory exists (reduces “Could not find Hadoop configuration” warnings next to ODP’s **`…/flink`** + **`…/hadoop`** layout).
- Resolves **`FLINK_HOME`** as the first **`/usr/odp/3.*/flink`** match (or set **`FLINK_HOME`** explicitly).
- Starts **`bin/yarn-session.sh --detached`** with defaults **`-s 1 -jm 1024m -tm 4096m`** so TaskManager **Total Flink Memory** is large enough for Flink 2.x defaults (framework + network + managed mins). Override via **`FLINK_YARN_SESSION_ARGS`** if your YARN queue cannot allocate 4 GiB per TM.
- Parses **`application_*_*`** from the session log when possible and runs  
  **`flink run -t yarn-session -Dyarn.application.id=...`**; if no id is found, runs **`flink run`** without **`yarn-session`** target (may depend on your Flink/YARN setup).
- Submits **`examples/streaming/TopSpeedWindowing.jar`** (override **`FLINK_SMOKE_JAR`**).
- By default **`yarn application -kill`** on exit if an application id was found (**`FLINK_CLEANUP_SESSION=0`** to leave the session running).

```bash
# Ensure YARN/HDFS auth works (e.g. kinit) if your cluster uses Kerberos
sudo ./flink-sample-smoke.sh
```

**Env:** `FLINK_HOME`, `FLINK_KEYTAB`, `FLINK_PRINCIPAL_HOST`, `FLINK_KINIT_SKIP`, `HADOOP_CONF_DIR`, `FLINK_SMOKE_JAR`, `FLINK_YARN_SESSION_ARGS`, `FLINK_RUN_ARGS`, `FLINK_CLEANUP_SESSION`, `FLINK_SESSION_START_WAIT`.

---

## Common issues

- **`kinit` principal must match the keytab** (cluster suffix for headless users, **`<service>/<FQDN>`** for service keytabs).
- **Ambari:** use **`X-Requested-By: ambari`** and basic auth; the sample scripts match the bundled **`ambari.env.example`**.
- **HDFS `put` to `/tmp/hosts`:** second run can fail if the file exists; remove it or adjust the script.
- **Kafka consumer:** `--from-beginning` reads from the start of the log; **`KAFKA_MAX_MESSAGES`** may need raising if the topic already has data.
- **Kudu `table scan`:** native table name is usually **`impala::<db>.<table>`**; confirm with **`table list`** and set **`KUDU_NATIVE_TABLE`** if different.
- **Flink YARN session:** **`IllegalConfigurationException`** on TaskManager memory means **`-tm`** (and total process memory) is too small for Flink’s internal minimums — raise **`FLINK_YARN_SESSION_ARGS`** (e.g. **`-tm 4096m`** or higher), or reduce reserved fractions in Flink **`config.yaml`** on the cluster.

---

## Layout

```
sample-jobs/
  README.md                 # this file
  configs/
    ambari.env.example
    hive.env.example
  kafka/
    client-sasl.properties
  hbase/
    hbase-sample-smoke.hbase
  sql/
    hive-sample-smoke.sql
    impala-sample-smoke.sql
  scala/
    spark-sample-smoke.scala
  *.sh                      # smoke entrypoints (incl. Spark Pi + flink-sample-smoke.sh)
```
