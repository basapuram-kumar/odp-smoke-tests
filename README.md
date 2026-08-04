# Sample smoke jobs (ODP / Kerberos)

Shell helpers to run small end-to-end checks against Hadoop ecosystem components. They assume a Kerberos-enabled cluster, ODP-style paths under `/usr/odp/current/…`, and (where noted) Ambari for cluster metadata.

**Run these on the appropriate cluster nodes** (keytab principals must match the host or cluster naming on your environment).

---

## Configuration

Config files live under `configs/*.env` and are loaded automatically by the smoke scripts. Edit them for your cluster (Ambari URL/password, Ranger password, etc.). No copy/rename step is required.

### `configs/ambari.env` / `configs/ambari.config`

Used by Ambari smoke scripts (Quick Links UI, Service Checks, and others). Both filenames are accepted; `ambari.env` is preferred when both exist.

```bash
# edit AMBARI_BASE_URL / AMBARI_USER / AMBARI_PASSWORD if needed
vi configs/ambari.env
```

Variables: `AMBARI_BASE_URL`, `AMBARI_USER`, `AMBARI_PASSWORD` (optional `CLUSTER_NAME`).

After that, run scripts with no Ambari credentials on the command line:

```bash
./ambari-quicklinks-ui-smoke.sh
./ambari-service-checks-smoke.sh
SC_SERVICES=HDFS,YARN,ZOOKEEPER ./ambari-service-checks-smoke.sh
```

### `configs/ranger.env` (optional, for Ranger REST scripts)

```bash
# set RANGER_PASSWORD (and optionally RANGER_BASE_URL)
vi configs/ranger.env
```

Used by `ranger-yarn-all-queue-users-add.sh`, `ranger-plugin-connection-smoke.sh`, and `ranger-kms-sample-smoke.sh`.

### `configs/ranger-kms.env` (optional, for `ranger-kms-sample-smoke.sh`)

```bash
# optionally set KMS_PROVIDER / KMS_KEYTAB
vi configs/ranger-kms.env
```

You can skip Ambari for some flows by exporting **`CLUSTER_NAME`** (and, for Kudu CLI master override, **`KUDU_MASTER_ADDRESSES`** when documented below).

### `configs/hive.env` (optional)

Only if you want to override Hive-related settings. See `configs/hive.env`.

### `configs/zeppelin.env` (optional, for `zeppelin-editors-smoke.sh`)

```bash
# set ZEPPELIN_PASSWORD (and optionally ZEPPELIN_BASE_URL)
vi configs/zeppelin.env
```

### `configs/airflow.env` (optional, for `airflow-sample-smoke.sh`)

```bash
# optionally set AIRFLOW_BASE_URL / AIRFLOW_HOME
vi configs/airflow.env
```

### `configs/clickhouse.env` (optional, for `clickhouse-sample-smoke.sh`)

```bash
# optionally set CLICKHOUSE_HTTP_URL / password
vi configs/clickhouse.env
```

### `configs/druid.env` (optional, for `druid-sample-smoke.sh`)

```bash
# optionally set DRUID_BROKER_URL / DRUID_SKIP_INGEST
vi configs/druid.env
```

### `configs/pinot.env` (optional, for `pinot-sample-smoke.sh`)

```bash
# optionally set PINOT_CONTROLLER_URL / PINOT_SKIP_INGEST
vi configs/pinot.env
```

### `configs/ozone.env` (optional, for `ozone-sample-smoke.sh`)

```bash
# optionally set OZONE_CONF_DIR / OZONE_KEYTAB / OZONE_SKIP_HTTP
vi configs/ozone.env
```

### `configs/nifi.env`, `configs/nifi-registry.env` (optional)

```bash
# optionally set NIFI_URL / NIFI_REGISTRY_URL to skip Ambari discovery
vi configs/nifi.env
vi configs/nifi-registry.env
```

### `configs/jupyterhub.env` (optional, for `jupyterhub-sample-smoke.sh`)

```bash
# optionally set JUPYTERHUB_URL / JUPYTERHUB_BASE_URL / JUPYTERHUB_PASSWORD
vi configs/jupyterhub.env
```

### `configs/hue.env` (optional, for `hue-sample-smoke.sh`)

```bash
# set HUE_USER / HUE_PASSWORD if they differ from hue/hue
vi configs/hue.env
```

### `configs/knox.env` (optional, for `knox-sample-smoke.sh`)

```bash
# optionally set KNOX_URL / KNOX_USER / KNOX_PASSWORD
vi configs/knox.env
```

### `configs/infra-solr.env` (optional, for `infra-solr-sample-smoke.sh`)

```bash
# optionally set INFRA_SOLR_HOSTS / INFRA_SOLR_PORT / INFRA_SOLR_READ_KEYTAB
vi configs/infra-solr.env
```

### `configs/zookeeper.env` (optional, for `zookeeper-sample-smoke.sh`)

```bash
# optionally set ZOOKEEPER_HOSTS / ZOOKEEPER_ADMIN_PORT / ZOOKEEPER_EXPECTED_ZNODES
vi configs/zookeeper.env
```

### `configs/sqoop.env` (optional, for `sqoop-smoke-test.sh`)

Edit `configs/sqoop.env` if you need a non-default JDBC host, user, or password. If an older local file still has **`hive`** / **`sqoop_test`**, update it to match `sql/sqoop-smoke-mysql-setup.sql` (database **`sqoop_smoke`**, user **`sqoop_smoke`**).

---

## Scripts overview

| Script | Principal / auth | Typical host |
|--------|------------------|--------------|
| `run-all-smoke.sh` | Runs the smoke entrypoints below and consolidates their results | Cluster node, as root |
| `hdfs-headless-smoke.sh` | `hdfs-<cluster>` + hdfs headless keytab | Node with keytab |
| `yarn-sample-smoke.sh` | Same as HDFS (`hdfs-<cluster>`) | YARN client / edge |
| `hive-sample-smoke.sh` | `hive/<FQDN>` + Hive service keytab | HiveServer2 host |
| `hive-spark2-compat-smoke.sh` | **hive** + **spark** + **hdfs-\<cluster\>** keytabs | Hive + Spark2 client / edge |
| `sqoop-smoke-test.sh` | MySQL **`sqoop_smoke`** JDBC + **`hdfs-<cluster>`** (Ambari + hdfs headless keytab; skip with **`SQOOP_SKIP_KINIT=1`**) | Hadoop edge / gateway with **`sqoop`** + **`hdfs`** |
| `impala-sample-smoke.sh` | `impala/<FQDN>` + Impala service keytab | Impala / coordinator |
| `kudu-sample-smoke.sh` | Impala then `kudu/<FQDN>` for CLI | Impala + Kudu CLI keytab |
| `hbase-sample-smoke.sh` | `hbase-<cluster>` + HBase headless keytab | RegionServer / client |
| `oozie-sample-smoke.sh` | **`hdfs-<cluster>`** + hdfs headless keytab (Ambari; shell + hive2 workflows) | Oozie client / edge with **`oozie`** + **`hdfs`** |
| `kafka-sample-smoke.sh` | `kafka/<FQDN>` + Kafka service keytab | Broker / client (Kafka 2 path) |
| `kafka3-sample-smoke.sh` | Same keytab / principal | **Kafka 3** (`kafka3-broker`), multi-topic |
| `schema-registry-sample-smoke.sh` | **kafka** + **registry** service keytabs | Registry host + Kafka broker |
| `spark-sample-smoke.sh` | `spark/<FQDN>` + Spark service keytab | Spark gateway / HS host |
| `spark2-pi-sample-smoke.sh` | Same Spark service principal/keytab | Spark 2 client + YARN |
| `spark-3.3.3-pi-sample-smoke.sh` | Same | Spark **3.3.3** (`spark3_3_3_3-client`) + YARN |
| `spark-3.5.1-pi-sample-smoke.sh` | Same | Spark **3.5.1** (`spark3_3_5_1-client`) + YARN |
| `spark-3.5.5-pi-sample-smoke.sh` | Same | Spark **3.5.5** (`spark3-client`, glob `spark-examples_*.jar`) + YARN |
| `flink-sample-smoke.sh` | **`flink/<FQDN>`** + Flink service keytab | Flink on YARN (host with `flink.service.keytab`) |
| `ranger-yarn-all-queue-users-add.sh` | Ambari + **Ranger admin** REST (`RANGER_USER` / **`RANGER_PASSWORD`**) | Edge / ops host with **`curl`** + **`python3`** |
| `ranger-plugin-connection-smoke.sh` | Ambari (optional) + **Ranger admin** REST Test Connection for each enabled service | Host that can reach Ranger Admin (`6080`) |
| `ranger-kms-sample-smoke.sh` | **`rangerkms/<FQDN>`** (key create) + **`hdfs-<cluster>`** (createZone) + **`ambari-qa-<cluster>`** (put/get; needs Ranger policy) | Prefer a **`RANGER_KMS_SERVER`** host; set **`configs/ranger.env`** for EZ put/get |
| `zeppelin-editors-smoke.sh` | Zeppelin Shiro login (`ZEPPELIN_USER` / **`ZEPPELIN_PASSWORD`**) | Host that can reach Zeppelin UI/API (`9995`) |
| `airflow-sample-smoke.sh` | **`airflow-<cluster>`** headless keytab (optional) + Airflow CLI / health | Airflow host (`AIRFLOW_HOME` venv; web `:8889`) |
| `clickhouse-sample-smoke.sh` | ClickHouse HTTP (`default` user; optional Kerberos / native client) | Host that can reach ClickHouse HTTP (`8123`) |
| `druid-sample-smoke.sh` | **`druid-<cluster>`** headless keytab + SPNEGO REST (health, SQL, ingest) | Host that can reach Broker/Coordinator/Overlord |
| `pinot-sample-smoke.sh` | Pinot REST (optional basic auth; health, schema/table, ingest, SQL) | Host that can reach Controller/Broker (and Server/Minion) |
| `ozone-sample-smoke.sh` | **`hdfs-<cluster>`** + Ozone headless keytab (CLI data path, HA roles, SPNEGO web) | Host with the **`ozone`** client |
| `nifi-sample-smoke.sh` | NiFi REST (anonymous over HTTP, SPNEGO over HTTPS) | Host that can reach NiFi (`9090` / `9091`) |
| `nifi-registry-sample-smoke.sh` | NiFi Registry REST (anonymous over HTTP, SPNEGO over HTTPS) | Host that can reach the Registry (`61080` / `61443`) |
| `jupyterhub-sample-smoke.sh` | JupyterHub form login (**`JUPYTERHUB_PASSWORD`**; Ambari-discovered by default) | Host that can reach JupyterHub (`8000`) |
| `hue-sample-smoke.sh` | Hue form login (**`HUE_USER`** / **`HUE_PASSWORD`**; Ambari-discovered by default) | Host that can reach Hue UI (`8888`) |
| `knox-sample-smoke.sh` | Knox gateway HTTP basic against the topology's identity store (**`KNOX_PASSWORD`**; Ambari-discovered by default) | Host that can reach the gateway (`8443`) |
| `ambari-quicklinks-ui-smoke.sh` | Ambari admin REST to resolve every service Quick Link, then HTTP probe | Host that can reach Ambari and the service UI ports |
| `ambari-service-checks-smoke.sh` | Ambari admin REST: trigger + monitor each service check request | Host that can reach Ambari (checks run on cluster hosts) |
| `infra-solr-sample-smoke.sh` | **`infra-solr/<FQDN>`** + Infra Solr service keytab over SPNEGO (plus a **`dev`**-role keytab for the query read-back) | An **`INFRA_SOLR`** host (`8886`), as root for the keytab |
| `zookeeper-sample-smoke.sh` | Four-letter words and AdminServer need no auth; the `zkCli.sh` data path uses the ticket cache (**`ambari-qa-<cluster>`** + smokeuser keytab) | Host that can reach `2181` on every **`ZOOKEEPER_SERVER`**, as root for the keytab |

> **Note:** `yarn-sample-smoke.sh` uses the **HDFS headless** keytab and **`hdfs-<cluster>`** principal (not `yarn-ats-`). Override in the script or with env vars if your site differs.

---

## `run-all-smoke.sh`

Run the master runner as root from this directory. It executes the smoke scripts in a stable order, streams each script's output, and prints one summary containing exit codes, elapsed time, and parsed `PASS` / `FAIL` / `SKIPPED` counts when available.

```bash
# From a root shell:
./run-all-smoke.sh

# Run a small subset by short name:
SMOKE_ONLY=knox,ozone,zookeeper ./run-all-smoke.sh
```

Reports and per-script logs are written under `reports/smoke-YYYYMMDD-HHMMSS/`. Use `SMOKE_SKIP` to exclude scripts, `SMOKE_CONTINUE=0` to stop after the first failure, `SMOKE_REPORT_DIR` to choose another report directory, or `SMOKE_TIMEOUT_SECONDS` to set a per-script timeout. `SMOKE_SCRIPTS` is an alias for `SMOKE_ONLY`; lists may use commas or spaces and names may include or omit `.sh`.

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

## `hive-spark2-compat-smoke.sh`

Hive ↔ Spark2 table round-trip on a shared external HDFS path (managed warehouse is typically `hive`-only and blocks the `spark` user).

1. **HDFS:** `kinit` as **`hdfs-<cluster>`** (or **`HDFS_PRINCIPAL`**), `mkdir` + `chmod 777` on **`COMPAT_LOC`** (default `/tmp/hive_spark2_compat_<ts>`).
2. **Hive:** create DB + external **ORC** table `hive_written`, insert rows, select.
3. **Spark2:** `spark-sql` on YARN (`SPARK_MAJOR_VERSION=2`) reads `hive_written`, writes external **ORC** `spark_orc` and **Parquet** `spark_parquet`.
4. **Hive:** read `spark_orc` back (required PASS).

SQL templates under `sql/hive-spark2-compat-*.sql` (placeholders `__DB__` / `__LOC__`).

```bash
# Prefer setting cluster name so Ambari is not required for HDFS setup:
export CLUSTER_NAME=odp2007
# or: export HDFS_PRINCIPAL=hdfs-odp2007
sudo ./hive-spark2-compat-smoke.sh

# Optional Parquet probe (often fails on some ODP Hive 4 builds; non-fatal unless FAIL_ON_PARQUET=1):
TEST_PARQUET=1 sudo -E ./hive-spark2-compat-smoke.sh

# Drop DB + HDFS path after success:
CLEANUP=1 CLUSTER_NAME=odp2007 sudo -E ./hive-spark2-compat-smoke.sh
```

**Env:** `HIVE_KEYTAB`, `HIVE_PRINCIPAL_HOST`, `HIVE_JDBC_URL`, `SPARK_KEYTAB`, `SPARK_PRINCIPAL_HOST`, `SPARK2_CLIENT_HOME`, `SPARK_SQL`, `SPARK_MAJOR_VERSION`, `HDFS_KEYTAB`, `HDFS_PRINCIPAL`, `CLUSTER_NAME`, `AMBARI_*`, `COMPAT_DB`, `COMPAT_LOC`, `SKIP_HDFS_SETUP`, `TEST_PARQUET`, `FAIL_ON_PARQUET`, `CLEANUP`.

---

## `sqoop-smoke-test.sh`

- **Kerberos:** Ambari cluster name (or **`CLUSTER_NAME`**), then **`kinit`** as **`hdfs-<cluster>`** with `/etc/security/keytabs/hdfs.headless.keytab` (same pattern as HDFS/YARN smokes). Set **`SQOOP_SKIP_KINIT=1`** to skip.
- **One-time MySQL setup** (run as a MySQL admin from the repo root; use **`mysql -u root -p`** if `root` requires a password), or set **`SQOOP_MYSQL_SETUP=1`**:

```bash
mysql -u root < sql/sqoop-smoke-mysql-setup.sql
# or:
SQOOP_MYSQL_SETUP=1 ./sqoop-smoke-test.sh
```

  This creates database **`sqoop_smoke`**, user **`sqoop_smoke`** (password **`sqoop_smoke`**), table **`smoke_import`** with five rows, and empty **`smoke_export`** for optional round-trip tests.

- **Sqoop:** default is **import only** (`SQOOP_SKIP_EXPORT=1`): **`sqoop import`** from **`smoke_import`** into a fresh HDFS directory under **`/tmp/`**, then checks **`_SUCCESS`** and row count in **`part-m-*`**.

```bash
sudo ./sqoop-smoke-test.sh
```

**Env:** `SQOOP_CONFIG_FILE`, `AMBARI_*`, `CLUSTER_NAME`, `HDFS_KEYTAB`, `SQOOP_SKIP_KINIT`, `SQOOP_MYSQL_SETUP`, `SQOOP_MYSQL_HOST`, `SQOOP_MYSQL_*`, `SQOOP_SKIP_EXPORT`, `SQOOP_MYSQL_VERIFY`, `SQOOP_HDFS_BASE_DIR`, etc. See the script header and `configs/sqoop.env`.

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
- Optional best-effort drop of `sample_table_1` / `sample_table_2`, then **`hbase shell -n`** with stdin from `hbase/hbase-sample-smoke.hbase` (noninteractive; always ends with `exit`).

```bash
sudo ./hbase-sample-smoke.sh
```

**Env:** `HBASE_KEYTAB`, `HBASE_SMOKE_SCRIPT`, `HBASE_SMOKE_DROP_FIRST`, `CLUSTER_NAME`, Ambari vars.

---

## `oozie-sample-smoke.sh`

- Ambari cluster name (or **`CLUSTER_NAME`**), then **`kinit`** as **`hdfs-<cluster>`** with `/etc/security/keytabs/hdfs.headless.keytab` (YARN queue ACLs usually allow **hdfs**, not the **oozie** service user).
- Clears stale **`~/.oozie-auth-token-*`** and disables the Oozie auth token cache so the CLI does not reuse a prior principal.
- Resolves **`OOZIE_URL`** (`oozie.base.url`), **`nameNode`**, and **`resourceManager`** from cluster configs (overridable).
- Runs each workflow in **`OOZIE_WORKFLOWS`** (default `shell,hive`): stages `oozie/<name>/` under **`/user/hdfs/oozie_smoke_*/<name>`**, submits with **`oozie job -run`**, polls until **`SUCCEEDED`**, then prints a PASS/FAIL/SKIPPED summary. Exit is non-zero if any workflow fails.

| Workflow | Directory | What it does |
| --- | --- | --- |
| `shell` | `oozie/shell/` | shell action `echo Hello Oozie` + `capture-output` verified by a decision node |
| `hive` | `oozie/hive/` | **`hive2`** action (beeline to HiveServer2) running `hive_smoke.hql`: create database, create ORC table, insert 3 rows, `COUNT(*)` / `ORDER BY` / `SUM`, then drop |

```bash
sudo ./oozie-sample-smoke.sh

# One workflow only:
OOZIE_WORKFLOWS=hive sudo -E ./oozie-sample-smoke.sh
```

### Hive workflow notes

- Uses an Oozie **`hive2` credential** (`oozie.credentials.credentialclasses` must contain `hive2=...Hive2Credentials`) to get a HiveServer2 delegation token for the launcher.
- HiveServer2 is discovered from Ambari (`HIVE/HIVE_SERVER`) plus `hive-site.xml` (transport mode, port, `hive.server2.authentication.kerberos.principal` with `_HOST` resolved). If it cannot be resolved the workflow is reported **SKIPPED**, not failed.
- **`OOZIE_HIVE2_JDBC_URL` must not contain `principal=`** - Oozie appends it from the credential and HiveConnection rejects duplicates.
- The workflow creates a fresh per-run database so the submitting user owns it. Stock Ranger policies give group `public` only `create` on a database; `{OWNER}` then covers insert/select/drop. Reusing `default` fails on `DROP`, which Ranger authorizes against the database rather than the table.
- **Stuck at `status=RUNNING` until the timeout** usually means the query's Tez ApplicationMaster is unschedulable, not that the query is slow. Compare **`tez.am.resource.memory.mb`** against **`yarn.nodemanager.resource.memory-mb`**: an AM sized at (or near) a full NodeManager only starts on a completely idle node, so it waits in `ACCEPTED` forever while YARN still reports free memory cluster-wide. The timeout output prints both values and warns when they conflict. `./update-ambari-yarn-mapred-tez-configs.sh` derives safe sizes from NodeManager capacity.
- On timeout, suspend, or `Ctrl-C`, the script **kills the workflow**. A workflow left `RUNNING` keeps its launcher ApplicationMaster (and its container) alive indefinitely, and leaked launchers from earlier attempts are enough to starve later runs on a small cluster.

**Env:** `AMBARI_*`, `CLUSTER_NAME`, `HDFS_KEYTAB`, `OOZIE_URL`, `OOZIE_NAME_NODE`, `OOZIE_RESOURCE_MANAGER`, `OOZIE_QUEUE`, `OOZIE_WORKFLOWS`, `OOZIE_WORKFLOW_ROOT`, `OOZIE_WORKFLOW_DIR` (legacy single-dir mode), `OOZIE_HDFS_APP_DIR`, `OOZIE_POLL_SECONDS`, `OOZIE_TIMEOUT_SECONDS`, `HIVE_SITE_XML`, `OOZIE_HIVE2_JDBC_URL`, `OOZIE_HIVE2_PRINCIPAL`, `OOZIE_HIVE_DB`, `OOZIE_HIVE_TABLE`.

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

## `schema-registry-sample-smoke.sh`

- **`kinit kafka/<FQDN>`** (`kafka.service.keytab`) → optional **`kafka-topics --create`** for **`truck_events_stream`** (override **`SR_TOPIC`**).
- Copies the resulting credential cache, then **`kinit registry/<FQDN>`** (`registry.service.keytab`) so the default ccache matches the registry service host (as in your **`registry/ub22j11p3-49…`** keytab).
- Runs Hortonworks **`KafkaAvroSerDesApp`** from **`${REGISTRY_HOME}/examples/schema-registry/avro`**: producer (**`-sm`**) then consumer (**`-cm`**). Java uses **`KRB5CCNAME`** pointing at the **saved Kafka ccache** so Kafka SASL still sees a **kafka** ticket (registry alone is not valid as the Kafka client principal on most clusters).
- Resolves the examples JAR with a **glob** (default **`avro-examples-*.jar`** under **`${AVRO_EXAMPLES_DIR}`**; override **`AVRO_EXAMPLES_JAR_GLOB`**). If several match, picks the **last** after **`sort -V`** (highest-looking version).
- Builds **`-cp`** from explicit **`*.jar`** lists: **`${AVRO_EXAMPLES_DIR}/lib`**, optional **`/tmp/libs`**, then **Kafka** then **registry** — same idea as  
  **`…/avro-examples-*.jar:/tmp/libs/*:…/kafka3/libs/*:…/registry/libs/*`** (producer) and **`…/kafka/libs/*`** (consumer). **Producer** prefers **`${ODP_STACK_ROOT}/kafka3/libs`** or **`kafka3-broker/libs`**; **consumer** prefers **`${ODP_STACK_ROOT}/kafka/libs`** or **`kafka-broker/libs`**. **`ODP_STACK_ROOT`** defaults to the parent of **`REGISTRY_HOME`** when it ends with **`/registry`** (e.g. **`/usr/odp/current`** or **`/usr/odp/3.3.6.2-1`**). Override dirs with **`KAFKA3_LIBS_DIR`** / **`KAFKA_LIBS_DIR`**, the full classpath with **`REGISTRY_JAVA_CP`**, or extra jar dirs with **`REGISTRY_CP_DIRS`**.
- **`schema.registry.url`** defaults to **`http://$(hostname -f):7788/api/v1`**; **`bootstrap.servers`** default **`$(hostname -f):6667`** (same style as other smoke scripts).

```bash
sudo ./schema-registry-sample-smoke.sh
```

**Env:** `REGISTRY_HOME`, `AVRO_EXAMPLES_DIR`, `AVRO_EXAMPLES_JAR_GLOB`, `ODP_STACK_ROOT`, `KAFKA_HOME`, `KAFKA3_LIBS_DIR`, `KAFKA_LIBS_DIR`, `KAFKA_BOOTSTRAP`, `SCHEMA_REGISTRY_URL`, `KAFKA_CLIENT_CONFIG`, `KAFKA_JAAS_CONF`, `KAFKA_KEYTAB`, `REGISTRY_KEYTAB`, `KAFKA_PRINCIPAL_HOST`, `REGISTRY_PRINCIPAL_HOST`, `SR_TOPIC`, `REGISTRY_JAVA_CP`, `REGISTRY_CP_DIRS`, `SR_SKIP_TOPIC_CREATE`, `SR_SKIP_PRODUCER`, `SR_SKIP_CONSUMER`.

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

## `ranger-yarn-all-queue-users-add.sh`

- **Goal:** add UNIX users to the Ranger YARN policy **`all - queue`** (override **`RANGER_POLICY_NAME`**) by **GET → merge → PUT** so you do not hardcode policy id, guid, or service name in curl.
- **Ranger URL:** if **`RANGER_BASE_URL`** is unset (e.g. `http://ub20j11p3-43.acceldata.ce:6080`), it is discovered from Ambari: **`ranger-admin-site`** active tag → **`policymgr_external_url`**, else first **`RANGER_ADMIN`** host + **`ranger.service.http.port`** (default **6080**). Set **`RANGER_BASE_URL`** to skip Ambari entirely.
- **Ambari:** same **`configs/ambari.env`** / **`CLUSTER_NAME`** pattern as other scripts when discovery is used.
- **YARN service in Ranger:** defaults to the first Ranger service with **`type`** **`yarn`**; override **`RANGER_YARN_SERVICE_NAME`** (e.g. **`ub20j11p3_yarn`**). Optional **`RANGER_POLICY_ID`** skips listing.
- **Users to add:** **`RANGER_ADD_USERS`** (comma/space) and/or extra script arguments; merged into the first **`policyItems`** block that grants **`admin-queue`** (same layout as the default “all - queue” policy). **`RANGER_DRY_RUN=1`** prints merged JSON only.

```bash
vi configs/ranger.env   # set RANGER_PASSWORD
RANGER_ADD_USERS=registry,flink,nifiregistry ./ranger-yarn-all-queue-users-add.sh
# Or export only for one run:
export RANGER_PASSWORD='…'
RANGER_ADD_USERS=registry,flink ./ranger-yarn-all-queue-users-add.sh
# Or with explicit Ranger URL (no Ambari), still from ranger.env or env:
RANGER_BASE_URL=http://ranger-host:6080 RANGER_YARN_SERVICE_NAME=mycluster_yarn \
  RANGER_ADD_USERS=druid ./ranger-yarn-all-queue-users-add.sh
```

**Env:** `AMBARI_*`, `CLUSTER_NAME`, `RANGER_ENV_FILE` (alias: `RANGER_CONFIG_FILE`), `RANGER_PASSWORD_FILE`, `RANGER_BASE_URL`, `RANGER_USER`, `RANGER_PASSWORD`, `RANGER_YARN_SERVICE_NAME`, `RANGER_POLICY_NAME`, `RANGER_POLICY_ID`, `RANGER_ADD_USERS`, `RANGER_DRY_RUN`, `CURL_EXTRA_OPTS` (e.g. **`-k`** for TLS). Prefer **`configs/ranger.env`** instead of exporting **`RANGER_PASSWORD`** on the shell.

---

## `ranger-plugin-connection-smoke.sh`

- **Goal:** run Ranger Admin **Test Connection** for every **enabled** Ranger service (plugin repository), same as the UI button.
- **API:** `GET /service/public/v2/api/service` then `POST /service/plugins/services/validateConfig` with each service JSON.
- **Ranger URL / auth:** same **`configs/ranger.env`** + optional Ambari discovery as the YARN policy script (`RANGER_BASE_URL` / `RANGER_USER` / `RANGER_PASSWORD`).
- Prints a **PASS / FAIL / SKIP** summary. Exits **1** if any tested service fails (`RANGER_FAIL_ON_ERROR=1`, default).

```bash
vi configs/ranger.env   # set RANGER_PASSWORD (and optional RANGER_BASE_URL)
./ranger-plugin-connection-smoke.sh

# Only HDFS/YARN/Hive, or skip known-broken types:
RANGER_SERVICE_TYPES=hdfs,yarn,hive ./ranger-plugin-connection-smoke.sh
RANGER_SKIP_TYPES=knox,kms ./ranger-plugin-connection-smoke.sh
```

**Env:** `RANGER_INCLUDE_DISABLED`, `RANGER_SERVICE_TYPES`, `RANGER_SKIP_TYPES`, `RANGER_SKIP_SERVICES`, `RANGER_TIMEOUT_SECONDS`, `RANGER_FAIL_ON_ERROR`, plus shared Ranger/Ambari vars above.

---

## `ranger-kms-sample-smoke.sh`

- **Goal:** exercise Ranger KMS end-to-end: create a key, list/describe/roll it, then optionally create an HDFS encryption zone and put/get a file through it.
- **Provider URI:** `KMS_PROVIDER`, else Ambari `RANGER_KMS_SERVER` + `kms-env/kms_port` (+ SSL from `ranger-kms-site`), else `hadoop.security.key.provider.path` / `dfs.encryption.key.provider.uri` from local Hadoop conf.
- **Kerberos (key ops):** prefers **`/etc/security/keytabs/rangerkms.service.keytab`** as **`rangerkms/<FQDN>`**, which Ranger maps to **`keyadmin`**.
- **Kerberos (EZ):** **`hdfs-<cluster>`** for `createZone`; **`ambari-qa-<cluster>`** for put/get. Stock **`dbks-site`** sets **`hadoop.kms.blacklist.DECRYPT_EEK=hdfs`**, so put/get as `hdfs` always fails.
- **Ranger policy:** with **`configs/ranger.env`** (`RANGER_PASSWORD`), creates a temporary KMS policy granting **`ambari-qa`** `decrypteek` (and **`hdfs`** `generateeek`) on the smoke key, waits for KMS policy cache refresh, then deletes the policy on cleanup.
- **Cleanup:** removes the temp Ranger policy, EZ path, and smoke key unless **`KMS_KEEP_KEY=1`**.

```bash
# On a RANGER_KMS_SERVER host; ranger.env needed for EZ put/get:
vi configs/ranger.env   # set RANGER_PASSWORD
sudo ./ranger-kms-sample-smoke.sh

# Key lifecycle only:
KMS_SKIP_EZ=1 sudo -E ./ranger-kms-sample-smoke.sh
```

### Notes

- **Why not hdfs for key create?** Stock Ranger KMS policies grant `CREATE_KEY` / `GET_KEYS` to **`keyadmin`**, not **`hdfs`**.
- **Why not hdfs for EZ put/get?** `hadoop.kms.blacklist.DECRYPT_EEK=hdfs` (default in `dbks-site`). Use **`ambari-qa`** (or set `KMS_CLIENT_*`).
- **Policy cache:** after creating the Ranger policy the script retries put for up to **`KMS_POLICY_WAIT_SECONDS`** (default 45).
- **HTTP probe:** `GET /kms/v1/keys/names` answering `200`/`401`/`403` counts as PASS.

**Env:** `KMS_ENV_FILE`, `KMS_PROVIDER`, `KMS_HOSTS`, `KMS_PORT`, `KMS_SSL`, `KMS_KEYTAB`, `KMS_PRINCIPAL`, `KMS_HDFS_KEYTAB`, `KMS_HDFS_PRINCIPAL`, `KMS_CLIENT_KEYTAB`, `KMS_CLIENT_PRINCIPAL`, `KMS_CLIENT_USER`, `KMS_SKIP_RANGER_POLICY`, `KMS_POLICY_WAIT_SECONDS`, `RANGER_*`, `KMS_SKIP_KINIT`, `KMS_KEY_NAME`, `KMS_KEY_SIZE`, `KMS_CIPHER`, `KMS_EZ_PATH`, `KMS_SKIP_HTTP`, `KMS_SKIP_EZ`, `KMS_SKIP_ROLL`, `KMS_KEEP_KEY`, `CURL_EXTRA_OPTS`, Ambari vars.

---

## `zeppelin-editors-smoke.sh`

- **Goal:** smoke every **supported Zeppelin editor / interpreter** discovered from `GET /api/interpreter` (e.g. `%md.md`, `%sh.sh`, `%livy.spark`, `%jdbc.sql`, `%angular.angular`).
- Creates a temporary note, runs one sample paragraph per editor via synchronous `POST /api/notebook/run/{noteId}/{paragraphId}`, prints **PASS / FAIL / SKIP**, deletes the note.
- **Auth / URL:** `configs/zeppelin.env` (`ZEPPELIN_USER` / `ZEPPELIN_PASSWORD`), optional **`ZEPPELIN_BASE_URL`**, else Ambari discovery of **`ZEPPELIN_MASTER`** + `zeppelin.server.port`.

```bash
vi configs/zeppelin.env   # set password if needed
./zeppelin-editors-smoke.sh

# Subset:
ZEPPELIN_ONLY_GROUPS=md,sh,angular ./zeppelin-editors-smoke.sh
ZEPPELIN_SKIP_GROUPS=livy,jdbc ./zeppelin-editors-smoke.sh
```

**Env:** `ZEPPELIN_ENV_FILE`, `ZEPPELIN_BASE_URL`, `ZEPPELIN_USER`, `ZEPPELIN_PASSWORD`, `ZEPPELIN_ONLY_GROUPS`, `ZEPPELIN_SKIP_GROUPS`, `ZEPPELIN_ONLY_INTERPS`, `ZEPPELIN_SKIP_INTERPS`, `ZEPPELIN_TIMEOUT_SECONDS`, `ZEPPELIN_FAIL_ON_ERROR`, `ZEPPELIN_KEEP_NOTE`, Ambari vars when URL is discovered.

---

## `airflow-sample-smoke.sh`

- **Goal:** Airflow web **health** (`/api/v1/health`) plus trigger sample DAG **`odp_airflow_smoke`** (BashOperator `echo OK_AIRFLOW_SMOKE`) to **`success`**.
- Installs `airflow/odp_airflow_smoke_dag.py` into **`$AIRFLOW_HOME/dags`**, unpauses, triggers, polls `airflow dags list-runs`.
- **Auth / URL:** optional **`kinit`** as **`airflow-<cluster>`**; **`AIRFLOW_BASE_URL`** or Ambari **`AIRFLOW_WEBSERVER`** discovery (default port **8889**).

```bash
sudo ./airflow-sample-smoke.sh

# Skip Kerberos:
AIRFLOW_SKIP_KINIT=1 AIRFLOW_BASE_URL=http://o2009c11:8889 sudo -E ./airflow-sample-smoke.sh
```

**Env:** `AIRFLOW_HOME`, `AIRFLOW_BASE_URL`, `AIRFLOW_DAG_ID`, `AIRFLOW_DAG_FILE`, `AIRFLOW_TIMEOUT_SECONDS`, `AIRFLOW_SKIP_KINIT`, `AIRFLOW_KEYTAB`, `AIRFLOW_KEEP_DAG_FILE`, Ambari vars.

---

## `clickhouse-sample-smoke.sh`

- **Goal:** ClickHouse HTTP smoke: `SELECT version()`, create DB/table, insert 3 rows, verify `count()` and data, then drop DB.
- Optional **`CLICKHOUSE_USE_CLIENT=1`** also runs a native TCP client check (`clickhouse client`, port default **9001**).
- **URL:** **`CLICKHOUSE_HTTP_URL`** or Ambari **`CLICKHOUSE_SERVER`** + `http_port` (default **8123**).

```bash
./clickhouse-sample-smoke.sh

# Also exercise native client:
CLICKHOUSE_USE_CLIENT=1 ./clickhouse-sample-smoke.sh
```

**Env:** `CLICKHOUSE_HTTP_URL`, `CLICKHOUSE_HOST`, `CLICKHOUSE_HTTP_PORT`, `CLICKHOUSE_TCP_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_TABLE`, `CLICKHOUSE_KEEP_DB`, `CLICKHOUSE_SKIP_KINIT`, `CLICKHOUSE_USE_CLIENT`, `CLICKHOUSE_CLIENT`, Ambari vars.

---

## `druid-sample-smoke.sh`

- **Goal:** Druid role **health**, Kerberos **SQL `SELECT 1`**, then optional **inline `index_parallel` ingest** of `odp_druid_smoke` and **`COUNT(*)=3`**.
- Discovers Broker / Coordinator / Overlord / Router / Historical / MiddleManager from Ambari (ports from `druid-*-site`, e.g. coordinator **9081**, router **9888**).
- Spec: `druid/odp_druid_smoke_index.json`.

```bash
sudo ./druid-sample-smoke.sh

# Health + SQL only:
DRUID_SKIP_INGEST=1 sudo -E ./druid-sample-smoke.sh
```

**Env:** `DRUID_BROKER_URL`, `DRUID_COORDINATOR_URL`, `DRUID_OVERLORD_URL`, `DRUID_ROUTER_URL`, `DRUID_KEYTAB`, `DRUID_SKIP_KINIT`, `DRUID_SKIP_INGEST`, `DRUID_DATASOURCE`, `DRUID_TIMEOUT_SECONDS`, `DRUID_KEEP_DATASOURCE`, Ambari vars.

---

## `pinot-sample-smoke.sh`

- **Goal:** Pinot role **health** (`/health` -> `OK`), Controller **instances/tables**, **schema + OFFLINE table** create and read-back, Broker **SQL routing**, then **`/ingestFromFile`** of 3 JSON rows and Broker **SQL `COUNT(*)=3`**.
- Discovers Controller / Broker / Server / Minion from Ambari (ports from `pinot-*-conf`, defaults controller **9000**, broker **8099**, server admin **8097**, minion **9514**). Reads `enable_ssl` / `basic_auth` from `pinot-env`.
- Specs: `pinot/odp_pinot_smoke_schema.json`, `pinot/odp_pinot_smoke_table.json`.
- Host pre-reqs: `prereqs/install-pinot-prereqs-*.sh` (JDK 11 + Python `requests`).

```bash
./pinot-sample-smoke.sh

# Stop after schema/table + query routing (no row ingest):
PINOT_SKIP_INGEST=1 ./pinot-sample-smoke.sh

# Health + listing only:
PINOT_SKIP_TABLE=1 ./pinot-sample-smoke.sh
```

### Notes

- **`/ingestFromFile` needs `controller.local.temp.dir`.** The controller builds its ingestion working directory from that property, and when it is unset Pinot uses the **relative** path `ingestion_dir/...` which the controller process cannot create - `/ingestFromFile` then answers **500** on an otherwise healthy cluster. The stock ODP `pinot-controller-conf` template does not set it. The script recognises that specific error and reports the ingest step **SKIPPED** with the fix rather than failing. To exercise the real ingest path, add this line to Ambari > Pinot > Configs > *Pinot Controller Configuration* and restart `PINOT_CONTROLLER`:

```text
controller.local.temp.dir={{controller_data_dir}}/tmp
```

  Anchoring it to `controller_data_dir` keeps the scratch directory next to the segment store (default `/tmp/pinot/data/controller/tmp`). The controller creates it on startup via `FileUtils.forceMkdir`, so the directory does not have to exist beforehand. Use a plain local path instead if `controller.data.dir` points at remote storage.

- **Empty tables answer without a `resultTable`.** A freshly created OFFLINE table returns HTTP 200 with `exceptions: []` and no `resultTable`, so the routing check asserts "query answered without exceptions" instead of a row count. Only the post-ingest check compares against `PINOT_EXPECTED_COUNT`.
- **Error bodies are printed.** Pinot returns its diagnostics in the body of a `4xx`/`5xx`, so requests never use `curl -f` - the controller's message is shown as-is.

**Env:** `PINOT_CONTROLLER_URL`, `PINOT_BROKER_URL`, `PINOT_SERVER_URL`, `PINOT_MINION_URL`, `PINOT_CONTROLLER_SSL`, `PINOT_BASIC_AUTH`, `PINOT_USER`, `PINOT_PASSWORD`, `PINOT_SKIP_TABLE`, `PINOT_SKIP_INGEST`, `PINOT_TABLE`, `PINOT_KEEP_TABLE`, `PINOT_EXPECTED_COUNT`, `PINOT_TIMEOUT_SECONDS`, `CURL_EXTRA_OPTS`, Ambari vars.

---

## `ozone-sample-smoke.sh`

- **HA roles:** `ozone admin om roles --service-id=<id>` and `ozone admin scm roles`.
- **Web endpoints:** SPNEGO `GET` on every OM, SCM and S3 Gateway host from Ambari, plus Recon `/` and `/api/v1/clusterState`.
- **Data path:** `volume create` / `volume info`, `bucket create` / `bucket info`, `key put`, `key list`, `key info`, `key get` with a byte-for-byte content compare, `ozone fs -ls ofs://<service-id>/...`, then `key delete`.
- **Cleanup:** `bucket delete -r -y` then `volume delete`. The `-r` matters because `key delete` moves the key into the bucket trash rather than removing it, so a plain `bucket delete` fails with `BUCKET_NOT_EMPTY`.
- Prints a PASS/FAIL/SKIPPED summary and exits non-zero on any failure.

```bash
sudo ./ozone-sample-smoke.sh

# Data path only:
OZONE_SKIP_HTTP=1 sudo -E ./ozone-sample-smoke.sh

# Leave the smoke volume behind for inspection:
OZONE_KEEP_DATA=1 sudo -E ./ozone-sample-smoke.sh
```

### Notes

- **`JAVA_HOME`:** the ODP `ozone` wrapper fails with `JAVA_HOME is not set` under a bare `sudo`. The script resolves it from the ambari-agent command JSON, then `/usr/lib/jvm`, then `java` on `PATH`.
- **`OZONE_CONF_DIR`:** ODP ships an empty shared `/etc/hadoop-ozone/conf/ozone-site.xml`, so the CLI cannot resolve the OM service id from it. The script picks the first per-role directory (`ozone.om`, `ozone.scm`, ...) that actually declares `ozone.om.service.ids`.
- **Kerberos:** `/etc/security/keytabs/ozone.headless.keytab` normally holds `hdfs-<cluster>`, which is also in `ozone.administrators`. Falls back to `hdfs.headless.keytab`.

**Env:** `OZONE_CONF_DIR`, `OZONE_BIN`, `JAVA_HOME`, `OZONE_KEYTAB`, `OZONE_PRINCIPAL`, `OZONE_SKIP_KINIT`, `OZONE_SERVICE_ID`, `OZONE_VOLUME`, `OZONE_BUCKET`, `OZONE_KEY`, `OZONE_KEEP_DATA`, `OZONE_SKIP_HTTP`, `OZONE_SKIP_FS`, `CURL_EXTRA_OPTS`, Ambari vars.

---

## `nifi-sample-smoke.sh`

- Discovers the `NIFI_MASTER` host from Ambari, plus the port and TLS flag (`nifi.node.ssl.isenabled` picks `nifi.node.ssl.port` over `nifi.node.port`).
- **Status:** `/flow/about`, `/system-diagnostics` (heap), `/flow/status` (active threads), `/controller/cluster` (fails if any node is not `CONNECTED`), `/flow/processor-types`.
- **Write path:** creates a process group under root, adds a `GenerateFlowFile` processor, confirms it appears in the group's flow, then deletes the group. Deleting the group removes the processor with it, so cleanup only needs the group id.

```bash
./nifi-sample-smoke.sh

# Status endpoints only:
NIFI_SKIP_WRITE=1 ./nifi-sample-smoke.sh
```

**Env:** `NIFI_URL`, `NIFI_SKIP_KINIT`, `NIFI_KEYTAB`, `NIFI_PRINCIPAL`, `NIFI_SKIP_WRITE`, `NIFI_PG_NAME`, `NIFI_PROCESSOR_TYPE`, `NIFI_KEEP_FLOW`, `CURL_EXTRA_OPTS`, Ambari vars.

---

## `nifi-registry-sample-smoke.sh`

- Discovers the `NIFI_REGISTRY_MASTER` host, port and TLS flag from Ambari the same way.
- **Status:** `/about`, `/access` (reports the resolved identity), `/buckets`.
- **Write path:** create a bucket, read it back by id, create a flow in it, confirm the flow appears in the bucket listing, then delete the flow and the bucket. Both deletes pass the current `revision.version` as the `version` query parameter.

```bash
./nifi-registry-sample-smoke.sh

# Status endpoints only:
NIFI_REGISTRY_SKIP_WRITE=1 ./nifi-registry-sample-smoke.sh
```

**Env:** `NIFI_REGISTRY_URL`, `NIFI_REGISTRY_SKIP_KINIT`, `NIFI_REGISTRY_KEYTAB`, `NIFI_REGISTRY_PRINCIPAL`, `NIFI_REGISTRY_SKIP_WRITE`, `NIFI_REGISTRY_BUCKET`, `NIFI_REGISTRY_FLOW`, `NIFI_REGISTRY_KEEP_DATA`, `CURL_EXTRA_OPTS`, Ambari vars.

### Notes for both

- **Auth:** with TLS off (the ODP default here) NiFi runs `nifi.security.allow.anonymous.authentication=true` and rejects user authentication over plain HTTP with `409 User authentication/authorization is only supported when running over HTTPS`. The scripts therefore only add `--negotiate -u :` when the resolved URL is `https`, and `*_SKIP_KINIT` defaults to `1`.
- **JSON parsing:** NiFi embeds raw control characters in some component descriptions, so every response is parsed with Python's `strict=False`.

---

## `jupyterhub-sample-smoke.sh`

- Discovers everything from Ambari's `JUPYTER` service: the `JUPYTERHUB` host, `port`, `enable_ssl`, `initial_admin`, `dummy_password`, and `c.JupyterHub.base_url` parsed out of the rendered `jupyterhub_config.py`.
- **Unauthenticated:** `<base>/hub/api` (version), `<base>/hub/health`, `<base>/hub/login`.
- **Authenticated:** form login, then `/hub/api/user` (identity + admin flag), `/hub/api/info` (spawner and authenticator class), `/hub/api/users`.
- **Single-user server:** spawn via `POST /hub/api/users/<user>/server`, poll until `ready`, query the notebook server's `/api` (version) and `/api/kernelspecs` (default kernel), then stop it. A cleanup trap stops the server even if a check fails.

**Host pre-reqs (before Ambari install):**

```bash
# RHEL 8 / 9 (Python 3.8 + Node.js 20 + configurable-http-proxy):
# https://docs.acceldata.io/odp/odp-3.2.3.5-2/documentation/jupyter-prerequisites
sudo ./prereqs/install-jupyterhub-prereqs-rhel8.sh

# Ubuntu 20.04 / 22.04:
sudo ./prereqs/install-jupyterhub-prereqs-ubuntu20.sh
```

```bash
./jupyterhub-sample-smoke.sh

# Skip the spawn cycle:
JUPYTERHUB_SKIP_SPAWN=1 ./jupyterhub-sample-smoke.sh
```

### Notes

- **`base_url`:** JupyterHub is often mounted under a prefix (`/lab` here). Every path is built from it, and without it each request lands on the hub's 404 page. Override with `JUPYTERHUB_BASE_URL`.
- **XSRF, two scopes:** the `_xsrf` cookie is scoped per path. Hub API calls need the token from `<base>/hub/`, the single-user server needs the one from `<base>/user/<name>/`. Sending the wrong one returns `403`.
- **XSRF rotation:** logging in rotates `_xsrf`, but only an HTML handler reissues the cookie - the JSON API never does. The script fetches `<base>/hub/home` right after login, otherwise every later `POST` fails with `XSRF cookie does not match POST argument`.
- **Login redirect:** the login `POST` answers `302`. Redirects are not followed, because curl would replay the `POST` against the redirect target and get a `403`.

**Env:** `JUPYTERHUB_URL`, `JUPYTERHUB_BASE_URL`, `JUPYTERHUB_USER`, `JUPYTERHUB_PASSWORD`, `JUPYTERHUB_SKIP_SPAWN`, `JUPYTERHUB_KEEP_SERVER`, `JUPYTERHUB_SPAWN_TIMEOUT`, `JUPYTERHUB_POLL_SECONDS`, `CURL_EXTRA_OPTS`, Ambari vars.

---

## `hue-sample-smoke.sh`

- Discovers Ambari **`HUE_SERVER`** host, **`hue-env` `http_port`** (default **8888**), **`hue-desktop-site` `ssl_enable`**, and the auth backend from **`hue-auth-site`**.
- **Unauthenticated:** `/desktop/debug/is_alive`, `GET /` redirect to login, `/hue/accounts/login/` (CSRF token present).
- **Authenticated:** Django form login, then `/desktop/api2/get_config` (app list), `/useradmin/api/get_users`, `/notebook/api/get_history`.

```bash
./hue-sample-smoke.sh

# Liveness + login page only:
HUE_SKIP_AUTH=1 ./hue-sample-smoke.sh
```

**Env:** `HUE_URL`, `HUE_HOST`, `HUE_PORT`, `HUE_SSL`, `HUE_USER`, `HUE_PASSWORD`, `HUE_SKIP_AUTH`, `CURL_EXTRA_OPTS`, Ambari vars.

---

## `knox-sample-smoke.sh`

- Discovers the `KNOX_GATEWAY` host plus `gateway.port` and `gateway.path` from Ambari, and reads the topology's LDAP realm URL and the demo `users-ldif` credentials from the same config.
- **Reachability:** gateway port, TLS handshake, and the certificate's validity window (`openssl x509 -checkend 0`). Knox ships a self-signed certificate, so `CURL_EXTRA_OPTS` defaults to `-k`.
- **Topology deployment:** one request per topology (`admin`, `default`, `knoxsso`, `manager`, `homepage`, `metadata`). `200`/`302`/`401` means deployed; `404` means it never deployed and `5xx` that deployment failed.
- **Auth enforcement:** an unauthenticated admin API call must return `401` with a `WWW-Authenticate` header, which confirms the authentication provider is active.
- **Authenticated:** `/admin/api/v1/version`, `/admin/api/v1/topologies` (prints the deployed set as Knox sees it), and a `webhdfs/v1/?op=LISTSTATUS` through the gateway to prove the proxy path to a backend works.

```bash
./knox-sample-smoke.sh

# Include authenticated admin API + WebHDFS (needs Demo LDAP or real IdP):
KNOX_SKIP_AUTH=0 ./knox-sample-smoke.sh
```

### Notes

- **Topology probes must hit a real service.** Knox only routes paths a topology declares, so `/gateway/default/zzz` returns the same `404` as a topology that was never deployed. `KNOX_TOPOLOGY_PROBES` is therefore a list of `<topology>=<service-path>` pairs rather than bare topology names.
- **Demo LDAP.** On a stock ODP install the `default` topology authenticates through `ShiroProvider` against the Knox demo LDAP on port `33389`. That LDAP is not started automatically, and while it is down Knox answers `401` for every credential. The script probes the realm URL to tell this apart from a wrong password: store unreachable is reported **SKIPPED** with the reason, a reachable store rejecting the login is a **FAIL**. Start it from Ambari under Knox > Actions > Start Demo LDAP.
- **Ranger.** The Knox plugin is enabled here, so a `403` on an authenticated call is an authorization decision rather than a gateway problem - check the `knox` service policies in Ranger.

**Env:** `KNOX_URL`, `KNOX_TOPOLOGY_PROBES`, `KNOX_USER`, `KNOX_PASSWORD`, `KNOX_LDAP_URL`, `KNOX_SKIP_AUTH`, `KNOX_SKIP_WEBHDFS`, `KNOX_WEBHDFS_TOPOLOGY`, `CURL_EXTRA_OPTS`, Ambari vars.

---

## `ambari-quicklinks-ui-smoke.sh`

Resolves Ambari **Quick Links** for every STARTED service (NameNode UI, ResourceManager UI, Ranger, Hue, Zeppelin, ...) the same way Ambari builds them, then HTTP-probes each URL.

- Discovers cluster name, stack version, host IPs, component placement, and current configs from Ambari.
- Default `UI_LINK_MODE=ui` keeps main web consoles; set `all` to also probe logs/JMX/thread stacks.
- Default `UI_USE_IP=1` rewrites hostnames to Ambari-reported IPs so clients without cluster DNS still work.
- On Kerberos clusters, HTTP `401`/`403` still means the UI port is up (`UI_ACCEPT_AUTH=1`).
- Optional components that are not installed (for example Cruise Control, Grafana) are **SKIPPED**, not failed.

```bash
# All STARTED-service Quick Link UIs (reads configs/ambari.env):
./ambari-quicklinks-ui-smoke.sh

# Subset:
UI_SERVICES=HDFS,YARN,RANGER,HUE ./ambari-quicklinks-ui-smoke.sh

# Write a TSV report:
UI_REPORT_FILE=reports/quicklinks-ui.tsv ./ambari-quicklinks-ui-smoke.sh
```

**Env:** `AMBARI_*`, `CLUSTER_NAME`, `UI_SERVICES`, `UI_SKIP_SERVICES`, `UI_LINK_MODE`, `UI_USE_IP`, `UI_ACCEPT_AUTH`, `UI_CONNECT_TIMEOUT`, `UI_MAX_TIME`, `UI_PROBE_AMBARI`, `UI_INCLUDE_NOT_STARTED`, `UI_REPORT_FILE`, `CURL_EXTRA_OPTS`.

---

## `ambari-service-checks-smoke.sh`

Triggers Ambari **Run Service Check** for each eligible service (same API as the Ambari UI / `ambari-llm-agent`), waits for each request to finish, and prints a PASS/FAIL/SKIPPED summary.

- Discovers services with `service_check_supported=true` on the stack.
- Default: only **STARTED** services. Set `SC_INCLUDE_NOT_STARTED=1` to include INSTALLED client services (TEZ, SQOOP, KERBEROS, ...).
- Uses `ZOOKEEPER_QUORUM_SERVICE_CHECK` for ZooKeeper; `{SERVICE}_SERVICE_CHECK` for everything else.
- Polls Ambari request status until `COMPLETED` / `FAILED` / `ABORTED` / `TIMEDOUT` (or `SC_TIMEOUT_SECONDS`).
- Failed checks include task stderr snippets in the summary and optional TSV report.
- Full-cluster runs can take a long time (Hive/Oozie especially). Prefer `SC_SERVICES=...` for a focused pass.

```bash
# All STARTED services that support service checks (reads configs/ambari.env):
./ambari-service-checks-smoke.sh

# Focused set:
SC_SERVICES=HDFS,YARN,ZOOKEEPER,MAPREDUCE2 ./ambari-service-checks-smoke.sh

# Parallel (use carefully) + TSV report:
SC_PARALLEL=3 SC_REPORT_FILE=reports/service-checks.tsv ./ambari-service-checks-smoke.sh
```

**Env:** `AMBARI_*`, `CLUSTER_NAME`, `SC_SERVICES`, `SC_SKIP_SERVICES`, `SC_INCLUDE_NOT_STARTED`, `SC_PARALLEL`, `SC_POLL_SECONDS`, `SC_TIMEOUT_SECONDS`, `SC_STAGGER_SECONDS`, `SC_FAIL_FAST`, `SC_REPORT_FILE`, `CURL_EXTRA_OPTS`.

---

## `zookeeper-sample-smoke.sh`

Checks that the ensemble is a healthy quorum rather than three processes that merely accept connections. The highest-value failure it catches is a missing or duplicated leader, so the role assertion is deliberately strict.

- Discovers the `ZOOKEEPER_SERVER` and `ZOOKEEPER_CLIENT` hosts from Ambari, and `clientPort`, `admin.serverPort`, `admin.enableServer`, `4lw.commands.whitelist`, `dataDir` and the `tickTime` / `initLimit` / `syncLimit` triple from the rendered `zoo.cfg` (falling back to Ambari's `zoo.cfg` type). Also reports `authProvider.1`, `requireClientAuthScheme`, `jaasLoginRenew`, `zk_user` and the keytab and principal from `zookeeper-env`.
- **Liveness:** the client port is open on every server and the `ruok` four-letter word answers exactly `imok`.
- **Quorum roles:** `srvr` on each server, parsing `Mode:`. Exactly one `leader` and every other live server a `follower` (`observer` peers are excluded from the follower count, and a single-server `standalone` ensemble counts as the leader). No leader is reported as a lost quorum, more than one as a split ensemble.
- **Membership:** `conf` reports the `server.N` list the process is actually running with, compared against the Ambari host list on short hostnames. This catches a `ZOOKEEPER_SERVER` that was added in Ambari but never pushed to `zoo.cfg`.
- **Metrics:** `mntr` per server for `zk_num_alive_connections`, `zk_znode_count` and `zk_watch_count`, plus `zk_followers` / `zk_synced_followers` on the leader. `zk_synced_followers` must equal the number of followers the role check found - a follower that is up but not syncing shows here and nowhere else.
- **Replica consistency:** `zk_znode_count` across all servers must fall within `ZOOKEEPER_ZNODE_COUNT_TOLERANCE` (default `25`). The servers are sampled one after another on a live cluster, so an exact match is not required.
- **AdminServer:** `GET /commands/ruok` on each server, asserting the JSON has `"error": null`.
- **Data path:** against the full connection string, `create` a `/odp_zk_smoke_<timestamp>` znode with a known value, `get` it back and compare, `set` a second value and re-`get`, `ls /` to confirm it is listed, then `delete` it and confirm the re-read raises `NoNodeException`. A cleanup trap removes the znode even if a check fails.
- **Service znodes:** prints the top-level listing and which well-known service znodes (`hbase-secure`, `infra-solr`, `brokers`, `hiveserver2`, `rmstore`, ...) are present. Set `ZOOKEEPER_EXPECTED_ZNODES` to turn that into a hard check.

```bash
sudo ./zookeeper-sample-smoke.sh

# Ensemble health only, no client:
ZOOKEEPER_SKIP_CLI=1 sudo -E ./zookeeper-sample-smoke.sh

# Require the znodes other services depend on:
ZOOKEEPER_EXPECTED_ZNODES="/hbase-secure /infra-solr /brokers /hiveserver2 /rmstore" sudo -E ./zookeeper-sample-smoke.sh
```

### Notes

- **The Kerberos half of `zoo.cfg` is not in Ambari.** `authProvider.1`, `jaasLoginRenew` and `kerberos.removeHostFromPrincipal` are injected by the stack when security is enabled and never appear in Ambari's `zoo.cfg` config type, so reading only Ambari makes a Kerberized ensemble look unauthenticated. The script prefers the rendered `/usr/odp/current/zookeeper-server/conf/zoo.cfg` and falls back to Ambari for hosts with no ZooKeeper package.
- **SASL is accepted but not required here.** `authProvider.1` is the `SASLAuthenticationProvider`, but `requireClientAuthScheme` is unset and `/` carries `world:anyone:cdrwa`, so an unauthenticated client can still create and read znodes under the root. The shipped `zookeeper_client_jaas.conf` uses `useTicketCache=true`, so the CLI wants a ticket in the cache but only downgrades to an anonymous session without one - a missing keytab is therefore **SKIPPED**, not **FAIL**. The default login is `ambari-qa-<cluster>` from `smokeuser.headless.keytab`; `zk.service.keytab` only holds `zookeeper/<FQDN>` and is a server identity.
- **`admin.serverPort`.** ZooKeeper's AdminServer defaults to `8080`, which collides with Ambari, so ODP moves it (`8081` here). The script reads the port from config rather than assuming. Nothing listening is **SKIPPED**; something listening that does not answer the AdminServer contract is a **FAIL** - note that an unrelated JVM on the port will correctly fail this check.
- **`4lw.commands.whitelist`.** ODP sets it to `*`, but on a locked-down cluster a command that is not whitelisted still replies, with a refusal string rather than a connection error. Both the config value and that refusal text are checked, and an unavailable command is **SKIPPED** with the reason. Pin the list with `ZOOKEEPER_4LW_WHITELIST` when running somewhere the config cannot be read.
- **`zkCli.sh` output is unreliable.** It prints its banner, the log4j appender and the watcher events onto the same stream as the result, and it exits `0` for several server-side errors. Every assertion is made on the filtered text - the returned value, `Created <path>`, `NoNodeException` - and never on the exit code.
- **`JAVA_HOME`:** same problem as the Ozone wrapper under a bare `sudo`; resolved from the ambari-agent command JSON, then `/usr/lib/jvm`, then `java` on `PATH`.

**Env:** `ZOOKEEPER_HOSTS`, `ZOOKEEPER_CLIENT_PORT`, `ZOOKEEPER_CONNECT`, `ZOOKEEPER_4LW_WHITELIST`, `ZOOKEEPER_ADMIN_PORT`, `ZOOKEEPER_SKIP_ADMIN`, `ZOOKEEPER_SKIP_CLI`, `ZOOKEEPER_SKIP_KINIT`, `ZOOKEEPER_KEYTAB`, `ZOOKEEPER_PRINCIPAL`, `ZOOKEEPER_CLI_BIN`, `JAVA_HOME`, `ZOOKEEPER_ZNODE`, `ZOOKEEPER_KEEP_ZNODE`, `ZOOKEEPER_EXPECTED_ZNODES`, `ZOOKEEPER_ZNODE_COUNT_TOLERANCE`, `ZOOKEEPER_4LW_TIMEOUT`, `ZOOKEEPER_CLI_TIMEOUT`, `CURL_EXTRA_OPTS`, Ambari vars.

---

## `infra-solr-sample-smoke.sh`

Checks that the SolrCloud behind `AMBARI_INFRA_SOLR` is up, that its cluster state is clean, and that a collection can be created, indexed into, queried and dropped.

- Discovers the `INFRA_SOLR` hosts from Ambari plus `infra_solr_port`, `infra_solr_ssl_enabled`, `infra_solr_znode` and `infra_solr_kerberos_keytab` from `infra-solr-env`.
- **Kerberos:** the HTTP endpoints run SPNEGO, so every call goes out with `--negotiate -u :` after a `kinit` with the Infra Solr service keytab.
- **Per-host liveness:** `/solr/admin/info/system` on every `INFRA_SOLR` host, reporting the Solr version and mode, and asserting that the reported `zkHost` ends with the discovered znode.
- **Cluster status:** `action=CLUSTERSTATUS` - the live node count must equal the number of `INFRA_SOLR` hosts, and every shard and every replica of every collection must be `active` and sitting on a live node.
- **Listings:** `action=LIST` for collections and `/solr/admin/configs?action=LIST` for configsets.
- **Write path:** `action=CREATE` (`numShards=1`, `replicationFactor=1`) against the `_default` configset, index `INFRA_SOLR_DOC_COUNT` documents with a commit, query them back and assert `numFound`, then `action=DELETE`. A cleanup trap drops the collection even if a check fails.

```bash
sudo ./infra-solr-sample-smoke.sh

# Read-only checks:
INFRA_SOLR_SKIP_WRITE=1 sudo -E ./infra-solr-sample-smoke.sh

# Leave the smoke collection behind for inspection:
INFRA_SOLR_KEEP_COLLECTION=1 sudo -E ./infra-solr-sample-smoke.sh
```

### Notes

- **Run as root.** `/etc/security/keytabs/ambari-infra-solr.service.keytab` is mode `0400` owned by `infra-solr`. Without a usable SPNEGO identity every endpoint answers `401 Authentication required`, and the script reports the affected checks **SKIPPED** (not FAIL) - the same pattern as Knox when its identity store is down.
- **Principal comes from the keytab.** Ambari only stores the `_HOST` template (`infra-solr/_HOST@REALM`), which `kinit` will not expand, and the keytab holds host-scoped entries. The script scans every `klist -kt` field for a principal matching the local hostname (field positions vary by krb5 version/locale), then falls back to expanding Ambari's `_HOST` template against a candidate that actually appears in the keytab. Override with **`INFRA_SOLR_PRINCIPAL`** if needed.
- **`admin` cannot read.** `security.json` grants the Solr `read` permission to the `dev` role only, and `infra-solr@REALM` maps to `admin` alone. That identity can create a collection, index into it and drop it, but `/select` answers `403`. For the read-back the script `kinit`s a second identity from `INFRA_SOLR_READ_CANDIDATES` (`rangeradmin`, `logsearch`, `atlas` - the stock members of `dev`); if none of those keytabs is readable the query is **SKIPPED** rather than FAIL, because it is an authorization policy and not a Solr fault. `update` has no permission entry at all, so indexing is open to any authenticated user.
- **Query the node that hosts the replica.** A request for a collection whose replica lives on another node is proxied there, and the proxied hop re-authenticates as the Solr node identity (`infra-solr`, role `admin`) instead of the caller - so `/select` intermittently returned `403` depending on where `CREATE` happened to place the replica. The script reads the replica's `base_url` out of `CLUSTERSTATUS` and sends the update and the query straight to it.
- **Configset.** `_default` is picked when the cluster lists it, since its managed schema accepts arbitrary fields (Solr warns that data-driven schemas are not for production, which is fine for a throwaway collection). Otherwise the first configset is used, and with no configsets at all the whole write path is **SKIPPED**.

**Env:** `INFRA_SOLR_ENV_FILE`, `INFRA_SOLR_HOSTS`, `INFRA_SOLR_PORT`, `INFRA_SOLR_SSL`, `INFRA_SOLR_ZNODE`, `INFRA_SOLR_KEYTAB`, `INFRA_SOLR_PRINCIPAL`, `INFRA_SOLR_SKIP_KINIT`, `INFRA_SOLR_READ_KEYTAB`, `INFRA_SOLR_READ_PRINCIPAL`, `INFRA_SOLR_READ_CANDIDATES`, `INFRA_SOLR_SKIP_WRITE`, `INFRA_SOLR_COLLECTION`, `INFRA_SOLR_CONFIGSET`, `INFRA_SOLR_DOC_COUNT`, `INFRA_SOLR_KEEP_COLLECTION`, `CURL_EXTRA_OPTS`, Ambari vars.

---

## Common issues

- **`kinit` principal must match the keytab** (cluster suffix for headless users, **`<service>/<FQDN>`** for service keytabs).
- **Ambari:** use **`X-Requested-By: ambari`** and basic auth; the sample scripts match the bundled **`ambari.env`**.
- **HDFS `put` to `/tmp/hosts`:** second run can fail if the file exists; remove it or adjust the script.
- **Kafka consumer:** `--from-beginning` reads from the start of the log; **`KAFKA_MAX_MESSAGES`** may need raising if the topic already has data.
- **Kudu `table scan`:** native table name is usually **`impala::<db>.<table>`**; confirm with **`table list`** and set **`KUDU_NATIVE_TABLE`** if different.
- **Flink YARN session:** **`IllegalConfigurationException`** on TaskManager memory means **`-tm`** (and total process memory) is too small for Flink’s internal minimums — raise **`FLINK_YARN_SESSION_ARGS`** (e.g. **`-tm 4096m`** or higher), or reduce reserved fractions in Flink **`config.yaml`** on the cluster.
- **Ranger policy PUT:** Ranger expects the **full** policy document returned by GET (with merged **`users`**). If PUT fails after an upgrade, set **`RANGER_POLICY_ID`** and compare your Ranger version’s REST schema to the payload from **`RANGER_DRY_RUN=1`**.
- **Cross-host principal mismatch:** host-based service scripts (Hive/Impala/Kafka/Kafka3/Schema-Registry/Spark/Flink/Kudu CLI) default to **`hostname -f`** for `<service>/<host>`. If a run unexpectedly uses another host, check and unset stale shell overrides like **`*_PRINCIPAL_HOST`**; for Hive, config-file host pinning is ignored by default unless **`HIVE_USE_CONFIG_PRINCIPAL_HOST=1`**.
- **Hive ↔ Spark2 managed warehouse:** `spark` often cannot traverse `/warehouse/tablespace/managed/hive` (`drwx------`). `hive-spark2-compat-smoke.sh` uses an external `/tmp/...` location for that reason. Hive reading Spark **Parquet** may still fail (missing JTS / Parquet–Hadoop API skew); ORC round-trip is the required path — use **`TEST_PARQUET=1`** only as a probe.

---

## Layout

```
sample-jobs/
  README.md                 # this file
  prereqs/
    install-jupyterhub-prereqs-rhel8.sh
    install-jupyterhub-prereqs-ubuntu20.sh
    install-airflow-prereqs-*.sh
    install-pinot-prereqs-*.sh
  configs/
    ambari.env
    hive.env
    sqoop.env
  kafka/
    client-sasl.properties
  hbase/
    hbase-sample-smoke.hbase
  oozie/
    shell/workflow.xml
    hive/workflow.xml
    hive/hive_smoke.hql
  druid/
    odp_druid_smoke_index.json
  pinot/
    odp_pinot_smoke_schema.json
    odp_pinot_smoke_table.json
  sql/
    hive-sample-smoke.sql
    hive-spark2-compat-*.sql
    impala-sample-smoke.sql
    sqoop-smoke-mysql-setup.sql
  sqoop-smoke-test.sh
  hive-spark2-compat-smoke.sh
  scala/
    spark-sample-smoke.scala
  *.sh                      # smoke entrypoints (incl. Spark Pi + flink-sample-smoke.sh)
```
