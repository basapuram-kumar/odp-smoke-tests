# Cluster service / component log verification

Utility to verify that every Ambari **STARTED** host-component is writing logs under `/var/log/` (service-named dirs, with known path overrides), optionally after an Ambari restart.

## What it does

1. Discovers the cluster, hosts, and STARTED host-components from Ambari REST
2. SSHs to each host IP (`acceldata` + PEM; runs probes with `sudo`)
3. Snapshots matching log files under `/var/log/...`
4. Optionally restarts the component via Ambari `RESTART`, waits for completion, then re-checks growth
5. Tails recent log lines for `ERROR` / `FATAL` / `Exception`
6. Writes a markdown + TSV report under `reports/log-verify/`

Client-only components (`*_CLIENT`, TEZ, SQOOP, KERBEROS, ...) are skipped by default.

## Config

Edit `configs/log-verify.env` (no cluster host IPs -- those are discovered from Ambari):

- `AMBARI_BASE_URL` -- leave empty to auto-use `http://127.0.0.1:8080` when Ambari is local; otherwise export it
- `AMBARI_USER` / `AMBARI_PASSWORD`
- `SSH_USER` / `SSH_KEY` -- leave `SSH_KEY` empty to try common PEMs / `~/.ssh/id_rsa`; script chmods key to `600` if needed
- `LOG_VERIFY_RESTART=1` to restart each component before checking growth
- `LOG_VERIFY_SERVICES` / `LOG_VERIFY_COMPONENTS` / `LOG_VERIFY_HOSTS` to limit scope

## Usage

```bash
cd odp-smoke-tests

# Plan only (no SSH probes / no restart)
./cluster-service-log-verify.sh --dry-run

# Verify existing logs only (no restart) - recommended first pass
LOG_VERIFY_RESTART=0 ./cluster-service-log-verify.sh --no-restart

# Restart every STARTED component and verify log growth (slow / disruptive)
LOG_VERIFY_RESTART=1 ./cluster-service-log-verify.sh

# Limit scope
LOG_VERIFY_SERVICES=HDFS,YARN,ZOOKEEPER LOG_VERIFY_RESTART=0 ./cluster-service-log-verify.sh --no-restart
./cluster-service-log-verify.sh --no-restart --services HDFS --hosts rhel8node1
./cluster-service-log-verify.sh --restart --components NAMENODE,ZOOKEEPER_SERVER
```

## Report

Each run writes under `reports/log-verify/`:

| File | Use |
|------|-----|
| `latest.html` / `cluster-log-verify-*.html` | **Interactive dashboard** (open in browser) |
| `latest.json` / `*.json` | Machine-readable; rebuild HTML via `./view-log-verify-report.sh` |
| `*.md` / `*.tsv` | Plain-text / spreadsheet exports |

HTML dashboard features: PASS/FAIL/WARN cards, search, status/service/issue filters, sortable columns, expandable error samples, attention list.

```bash
# After a run
./cluster-service-log-verify.sh --no-restart
# Open reports/log-verify/latest.html in a browser
# Or serve from the Ambari/jump host:
cd reports/log-verify && python3 -m http.server 8765
# browse http://<host>:8765/latest.html
```
