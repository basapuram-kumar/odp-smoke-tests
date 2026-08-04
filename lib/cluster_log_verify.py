#!/usr/bin/env python3
"""
Cluster service/component log verification.

Discovers STARTED host-components from Ambari, SSHs to each host, optionally
restarts the component, and verifies that /var/log/<service> (or mapped) logs
exist and are generating. Emits a markdown + TSV report.

Compatible with Python 3.6+ (RHEL 8 system python3).
"""

import argparse
import base64
import json
import os
import re
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


# ---------------------------------------------------------------------------
# Component -> log directory / file pattern mapping
# Paths are searched in order; first existing dir wins (then sibling dirs too).
# ---------------------------------------------------------------------------

# (dirs, positive filename substrings/globs-as-regex, optional negative substrings)
ComponentLogSpec = Tuple[List[str], List[str], List[str]]

LOG_SPECS: Dict[str, Dict[str, ComponentLogSpec]] = {
    "HDFS": {
        "NAMENODE": (["/var/log/hadoop/hdfs"], ["namenode"], ["datanode", "secondary"]),
        "DATANODE": (["/var/log/hadoop/hdfs"], ["datanode"], []),
        "SECONDARY_NAMENODE": (
            ["/var/log/hadoop/hdfs"],
            ["secondarynamenode", "secondary"],
            [],
        ),
        "JOURNALNODE": (["/var/log/hadoop/hdfs"], ["journalnode"], []),
        "ZKFC": (["/var/log/hadoop/hdfs"], ["zkfc"], []),
    },
    "YARN": {
        "RESOURCEMANAGER": (
            ["/var/log/hadoop-yarn/yarn", "/var/log/hadoop/yarn"],
            ["resourcemanager"],
            [],
        ),
        "NODEMANAGER": (
            [
                "/var/log/hadoop-yarn/yarn",
                "/var/log/hadoop-yarn/nodemanager",
                "/var/log/hadoop/yarn",
            ],
            ["nodemanager"],
            [],
        ),
        "APP_TIMELINE_SERVER": (
            ["/var/log/hadoop-yarn/yarn", "/var/log/hadoop-yarn"],
            ["timelineserver", "applicationhistory", "timeline"],
            ["timelinereader", "registrydns"],
        ),
        "TIMELINE_READER": (
            ["/var/log/hadoop-yarn/yarn"],
            ["timelinereader", "timeline"],
            ["registrydns"],
        ),
        "YARN_REGISTRY_DNS": (
            ["/var/log/hadoop-yarn/yarn", "/var/log/hadoop-yarn"],
            ["registrydns", "registry"],
            [],
        ),
    },
    "MAPREDUCE2": {
        "HISTORYSERVER": (
            ["/var/log/hadoop-mapreduce/mapred", "/var/log/hadoop-mapreduce"],
            ["historyserver", "jobhistory"],
            [],
        ),
    },
    "ZOOKEEPER": {
        "ZOOKEEPER_SERVER": (["/var/log/zookeeper"], ["zookeeper", "zk"], []),
    },
    "HBASE": {
        "HBASE_MASTER": (["/var/log/hbase"], ["master"], ["regionserver"]),
        "HBASE_REGIONSERVER": (["/var/log/hbase"], ["regionserver"], []),
        "HBASE_THRIFT_SERVER": (["/var/log/hbase"], ["thrift"], []),
        "PHOENIX_QUERY_SERVER": (["/var/log/hbase", "/var/log/phoenix"], ["phoenix", "queryserver"], []),
    },
    "HIVE": {
        "HIVE_METASTORE": (["/var/log/hive"], ["metastore", "hivemetastore"], ["server2", "hiveserver"]),
        "HIVE_SERVER": (["/var/log/hive"], ["hiveserver2", "hive-server", "server2"], []),
        "HIVE_SERVER_INTERACTIVE": (["/var/log/hive"], ["hiveserver2", "interactive", "llap"], []),
        "WEBHCAT_SERVER": (["/var/log/hive-hcatalog", "/var/log/webhcat"], ["webhcat", "templeton"], []),
    },
    "HTTPFS": {
        "HTTPFS_GATEWAY": (["/var/log/hadoop/httpfs", "/var/log/hadoop/hdfs"], ["httpfs"], []),
    },
    "KAFKA": {
        "KAFKA_BROKER": (["/var/log/kafka"], ["server", "kafka", "controller", "state-change", "log-cleaner"], []),
    },
    "KAFKA3": {
        "KAFKA3_BROKER": (["/var/log/kafka3", "/var/log/kafka"], ["server", "kafka", "controller"], []),
        "KAFKA3_CONNECT": (["/var/log/kafka3", "/var/log/kafka"], ["connect"], []),
        "KAFKA3_MIRRORMAKER": (["/var/log/kafka3", "/var/log/kafka"], ["mirror", "mirrormaker"], []),
    },
    "OOZIE": {
        "OOZIE_SERVER": (["/var/log/oozie"], ["oozie", "jetty"], []),
    },
    "RANGER": {
        "RANGER_ADMIN": (["/var/log/ranger/admin", "/var/log/ranger"], ["xa_portal", "ranger", "catalina", "access"], []),
        "RANGER_USERSYNC": (["/var/log/ranger/usersync", "/var/log/ranger"], ["usersync", "unixAuth"], []),
        "RANGER_TAGSYNC": (["/var/log/ranger/tagsync", "/var/log/ranger"], ["tagsync"], []),
    },
    "RANGER_KMS": {
        "RANGER_KMS_SERVER": (["/var/log/ranger/kms", "/var/log/ranger/kms"], ["kms", "catalina"], []),
    },
    "KNOX": {
        "KNOX_GATEWAY": (["/var/log/knox"], ["gateway", "knox"], []),
    },
    "SPARK2": {
        "SPARK2_JOBHISTORYSERVER": (["/var/log/spark2", "/var/log/spark"], ["HistoryServer", "history"], []),
        "SPARK2_THRIFTSERVER": (["/var/log/spark2", "/var/log/spark"], ["ThriftServer", "thrift"], []),
    },
    "SPARK3": {
        "SPARK3_JOBHISTORYSERVER": (
            ["/var/log/spark3", "/var/log/spark351", "/var/log/spark333", "/var/log/spark"],
            ["HistoryServer", "history"],
            [],
        ),
        "SPARK3_THRIFTSERVER": (
            ["/var/log/spark3", "/var/log/spark351", "/var/log/spark333", "/var/log/spark"],
            ["ThriftServer", "HiveThriftServer", "thrift"],
            [],
        ),
        "LIVY3_SERVER": (["/var/log/livy3", "/var/log/livy", "/var/log/livy351", "/var/log/livy333"], ["livy"], []),
    },
    "ZEPPELIN": {
        "ZEPPELIN_MASTER": (["/var/log/zeppelin"], ["zeppelin"], []),
    },
    "AIRFLOW": {
        "AIRFLOW_SCHEDULER": (
            ["/var/log/airflow/logs/scheduler", "/var/log/airflow/logs", "/var/log/airflow"],
            ["scheduler"],
            [],
        ),
        "AIRFLOW_WEBSERVER": (
            ["/var/log/airflow/logs", "/var/log/airflow"],
            ["webserver"],
            [],
        ),
        "AIRFLOW_WORKER": (
            ["/var/log/airflow/logs", "/var/log/airflow"],
            ["worker", "celery"],
            [],
        ),
    },
    "AMBARI_INFRA_SOLR": {
        "INFRA_SOLR": (["/var/log/ambari-infra-solr"], ["solr"], []),
    },
    "DRUID": {
        "DRUID_BROKER": (["/var/log/druid"], ["broker"], []),
        "DRUID_COORDINATOR": (["/var/log/druid"], ["coordinator"], []),
        "DRUID_HISTORICAL": (["/var/log/druid"], ["historical"], []),
        "DRUID_MIDDLEMANAGER": (["/var/log/druid"], ["middleManager", "middlemanager"], []),
        "DRUID_OVERLORD": (["/var/log/druid"], ["overlord"], []),
        "DRUID_ROUTER": (["/var/log/druid"], ["router"], []),
    },
    "FLINK": {
        "FLINK_JOBHISTORYSERVER": (
            ["/var/log/flink", "/var/log/flink-cli"],
            ["history", "jobmanager", "standalone", "flink"],
            [],
        ),
    },
    "HUE": {
        "HUE_SERVER": (["/var/log/hue"], ["runcpserver", "hue", "error", "access", "kt_renewer"], []),
    },
    "IMPALA": {
        "IMPALA_CATALOG_SERVICE": (
            ["/var/log/impala", "/var/log/impalad"],
            ["catalogd", "catalog", "impala"],
            [],
        ),
        "IMPALA_STATE_STORE": (
            ["/var/log/impala", "/var/log/impalad"],
            ["statestored", "statestore", "impala"],
            [],
        ),
        "IMPALA_DAEMON": (
            ["/var/log/impala", "/var/log/impalad"],
            ["impalad", "impaladaemon", "impala"],
            [],
        ),
    },
    "JUPYTER": {
        "JUPYTERHUB": (["/var/log/jupyterhub", "/var/log/jupyter"], ["jupyterhub", "jupyter"], []),
    },
    "KUDU": {
        "KUDU_MASTER": (["/var/log/kudu"], ["kudu-master", "master"], ["tserver"]),
        "KUDU_TSERVER": (["/var/log/kudu"], ["kudu-tserver", "tserver"], []),
    },
    "NIFI": {
        "NIFI_MASTER": (["/var/log/nifi"], ["nifi-app", "nifi-bootstrap", "nifi"], []),
    },
    "NIFI_REGISTRY": {
        "NIFI_REGISTRY_MASTER": (["/var/log/nifi-registry"], ["nifi-registry", "nifi"], []),
    },
    "OZONE": {
        "OZONE_MANAGER": (["/var/log/hadoop-ozone", "/var/log/ozone"], ["om-", "ozone-om", "OMAudit", "om."], []),
        "OZONE_STORAGE_CONTAINER_MANAGER": (
            ["/var/log/hadoop-ozone", "/var/log/ozone"],
            ["scm-", "ozone-scm", "SCMAudit", "scm."],
            [],
        ),
        "OZONE_DATANODE": (
            ["/var/log/hadoop-ozone", "/var/log/ozone"],
            ["dn-", "datanode", "ozone-datanode"],
            [],
        ),
        "OZONE_RECON": (["/var/log/hadoop-ozone", "/var/log/ozone"], ["recon", "ozone-recon"], []),
        "OZONE_S3_GATEWAY": (["/var/log/hadoop-ozone", "/var/log/ozone"], ["s3g", "s3gateway", "ozone-s3"], []),
    },
    "REGISTRY": {
        "REGISTRY_SERVER": (["/var/log/registry"], ["registry"], []),
    },
}

# Services / components that are client-only or have no daemon logs by design
DEFAULT_SKIP_COMPONENTS = {
    "HDFS_CLIENT",
    "YARN_CLIENT",
    "MAPREDUCE2_CLIENT",
    "ZOOKEEPER_CLIENT",
    "HBASE_CLIENT",
    "HIVE_CLIENT",
    "TEZ_CLIENT",
    "SPARK2_CLIENT",
    "SPARK3_CLIENT",
    "FLINK_CLIENT",
    "IMPALA_CLIENT",
    "OOZIE_CLIENT",
    "INFRA_SOLR_CLIENT",
    "SQOOP",
    "KERBEROS_CLIENT",
    "PIG",
    "HCAT",
    "SLIDER",
    "MAHOUT",
}

DEFAULT_SKIP_SERVICES = {"KERBEROS", "TEZ", "SQOOP"}

# Fallback: /var/log/<service_lower> and soft name variants
SERVICE_LOG_DIR_FALLBACKS: Dict[str, List[str]] = {
    "AMBARI_INFRA_SOLR": ["/var/log/ambari-infra-solr"],
    "MAPREDUCE2": ["/var/log/hadoop-mapreduce", "/var/log/hadoop/mapreduce"],
    "YARN": ["/var/log/hadoop-yarn", "/var/log/hadoop/yarn"],
    "HDFS": ["/var/log/hadoop/hdfs", "/var/log/hadoop"],
    "OZONE": ["/var/log/hadoop-ozone", "/var/log/ozone"],
    "SPARK2": ["/var/log/spark2", "/var/log/spark"],
    "SPARK3": ["/var/log/spark3", "/var/log/spark"],
    "RANGER_KMS": ["/var/log/ranger/kms", "/var/log/ranger"],
    "NIFI_REGISTRY": ["/var/log/nifi-registry"],
    "JUPYTER": ["/var/log/jupyterhub", "/var/log/jupyter"],
    "HTTPFS": ["/var/log/hadoop/httpfs"],
}


class HostComponent(object):
    def __init__(self, service, component, host_name, host_ip, state):
        self.service = service
        self.component = component
        self.host_name = host_name
        self.host_ip = host_ip
        self.state = state


class LogFileInfo(object):
    def __init__(self, path, size, mtime):
        self.path = path
        self.size = size
        self.mtime = mtime


class ComponentResult(object):
    def __init__(self, service, component, host_name, host_ip, state_before):
        self.service = service
        self.component = component
        self.host_name = host_name
        self.host_ip = host_ip
        self.state_before = state_before
        self.log_dirs_found = []  # type: List[str]
        self.log_files_before = []  # type: List[LogFileInfo]
        self.log_files_after = []  # type: List[LogFileInfo]
        self.restarted = False
        self.restart_ok = None  # type: Optional[bool]
        self.restart_request_id = None  # type: Optional[str]
        self.restart_error = ""
        self.logs_exist = False
        self.logs_generating = None  # type: Optional[bool]
        self.missing_log = False
        self.errors_found = []  # type: List[str]
        self.status = "UNKNOWN"  # PASS / FAIL / WARN / SKIPPED
        self.notes = []  # type: List[str]


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(level: str, msg: str) -> None:
    print("[%s] %s" % (level, msg), flush=True)


def split_csv(value: str) -> List[str]:
    if not value:
        return []
    return [p.strip().upper() for p in re.split(r"[, \t]+", value) if p.strip()]


class AmbariClient:
    def __init__(self, base_url: str, user: str, password: str, insecure: bool = False):
        self.base = base_url.rstrip("/")
        self.user = user
        self.password = password
        self.insecure = insecure
        token = base64.b64encode(("%s:%s" % (user, password)).encode()).decode()
        self._auth = "Basic %s" % token

    def request(self, method: str, path: str, body: Any = None, timeout: int = 90) -> Tuple[int, Any]:
        url = path if path.startswith("http") else self.base + path
        data = None
        headers = {"Authorization": self._auth, "X-Requested-By": "ambari"}
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        ctx = None
        if self.insecure and self.base.startswith("https"):
            import ssl

            ctx = ssl._create_unverified_context()
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
                raw = resp.read().decode()
                return resp.status, json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            raw = e.read().decode() if e.fp else ""
            try:
                payload = json.loads(raw) if raw else {"message": str(e)}
            except Exception:
                payload = {"message": raw or str(e)}
            return e.code, payload
        except Exception as e:
            return 0, {"message": str(e)}

    def discover_cluster(self, preferred: str = "") -> str:
        if preferred:
            return preferred
        code, data = self.request("GET", "/api/v1/clusters/")
        if code != 200:
            raise RuntimeError("failed to list clusters: %s" % data)
        items = data.get("items") or []
        if not items:
            raise RuntimeError("no clusters found")
        return items[0]["Clusters"]["cluster_name"]

    def list_hosts(self, cluster: str) -> Dict[str, str]:
        fields = "Hosts/host_name,Hosts/ip,Hosts/public_host_name"
        code, data = self.request(
            "GET",
            "/api/v1/clusters/%s/hosts?fields=%s"
            % (urllib.parse.quote(cluster), urllib.parse.quote(fields)),
        )
        if code != 200:
            raise RuntimeError("failed to list hosts: %s" % data)
        out: Dict[str, str] = {}
        for item in data.get("items") or []:
            h = item["Hosts"]
            name = h["host_name"]
            ip = h.get("ip") or h.get("public_host_name") or name
            out[name] = ip
        return out

    def list_started_host_components(self, cluster: str) -> List[HostComponent]:
        fields = "host_components/HostRoles"
        code, data = self.request(
            "GET",
            "/api/v1/clusters/%s/hosts?fields=%s"
            % (urllib.parse.quote(cluster), urllib.parse.quote(fields)),
        )
        if code != 200:
            raise RuntimeError("failed to list host_components: %s" % data)
        hosts = self.list_hosts(cluster)
        result: List[HostComponent] = []
        for host in data.get("items") or []:
            hn = host["Hosts"]["host_name"]
            hip = hosts.get(hn, hn)
            for hc in host.get("host_components") or []:
                roles = hc["HostRoles"]
                result.append(
                    HostComponent(
                        service=roles["service_name"],
                        component=roles["component_name"],
                        host_name=hn,
                        host_ip=hip,
                        state=roles.get("state") or "",
                    )
                )
        return result

    def restart_host_component(
        self, cluster: str, service: str, component: str, host_name: str, context: str
    ) -> Tuple[bool, str, str]:
        body = {
            "RequestInfo": {
                "command": "RESTART",
                "context": context,
                "operation_level": {
                    "level": "HOST_COMPONENT",
                    "cluster_name": cluster,
                    "host_name": host_name,
                    "service_name": service,
                },
            },
            "Requests/resource_filters": [
                {
                    "service_name": service,
                    "component_name": component,
                    "hosts": host_name,
                }
            ],
        }
        code, data = self.request(
            "POST",
            "/api/v1/clusters/%s/requests" % urllib.parse.quote(cluster),
            body=body,
        )
        if code not in (200, 201, 202):
            return False, "", "HTTP %s: %s" % (code, data)
        req_id = ""
        try:
            req_id = str(data["Requests"]["id"])
        except Exception:
            req_id = ""
        return True, req_id, ""

    def wait_request(self, cluster: str, request_id: str, timeout: int, poll: int = 5) -> Tuple[bool, str]:
        if not request_id:
            return False, "missing request id"
        deadline = time.time() + timeout
        last = ""
        while time.time() < deadline:
            code, data = self.request(
                "GET",
                "/api/v1/clusters/%s/requests/%s?fields=Requests/request_status,Requests/progress_percent"
                % (urllib.parse.quote(cluster), urllib.parse.quote(request_id)),
            )
            if code != 200:
                last = "poll failed: %s" % data
                time.sleep(poll)
                continue
            status = (data.get("Requests") or {}).get("request_status") or ""
            last = status
            if status in ("COMPLETED", "FAILED", "ABORTED", "TIMEDOUT"):
                return status == "COMPLETED", status
            time.sleep(poll)
        return False, "TIMEOUT (last=%s)" % last

    def get_host_component_state(self, cluster: str, host: str, component: str) -> str:
        code, data = self.request(
            "GET",
            "/api/v1/clusters/%s/hosts/%s/host_components/%s"
            % (
                urllib.parse.quote(cluster),
                urllib.parse.quote(host),
                urllib.parse.quote(component),
            ),
        )
        if code != 200:
            return ""
        return (data.get("HostRoles") or {}).get("state") or ""


class SSHRunner:
    def __init__(self, user: str, key: str, extra_opts: str = ""):
        self.user = user
        self.key = key
        self.extra_opts = extra_opts

    def _base_cmd(self, host: str) -> List[str]:
        cmd = ["ssh"]
        if self.key:
            cmd.extend(["-i", self.key])
        if self.extra_opts:
            cmd.extend(shlex.split(self.extra_opts))
        cmd.append("%s@%s" % (self.user, host))
        return cmd

    def run(self, host, remote, timeout=120):
        # type: (str, str, int) -> Tuple[int, str, str]
        cmd = self._base_cmd(host) + [remote]
        try:
            p = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
                universal_newlines=True,
            )
            return p.returncode, p.stdout or "", p.stderr or ""
        except subprocess.TimeoutExpired as e:
            out = e.stdout if isinstance(e.stdout, str) else (e.stdout or b"").decode("utf-8", "replace") if e.stdout else ""
            return 124, out, "ssh timeout: %s" % e
        except Exception as e:
            return 1, "", str(e)

    def sudo_bash(self, host, script, timeout=180):
        # type: (str, str, int) -> Tuple[int, str, str]
        # Pipe script to sudo bash -s on the remote host
        remote = "sudo bash -s"
        cmd = self._base_cmd(host) + [remote]
        try:
            p = subprocess.run(
                cmd,
                input=script,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
                universal_newlines=True,
            )
            return p.returncode, p.stdout or "", p.stderr or ""
        except subprocess.TimeoutExpired as e:
            out = e.stdout if isinstance(e.stdout, str) else (e.stdout or b"").decode("utf-8", "replace") if e.stdout else ""
            return 124, out, "ssh timeout: %s" % e
        except Exception as e:
            return 1, "", str(e)


def resolve_log_spec(service: str, component: str) -> ComponentLogSpec:
    svc = LOG_SPECS.get(service) or {}
    if component in svc:
        return svc[component]
    # Fallback directories from service name
    dirs = list(SERVICE_LOG_DIR_FALLBACKS.get(service) or [])
    soft = service.lower().replace("_", "-")
    soft2 = service.lower().replace("_", "")
    for candidate in (
        "/var/log/%s" % soft,
        "/var/log/%s" % soft2,
        "/var/log/%s" % service.lower(),
    ):
        if candidate not in dirs:
            dirs.append(candidate)
    # Use component tokens as positive matchers
    tokens = [t for t in re.split(r"[_\-]+", component.lower()) if t and t not in ("server", "master")]
    if not tokens:
        tokens = [component.lower()]
    return (dirs, tokens, [])


REMOTE_PROBE_SCRIPT = r'''
set -euo pipefail
DIRS_CSV="$1"
POS_CSV="$2"
NEG_CSV="$3"
TAIL_N="$4"
MODE="$5"   # snapshot | errors

IFS=',' read -r -a DIRS <<< "$DIRS_CSV"
IFS=',' read -r -a POS <<< "$POS_CSV"
IFS=',' read -r -a NEG <<< "$NEG_CSV"

existing=()
for d in "${DIRS[@]}"; do
  [ -z "$d" ] && continue
  if [ -d "$d" ]; then
    existing+=("$d")
  fi
done

# Also accept any /var/log dir that contains a positive token in its name when empty
if [ ${#existing[@]} -eq 0 ]; then
  for d in /var/log/*; do
    [ -d "$d" ] || continue
    base="$(basename "$d")"
    for p in "${POS[@]}"; do
      [ -z "$p" ] && continue
      case "$base" in
        *"$p"*) existing+=("$d"); break ;;
      esac
    done
  done
fi

printf 'DIRS|'
(IFS=','; echo "${existing[*]}")
printf '\n'

match_file() {
  local f="$1" base
  base="$(basename "$f")"
  case "$base" in
    *.gz|*.zip|*.tgz) return 1 ;;
  esac
  # skip pure rotated dated empties without extension interest? keep .log .out .err INFO WARNING
  local ok=0
  if [ ${#POS[@]} -eq 0 ] || [ -z "${POS[0]:-}" ]; then
    ok=1
  else
    for p in "${POS[@]}"; do
      [ -z "$p" ] && continue
      case "$base" in
        *"$p"*) ok=1; break ;;
      esac
      # Also allow directory-path tokens (e.g. .../scheduler/.../foo.py.log)
      case "$f" in
        *"$p"*) ok=1; break ;;
      esac
    done
  fi
  [ "$ok" -eq 1 ] || return 1
  for n in "${NEG[@]}"; do
    [ -z "$n" ] && continue
    case "$base" in
      *"$n"*) return 1 ;;
    esac
  done
  return 0
}

files=()
for d in "${existing[@]}"; do
  while IFS= read -r -d '' f; do
    if match_file "$f"; then
      files+=("$f")
    fi
  done < <(find "$d" -maxdepth 8 \( -type f -o -type l \) \
    \( -name '*.log' -o -name '*.out' -o -name '*.err' -o -name '*.INFO' -o -name '*.WARNING' -o -name '*.ERROR' \
       -o -name '*log' -o -name '*.current' -o -name '*INFO*' -o -name '*WARNING*' -o -name '*ERROR*' \) \
    -print0 2>/dev/null || true)
done

# Dedup
declare -A SEEN=()
uniq=()
for f in "${files[@]}"; do
  [ -n "${SEEN[$f]:-}" ] && continue
  SEEN[$f]=1
  uniq+=("$f")
done
files=("${uniq[@]}")

if [ "$MODE" = "snapshot" ]; then
  for f in "${files[@]}"; do
    size=$(stat -L -c '%s' "$f" 2>/dev/null || echo 0)
    mtime=$(stat -L -c '%Y' "$f" 2>/dev/null || echo 0)
    printf 'FILE|%s|%s|%s\n' "$f" "$size" "$mtime"
  done
  exit 0
fi

# errors mode: print matching ERROR/FATAL/Exception lines from newest files
# Prefer newest by mtime
IFS=$'\n' sorted=($(for f in "${files[@]}"; do
  mtime=$(stat -L -c '%Y' "$f" 2>/dev/null || echo 0)
  printf '%s\t%s\n' "$mtime" "$f"
done | sort -nr | head -20 | cut -f2-)) || true

count=0
for f in "${sorted[@]:-}"; do
  [ -z "$f" ] && continue
  # shellcheck disable=SC2002
  while IFS= read -r line; do
    printf 'ERR|%s|%s\n' "$f" "$line"
    count=$((count+1))
    [ "$count" -ge 40 ] && break 2
  done < <(tail -n "$TAIL_N" "$f" 2>/dev/null | grep -E 'ERROR|FATAL|Exception|WARN[[:space:]]' | grep -E 'ERROR|FATAL|Exception' | tail -n 12 || true)
done
'''


def parse_probe_snapshot(stdout: str) -> Tuple[List[str], List[LogFileInfo]]:
    dirs: List[str] = []
    files: List[LogFileInfo] = []
    for line in stdout.splitlines():
        if line.startswith("DIRS|"):
            payload = line[5:].strip()
            if payload:
                dirs = [p for p in payload.split(",") if p]
        elif line.startswith("FILE|"):
            parts = line.split("|", 3)
            if len(parts) == 4:
                files.append(LogFileInfo(path=parts[1], size=int(parts[2] or 0), mtime=float(parts[3] or 0)))
    return dirs, files


def parse_probe_errors(stdout: str) -> List[str]:
    out: List[str] = []
    for line in stdout.splitlines():
        if line.startswith("ERR|"):
            parts = line.split("|", 2)
            if len(parts) == 3:
                path, msg = parts[1], parts[2]
                # Keep short
                msg = msg.strip()
                if len(msg) > 240:
                    msg = msg[:237] + "..."
                out.append("%s :: %s" % (path, msg))
    return out


def csv_join(items: Sequence[str]) -> str:
    return ",".join(items)


def logs_are_generating(
    before: List[LogFileInfo],
    after: List[LogFileInfo],
    stale_hours: float,
    restarted: bool,
) -> Tuple[bool, str]:
    if not after:
        return False, "no matching log files found"
    after_map = {f.path: f for f in after}
    before_map = {f.path: f for f in before}

    if restarted:
        grew = False
        new_files = False
        for path, af in after_map.items():
            bf = before_map.get(path)
            if bf is None:
                new_files = True
                continue
            if af.size > bf.size or af.mtime > bf.mtime + 0.5:
                grew = True
        if grew or new_files:
            detail = []
            if new_files:
                detail.append("new log file(s)")
            if grew:
                detail.append("size/mtime advanced")
            return True, "; ".join(detail)
        return False, "log files present but no growth after restart"

    # No restart: consider fresh enough if any file mtime within stale window
    now = time.time()
    cutoff = now - stale_hours * 3600
    fresh = [f for f in after if f.mtime >= cutoff and f.size > 0]
    if fresh:
        newest = max(fresh, key=lambda x: x.mtime)
        age_h = (now - newest.mtime) / 3600.0
        return True, "newest log age=%.2fh size=%d path=%s" % (age_h, newest.size, newest.path)
    nonempty = [f for f in after if f.size > 0]
    if nonempty:
        newest = max(nonempty, key=lambda x: x.mtime)
        age_h = (now - newest.mtime) / 3600.0
        return False, "logs exist but stale (newest age=%.2fh > %sh)" % (age_h, stale_hours)
    return False, "log files exist but all empty"


def evaluate_status(result: ComponentResult) -> None:
    if result.missing_log or not result.logs_exist:
        result.status = "FAIL"
        result.notes.append("MISSING_LOG")
        return
    if result.restarted and result.restart_ok is False:
        result.status = "FAIL"
        result.notes.append("RESTART_FAILED")
        return
    if result.logs_generating is False:
        result.status = "FAIL"
        result.notes.append("NOT_GENERATING")
        return
    if result.errors_found:
        result.status = "WARN"
        result.notes.append("ERRORS_IN_LOG")
        return
    result.status = "PASS"


def write_reports(results: List[ComponentResult], report_dir: Path, meta: Dict[str, Any]) -> Tuple[Path, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    md_path = report_dir / ("cluster-log-verify-%s.md" % stamp)
    tsv_path = report_dir / ("cluster-log-verify-%s.tsv" % stamp)

    counts = {"PASS": 0, "FAIL": 0, "WARN": 0, "SKIPPED": 0, "UNKNOWN": 0}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1

    missing = [r for r in results if r.missing_log or not r.logs_exist]
    not_gen = [r for r in results if r.logs_generating is False]
    with_err = [r for r in results if r.errors_found]
    restart_fail = [r for r in results if r.restarted and r.restart_ok is False]

    lines: List[str] = []
    lines.append("# Cluster service log verification report")
    lines.append("")
    lines.append("- Generated: `%s`" % utc_now())
    lines.append("- Ambari: `%s`" % meta.get("ambari"))
    lines.append("- Cluster: `%s`" % meta.get("cluster"))
    lines.append("- Restart mode: `%s`" % ("ON" if meta.get("restart") else "OFF"))
    lines.append(
        "- Summary: PASS=%d FAIL=%d WARN=%d SKIPPED=%d total=%d"
        % (counts.get("PASS", 0), counts.get("FAIL", 0), counts.get("WARN", 0), counts.get("SKIPPED", 0), len(results))
    )
    lines.append("")

    lines.append("## Missing / not generating logs")
    lines.append("")
    if not missing and not not_gen:
        lines.append("None.")
    else:
        lines.append("| Service | Component | Host | Issue |")
        lines.append("|---------|-----------|------|-------|")
        seen = set()
        for r in missing + not_gen:
            key = (r.service, r.component, r.host_name, "missing" if r.missing_log or not r.logs_exist else "not_gen")
            if key in seen:
                continue
            seen.add(key)
            issue = "MISSING_LOG" if (r.missing_log or not r.logs_exist) else "NOT_GENERATING"
            lines.append(
                "| %s | %s | %s (%s) | %s |"
                % (r.service, r.component, r.host_name, r.host_ip, issue)
            )
    lines.append("")

    if restart_fail:
        lines.append("## Restart failures")
        lines.append("")
        lines.append("| Service | Component | Host | Error |")
        lines.append("|---------|-----------|------|-------|")
        for r in restart_fail:
            lines.append(
                "| %s | %s | %s | %s |"
                % (r.service, r.component, r.host_name, (r.restart_error or "unknown")[:120])
            )
        lines.append("")

    lines.append("## Components with ERROR/FATAL/Exception samples")
    lines.append("")
    if not with_err:
        lines.append("None captured in tailed logs.")
    else:
        for r in with_err:
            lines.append("### %s / %s @ %s" % (r.service, r.component, r.host_name))
            lines.append("")
            for e in r.errors_found[: meta.get("error_sample_max", 8)]:
                lines.append("- `%s`" % e.replace("`", "'"))
            lines.append("")

    lines.append("## Component-wise results")
    lines.append("")
    lines.append(
        "| Status | Service | Component | Host | IP | Restart | Logs exist | Generating | Log dirs | Notes |"
    )
    lines.append("|--------|---------|-----------|------|----|---------|------------|------------|----------|-------|")
    for r in sorted(results, key=lambda x: (x.status != "PASS", x.service, x.component, x.host_name)):
        gen = "-" if r.logs_generating is None else ("YES" if r.logs_generating else "NO")
        rst = "-"
        if r.restarted:
            rst = "OK" if r.restart_ok else "FAIL"
        dirs = ", ".join(r.log_dirs_found) if r.log_dirs_found else "-"
        notes = ",".join(r.notes) if r.notes else "-"
        lines.append(
            "| %s | %s | %s | %s | %s | %s | %s | %s | `%s` | %s |"
            % (
                r.status,
                r.service,
                r.component,
                r.host_name,
                r.host_ip,
                rst,
                "YES" if r.logs_exist else "NO",
                gen,
                dirs,
                notes,
            )
        )
    lines.append("")

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with tsv_path.open("w", encoding="utf-8") as fh:
        fh.write(
            "\t".join(
                [
                    "status",
                    "service",
                    "component",
                    "host",
                    "ip",
                    "restarted",
                    "restart_ok",
                    "logs_exist",
                    "logs_generating",
                    "log_dirs",
                    "notes",
                    "error_sample",
                ]
            )
            + "\n"
        )
        for r in results:
            fh.write(
                "\t".join(
                    [
                        r.status,
                        r.service,
                        r.component,
                        r.host_name,
                        r.host_ip,
                        "1" if r.restarted else "0",
                        "" if r.restart_ok is None else ("1" if r.restart_ok else "0"),
                        "1" if r.logs_exist else "0",
                        "" if r.logs_generating is None else ("1" if r.logs_generating else "0"),
                        ",".join(r.log_dirs_found),
                        ",".join(r.notes),
                        " || ".join(r.errors_found[:3]).replace("\t", " "),
                    ]
                )
                + "\n"
            )

    return md_path, tsv_path


def probe_component_logs(
    ssh: SSHRunner,
    host_ip: str,
    dirs: List[str],
    positives: List[str],
    negatives: List[str],
    tail_n: int,
    mode: str,
) -> Tuple[int, str, str]:
    # Write probe script on the remote host, then run with args.
    script = (
        "cat > /tmp/log_verify_probe.sh <<'EOS'\n"
        + REMOTE_PROBE_SCRIPT
        + "\nEOS\n"
        + "chmod +x /tmp/log_verify_probe.sh\n"
        + "bash /tmp/log_verify_probe.sh %s %s %s %s %s\n"
        % (
            shlex.quote(csv_join(dirs)),
            shlex.quote(csv_join(positives)),
            shlex.quote(csv_join(negatives)),
            shlex.quote(str(tail_n)),
            shlex.quote(mode),
        )
    )
    return ssh.sudo_bash(host_ip, script, timeout=180)


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Ambari cluster service/component logs")
    parser.add_argument("--ambari", default=os.environ.get("AMBARI_BASE_URL", ""))
    parser.add_argument("--user", default=os.environ.get("AMBARI_USER", "admin"))
    parser.add_argument("--password", default=os.environ.get("AMBARI_PASSWORD", "admin"))
    parser.add_argument("--cluster", default=os.environ.get("CLUSTER_NAME", ""))
    parser.add_argument("--ssh-user", default=os.environ.get("SSH_USER", "acceldata"))
    parser.add_argument("--ssh-key", default=os.environ.get("SSH_KEY", ""))
    parser.add_argument("--ssh-opts", default=os.environ.get("SSH_OPTS", "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=15"))
    parser.add_argument("--restart", action="store_true", default=os.environ.get("LOG_VERIFY_RESTART", "0") == "1")
    parser.add_argument("--no-restart", action="store_true")
    parser.add_argument("--services", default=os.environ.get("LOG_VERIFY_SERVICES", ""))
    parser.add_argument("--skip-services", default=os.environ.get("LOG_VERIFY_SKIP_SERVICES", ""))
    parser.add_argument("--components", default=os.environ.get("LOG_VERIFY_COMPONENTS", ""))
    parser.add_argument(
        "--hosts",
        default=os.environ.get("LOG_VERIFY_HOSTS", ""),
        help="Limit to host names or IPs (comma/space separated)",
    )
    parser.add_argument("--stale-hours", type=float, default=float(os.environ.get("LOG_STALE_HOURS", "24")))
    parser.add_argument("--settle-seconds", type=int, default=int(os.environ.get("LOG_SETTLE_SECONDS", "20")))
    parser.add_argument("--restart-timeout", type=int, default=int(os.environ.get("LOG_RESTART_TIMEOUT_SECONDS", "600")))
    parser.add_argument("--error-tail", type=int, default=int(os.environ.get("LOG_ERROR_TAIL_LINES", "200")))
    parser.add_argument("--error-sample-max", type=int, default=int(os.environ.get("LOG_ERROR_SAMPLE_MAX", "8")))
    parser.add_argument("--report-dir", default=os.environ.get("LOG_VERIFY_REPORT_DIR", "reports/log-verify"))
    parser.add_argument("--insecure", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Discover and print plan only")
    args = parser.parse_args()

    if args.no_restart:
        args.restart = False

    if not args.ambari:
        log("ERROR", "AMBARI_BASE_URL / --ambari is required")
        return 2
    ssh_key = os.path.expanduser(args.ssh_key or "")
    args.ssh_key = ssh_key
    if not ssh_key or not Path(ssh_key).exists():
        log("ERROR", "SSH key not found: %s" % ssh_key)
        return 2

    services_filter = set(split_csv(args.services))
    skip_services = set(split_csv(args.skip_services)) or set(DEFAULT_SKIP_SERVICES)
    components_filter = set(split_csv(args.components))
    hosts_filter = {h.strip().lower() for h in re.split(r"[, \t]+", args.hosts or "") if h.strip()}

    ambari = AmbariClient(args.ambari, args.user, args.password, insecure=args.insecure)
    cluster = ambari.discover_cluster(args.cluster)
    log("INFO", "Cluster=%s Ambari=%s restart=%s" % (cluster, args.ambari, args.restart))

    all_hc = ambari.list_started_host_components(cluster)
    targets: List[HostComponent] = []
    for hc in all_hc:
        if hc.state != "STARTED":
            continue
        if hc.component in DEFAULT_SKIP_COMPONENTS:
            continue
        if hc.service in skip_services:
            continue
        if services_filter and hc.service not in services_filter:
            continue
        if components_filter and hc.component not in components_filter:
            continue
        if hosts_filter:
            if hc.host_name.lower() not in hosts_filter and hc.host_ip.lower() not in hosts_filter:
                continue
        # Skip pure client-looking names
        if hc.component.endswith("_CLIENT"):
            continue
        targets.append(hc)

    targets.sort(key=lambda x: (x.service, x.component, x.host_name))
    log("INFO", "Selected %d STARTED host-components for verification" % len(targets))

    if args.dry_run:
        for hc in targets:
            dirs, pos, neg = resolve_log_spec(hc.service, hc.component)
            print(
                "%s\t%s\t%s\t%s\tdirs=%s\tmatch=%s"
                % (hc.service, hc.component, hc.host_name, hc.host_ip, ",".join(dirs), ",".join(pos))
            )
        return 0

    ssh = SSHRunner(args.ssh_user, args.ssh_key, args.ssh_opts)

    # Connectivity preflight
    host_ips = sorted({hc.host_ip for hc in targets})
    for ip in host_ips:
        code, out, err = ssh.run(ip, "hostname", timeout=30)
        if code != 0:
            log("ERROR", "SSH failed to %s: %s %s" % (ip, out, err))
            return 3
        log("INFO", "SSH OK %s -> %s" % (ip, out.strip()))

    results: List[ComponentResult] = []
    total = len(targets)
    for idx, hc in enumerate(targets, 1):
        log(
            "INFO",
            "(%d/%d) %s/%s on %s (%s)"
            % (idx, total, hc.service, hc.component, hc.host_name, hc.host_ip),
        )
        dirs, positives, negatives = resolve_log_spec(hc.service, hc.component)
        result = ComponentResult(
            service=hc.service,
            component=hc.component,
            host_name=hc.host_name,
            host_ip=hc.host_ip,
            state_before=hc.state,
        )

        code, out, err = probe_component_logs(
            ssh, hc.host_ip, dirs, positives, negatives, args.error_tail, "snapshot"
        )
        if code != 0:
            result.notes.append("PROBE_FAILED")
            result.missing_log = True
            result.status = "FAIL"
            result.restart_error = (err or out or "probe failed")[:300]
            log("WARN", "probe failed: %s" % result.restart_error)
            evaluate_status(result)
            results.append(result)
            continue

        found_dirs, files_before = parse_probe_snapshot(out)
        result.log_dirs_found = found_dirs
        result.log_files_before = files_before
        result.logs_exist = bool(files_before)

        if args.restart:
            result.restarted = True
            ok, req_id, rerr = ambari.restart_host_component(
                cluster,
                hc.service,
                hc.component,
                hc.host_name,
                "log-verify restart %s/%s" % (hc.service, hc.component),
            )
            result.restart_request_id = req_id
            if not ok:
                result.restart_ok = False
                result.restart_error = rerr
                log("WARN", "restart submit failed: %s" % rerr)
            else:
                log("INFO", "restart request id=%s waiting..." % req_id)
                wok, wstatus = ambari.wait_request(cluster, req_id, args.restart_timeout)
                result.restart_ok = wok
                if not wok:
                    result.restart_error = wstatus
                    log("WARN", "restart finished with %s" % wstatus)
                else:
                    # confirm STARTED
                    time.sleep(2)
                    st = ambari.get_host_component_state(cluster, hc.host_name, hc.component)
                    if st and st != "STARTED":
                        result.restart_ok = False
                        result.restart_error = "post-restart state=%s" % st
                    else:
                        time.sleep(max(1, args.settle_seconds))

            code2, out2, err2 = probe_component_logs(
                ssh, hc.host_ip, dirs, positives, negatives, args.error_tail, "snapshot"
            )
            if code2 == 0:
                found_dirs2, files_after = parse_probe_snapshot(out2)
                result.log_dirs_found = found_dirs2 or result.log_dirs_found
                result.log_files_after = files_after
                result.logs_exist = bool(files_after)
            else:
                result.notes.append("POST_PROBE_FAILED")
                result.log_files_after = []
        else:
            result.log_files_after = files_before

        generating, detail = logs_are_generating(
            result.log_files_before,
            result.log_files_after or result.log_files_before,
            args.stale_hours,
            restarted=bool(args.restart and result.restart_ok),
        )
        # If we did not restart (or restart failed), still evaluate freshness on current files
        if not args.restart or not result.restart_ok:
            generating, detail = logs_are_generating(
                result.log_files_before,
                result.log_files_after or result.log_files_before,
                args.stale_hours,
                restarted=False,
            )
        result.logs_generating = generating
        result.notes.append(detail)
        result.missing_log = not result.logs_exist

        # Error samples
        code3, out3, _ = probe_component_logs(
            ssh, hc.host_ip, dirs, positives, negatives, args.error_tail, "errors"
        )
        if code3 == 0:
            errs = parse_probe_errors(out3)
            result.errors_found = errs[: args.error_sample_max]

        evaluate_status(result)
        log(
            "INFO",
            "-> %s exist=%s generating=%s errors=%d"
            % (result.status, result.logs_exist, result.logs_generating, len(result.errors_found)),
        )
        results.append(result)

    report_dir = Path(args.report_dir)
    if not report_dir.is_absolute():
        # relative to CWD (wrapper cds to script dir)
        report_dir = Path.cwd() / report_dir
    md_path, tsv_path = write_reports(
        results,
        report_dir,
        {
            "ambari": args.ambari,
            "cluster": cluster,
            "restart": args.restart,
            "error_sample_max": args.error_sample_max,
        },
    )

    fail = sum(1 for r in results if r.status == "FAIL")
    warn = sum(1 for r in results if r.status == "WARN")
    passed = sum(1 for r in results if r.status == "PASS")
    log("INFO", "Done. PASS=%d WARN=%d FAIL=%d" % (passed, warn, fail))
    log("INFO", "Markdown report: %s" % md_path)
    log("INFO", "TSV report: %s" % tsv_path)

    missing_names = sorted(
        {
            "%s/%s@%s" % (r.service, r.component, r.host_name)
            for r in results
            if r.missing_log or r.logs_generating is False
        }
    )
    if missing_names:
        log("WARN", "Components missing or not generating logs:")
        for name in missing_names:
            log("WARN", "  - %s" % name)

    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
