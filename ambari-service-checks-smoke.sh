#!/usr/bin/env bash
#
# Smoke: Ambari Service Checks - trigger, monitor, and summarize.
#
# Mirrors the Ambari UI / ambari-llm-agent "Run Service Check" flow:
#   1) Discover cluster + stack from Ambari
#   2) Select services that advertise service_check_supported
#   3) POST each service check request (ZOOKEEPER uses ZOOKEEPER_QUORUM_SERVICE_CHECK)
#   4) Poll the Ambari request until COMPLETED / FAILED / ABORTED / TIMEDOUT
#   5) Print a PASS / FAIL / SKIPPED summary (and optional TSV report)
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   SC_SERVICES              comma/space list to include (e.g. HDFS,YARN,ZOOKEEPER)
#   SC_SKIP_SERVICES         comma/space list to exclude
#   SC_INCLUDE_NOT_STARTED   0 (default) only STARTED services; 1 also INSTALLED/etc.
#   SC_PARALLEL              how many checks to run at once (default 1 = sequential)
#   SC_POLL_SECONDS          poll interval (default 5)
#   SC_TIMEOUT_SECONDS       per-request timeout (default 900)
#   SC_STAGGER_SECONDS       pause between submitting checks (default 2)
#   SC_FAIL_FAST             0 (default) finish all; 1 stop submitting after a FAIL
#   SC_REPORT_FILE           optional TSV report path
#   CURL_EXTRA_OPTS          extra curl flags (e.g. -k)
#
# Usage:
#   ./ambari-service-checks-smoke.sh
#   SC_SERVICES=HDFS,YARN,ZOOKEEPER ./ambari-service-checks-smoke.sh
#   SC_REPORT_FILE=reports/service-checks.tsv ./ambari-service-checks-smoke.sh
#
# Ambari URL/user/password are read from configs/ambari.env or configs/ambari.config
# (or AMBARI_CONFIG_FILE). Shell exports still override the file.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

die() {
  echo "[ERROR] $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

strip_quotes() {
  local v="$1"
  if [[ ${#v} -ge 2 && ${v:0:1} == '"' && ${v: -1} == '"' ]]; then
    printf '%s' "${v:1:${#v}-2}"
  elif [[ ${#v} -ge 2 && ${v:0:1} == "'" && ${v: -1} == "'" ]]; then
    printf '%s' "${v:1:${#v}-2}"
  else
    printf '%s' "$v"
  fi
}

load_env_file() {
  local f="$1" key val line
  [[ -f "$f" ]] || return 0
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ -z "${line//[[:space:]]/}" ]] && continue
    line="${line#export[[:space:]]}"
    [[ "$line" != *=* ]] && continue
    key="${line%%=*}"
    val="${line#*=}"
    key="${key%"${key##*[![:space:]]}"}"
    key="${key#"${key%%[![:space:]]*}"}"
    val="${val%"${val##*[![:space:]]}"}"
    val="${val#"${val%%[![:space:]]*}"}"
    val="$(strip_quotes "$val")"
    case "$key" in
      AMBARI_BASE_URL) _cfg_AMBARI_BASE_URL="$val" ;;
      AMBARI_USER) _cfg_AMBARI_USER="$val" ;;
      AMBARI_PASSWORD) _cfg_AMBARI_PASSWORD="$val" ;;
      CLUSTER_NAME) _cfg_CLUSTER_NAME="$val" ;;
    esac
  done <"$f"
}

resolve_ambari_config_file() {
  local f
  if [[ -n "${AMBARI_CONFIG_FILE:-}" ]]; then
    [[ -f "$AMBARI_CONFIG_FILE" ]] || die "AMBARI_CONFIG_FILE not found: $AMBARI_CONFIG_FILE"
    printf '%s' "$AMBARI_CONFIG_FILE"
    return 0
  fi
  for f in \
    "${SCRIPT_DIR}/configs/ambari.env" \
    "${SCRIPT_DIR}/configs/ambari.config"; do
    if [[ -f "$f" ]]; then
      printf '%s' "$f"
      return 0
    fi
  done
  return 1
}

need_cmd curl
need_cmd python3

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""
_cfg_CLUSTER_NAME=""

AMBARI_CONFIG_FILE="$(resolve_ambari_config_file)" || \
  die "Missing Ambari config. Create configs/ambari.env (or configs/ambari.config) from configs/ambari.env.example."

load_env_file "$AMBARI_CONFIG_FILE"

AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"
CLUSTER_NAME="${CLUSTER_NAME:-${_cfg_CLUSTER_NAME:-}}"

[[ -n "$AMBARI_BASE_URL" ]] || die "AMBARI_BASE_URL is empty in ${AMBARI_CONFIG_FILE}"
[[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]] || \
  die "AMBARI_USER / AMBARI_PASSWORD missing in ${AMBARI_CONFIG_FILE}"

SC_SERVICES="${SC_SERVICES:-}"
SC_SKIP_SERVICES="${SC_SKIP_SERVICES:-}"
SC_INCLUDE_NOT_STARTED="${SC_INCLUDE_NOT_STARTED:-0}"
SC_PARALLEL="${SC_PARALLEL:-1}"
SC_POLL_SECONDS="${SC_POLL_SECONDS:-5}"
SC_TIMEOUT_SECONDS="${SC_TIMEOUT_SECONDS:-900}"
SC_STAGGER_SECONDS="${SC_STAGGER_SECONDS:-2}"
SC_FAIL_FAST="${SC_FAIL_FAST:-0}"
SC_REPORT_FILE="${SC_REPORT_FILE:-}"
CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}"

case "$SC_INCLUDE_NOT_STARTED" in 0|1) ;; *) die "SC_INCLUDE_NOT_STARTED must be 0 or 1" ;; esac
case "$SC_FAIL_FAST" in 0|1) ;; *) die "SC_FAIL_FAST must be 0 or 1" ;; esac
[[ "$SC_PARALLEL" =~ ^[1-9][0-9]*$ ]] || die "SC_PARALLEL must be a positive integer"
[[ "$SC_POLL_SECONDS" =~ ^[1-9][0-9]*$ ]] || die "SC_POLL_SECONDS must be a positive integer"
[[ "$SC_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] || die "SC_TIMEOUT_SECONDS must be a positive integer"

if [[ -n "$SC_REPORT_FILE" ]]; then
  mkdir -p "$(dirname "$SC_REPORT_FILE")"
fi

echo "[INFO] Ambari Service Checks smoke"
echo "[INFO] Config: ${AMBARI_CONFIG_FILE}"
echo "[INFO] Ambari: ${AMBARI_BASE_URL} (user=${AMBARI_USER})"
echo "[INFO] parallel=${SC_PARALLEL} poll=${SC_POLL_SECONDS}s timeout=${SC_TIMEOUT_SECONDS}s"

export AMBARI_BASE_URL AMBARI_USER AMBARI_PASSWORD CLUSTER_NAME
export SC_SERVICES SC_SKIP_SERVICES SC_INCLUDE_NOT_STARTED
export SC_PARALLEL SC_POLL_SECONDS SC_TIMEOUT_SECONDS SC_STAGGER_SECONDS SC_FAIL_FAST
export SC_REPORT_FILE CURL_EXTRA_OPTS

python3 - <<'PY'
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

base = os.environ["AMBARI_BASE_URL"].rstrip("/")
user = os.environ["AMBARI_USER"]
password = os.environ["AMBARI_PASSWORD"]
cluster = os.environ.get("CLUSTER_NAME") or ""
services_filter = {
    s.strip().upper()
    for s in re.split(r"[, \t]+", os.environ.get("SC_SERVICES") or "")
    if s.strip()
}
skip_filter = {
    s.strip().upper()
    for s in re.split(r"[, \t]+", os.environ.get("SC_SKIP_SERVICES") or "")
    if s.strip()
}
include_not_started = os.environ.get("SC_INCLUDE_NOT_STARTED") == "1"
parallel = int(os.environ.get("SC_PARALLEL") or "1")
poll_seconds = int(os.environ.get("SC_POLL_SECONDS") or "5")
timeout_seconds = int(os.environ.get("SC_TIMEOUT_SECONDS") or "900")
stagger_seconds = float(os.environ.get("SC_STAGGER_SECONDS") or "2")
fail_fast = os.environ.get("SC_FAIL_FAST") == "1"
report_file = os.environ.get("SC_REPORT_FILE") or ""
curl_extra = os.environ.get("CURL_EXTRA_OPTS") or ""

# Known Ambari UI special-cases (see ambari-web service item controller).
COMMAND_OVERRIDES = {
    "ZOOKEEPER": "ZOOKEEPER_QUORUM_SERVICE_CHECK",
}


def ambari_request(method, path, body=None, timeout=60):
    url = path if path.startswith("http") else base + path
    data = None
    headers = {
        "Authorization": "Basic "
        + __import__("base64")
        .b64encode(("%s:%s" % (user, password)).encode())
        .decode(),
        "X-Requested-By": "ambari",
    }
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    # CURL_EXTRA_OPTS may include -k; mirror that for HTTPS Ambari.
    ctx = None
    if base.startswith("https") and ("-k" in curl_extra.split() or "--insecure" in curl_extra.split()):
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


def check_command_for(service_name):
    return COMMAND_OVERRIDES.get(service_name, "%s_SERVICE_CHECK" % service_name)


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# Discover cluster
if not cluster:
    code, data = ambari_request("GET", "/api/v1/clusters/")
    if code != 200:
        print("[ERROR] failed to list clusters: %s" % data, file=sys.stderr)
        sys.exit(2)
    items = data.get("items") or []
    if not items:
        print("[ERROR] no clusters found in Ambari", file=sys.stderr)
        sys.exit(2)
    cluster = items[0]["Clusters"]["cluster_name"]

code, cluster_info = ambari_request(
    "GET", "/api/v1/clusters/%s?fields=Clusters/version" % cluster
)
if code != 200:
    print("[ERROR] failed to read cluster: %s" % cluster_info, file=sys.stderr)
    sys.exit(2)
version = cluster_info["Clusters"]["version"]
if "-" not in version:
    print("[ERROR] unexpected Clusters/version: %s" % version, file=sys.stderr)
    sys.exit(2)
stack_name, stack_version = version.split("-", 1)
print("[INFO] Cluster: %s  Stack: %s-%s" % (cluster, stack_name, stack_version))

# Stack: which services support service checks
code, stack_svcs = ambari_request(
    "GET",
    "/api/v1/stacks/%s/versions/%s/services?fields=StackServices/service_name,"
    "StackServices/service_check_supported" % (stack_name, stack_version),
)
supported = set()
if code == 200:
    for item in stack_svcs.get("items") or []:
        ss = item.get("StackServices") or {}
        if ss.get("service_check_supported"):
            supported.add(ss.get("service_name"))
else:
    print(
        "[WARN] could not read service_check_supported from stack; will try all services",
        file=sys.stderr,
    )

# Cluster services + state
code, svc_data = ambari_request(
    "GET",
    "/api/v1/clusters/%s/services?fields=ServiceInfo/service_name,ServiceInfo/state"
    % cluster,
)
if code != 200:
    print("[ERROR] failed to list services: %s" % svc_data, file=sys.stderr)
    sys.exit(2)

candidates = []
skipped = []
for item in svc_data.get("items") or []:
    si = item["ServiceInfo"]
    name = si["service_name"]
    state = si.get("state") or ""
    if services_filter and name.upper() not in services_filter:
        continue
    if name.upper() in skip_filter:
        skipped.append({"service": name, "state": state, "reason": "excluded by SC_SKIP_SERVICES"})
        continue
    if supported and name not in supported:
        skipped.append({"service": name, "state": state, "reason": "service check not supported"})
        continue
    if state != "STARTED" and not include_not_started:
        skipped.append({"service": name, "state": state, "reason": "service not STARTED"})
        continue
    candidates.append({"service": name, "state": state, "command": check_command_for(name)})

candidates.sort(key=lambda x: x["service"])

print("[INFO] Service checks to run: %d" % len(candidates))
if skipped:
    print("[INFO] Skipping %d service(s) up front" % len(skipped))
print()


def fetch_failed_task_detail(request_id):
    code, data = ambari_request(
        "GET",
        "/api/v1/clusters/%s/requests/%s/tasks?fields=Tasks/status,Tasks/command_detail,"
        "Tasks/host_name,Tasks/stderr,Tasks/stdout,Tasks/exit_code,Tasks/error_log"
        % (cluster, request_id),
    )
    if code != 200:
        return ""
    details = []
    for item in data.get("items") or []:
        t = item.get("Tasks") or {}
        status = t.get("status")
        if status in ("FAILED", "TIMEDOUT", "ABORTED"):
            host = t.get("host_name") or "?"
            detail = t.get("command_detail") or ""
            exit_code = t.get("exit_code")
            stderr = (t.get("stderr") or "").strip()
            if len(stderr) > 800:
                stderr = "... " + stderr[-800:]
            line = "%s on %s exit=%s" % (detail, host, exit_code)
            if stderr:
                line += " | stderr: " + stderr.replace("\n", " | ")
            details.append(line)
    return " ;; ".join(details)


def trigger_and_wait(svc):
    name = svc["service"]
    command = svc["command"]
    started = time.time()
    body = {
        "RequestInfo": {
            "context": "%s Service Check" % name,
            "command": command,
        },
        "Requests/resource_filters": [{"service_name": name}],
    }
    code, data = ambari_request(
        "POST", "/api/v1/clusters/%s/requests" % cluster, body=body
    )
    if code not in (200, 201, 202):
        msg = data.get("message") or data.get("status") or str(data)
        return {
            "service": name,
            "state": svc["state"],
            "command": command,
            "request_id": "",
            "status": "SUBMIT_FAILED",
            "result": "FAIL",
            "progress": 0,
            "elapsed_sec": round(time.time() - started, 1),
            "detail": msg,
            "timestamp": utc_now(),
        }

    request_id = None
    if isinstance(data.get("Requests"), dict):
        request_id = data["Requests"].get("id")
    if request_id is None and data.get("href"):
        request_id = str(data["href"]).rstrip("/").split("/")[-1]
    if request_id is None:
        request_id = data.get("id")

    if request_id is None:
        return {
            "service": name,
            "state": svc["state"],
            "command": command,
            "request_id": "",
            "status": "SUBMIT_FAILED",
            "result": "FAIL",
            "progress": 0,
            "elapsed_sec": round(time.time() - started, 1),
            "detail": "Ambari accepted request but returned no request id: %s" % data,
            "timestamp": utc_now(),
        }

    print(
        "[INFO] %s: submitted request %s (%s)" % (name, request_id, command),
        flush=True,
    )

    terminal = {"COMPLETED", "FAILED", "ABORTED", "TIMEDOUT"}
    last_status = "PENDING"
    progress = 0.0
    while True:
        elapsed = time.time() - started
        if elapsed > timeout_seconds:
            return {
                "service": name,
                "state": svc["state"],
                "command": command,
                "request_id": str(request_id),
                "status": "TIMEOUT",
                "result": "FAIL",
                "progress": progress,
                "elapsed_sec": round(elapsed, 1),
                "detail": "exceeded SC_TIMEOUT_SECONDS=%s (last=%s)"
                % (timeout_seconds, last_status),
                "timestamp": utc_now(),
            }

        code, req = ambari_request(
            "GET",
            "/api/v1/clusters/%s/requests/%s?fields=Requests/request_status,"
            "Requests/progress_percent,Requests/failed_task_count,"
            "Requests/aborted_task_count,Requests/timed_out_task_count"
            % (cluster, request_id),
        )
        if code != 200:
            time.sleep(poll_seconds)
            continue

        r = req.get("Requests") or {}
        last_status = r.get("request_status") or last_status
        progress = r.get("progress_percent") or progress
        if last_status in terminal:
            detail = ""
            result = "PASS" if last_status == "COMPLETED" else "FAIL"
            if result == "FAIL":
                detail = fetch_failed_task_detail(request_id)
            return {
                "service": name,
                "state": svc["state"],
                "command": command,
                "request_id": str(request_id),
                "status": last_status,
                "result": result,
                "progress": progress,
                "elapsed_sec": round(time.time() - started, 1),
                "detail": detail,
                "timestamp": utc_now(),
            }

        time.sleep(poll_seconds)


results = []
pass_n = fail_n = skip_n = 0

for s in skipped:
    skip_n += 1
    print("  [SKIPPED] %s (%s): %s" % (s["service"], s["state"], s["reason"]))

if not candidates:
    print()
    print("[WARN] No service checks to run")
else:
    print()
    print("[INFO] Running service checks...")
    # Submit with limited parallelism; stagger submissions slightly.
    with ThreadPoolExecutor(max_workers=parallel) as pool:
        futures = {}
        stop_submit = False
        for idx, svc in enumerate(candidates):
            if stop_submit:
                skip_n += 1
                print(
                    "  [SKIPPED] %s (%s): skipped after prior failure (SC_FAIL_FAST=1)"
                    % (svc["service"], svc["state"])
                )
                continue
            fut = pool.submit(trigger_and_wait, svc)
            futures[fut] = svc["service"]
            if idx < len(candidates) - 1 and stagger_seconds > 0 and parallel == 1:
                # Sequential mode: wait for this one before next (via as_completed below)
                # Actually for sequential we still use the pool with 1 worker; stagger
                # between submits only matters for parallel > 1.
                pass
            elif parallel > 1 and stagger_seconds > 0 and idx < len(candidates) - 1:
                time.sleep(stagger_seconds)

            # In sequential mode, wait immediately so progress is ordered.
            if parallel == 1:
                try:
                    row = fut.result()
                except Exception as e:
                    row = {
                        "service": svc["service"],
                        "state": svc["state"],
                        "command": svc["command"],
                        "request_id": "",
                        "status": "ERROR",
                        "result": "FAIL",
                        "progress": 0,
                        "elapsed_sec": 0,
                        "detail": str(e),
                        "timestamp": utc_now(),
                    }
                results.append(row)
                if row["result"] == "PASS":
                    pass_n += 1
                    print(
                        "  [PASS] %s request=%s status=%s (%.1fs)"
                        % (
                            row["service"],
                            row["request_id"],
                            row["status"],
                            row["elapsed_sec"],
                        )
                    )
                else:
                    fail_n += 1
                    print(
                        "  [FAIL] %s request=%s status=%s (%.1fs) %s"
                        % (
                            row["service"],
                            row.get("request_id") or "-",
                            row["status"],
                            row["elapsed_sec"],
                            row.get("detail") or "",
                        ),
                        file=sys.stderr,
                    )
                    if fail_fast:
                        stop_submit = True
                futures.pop(fut, None)
                if (
                    not stop_submit
                    and stagger_seconds > 0
                    and idx < len(candidates) - 1
                ):
                    time.sleep(stagger_seconds)

        # Parallel mode: collect remaining futures
        if parallel > 1:
            for fut in as_completed(futures):
                svc_name = futures[fut]
                try:
                    row = fut.result()
                except Exception as e:
                    row = {
                        "service": svc_name,
                        "state": "",
                        "command": "",
                        "request_id": "",
                        "status": "ERROR",
                        "result": "FAIL",
                        "progress": 0,
                        "elapsed_sec": 0,
                        "detail": str(e),
                        "timestamp": utc_now(),
                    }
                results.append(row)
                if row["result"] == "PASS":
                    pass_n += 1
                    print(
                        "  [PASS] %s request=%s status=%s (%.1fs)"
                        % (
                            row["service"],
                            row["request_id"],
                            row["status"],
                            row["elapsed_sec"],
                        )
                    )
                else:
                    fail_n += 1
                    print(
                        "  [FAIL] %s request=%s status=%s (%.1fs) %s"
                        % (
                            row["service"],
                            row.get("request_id") or "-",
                            row["status"],
                            row["elapsed_sec"],
                            row.get("detail") or "",
                        ),
                        file=sys.stderr,
                    )

results.sort(key=lambda r: r.get("service") or "")

if report_file:
    with open(report_file, "w") as fh:
        fh.write(
            "service\tstate\tcommand\trequest_id\tstatus\tresult\telapsed_sec\tdetail\n"
        )
        for s in skipped:
            fh.write(
                "%s\t%s\t\t\t\tSKIPPED\t\t%s\n"
                % (s["service"], s["state"], s["reason"])
            )
        for row in results:
            detail = (row.get("detail") or "").replace("\t", " ").replace("\n", " | ")
            fh.write(
                "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n"
                % (
                    row.get("service") or "",
                    row.get("state") or "",
                    row.get("command") or "",
                    row.get("request_id") or "",
                    row.get("status") or "",
                    row.get("result") or "",
                    row.get("elapsed_sec") or "",
                    detail,
                )
            )

print()
print("========== SERVICE CHECK SUMMARY ==========")
print("Cluster : %s (%s-%s)" % (cluster, stack_name, stack_version))
print("PASS    : %d" % pass_n)
print("FAIL    : %d" % fail_n)
print("SKIPPED : %d" % skip_n)
if report_file:
    print("Report  : %s" % report_file)
if fail_n:
    print("-------- FAILED --------")
    for row in results:
        if row.get("result") != "PASS":
            print(
                "  - %s: %s%s"
                % (
                    row.get("service"),
                    row.get("status"),
                    (" | " + row["detail"]) if row.get("detail") else "",
                )
            )
print("===========================================")

if fail_n > 0:
    print("[ERROR] %d service check(s) failed" % fail_n, file=sys.stderr)
    sys.exit(1)

print("[INFO] All submitted service checks PASSED")
sys.exit(0)
PY
