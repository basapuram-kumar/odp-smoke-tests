#!/usr/bin/env bash
#
# Smoke: Ambari service Quick Links / web UIs - resolve each Ambari Quick Link
# the same way the Ambari UI does, then HTTP-probe whether it is reachable.
#
# For every STARTED service that publishes Ambari quicklinks (NameNode UI,
# ResourceManager UI, Ranger Admin UI, Hue, Zeppelin, ...), this script:
#   1) Discovers the cluster + stack from Ambari
#   2) Loads quicklink definitions from the stack API
#   3) Resolves host/port/protocol from component placement + current configs
#   4) Optionally rewrites hostnames to Ambari-reported IPs (for clients
#      without cluster DNS)
#   5) curl-probes each URL and reports UP / UP_AUTH / DOWN
#
# On Kerberos clusters many UIs answer HTTP 401/403 without a ticket. Those
# still prove the port is listening, so they count as PASS by default
# (UI_ACCEPT_AUTH=1).
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   UI_SERVICES          comma/space list to include (e.g. HDFS,YARN,RANGER)
#   UI_SKIP_SERVICES     comma/space list to exclude
#   UI_LINK_MODE         ui (default) | all
#                        ui keeps main web consoles; all includes logs/jmx/stacks
#   UI_USE_IP            1 (default) rewrite hostnames to Ambari host IPs
#   UI_ACCEPT_AUTH       1 (default) treat HTTP 401/403 as reachable (PASS)
#   UI_CONNECT_TIMEOUT   curl --connect-timeout seconds (default 5)
#   UI_MAX_TIME          curl --max-time seconds (default 15)
#   UI_PROBE_AMBARI      1 (default) also probe Ambari itself
#   UI_INCLUDE_NOT_STARTED
#                        0 (default) only STARTED services; 1 also probes others
#   CURL_EXTRA_OPTS      default -k (HTTPS self-signed certs)
#   UI_REPORT_FILE       optional path for a machine-readable TSV report
#
# Usage:
#   ./ambari-quicklinks-ui-smoke.sh
#   UI_SERVICES=HDFS,YARN,RANGER ./ambari-quicklinks-ui-smoke.sh
#   UI_LINK_MODE=all ./ambari-quicklinks-ui-smoke.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"

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

PASS_N=0
FAIL_N=0
SKIP_N=0
declare -a PASS_ITEMS=()
declare -a FAIL_ITEMS=()
declare -a SKIP_ITEMS=()

record_pass() {
  PASS_N=$((PASS_N + 1))
  PASS_ITEMS+=("$1")
  echo "  [PASS] $1"
}

record_fail() {
  FAIL_N=$((FAIL_N + 1))
  FAIL_ITEMS+=("$1")
  echo "  [FAIL] $1" >&2
}

record_skip() {
  SKIP_N=$((SKIP_N + 1))
  SKIP_ITEMS+=("$1")
  echo "  [SKIPPED] $1"
}

http_probe() {
  local url="$1" code
  code="$(curl -sS -o /dev/null \
    --connect-timeout "$UI_CONNECT_TIMEOUT" \
    --max-time "$UI_MAX_TIME" \
    -w '%{http_code}' \
    $CURL_EXTRA_OPTS \
    "$url" 2>/dev/null || true)"
  [[ -n "$code" ]] || code="000"
  printf '%s' "$code"
}

classify_http() {
  local code="$1"
  case "$code" in
    2??|3??) printf 'UP' ;;
    401|403)
      if [[ "$UI_ACCEPT_AUTH" == "1" ]]; then
        printf 'UP_AUTH'
      else
        printf 'DOWN'
      fi
      ;;
    *) printf 'DOWN' ;;
  esac
}

need_cmd curl
need_cmd python3

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""
_cfg_CLUSTER_NAME=""

if [[ -f "$AMBARI_CONFIG_FILE" ]]; then
  load_env_file "$AMBARI_CONFIG_FILE"
fi

AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://10.101.11.138:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"
CLUSTER_NAME="${CLUSTER_NAME:-${_cfg_CLUSTER_NAME:-}}"

UI_SERVICES="${UI_SERVICES:-}"
UI_SKIP_SERVICES="${UI_SKIP_SERVICES:-}"
UI_LINK_MODE="${UI_LINK_MODE:-ui}"
UI_USE_IP="${UI_USE_IP:-1}"
UI_ACCEPT_AUTH="${UI_ACCEPT_AUTH:-1}"
UI_CONNECT_TIMEOUT="${UI_CONNECT_TIMEOUT:-5}"
UI_MAX_TIME="${UI_MAX_TIME:-15}"
UI_PROBE_AMBARI="${UI_PROBE_AMBARI:-1}"
UI_INCLUDE_NOT_STARTED="${UI_INCLUDE_NOT_STARTED:-0}"
CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:--k}"
UI_REPORT_FILE="${UI_REPORT_FILE:-}"

case "$UI_LINK_MODE" in
  ui|all) ;;
  *) die "UI_LINK_MODE must be ui or all" ;;
esac
case "$UI_USE_IP" in 0|1) ;; *) die "UI_USE_IP must be 0 or 1" ;; esac
case "$UI_ACCEPT_AUTH" in 0|1) ;; *) die "UI_ACCEPT_AUTH must be 0 or 1" ;; esac
case "$UI_PROBE_AMBARI" in 0|1) ;; *) die "UI_PROBE_AMBARI must be 0 or 1" ;; esac
case "$UI_INCLUDE_NOT_STARTED" in 0|1) ;; *) die "UI_INCLUDE_NOT_STARTED must be 0 or 1" ;; esac

[[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]] || \
  die "Missing Ambari credentials. Create ${AMBARI_CONFIG_FILE} (copy from ${SCRIPT_DIR}/configs/ambari.env.example) or set AMBARI_USER and AMBARI_PASSWORD."

echo "[INFO] Ambari Quick Links UI smoke"
echo "[INFO] Ambari: ${AMBARI_BASE_URL}"
echo "[INFO] link mode: ${UI_LINK_MODE}  use_ip: ${UI_USE_IP}  accept_auth: ${UI_ACCEPT_AUTH}"

RESOLVED_JSON="$(
  AMBARI_BASE_URL="$AMBARI_BASE_URL" \
  AMBARI_USER="$AMBARI_USER" \
  AMBARI_PASSWORD="$AMBARI_PASSWORD" \
  CLUSTER_NAME="$CLUSTER_NAME" \
  UI_SERVICES="$UI_SERVICES" \
  UI_SKIP_SERVICES="$UI_SKIP_SERVICES" \
  UI_LINK_MODE="$UI_LINK_MODE" \
  UI_USE_IP="$UI_USE_IP" \
  UI_INCLUDE_NOT_STARTED="$UI_INCLUDE_NOT_STARTED" \
  python3 - <<'PY'
import json
import os
import re
import sys
import urllib.error
import urllib.request
from urllib.parse import urlsplit, urlunsplit

base = os.environ["AMBARI_BASE_URL"].rstrip("/")
user = os.environ["AMBARI_USER"]
password = os.environ["AMBARI_PASSWORD"]
cluster = os.environ.get("CLUSTER_NAME") or ""
services_filter = {s.strip().upper() for s in re.split(r"[, \t]+", os.environ.get("UI_SERVICES") or "") if s.strip()}
skip_filter = {s.strip().upper() for s in re.split(r"[, \t]+", os.environ.get("UI_SKIP_SERVICES") or "") if s.strip()}
link_mode = os.environ.get("UI_LINK_MODE") or "ui"
use_ip = os.environ.get("UI_USE_IP") == "1"
include_not_started = os.environ.get("UI_INCLUDE_NOT_STARTED") == "1"

def ambari_get(path):
    url = path if path.startswith("http") else base + path
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": "Basic "
            + __import__("base64").b64encode(("%s:%s" % (user, password)).encode()).decode(),
            "X-Requested-By": "ambari",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.load(resp)

def reverse_type(t):
    t = (t or "").lower()
    if t == "https":
        return "http"
    if t == "http":
        return "https"
    return ""

def meet_desired(configs, site, prop, desired):
    props = configs.get(site) or {}
    val = props.get(prop)
    if desired == "NOT_EXIST":
        return val is None
    if desired == "EXIST":
        return val is not None
    return val == desired

def set_protocol(configs, protocol_cfg):
    hdfs = configs.get("hdfs-site") or {}
    hadoop_ssl = hdfs.get("dfs.http.policy") == "HTTPS_ONLY"
    if not protocol_cfg:
        return "https" if hadoop_ssl else "http"
    ptype = protocol_cfg.get("type") or ""
    if ptype == "HTTPS_ONLY":
        return "https"
    if ptype == "HTTP_ONLY":
        return "http"
    ptype_l = ptype.lower()
    checks = protocol_cfg.get("checks") or []
    if not checks:
        return "https" if hadoop_ssl else "http"
    failed = 0
    for check in checks:
        if not meet_desired(configs, check.get("site"), check.get("property"), check.get("desired")):
            failed += 1
    return reverse_type(ptype_l) if failed else ptype_l

def set_port(port_cfg, protocol, configs):
    if not port_cfg:
        return ""
    default_port = port_cfg.get(protocol + "_default_port") or ""
    prop_name = port_cfg.get(protocol + "_property")
    site = port_cfg.get("site")
    props = configs.get(site) or {}
    property_value = props.get(prop_name) if prop_name else None
    if not property_value:
        return str(default_port)
    regex_value = port_cfg.get("regex") or ""
    if protocol == "https" and port_cfg.get("https_regex"):
        regex_value = port_cfg.get("https_regex")
    regex_value = (regex_value or "").strip()
    if regex_value:
        m = re.search(regex_value, str(property_value))
        if m and m.lastindex:
            return m.group(1)
        return str(default_port)
    return str(property_value)

def parse_host_from_uri(uri):
    if not uri:
        return None
    m = re.search(r"://([^/:]+)", str(uri))
    if m:
        return m.group(1)
    return str(uri)

def keep_link(link):
    if link.get("remove") or link.get("removed"):
        return False
    if link.get("visible") is False:
        return False
    if link_mode == "all":
        return True
    name = (link.get("name") or "").lower()
    label = (link.get("label") or "").lower()
    skip_tokens = ("_logs", "_jmx", "thread_stacks", "debug_dump", "zookeeper_info", "jdbc_jar")
    if any(t in name for t in skip_tokens) or label in ("thread stacks",):
        return False
    if "_ui" in name or name.endswith("ui"):
        return True
    if "console" in name or "console" in label:
        return True
    if "ui" in label or "web" in label:
        return True
    if name in ("admin_server",):
        return True
    return False

def resolve_placeholders(url, configs):
    def repl(m):
        site, prop = m.group(1), m.group(2)
        props = configs.get(site) or {}
        return props.get(prop) if props.get(prop) is not None else m.group(0)
    return re.sub(r"\$\{([^/]+)/([^}]+)\}", repl, url)

def fmt_ambari_url(template, values):
    """Expand Ambari %@ placeholders without using Python % formatting."""
    parts = (template or "").split("%@")
    if len(parts) == 1:
        return template or ""
    out = [parts[0]]
    for i in range(1, len(parts)):
        idx = i - 1
        out.append(values[idx] if idx < len(values) else "")
        out.append(parts[i])
    return "".join(out)

def rewrite_host_to_ip(url, host_ip):
    parts = urlsplit(url)
    hostname = parts.hostname
    if not hostname:
        return url
    ip = host_ip.get(hostname) or host_ip.get(hostname.split(".")[0])
    if not ip:
        return url
    netloc = ip
    if parts.port:
        netloc = "%s:%s" % (ip, parts.port)
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))

# Discover cluster
if not cluster:
    data = ambari_get("/api/v1/clusters/")
    items = data.get("items") or []
    if not items:
        print("ERROR: no clusters found in Ambari", file=sys.stderr)
        sys.exit(2)
    cluster = items[0]["Clusters"]["cluster_name"]

cluster_info = ambari_get("/api/v1/clusters/%s?fields=Clusters/version" % cluster)
version = cluster_info["Clusters"]["version"]  # e.g. ODP-3.2
if "-" not in version:
    print("ERROR: unexpected Clusters/version: %s" % version, file=sys.stderr)
    sys.exit(2)
stack_name, stack_version = version.split("-", 1)

# Host -> IP map
hosts_data = ambari_get(
    "/api/v1/clusters/%s/hosts?fields=Hosts/host_name,Hosts/ip,Hosts/public_host_name" % cluster
)
host_ip = {}
public_name = {}
for item in hosts_data.get("items") or []:
    h = item["Hosts"]
    hn = h.get("host_name")
    ip = h.get("ip")
    ph = h.get("public_host_name") or hn
    if hn and ip:
        host_ip[hn] = ip
        host_ip[ph] = ip
    if hn:
        public_name[hn] = ph

# Services
svc_data = ambari_get(
    "/api/v1/clusters/%s/services?fields=ServiceInfo/service_name,ServiceInfo/state" % cluster
)
services = []
for item in svc_data.get("items") or []:
    si = item["ServiceInfo"]
    name = si["service_name"]
    state = si.get("state")
    if services_filter and name.upper() not in services_filter:
        continue
    if name.upper() in skip_filter:
        continue
    services.append({"name": name, "state": state})

# Component hosts: /hosts?fields=host_components/...
hc_data = ambari_get(
    "/api/v1/clusters/%s/hosts?fields=Hosts/host_name,Hosts/public_host_name,"
    "host_components/HostRoles/component_name,host_components/HostRoles/state" % cluster
)
component_hosts = {}  # component -> [(host_name, public, state)]
for item in hc_data.get("items") or []:
    hn = item["Hosts"]["host_name"]
    ph = item["Hosts"].get("public_host_name") or hn
    for hc in item.get("host_components") or []:
        roles = hc.get("HostRoles") or {}
        cname = roles.get("component_name")
        cstate = roles.get("state")
        if not cname:
            continue
        component_hosts.setdefault(cname, []).append((hn, ph, cstate))

# Current configs: collect site types we may need while resolving.
# Fetch per-service current configs lazily.
config_cache = {}

def load_service_configs(service_name):
    if service_name in config_cache:
        return config_cache[service_name]
    url = (
        "/api/v1/clusters/%s/configurations/service_config_versions"
        "?service_name=%s&is_current=true" % (cluster, service_name)
    )
    try:
        data = ambari_get(url)
    except Exception:
        config_cache[service_name] = {}
        return config_cache[service_name]
    merged = {}
    for item in data.get("items") or []:
        for cfg in item.get("configurations") or []:
            merged[cfg.get("type")] = cfg.get("properties") or {}
    # Also pull admin-properties for Ranger when present under RANGER
    config_cache[service_name] = merged
    return merged

# Cross-service sites sometimes needed (hdfs-site for ssl defaults, admin-properties)
shared = {}
for svc in ("HDFS", "RANGER"):
    shared.update(load_service_configs(svc))

def get_quicklinks(service_name):
    # Prefer exact stack version; fall back to nearby versions if needed.
    candidates = [stack_version]
    try:
        major_minor = float(".".join(stack_version.split(".")[:2]))
        for delta in (0.1, -0.1, 0.2, -0.2):
            alt = "%.1f" % (major_minor + delta)
            if alt not in candidates:
                candidates.append(alt)
    except Exception:
        pass
    for ver in candidates:
        path = (
            "/api/v1/stacks/%s/versions/%s/services/%s/quicklinks?fields=*"
            % (stack_name, ver, service_name)
        )
        try:
            data = ambari_get(path)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                continue
            raise
        items = data.get("items") or []
        if items:
            return items, ver
    return [], None

results = {
    "cluster": cluster,
    "stack": "%s-%s" % (stack_name, stack_version),
    "links": [],
    "skipped_services": [],
}

for svc in services:
    sname = svc["name"]
    sstate = svc["state"]
    if sstate != "STARTED" and not include_not_started:
        results["skipped_services"].append(
            {"service": sname, "state": sstate, "reason": "service not STARTED"}
        )
        continue

    ql_items, used_ver = get_quicklinks(sname)
    if not ql_items:
        results["skipped_services"].append(
            {"service": sname, "state": sstate, "reason": "no Ambari quicklinks"}
        )
        continue

    configs = dict(shared)
    configs.update(load_service_configs(sname))

    for qitem in ql_items:
        qinfo = qitem.get("QuickLinkInfo") or {}
        qdata = qinfo.get("quicklink_data") or {}
        qcfg_root = qdata.get("QuickLinksConfiguration") or {}
        configuration = qcfg_root.get("configuration") or {}
        protocol_cfg = configuration.get("protocol")
        for link in configuration.get("links") or []:
            if not keep_link(link):
                continue
            component = link.get("component_name")
            hosts = component_hosts.get(component) or []
            if not hosts:
                results["links"].append(
                    {
                        "service": sname,
                        "service_state": sstate,
                        "name": link.get("name"),
                        "label": link.get("label"),
                        "component": component,
                        "host": "",
                        "url": "",
                        "status": "SKIP",
                        "reason": "component not installed",
                    }
                )
                continue

            protocol = set_protocol(configs, link.get("protocol") or protocol_cfg)

            # Ranger external URL shortcut
            if sname == "RANGER":
                admin_props = configs.get("admin-properties") or {}
                ext = admin_props.get("policymgr_external_url")
                if ext:
                    url = ext
                    if use_ip:
                        url = rewrite_host_to_ip(url, host_ip)
                    results["links"].append(
                        {
                            "service": sname,
                            "service_state": sstate,
                            "name": link.get("name"),
                            "label": link.get("label"),
                            "component": component,
                            "host": parse_host_from_uri(ext) or "",
                            "url": url,
                            "status": "READY",
                            "reason": "",
                        }
                    )
                    continue

            for hn, ph, cstate in hosts:
                if cstate != "STARTED" and not include_not_started:
                    results["links"].append(
                        {
                            "service": sname,
                            "service_state": sstate,
                            "name": link.get("name"),
                            "label": link.get("label"),
                            "component": component,
                            "host": ph,
                            "url": "",
                            "status": "SKIP",
                            "reason": "component state %s" % cstate,
                        }
                    )
                    continue

                # Host override from config
                host_for_url = ph
                if link.get("host"):
                    host_meta = link["host"]
                    host_prop = host_meta.get(protocol + "_property")
                    site = host_meta.get("site")
                    site_props = configs.get(site) or {}
                    host_for_url = parse_host_from_uri(site_props.get(host_prop)) or host_for_url

                port = set_port(link.get("port"), protocol, configs)
                template = link.get("url") or "%@://%@:%@"
                requires_user = str(link.get("requires_user_name") or "").lower() == "true"
                if requires_user:
                    url = fmt_ambari_url(template, [protocol, host_for_url, port, user])
                else:
                    url = fmt_ambari_url(template, [protocol, host_for_url, port])

                url = resolve_placeholders(url, configs)
                if use_ip:
                    url = rewrite_host_to_ip(url, host_ip)

                results["links"].append(
                    {
                        "service": sname,
                        "service_state": sstate,
                        "name": link.get("name"),
                        "label": link.get("label"),
                        "component": component,
                        "host": host_for_url,
                        "component_state": cstate,
                        "url": url,
                        "status": "READY",
                        "reason": "",
                        "stack_quicklinks_version": used_ver,
                    }
                )

print(json.dumps(results))
PY
)" || die "failed to resolve Ambari quicklinks"

CLUSTER_RESOLVED="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["cluster"])' <<<"$RESOLVED_JSON")"
STACK_RESOLVED="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["stack"])' <<<"$RESOLVED_JSON")"
echo "[INFO] Cluster: ${CLUSTER_RESOLVED}  Stack: ${STACK_RESOLVED}"
echo

if [[ -n "$UI_REPORT_FILE" ]]; then
  mkdir -p "$(dirname "$UI_REPORT_FILE")"
  printf 'service\tlink\tlabel\thost\turl\thttp_code\tstatus\n' >"$UI_REPORT_FILE"
fi

# Ambari itself
if [[ "$UI_PROBE_AMBARI" == "1" ]]; then
  echo "[INFO] Probing Ambari UI"
  ambari_code="$(http_probe "${AMBARI_BASE_URL%/}/")"
  ambari_status="$(classify_http "$ambari_code")"
  case "$ambari_status" in
    UP|UP_AUTH)
      record_pass "AMBARI Ambari UI -> HTTP ${ambari_code} (${ambari_status}) ${AMBARI_BASE_URL}"
      ;;
    *)
      record_fail "AMBARI Ambari UI -> HTTP ${ambari_code} (DOWN) ${AMBARI_BASE_URL}"
      ;;
  esac
  if [[ -n "$UI_REPORT_FILE" ]]; then
    printf 'AMBARI\tambari_ui\tAmbari UI\t-\t%s\t%s\t%s\n' \
      "${AMBARI_BASE_URL}" "$ambari_code" "$ambari_status" >>"$UI_REPORT_FILE"
  fi
  echo
fi

# Skipped services
while IFS= read -r line; do
  [[ -n "$line" ]] || continue
  record_skip "$line"
done < <(python3 -c '
import json,sys
d=json.load(sys.stdin)
for s in d.get("skipped_services") or []:
    print("%s (%s): %s" % (s.get("service"), s.get("state"), s.get("reason")))
' <<<"$RESOLVED_JSON")

echo
echo "[INFO] Probing service Quick Links"

# Probe each resolved link (one JSON object per line).
while IFS= read -r link_json; do
  [[ -n "$link_json" ]] || continue
  eval "$(python3 -c '
import json,shlex,sys
link=json.loads(sys.argv[1])
def emit(k,v):
    print("%s=%s" % (k, shlex.quote("" if v is None else str(v))))
emit("service", link.get("service"))
emit("name", link.get("name"))
emit("label", link.get("label"))
emit("host", link.get("host"))
emit("url", link.get("url"))
emit("status", link.get("status"))
emit("reason", link.get("reason"))
' "$link_json")"

  display="${service} ${label:-$name}"
  if [[ "$status" == "SKIP" ]]; then
    record_skip "${display}${host:+ on ${host}} (${reason:-skipped})"
    if [[ -n "$UI_REPORT_FILE" ]]; then
      printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$service" "$name" "$label" "$host" "" "" "SKIP:${reason}" >>"$UI_REPORT_FILE"
    fi
    continue
  fi
  if [[ -z "$url" ]]; then
    record_fail "${display}: empty URL"
    continue
  fi
  code="$(http_probe "$url")"
  http_status="$(classify_http "$code")"
  case "$http_status" in
    UP|UP_AUTH)
      record_pass "${display} @ ${host} -> HTTP ${code} (${http_status}) ${url}"
      ;;
    *)
      record_fail "${display} @ ${host} -> HTTP ${code} (DOWN) ${url}"
      ;;
  esac
  if [[ -n "$UI_REPORT_FILE" ]]; then
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$service" "$name" "$label" "$host" "$url" "$code" "$http_status" >>"$UI_REPORT_FILE"
  fi
done < <(python3 -c '
import json,sys
d=json.load(sys.stdin)
for link in d.get("links") or []:
    print(json.dumps(link, separators=(",", ":")))
' <<<"$RESOLVED_JSON")

echo
echo "========== SUMMARY =========="
echo "Cluster : ${CLUSTER_RESOLVED} (${STACK_RESOLVED})"
echo "PASS    : ${PASS_N}"
echo "FAIL    : ${FAIL_N}"
echo "SKIPPED : ${SKIP_N}"
if [[ -n "$UI_REPORT_FILE" ]]; then
  echo "Report  : ${UI_REPORT_FILE}"
fi
echo "============================="

if (( FAIL_N > 0 )); then
  echo "[ERROR] ${FAIL_N} Quick Link UI check(s) failed" >&2
  exit 1
fi

echo "[INFO] All probed Quick Link UIs are reachable"
exit 0
