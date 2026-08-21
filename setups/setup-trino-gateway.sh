#!/usr/bin/env bash
#
# Setup helper: make Ambari Trino Gateway startable on ODP.
#
# Fixes gaps seen on ODP 3.3 clusters:
#   1) config.yaml emits http-server.https.truststore.* when SSL is off
#      -> Airlift rejects unused properties and the JVM exits (status=100)
#   2) ssl_enabled=true with empty keystore path
#   3) Ambari START returns COMPLETED while systemd unit later fails
#      -> this script verifies systemd is active and port is listening
#   4) Optional Kerberos keytab for HTTP/trino-gateway when missing
#
# Not a smoke test. Prefer probing http://<host>:9105 afterward.
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   TRINO_GATEWAY_ENV_FILE default <repo>/configs/trino-gateway.env
#   SSH_USER / SSH_KEY
#   TRINO_GATEWAY_SKIP_CONFIG=1 / TRINO_GATEWAY_SKIP_KEYTAB=1 / TRINO_GATEWAY_SKIP_START=1
#   TRINO_GATEWAY_DISABLE_EMPTY_SSL=1 (default) - set ssl_enabled=false when keystore empty
#   TRINO_GATEWAY_FIX_TRUSTSTORE=1 (default) - nest truststore props under SSL {% if %}
#
# Usage:
#   ./setups/setup-trino-gateway.sh
#   SSH_KEY=$HOME/Downloads/usdc.pem ./setups/setup-trino-gateway.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${REPO_DIR}/configs/ambari.env}"
TRINO_GATEWAY_ENV_FILE="${TRINO_GATEWAY_ENV_FILE:-${REPO_DIR}/configs/trino-gateway.env}"

die() {
  echo "ERROR: $*" >&2
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
      TRINO_GATEWAY_*|SSH_*)
        [[ -n "${!key:-}" ]] || printf -v "$key" '%s' "$val"
        ;;
    esac
  done <"$f"
}

need_cmd curl
need_cmd python3
need_cmd ssh

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""
load_env_file "$TRINO_GATEWAY_ENV_FILE"
[[ -f "$AMBARI_CONFIG_FILE" ]] && load_env_file "$AMBARI_CONFIG_FILE"

AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://127.0.0.1:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-admin}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-admin}}"
CLUSTER_NAME="${CLUSTER_NAME:-}"
SSH_USER="${SSH_USER:-acceldata}"
SSH_KEY="${SSH_KEY:-${HOME}/Downloads/usdc.pem}"
TRINO_GATEWAY_SKIP_CONFIG="${TRINO_GATEWAY_SKIP_CONFIG:-0}"
TRINO_GATEWAY_SKIP_KEYTAB="${TRINO_GATEWAY_SKIP_KEYTAB:-0}"
TRINO_GATEWAY_SKIP_START="${TRINO_GATEWAY_SKIP_START:-0}"
TRINO_GATEWAY_DISABLE_EMPTY_SSL="${TRINO_GATEWAY_DISABLE_EMPTY_SSL:-1}"
TRINO_GATEWAY_FIX_TRUSTSTORE="${TRINO_GATEWAY_FIX_TRUSTSTORE:-1}"
TRINO_GATEWAY_KEYTAB="${TRINO_GATEWAY_KEYTAB:-/etc/security/keytabs/trino-gateway.service.keytab}"

ssh_base=(ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=20)
[[ -f "$SSH_KEY" ]] && ssh_base+=(-i "$SSH_KEY")

ambari_json() {
  local method="$1" path="$2" body="${3:-}"
  python3 - "$AMBARI_BASE_URL" "$AMBARI_USER" "$AMBARI_PASSWORD" "$method" "$path" "$body" <<'PY'
import json, sys, urllib.request, base64
base, user, pw, method, path, body = sys.argv[1:7]
url = base.rstrip("/") + path
req = urllib.request.Request(url, method=method)
req.add_header("Authorization", "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode())
req.add_header("X-Requested-By", "ambari")
data = body.encode() if body else None
if data is not None:
    req.add_header("Content-Type", "application/json")
with urllib.request.urlopen(req, data=data, timeout=180) as resp:
    raw = resp.read()
    print(raw.decode() if raw.strip() else "{}")
PY
}

ambari_poll() {
  local rid="$1" label="$2" i st
  [[ -n "$rid" ]] || return 0
  echo "[INFO] $label request=$rid"
  for i in $(seq 0 90); do
    st="$(ambari_json GET "/api/v1/clusters/${CLUSTER_NAME}/requests/${rid}?fields=Requests/request_status" \
      | python3 -c 'import sys,json; print(json.load(sys.stdin)["Requests"]["request_status"])')"
    echo "    [$i] $st"
    case "$st" in
      COMPLETED) return 0 ;;
      FAILED|ABORTED|TIMEDOUT) die "Ambari $label failed: $st (request $rid)" ;;
    esac
    sleep 4
  done
  die "Ambari $label timed out (request $rid)"
}

if [[ -z "$CLUSTER_NAME" ]]; then
  CLUSTER_NAME="$(ambari_json GET "/api/v1/clusters/" | python3 -c '
import json,sys
items=json.load(sys.stdin).get("items") or []
print((items[0].get("Clusters") or {}).get("cluster_name") or "")
')"
fi
[[ -n "$CLUSTER_NAME" ]] || die "CLUSTER_NAME required"

echo "[INFO] Ambari=${AMBARI_BASE_URL} cluster=${CLUSTER_NAME}"

# Ensure TRINO_GATEWAY service exists
svc_state="$(ambari_json GET "/api/v1/clusters/${CLUSTER_NAME}/services/TRINO_GATEWAY?fields=ServiceInfo/state" \
  | python3 -c 'import sys,json
try:
  print(json.load(sys.stdin)["ServiceInfo"]["state"])
except Exception:
  print("")
' 2>/dev/null || true)"
[[ -n "$svc_state" ]] || die "TRINO_GATEWAY is not installed on cluster ${CLUSTER_NAME}"
echo "[INFO] TRINO_GATEWAY Ambari state=${svc_state}"

DISC_FILE="$(mktemp)"
python3 - "$AMBARI_BASE_URL" "$AMBARI_USER" "$AMBARI_PASSWORD" "$CLUSTER_NAME" >"$DISC_FILE" <<'PY'
import json, sys, urllib.request, base64, urllib.parse
base, user, pw, cluster = sys.argv[1:5]
auth = base64.b64encode(f"{user}:{pw}".encode()).decode()

def get(path):
    r = urllib.request.Request(base.rstrip("/") + path)
    r.add_header("Authorization", "Basic " + auth)
    with urllib.request.urlopen(r, timeout=90) as resp:
        return json.loads(resp.read())

qc = urllib.parse.quote(cluster, safe="")
dc = get(f"/api/v1/clusters/{qc}?fields=Clusters/desired_configs")["Clusters"]["desired_configs"]

def props(ctype):
    meta = dc.get(ctype) or {}
    tag = meta.get("tag")
    if not tag:
        return {}
    items = get(
        f"/api/v1/clusters/{qc}/configurations?type={urllib.parse.quote(ctype)}&tag={urllib.parse.quote(tag)}"
    ).get("items") or []
    return (items[0].get("properties") or {}) if items else {}

kenv = props("kerberos-env")
cenv = props("cluster-env")
genv = props("trino-gateway-env")
gssl = props("trino-gateway-ssl")
gcfg = props("trino-gateway-config")
hosts_info = {
    (h["Hosts"]["host_name"]): (h["Hosts"].get("ip") or h["Hosts"]["host_name"])
    for h in get(f"/api/v1/clusters/{qc}/hosts?fields=Hosts/host_name,Hosts/ip").get("items") or []
}

print("meta\trealm\t%s" % (kenv.get("realm") or "ADSRE.COM").strip())
print("meta\tsecurity\t%s" % str(cenv.get("security_enabled") or "false").lower())
print("meta\thttp_port\t%s" % (gcfg.get("http_port") or "9105").strip())
print("meta\tssl_enabled\t%s" % str(gssl.get("ssl_enabled") or "false").lower())
print("meta\tkeystore_path\t%s" % (gssl.get("trino-gateway.keystore_path") or "").strip())
print("meta\tkeytab\t%s" % (genv.get("trino-gateway.kerberos.keytab") or "/etc/security/keytabs/trino-gateway.service.keytab").strip())
print("meta\tprincipal\t%s" % (genv.get("trino-gateway.kerberos.principal") or "HTTP/trino-gateway@ADSRE.COM").strip())
print("meta\tjdbc_url\t%s" % (gcfg.get("datastore.jdbc_url") or "").strip())

hc = get(
    f"/api/v1/clusters/{qc}/host_components"
    f"?HostRoles/component_name=TRINO_GATEWAY"
    f"&fields=HostRoles/host_name,HostRoles/public_host_name,HostRoles/state"
)
for it in hc.get("items") or []:
    hr = it.get("HostRoles") or {}
    host = hr.get("public_host_name") or hr.get("host_name") or ""
    if not host:
        continue
    ip = hosts_info.get(hr.get("host_name") or host, host)
    print("host\tGW\tTRINO_GATEWAY\t%s\t%s\t%s" % (host, hr.get("state") or "", ip))
PY

REALM="ADSRE.COM"
DISC_SECURITY="false"
HTTP_PORT="9105"
DISC_SSL_ENABLED="false"
DISC_KEYSTORE=""
DISC_KEYTAB="$TRINO_GATEWAY_KEYTAB"
DISC_PRINCIPAL="HTTP/trino-gateway@ADSRE.COM"
DISC_JDBC=""
HOST_ROWS=()
while IFS=$'\t' read -r kind a b c d e; do
  case "$kind" in
    meta)
      case "$a" in
        realm) REALM="$b" ;;
        security) DISC_SECURITY="$b" ;;
        http_port) HTTP_PORT="$b" ;;
        ssl_enabled) DISC_SSL_ENABLED="$b" ;;
        keystore_path) DISC_KEYSTORE="$b" ;;
        keytab) DISC_KEYTAB="$b" ;;
        principal) DISC_PRINCIPAL="$b" ;;
        jdbc_url) DISC_JDBC="$b" ;;
      esac
      ;;
    host)
      # kind role comp host state ip
      HOST_ROWS+=("${a}|${b}|${c}|${d}|${e}")
      ;;
  esac
done <"$DISC_FILE"
rm -f "$DISC_FILE"

[[ ${#HOST_ROWS[@]} -gt 0 ]] || die "No TRINO_GATEWAY host_components found"
echo "[INFO] security=${DISC_SECURITY} ssl_enabled=${DISC_SSL_ENABLED} http_port=${HTTP_PORT}"
echo "[INFO] jdbc=${DISC_JDBC:-"(unset)"}"
for row in "${HOST_ROWS[@]}"; do
  IFS='|' read -r role comp host state ip <<<"$row"
  echo "[INFO] host ${host} (${ip}) state=${state}"
done

if [[ "$TRINO_GATEWAY_SKIP_CONFIG" != "1" ]]; then
  echo "[INFO] patching Ambari trino-gateway configs"
  python3 - "$AMBARI_BASE_URL" "$AMBARI_USER" "$AMBARI_PASSWORD" "$CLUSTER_NAME" \
    "$TRINO_GATEWAY_DISABLE_EMPTY_SSL" "$TRINO_GATEWAY_FIX_TRUSTSTORE" <<'PY'
import json, sys, time, urllib.request, base64, urllib.parse, re
base, user, pw, cluster, disable_empty, fix_ts = sys.argv[1:7]
auth = base64.b64encode(f"{user}:{pw}".encode()).decode()

def req(method, path, body=None):
    r = urllib.request.Request(base.rstrip("/") + path, method=method)
    r.add_header("Authorization", "Basic " + auth)
    r.add_header("X-Requested-By", "ambari")
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        r.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(r, data=data, timeout=120) as resp:
        raw = resp.read()
        return json.loads(raw) if raw.strip() else {}

qc = urllib.parse.quote(cluster, safe="")
dc = req("GET", f"/api/v1/clusters/{qc}?fields=Clusters/desired_configs")["Clusters"]["desired_configs"]

def get_props(ctype):
    tag = dc[ctype]["tag"]
    return dict(req("GET", f"/api/v1/clusters/{qc}/configurations?type={urllib.parse.quote(ctype)}&tag={urllib.parse.quote(tag)}")["items"][0]["properties"])

def put_props(ctype, props, note):
    tag = "version" + str(int(time.time() * 1000))
    req("PUT", f"/api/v1/clusters/{qc}", {
        "Clusters": {
            "desired_config": [{
                "type": ctype,
                "tag": tag,
                "properties": props,
                "service_config_version_note": note,
            }]
        }
    })
    print("[OK] updated", ctype)
    dc[ctype] = {"tag": tag}

changed_any = False

# 1) disable empty SSL
if disable_empty == "1" and "trino-gateway-ssl" in dc:
    sslp = get_props("trino-gateway-ssl")
    ks = (sslp.get("trino-gateway.keystore_path") or "").strip()
    if str(sslp.get("ssl_enabled") or "false").lower() == "true" and not ks:
        sslp["ssl_enabled"] = "false"
        put_props("trino-gateway-ssl", sslp, "setup-trino-gateway.sh: disable empty SSL")
        changed_any = True
        print("[INFO] ssl_enabled=false (empty keystore path)")
    else:
        print("[OK] trino-gateway-ssl ssl_enabled/keystore OK")

# 2) nest truststore props under SSL {% if %} in Jinja template
if fix_ts == "1" and "trino-gateway-config" in dc:
    cfg = get_props("trino-gateway-config")
    tpl = cfg.get("trino_gateway_config") or ""
    if not tpl:
        print("[WARN] trino_gateway_config content empty; skip truststore patch")
    else:
        # Bug layout: truststore lines after SSL {% endif %} (often after hostname if).
        m = re.search(
            r"(\{% if trino_gateway_ssl_enabled %\}.*?\{% endif %\})"
            r"(\s*\{% if disable_hostname_validation %\}.*?\{% endif %\})?"
            r"(\s*http-server\.https\.truststore\.path:[^\n]*\n\s*http-server\.https\.truststore\.key:[^\n]*\n?)",
            tpl,
            re.S,
        )
        if m:
            ssl_block, host_block = m.group(1), m.group(2) or ""
            new_ssl = ssl_block.replace(
                "{% endif %}",
                "http-server.https.truststore.path: {{java_truststore_path}}\n"
                "    http-server.https.truststore.key: ${ENV:TRUSTSTORE_PASSWORD}\n"
                "    {% endif %}",
                1,
            )
            new_tpl = tpl[: m.start()] + new_ssl + host_block + tpl[m.end() :]
            cfg["trino_gateway_config"] = new_tpl
            put_props(
                "trino-gateway-config",
                cfg,
                "setup-trino-gateway.sh: nest HTTPS truststore under SSL if-block",
            )
            changed_any = True
            print("[INFO] nested truststore props under {% if trino_gateway_ssl_enabled %}")
        elif "http-server.https.truststore.path" in tpl:
            print("[OK] trino_gateway_config truststore placement looks OK")
        else:
            print("[OK] no truststore props in template")

if not changed_any:
    print("[OK] no Ambari config changes required")
PY
fi

if [[ "$TRINO_GATEWAY_SKIP_KEYTAB" != "1" && "$DISC_SECURITY" == "true" ]]; then
  for row in "${HOST_ROWS[@]}"; do
    IFS='|' read -r role comp host state ip <<<"$row"
    echo "[INFO] ensure keytab on ${host}"
    if "${ssh_base[@]}" "${SSH_USER}@${ip}" "sudo test -s '${DISC_KEYTAB}'"; then
      echo "[OK] ${DISC_KEYTAB} present on ${host}"
    else
      echo "[WARN] ${DISC_KEYTAB} missing on ${host}; create via Ambari Kerberos regenerate or kadmin"
      echo "[WARN] expected principal ${DISC_PRINCIPAL} (realm ${REALM})"
    fi
  done
fi

if [[ "$TRINO_GATEWAY_SKIP_START" != "1" ]]; then
  echo "[INFO] Ambari START TRINO_GATEWAY"
  out="$(ambari_json PUT "/api/v1/clusters/${CLUSTER_NAME}/services/TRINO_GATEWAY" \
    "{\"RequestInfo\":{\"context\":\"Start Trino Gateway (setup-trino-gateway.sh)\"},\"ServiceInfo\":{\"state\":\"STARTED\"}}")"
  rid="$(printf '%s' "$out" | python3 -c 'import sys,json; print((json.load(sys.stdin).get("Requests") or {}).get("id") or "")')"
  if [[ -n "$rid" ]]; then
    ambari_poll "$rid" "START TRINO_GATEWAY"
  else
    echo "[WARN] Ambari START returned no request id (already STARTED?)"
  fi
  # Ambari may report COMPLETED while systemd later fails; verify on host.
  sleep 5
  for row in "${HOST_ROWS[@]}"; do
    IFS='|' read -r role comp host state ip <<<"$row"
    echo "[INFO] verify systemd + port on ${host}"
    "${ssh_base[@]}" "${SSH_USER}@${ip}" "bash -s" <<REMOTE || die "Trino Gateway not healthy on ${host}"
set -euo pipefail
if ! systemctl is-active --quiet trino-gateway; then
  echo "[ERROR] trino-gateway.service is not active:"
  systemctl status trino-gateway --no-pager -l | head -40 || true
  journalctl -u trino-gateway -n 40 --no-pager || true
  exit 1
fi
if ! ss -lntp 2>/dev/null | grep -q ":${HTTP_PORT}"; then
  echo "[ERROR] nothing listening on :${HTTP_PORT}"
  systemctl status trino-gateway --no-pager -l | head -20 || true
  exit 1
fi
code=\$(curl -sS -o /dev/null -w '%{http_code}' --max-time 10 "http://127.0.0.1:${HTTP_PORT}/" || true)
echo "[OK] systemd active; :${HTTP_PORT} listening; HTTP \${code:-000}"
REMOTE
  done
fi

echo "OK: Trino Gateway setup finished. UI: http://<gateway-host>:${HTTP_PORT}/"
