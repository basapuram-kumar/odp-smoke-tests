#!/usr/bin/env bash
#
# Setup helper: make Ambari Trino startable on Kerberized ODP.
#
# Fixes gaps seen on ODP 3.3 Kerberos clusters:
#   1) Workers left INSTALL_FAILED / package missing after aborted Install Services
#   2) Missing trino/<host>@REALM + /etc/security/keytabs/trino.keytab
#   3) ssl_enabled=true with empty ssl_keystore -> FileNotFoundException PEM keystore
#   4) Kerberos auth without internal-communication.shared-secret when SSL is off
#      (mpack template only emits shared-secret inside the SSL block)
#
# Not a smoke test. Prefer ./trino-sample-smoke.sh afterward.
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   TRINO_ENV_FILE / TRINO_CONFIG_FILE  default <repo>/configs/trino.env
#   SSH_USER / SSH_KEY
#   TRINO_KEYTAB / TRINO_SHARED_KEY
#   TRINO_SKIP_PACKAGE=1 / TRINO_SKIP_KEYTAB=1 / TRINO_SKIP_CONFIG=1 / TRINO_SKIP_START=1
#   TRINO_DISABLE_EMPTY_SSL=1 (default) - set ssl_enabled=false when keystore empty
#
# Usage:
#   ./setups/setup-trino.sh
#   SSH_KEY=$HOME/Downloads/usdc.pem ./setups/setup-trino.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${REPO_DIR}/configs/ambari.env}"
TRINO_ENV_FILE="${TRINO_ENV_FILE:-${TRINO_CONFIG_FILE:-${REPO_DIR}/configs/trino.env}}"

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
      TRINO_*|SSH_*)
        [[ -n "${!key:-}" ]] || printf -v "$key" '%s' "$val"
        ;;
    esac
  done <"$f"
}

need_cmd curl
need_cmd python3
need_cmd ssh
need_cmd scp

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""
load_env_file "$TRINO_ENV_FILE"
[[ -f "$AMBARI_CONFIG_FILE" ]] && load_env_file "$AMBARI_CONFIG_FILE"

AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://127.0.0.1:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-admin}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-admin}}"
CLUSTER_NAME="${CLUSTER_NAME:-}"
SSH_USER="${SSH_USER:-acceldata}"
SSH_KEY="${SSH_KEY:-${HOME}/Downloads/usdc.pem}"
TRINO_KEYTAB="${TRINO_KEYTAB:-/etc/security/keytabs/trino.keytab}"
TRINO_SKIP_PACKAGE="${TRINO_SKIP_PACKAGE:-0}"
TRINO_SKIP_KEYTAB="${TRINO_SKIP_KEYTAB:-0}"
TRINO_SKIP_CONFIG="${TRINO_SKIP_CONFIG:-0}"
TRINO_SKIP_START="${TRINO_SKIP_START:-0}"
TRINO_DISABLE_EMPTY_SSL="${TRINO_DISABLE_EMPTY_SSL:-1}"

ssh_base=(ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=20)
scp_base=(scp -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=20)
[[ -f "$SSH_KEY" ]] && ssh_base+=(-i "$SSH_KEY") && scp_base+=(-i "$SSH_KEY")

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

envp = props("trino-env")
kenv = props("kerberos-env")
cenv = props("cluster-env")
hosts_info = {
    (h["Hosts"]["host_name"]): (h["Hosts"].get("ip") or h["Hosts"]["host_name"])
    for h in get(f"/api/v1/clusters/{qc}/hosts?fields=Hosts/host_name,Hosts/ip").get("items") or []
}

def emit(comp, role):
    hc = get(
        f"/api/v1/clusters/{qc}/host_components"
        f"?HostRoles/component_name={comp}"
        f"&fields=HostRoles/host_name,HostRoles/public_host_name,HostRoles/state"
    )
    for it in hc.get("items") or []:
        hr = it.get("HostRoles") or {}
        host = hr.get("public_host_name") or hr.get("host_name") or ""
        if not host:
            continue
        ip = hosts_info.get(hr.get("host_name") or host, host)
        print("host\t%s\t%s\t%s\t%s\t%s" % (role, comp, host, hr.get("state") or "", ip))

print("meta\trealm\t%s" % (kenv.get("realm") or "ADSRE.COM").strip())
print("meta\tsecurity\t%s" % str(cenv.get("security_enabled") or "false").lower())
print("meta\thttp_port\t%s" % (envp.get("http_server_port") or "9097").strip())
print("meta\tssl_enabled\t%s" % str(envp.get("ssl_enabled") or "false").lower())
print("meta\tssl_keystore\t%s" % (envp.get("ssl_keystore") or "").strip())
print("meta\tshared_key_set\t%s" % ("1" if (envp.get("shared_key") or "").strip() else "0"))
print("meta\tkeytab\t%s" % (envp.get("trino.kerberos.keytab") or "/etc/security/keytabs/trino.keytab").strip())
emit("TRINO_COORDINATOR", "COORD")
emit("TRINO_WORKER", "WORKER")
PY

REALM="ADSRE.COM"
DISC_SECURITY="false"
HTTP_PORT="9097"
DISC_SSL_ENABLED="false"
DISC_SSL_KEYSTORE=""
DISC_SHARED_KEY_SET="0"
HOST_ROWS=()
while IFS=$'\t' read -r kind a b c d e; do
  case "$kind" in
    meta)
      case "$a" in
        realm) REALM="$b" ;;
        security) DISC_SECURITY="$b" ;;
        http_port) HTTP_PORT="$b" ;;
        ssl_enabled) DISC_SSL_ENABLED="$b" ;;
        ssl_keystore) DISC_SSL_KEYSTORE="$b" ;;
        shared_key_set) DISC_SHARED_KEY_SET="$b" ;;
        keytab) TRINO_KEYTAB="${TRINO_KEYTAB:-$b}" ;;
      esac
      ;;
    host)
      # role comp host state ip
      HOST_ROWS+=("$a|$b|$c|$d|$e")
      ;;
  esac
done <"$DISC_FILE"
rm -f "$DISC_FILE"

HTTP_PORT="${TRINO_HTTP_PORT:-$HTTP_PORT}"
[[ ${#HOST_ROWS[@]} -gt 0 ]] || die "No TRINO_COORDINATOR / TRINO_WORKER hosts found"

echo "---- Trino setup ----"
echo "    Ambari: ${AMBARI_BASE_URL} cluster=${CLUSTER_NAME}"
echo "    realm=${REALM} security=${DISC_SECURITY}"
echo "    ssl_enabled=${DISC_SSL_ENABLED} keystore='${DISC_SSL_KEYSTORE}' shared_key_set=${DISC_SHARED_KEY_SET}"
echo "    keytab=${TRINO_KEYTAB}"
for row in "${HOST_ROWS[@]}"; do
  echo "    $row"
done

if [[ "$TRINO_SKIP_PACKAGE" != "1" ]]; then
  for row in "${HOST_ROWS[@]}"; do
    IFS='|' read -r role comp host state ip <<<"$row"
    if [[ "$state" == "INSTALL_FAILED" || "$state" == "INIT" || "$state" == "UNKNOWN" ]]; then
      echo "[INFO] Ambari INSTALL ${comp} on ${host} (state=${state})"
      out="$(ambari_json PUT "/api/v1/clusters/${CLUSTER_NAME}/hosts/${host}/host_components/${comp}" \
        "{\"RequestInfo\":{\"context\":\"Install ${comp} on ${host} (setup-trino.sh)\",\"operation_level\":{\"level\":\"HOST_COMPONENT\",\"cluster_name\":\"${CLUSTER_NAME}\",\"host_name\":\"${host}\",\"service_name\":\"TRINO\",\"hostcomponent_name\":\"${comp}\"}},\"HostRoles\":{\"state\":\"INSTALLED\"}}")"
      rid="$(printf '%s' "$out" | python3 -c 'import sys,json; print((json.load(sys.stdin).get("Requests") or {}).get("id") or "")')"
      ambari_poll "$rid" "INSTALL ${host}/${comp}"
    fi
    echo "[INFO] ensure Trino RPM on ${host} (${ip})"
    "${ssh_base[@]}" "${SSH_USER}@${ip}" 'bash -s' <<'REMOTE'
set -euo pipefail
if rpm -qa 2>/dev/null | grep -q '^trino_'; then
  echo "[OK] trino rpm already installed"
  exit 0
fi
PKG="$(yum search trino_ 2>/dev/null | awk "/^trino_[0-9].*x86_64/{print \$1}" | head -1 || true)"
[[ -n "$PKG" ]] || { echo "[FAIL] no trino_ package in yum"; exit 2; }
echo "[INFO] yum install $PKG"
sudo yum install -y "$PKG"
rpm -qa | grep '^trino_'
REMOTE
  done
fi

if [[ "$TRINO_SKIP_KEYTAB" != "1" && "$DISC_SECURITY" == "true" ]]; then
  KDC_IP=""
  for row in "${HOST_ROWS[@]}"; do
    IFS='|' read -r role comp host state ip <<<"$row"
    if "${ssh_base[@]}" "${SSH_USER}@${ip}" 'command -v kadmin.local >/dev/null && sudo systemctl is-active krb5kdc >/dev/null 2>&1'; then
      KDC_IP="$ip"
      break
    fi
  done
  [[ -n "$KDC_IP" ]] || die "No host with kadmin.local/krb5kdc found; set keytabs manually or run on KDC"

  echo "[INFO] using KDC host ${KDC_IP} for trino keytabs"
  TMPDIR_LOCAL="$(mktemp -d)"
  cleanup() { rm -rf "$TMPDIR_LOCAL"; }
  trap cleanup EXIT

  SHORTS=()
  for row in "${HOST_ROWS[@]}"; do
    IFS='|' read -r role comp host state ip <<<"$row"
    SHORTS+=("${host%%.*}")
  done
  mapfile -t SHORTS < <(printf '%s\n' "${SHORTS[@]}" | awk '!a[$0]++')

  SHORTS_STR="$(printf '%s ' "${SHORTS[@]}")"
  "${ssh_base[@]}" "${SSH_USER}@${KDC_IP}" "bash -s" <<REMOTE
set -euo pipefail
REALM='${REALM}'
for H in ${SHORTS_STR}; do
  P="trino/\${H}@\${REALM}"
  echo "[INFO] ensure principal \$P"
  sudo kadmin.local -q "addprinc -randkey \$P" >/dev/null 2>&1 || true
  TMP="/tmp/trino-\${H}.keytab"
  sudo rm -f "\$TMP"
  sudo kadmin.local -q "ktadd -k \$TMP \$P" >/dev/null
  sudo chmod 644 "\$TMP"
done
REMOTE

  for row in "${HOST_ROWS[@]}"; do
    IFS='|' read -r role comp host state ip <<<"$row"
    short="${host%%.*}"
    local_kt="${TMPDIR_LOCAL}/trino-${short}.keytab"
    "${scp_base[@]}" "${SSH_USER}@${KDC_IP}:/tmp/trino-${short}.keytab" "$local_kt"
    "${scp_base[@]}" "$local_kt" "${SSH_USER}@${ip}:/tmp/trino.keytab"
    "${ssh_base[@]}" "${SSH_USER}@${ip}" "bash -s" <<REMOTE
set -euo pipefail
KT='${TRINO_KEYTAB}'
SHORT='${short}'
REALM='${REALM}'
id trino >/dev/null 2>&1 || sudo useradd -r -g hadoop trino || true
sudo install -o trino -g hadoop -m 440 /tmp/trino.keytab "\$KT"
sudo -u trino kinit -kt "\$KT" "trino/\${SHORT}@\${REALM}"
sudo -u trino kdestroy >/dev/null 2>&1 || true
echo "[OK] \$KT on \$(hostname -s)"
REMOTE
  done
fi

if [[ "$TRINO_SKIP_CONFIG" != "1" ]]; then
  echo "[INFO] ensuring Ambari trino-env shared_key / SSL and config-properties templates"
  python3 - "$AMBARI_BASE_URL" "$AMBARI_USER" "$AMBARI_PASSWORD" "$CLUSTER_NAME" \
    "${TRINO_SHARED_KEY:-}" "$TRINO_DISABLE_EMPTY_SSL" "$DISC_SECURITY" <<'PY'
import json, sys, time, secrets, urllib.request, base64, urllib.parse
base, user, pw, cluster, shared_key, disable_empty_ssl, security = sys.argv[1:8]
auth = base64.b64encode(f"{user}:{pw}".encode()).decode()

def req(method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    r = urllib.request.Request(base.rstrip("/") + path, data=data, method=method)
    r.add_header("Authorization", "Basic " + auth)
    r.add_header("X-Requested-By", "ambari")
    if body is not None:
        r.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(r, timeout=180) as resp:
        return json.loads(resp.read() or b"{}")

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

envp = get_props("trino-env")
changed = False
if security == "true" and not (shared_key or "").strip() and not (envp.get("shared_key") or "").strip():
    envp["shared_key"] = secrets.token_urlsafe(32)
    changed = True
    print("[INFO] generated trino-env/shared_key")
elif (shared_key or "").strip():
    envp["shared_key"] = shared_key.strip()
    changed = True

if disable_empty_ssl == "1":
    ks = (envp.get("ssl_keystore") or "").strip()
    if str(envp.get("ssl_enabled") or "false").lower() == "true" and not ks:
        envp["ssl_enabled"] = "false"
        changed = True
        print("[INFO] ssl_enabled=false (empty ssl_keystore)")

if changed:
    put_props("trino-env", envp, "setup-trino.sh: shared_key / disable empty SSL")
    dc = req("GET", f"/api/v1/clusters/{qc}?fields=Clusters/desired_configs")["Clusters"]["desired_configs"]

cp = get_props("config-properties")
needle = "{% else %}\nhttp-server.http.port={{http_port}}\nhttp-server.http.enabled=true\n{% endif %}"
repl = (
    "{% else %}\n"
    "http-server.http.port={{http_port}}\n"
    "http-server.http.enabled=true\n"
    "{% if security_enabled or ldap_enabled %}\n"
    "internal-communication.shared-secret={{shared_key}}\n"
    "{% endif %}\n"
    "{% endif %}"
)
tpl_changed = False
for key in ("coordinator_config", "worker_config"):
    text = cp.get(key) or ""
    if needle not in text:
        continue
    parts = text.split("{% else %}", 1)
    if len(parts) != 2:
        continue
    else_body = parts[1].split("{% endif %}", 1)[0]
    if "internal-communication.shared-secret={{shared_key}}" in else_body:
        continue
    cp[key] = text.replace(needle, repl, 1)
    tpl_changed = True
    print("[INFO] patched", key, "else-branch shared-secret")

if tpl_changed:
    put_props("config-properties", cp, "setup-trino.sh: emit shared-secret when Kerberos/LDAP without SSL")
else:
    print("[OK] config-properties templates already include shared-secret for non-SSL auth")
PY
fi

if [[ "$TRINO_SKIP_START" != "1" ]]; then
  echo "[INFO] Ambari START TRINO"
  out="$(ambari_json PUT "/api/v1/clusters/${CLUSTER_NAME}/services/TRINO" \
    "{\"RequestInfo\":{\"context\":\"Start Trino (setup-trino.sh)\"},\"ServiceInfo\":{\"state\":\"STARTED\"}}")"
  rid="$(printf '%s' "$out" | python3 -c 'import sys,json; print((json.load(sys.stdin).get("Requests") or {}).get("id") or "")')"
  if [[ -n "$rid" ]]; then
    ambari_poll "$rid" "START TRINO"
  else
    echo "[WARN] Ambari START returned no request id (already STARTED?)"
  fi
  sleep 8
fi

COORD_IP=""
COORD_HOST=""
for row in "${HOST_ROWS[@]}"; do
  IFS='|' read -r role comp host state ip <<<"$row"
  if [[ "$role" == "COORD" ]]; then
    COORD_HOST="$host"
    COORD_IP="$ip"
    break
  fi
done
if [[ -n "$COORD_IP" ]]; then
  echo "[INFO] probe http://${COORD_HOST}:${HTTP_PORT}/v1/info"
  code="$(curl -sS -o /tmp/trino-info.$$ -w '%{http_code}' --max-time 15 "http://${COORD_IP}:${HTTP_PORT}/v1/info" || true)"
  if [[ "$code" == "200" ]]; then
    echo "[OK] Trino /v1/info HTTP 200: $(head -c 180 /tmp/trino-info.$$)"
  else
    echo "[WARN] Trino /v1/info HTTP ${code:-000}; check systemctl status trino on ${COORD_HOST}"
  fi
  rm -f /tmp/trino-info.$$
fi

echo "OK: Trino setup finished. Next: ./trino-sample-smoke.sh"
