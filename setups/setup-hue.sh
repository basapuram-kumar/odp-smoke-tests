#!/usr/bin/env bash
#
# Setup helper: make Ambari Hue startable and UI-accessible on Kerberized ODP.
#
# Fixes the common post-install gaps seen on ODP 3.3:
#   1) MySQL metastore DB/user/grants (Ambari UI may show MySQL while
#      hue_metastore.ini still points at missing sqlite desktop.db)
#   2) hue.headless.keytab for hue-<cluster>@REALM (kt_renewer otherwise
#      takes the whole supervisor down)
#   3) Django syncdb + migrate
#   4) Ensure a form-login admin user exists
#   5) Ambari START of HUE
#
# Not a smoke test. Prefer ./hue-sample-smoke.sh afterward.
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   HUE_ENV_FILE / HUE_CONFIG_FILE  default <repo>/configs/hue.env
#   SSH_USER / SSH_KEY / SSH_HOST   remote Hue host (default: Ambari HUE_SERVER)
#   HUE_DB_HOST / HUE_DB_NAME / HUE_DB_USER / HUE_DB_PASS
#   HUE_USER / HUE_PASSWORD         form-login admin to create/reset
#   HUE_PRINCIPAL / HUE_KEYTAB
#   HUE_SKIP_KEYTAB=1 / HUE_SKIP_START=1 / HUE_SKIP_MIGRATE=1
#
# Usage:
#   ./setups/setup-hue.sh
#   SSH_KEY=$HOME/Downloads/usdc.pem ./setups/setup-hue.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${REPO_DIR}/configs/ambari.env}"
HUE_ENV_FILE="${HUE_ENV_FILE:-${HUE_CONFIG_FILE:-${REPO_DIR}/configs/hue.env}}"

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
      HUE_*|SSH_*|CURL_EXTRA_OPTS)
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
load_env_file "$HUE_ENV_FILE"
[[ -f "$AMBARI_CONFIG_FILE" ]] && load_env_file "$AMBARI_CONFIG_FILE"

AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://127.0.0.1:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-admin}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-admin}}"
CLUSTER_NAME="${CLUSTER_NAME:-}"
SSH_USER="${SSH_USER:-acceldata}"
SSH_KEY="${SSH_KEY:-${HOME}/Downloads/usdc.pem}"
HUE_DB_HOST="${HUE_DB_HOST:-odp1}"
HUE_DB_NAME="${HUE_DB_NAME:-hue}"
HUE_DB_USER="${HUE_DB_USER:-hue}"
HUE_DB_PASS="${HUE_DB_PASS:-hue}"
HUE_USER="${HUE_USER:-admin}"
HUE_PASSWORD="${HUE_PASSWORD:-admin}"
HUE_KEYTAB="${HUE_KEYTAB:-/etc/security/keytabs/hue.headless.keytab}"
HUE_SKIP_KEYTAB="${HUE_SKIP_KEYTAB:-0}"
HUE_SKIP_START="${HUE_SKIP_START:-0}"
HUE_SKIP_MIGRATE="${HUE_SKIP_MIGRATE:-0}"

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
with urllib.request.urlopen(req, data=data, timeout=120) as resp:
    raw = resp.read()
    print(raw.decode() if raw.strip() else "{}")
PY
}

if [[ -z "$CLUSTER_NAME" ]]; then
  CLUSTER_NAME="$(ambari_json GET "/api/v1/clusters/" | python3 -c '
import json,sys
items=json.load(sys.stdin).get("items") or []
print((items[0].get("Clusters") or {}).get("cluster_name") or "")
')"
fi
[[ -n "$CLUSTER_NAME" ]] || die "CLUSTER_NAME required"

# Discover HUE_SERVER host + realm/principal defaults from Ambari.
DISC="$(python3 - "$AMBARI_BASE_URL" "$AMBARI_USER" "$AMBARI_PASSWORD" "$CLUSTER_NAME" <<'PY'
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
    items = get(f"/api/v1/clusters/{qc}/configurations?type={urllib.parse.quote(ctype)}&tag={urllib.parse.quote(tag)}").get("items") or []
    return (items[0].get("properties") or {}) if items else {}

desktop = props("hue-desktop-site")
envp = props("hue-env")
kenv = props("kerberos-env")
hc = get(
    f"/api/v1/clusters/{qc}/host_components"
    f"?HostRoles/component_name=HUE_SERVER"
    f"&fields=HostRoles/host_name,HostRoles/public_host_name,HostRoles/state"
)
host = ""
for it in hc.get("items") or []:
    hr = it.get("HostRoles") or {}
    cand = hr.get("public_host_name") or hr.get("host_name")
    if (hr.get("state") or "").upper() == "STARTED" and cand:
        host = cand
        break
if not host:
    for it in hc.get("items") or []:
        hr = it.get("HostRoles") or {}
        host = hr.get("public_host_name") or hr.get("host_name") or ""
        if host:
            break
print("host=%s" % host)
print("db_host=%s" % (desktop.get("db_host") or "").strip())
print("db_name=%s" % (desktop.get("db_name") or "hue").strip())
print("db_user=%s" % (desktop.get("db_user") or "hue").strip())
print("realm=%s" % (kenv.get("realm") or "ADSRE.COM").strip())
print("keytab=%s" % (envp.get("hue_user_keytab") or "/etc/security/keytabs/hue.headless.keytab").strip())
PY
)"

DISC_HOST=""
DISC_DB_HOST=""
DISC_DB_NAME=""
DISC_DB_USER=""
DISC_REALM=""
DISC_KEYTAB=""
while IFS='=' read -r k v; do
  case "$k" in
    host) DISC_HOST="$v" ;;
    db_host) DISC_DB_HOST="$v" ;;
    db_name) DISC_DB_NAME="$v" ;;
    db_user) DISC_DB_USER="$v" ;;
    realm) DISC_REALM="$v" ;;
    keytab) DISC_KEYTAB="$v" ;;
  esac
done <<< "$DISC"

SSH_HOST="${SSH_HOST:-$DISC_HOST}"
[[ -n "$SSH_HOST" ]] || die "No HUE_SERVER host; set SSH_HOST"
HUE_DB_HOST="${HUE_DB_HOST:-${DISC_DB_HOST:-odp1}}"
HUE_DB_NAME="${HUE_DB_NAME:-${DISC_DB_NAME:-hue}}"
HUE_DB_USER="${HUE_DB_USER:-${DISC_DB_USER:-hue}}"
HUE_KEYTAB="${HUE_KEYTAB:-${DISC_KEYTAB:-/etc/security/keytabs/hue.headless.keytab}}"
HUE_PRINCIPAL="${HUE_PRINCIPAL:-hue-${CLUSTER_NAME}@${DISC_REALM:-ADSRE.COM}}"

ssh_base=(ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=20)
[[ -f "$SSH_KEY" ]] && ssh_base+=(-i "$SSH_KEY")

echo "---- Hue setup ----"
echo "    Ambari: ${AMBARI_BASE_URL} cluster=${CLUSTER_NAME}"
echo "    SSH: ${SSH_USER}@${SSH_HOST}"
echo "    DB: ${HUE_DB_USER}@${HUE_DB_HOST}/${HUE_DB_NAME}"
echo "    principal: ${HUE_PRINCIPAL}"
echo "    keytab: ${HUE_KEYTAB}"

"${ssh_base[@]}" "${SSH_USER}@${SSH_HOST}" "bash -s" <<REMOTE
set -euo pipefail
HUE_DB_HOST='${HUE_DB_HOST}'
HUE_DB_NAME='${HUE_DB_NAME}'
HUE_DB_USER='${HUE_DB_USER}'
HUE_DB_PASS='${HUE_DB_PASS}'
HUE_PRINCIPAL='${HUE_PRINCIPAL}'
HUE_KEYTAB='${HUE_KEYTAB}'
HUE_USER='${HUE_USER}'
HUE_PASSWORD='${HUE_PASSWORD}'
HUE_SKIP_KEYTAB='${HUE_SKIP_KEYTAB}'
HUE_SKIP_MIGRATE='${HUE_SKIP_MIGRATE}'

echo "[INFO] MySQL grants + database"
sudo mysql <<SQL
CREATE DATABASE IF NOT EXISTS \${HUE_DB_NAME} CHARACTER SET utf8 COLLATE utf8_general_ci;
CREATE USER IF NOT EXISTS '\${HUE_DB_USER}'@'localhost' IDENTIFIED WITH mysql_native_password BY '\${HUE_DB_PASS}';
CREATE USER IF NOT EXISTS '\${HUE_DB_USER}'@'127.0.0.1' IDENTIFIED WITH mysql_native_password BY '\${HUE_DB_PASS}';
CREATE USER IF NOT EXISTS '\${HUE_DB_USER}'@'\${HUE_DB_HOST}' IDENTIFIED WITH mysql_native_password BY '\${HUE_DB_PASS}';
ALTER USER '\${HUE_DB_USER}'@'localhost' IDENTIFIED WITH mysql_native_password BY '\${HUE_DB_PASS}';
ALTER USER '\${HUE_DB_USER}'@'127.0.0.1' IDENTIFIED WITH mysql_native_password BY '\${HUE_DB_PASS}';
ALTER USER '\${HUE_DB_USER}'@'\${HUE_DB_HOST}' IDENTIFIED WITH mysql_native_password BY '\${HUE_DB_PASS}';
GRANT ALL PRIVILEGES ON \${HUE_DB_NAME}.* TO '\${HUE_DB_USER}'@'localhost';
GRANT ALL PRIVILEGES ON \${HUE_DB_NAME}.* TO '\${HUE_DB_USER}'@'127.0.0.1';
GRANT ALL PRIVILEGES ON \${HUE_DB_NAME}.* TO '\${HUE_DB_USER}'@'\${HUE_DB_HOST}';
FLUSH PRIVILEGES;
SQL
mysql -u"\${HUE_DB_USER}" -p"\${HUE_DB_PASS}" -h"\${HUE_DB_HOST}" -e "USE \${HUE_DB_NAME}; SELECT DATABASE(), USER();" >/dev/null
echo "[OK] MySQL connectivity"

META=/etc/hue/conf/hue_metastore.ini
sudo tee "\$META" >/dev/null <<EOF
[desktop]
[[database]]
    engine=mysql
    host=\${HUE_DB_HOST}
    port=3306
    user=\${HUE_DB_USER}
    password=\${HUE_DB_PASS}
    options={}
    name=\${HUE_DB_NAME}
EOF
sudo chown hue:hadoop "\$META"
sudo chmod 600 "\$META"
echo "[OK] wrote \$META (MySQL)"

if [[ "\$HUE_SKIP_KEYTAB" != "1" ]]; then
  if command -v kadmin.local >/dev/null 2>&1 && sudo systemctl is-active krb5kdc >/dev/null 2>&1; then
    echo "[INFO] ensuring Kerberos principal/keytab \$HUE_PRINCIPAL"
    sudo kadmin.local -q "addprinc -randkey \$HUE_PRINCIPAL" >/dev/null 2>&1 || true
    sudo rm -f "\$HUE_KEYTAB"
    sudo kadmin.local -q "ktadd -k \$HUE_KEYTAB \$HUE_PRINCIPAL" >/dev/null
    sudo chown hue:hadoop "\$HUE_KEYTAB"
    sudo chmod 440 "\$HUE_KEYTAB"
    sudo -u hue kinit -kt "\$HUE_KEYTAB" "\$HUE_PRINCIPAL"
    sudo -u hue kdestroy >/dev/null 2>&1 || true
    echo "[OK] keytab \$HUE_KEYTAB"
  else
    echo "[WARN] kadmin.local/KDC not on this host; skip keytab create"
    sudo test -f "\$HUE_KEYTAB" || { echo "[FAIL] missing \$HUE_KEYTAB"; exit 2; }
  fi
fi

if [[ "\$HUE_SKIP_MIGRATE" != "1" ]]; then
  echo "[INFO] Hue syncdb + migrate"
  sudo -u hue bash -lc '
cd /var/log/hue
export HUE_CONF_DIR=/etc/hue/conf DESKTOP_LOG_DIR=/var/log/hue
/usr/odp/current/hue/build/env/bin/hue syncdb --noinput
/usr/odp/current/hue/build/env/bin/hue migrate --noinput
'
  cnt=\$(mysql -u"\${HUE_DB_USER}" -p"\${HUE_DB_PASS}" -h"\${HUE_DB_HOST}" -N -e \
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='\${HUE_DB_NAME}';")
  echo "[OK] hue schema tables=\$cnt"
fi

echo "[INFO] ensure form-login user \$HUE_USER"
sudo -u hue env HUE_CONF_DIR=/etc/hue/conf DESKTOP_LOG_DIR=/var/log/hue \
  HUE_SMOKE_USER="\$HUE_USER" HUE_SMOKE_PASSWORD="\$HUE_PASSWORD" \
  bash -c 'cd /var/log/hue && /usr/odp/current/hue/build/env/bin/hue shell' <<'PY'
from django.contrib.auth.models import User
import os
u, _ = User.objects.get_or_create(username=os.environ["HUE_SMOKE_USER"])
u.set_password(os.environ["HUE_SMOKE_PASSWORD"])
u.is_superuser = True
u.is_staff = True
u.save()
print("admin_ready", u.username)
PY
REMOTE

if [[ "$HUE_SKIP_START" != "1" ]]; then
  echo "[INFO] Ambari START HUE"
  out="$(ambari_json PUT "/api/v1/clusters/${CLUSTER_NAME}/services/HUE" \
    "{\"RequestInfo\":{\"context\":\"Start Hue (setup-hue.sh)\"},\"ServiceInfo\":{\"state\":\"STARTED\"}}")"
  rid="$(printf '%s' "$out" | python3 -c 'import sys,json; print((json.load(sys.stdin).get("Requests") or {}).get("id") or "")')"
  if [[ -n "$rid" ]]; then
    for i in $(seq 0 60); do
      st="$(ambari_json GET "/api/v1/clusters/${CLUSTER_NAME}/requests/${rid}?fields=Requests/request_status,Requests/progress_percent" \
        | python3 -c 'import sys,json; r=json.load(sys.stdin)["Requests"]; print(r.get("request_status"), r.get("progress_percent"))')"
      echo "    [$i] $st"
      case "$st" in
        COMPLETED*) break ;;
        FAILED*|ABORTED*|TIMEDOUT*) die "Ambari START failed: $st" ;;
      esac
      sleep 5
    done
  else
    echo "[WARN] Ambari START returned no request id (already STARTED?)"
  fi
  sleep 15
fi

echo "OK: Hue setup finished. Next: ./hue-sample-smoke.sh"
