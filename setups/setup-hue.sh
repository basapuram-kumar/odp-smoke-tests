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
HUE_DB_HOST="${HUE_DB_HOST:-}"
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
hosts_info = {
    h["Hosts"]["host_name"]: (h["Hosts"].get("ip") or h["Hosts"]["host_name"])
    for h in get(
        f"/api/v1/clusters/{qc}/hosts?fields=Hosts/host_name,Hosts/ip"
    ).get("items") or []
}
host = ""
host_name = ""
for it in hc.get("items") or []:
    hr = it.get("HostRoles") or {}
    cand = hr.get("public_host_name") or hr.get("host_name")
    if (hr.get("state") or "").upper() == "STARTED" and cand:
        host = cand
        host_name = hr.get("host_name") or cand
        break
if not host:
    for it in hc.get("items") or []:
        hr = it.get("HostRoles") or {}
        host = hr.get("public_host_name") or hr.get("host_name") or ""
        if host:
            host_name = hr.get("host_name") or host
            break
print("host=%s" % host)
print("host_ip=%s" % hosts_info.get(host_name, host))
print("db_host=%s" % (desktop.get("db_host") or "").strip())
print("db_name=%s" % (desktop.get("db_name") or "hue").strip())
print("db_user=%s" % (desktop.get("db_user") or "hue").strip())
print("realm=%s" % (kenv.get("realm") or "ADSRE.COM").strip())
print("keytab=%s" % (envp.get("hue_user_keytab") or "/etc/security/keytabs/hue.headless.keytab").strip())
PY
)"

DISC_HOST=""
DISC_HOST_IP=""
DISC_DB_HOST=""
DISC_DB_NAME=""
DISC_DB_USER=""
DISC_REALM=""
DISC_KEYTAB=""
while IFS='=' read -r k v; do
  case "$k" in
    host) DISC_HOST="$v" ;;
    host_ip) DISC_HOST_IP="$v" ;;
    db_host) DISC_DB_HOST="$v" ;;
    db_name) DISC_DB_NAME="$v" ;;
    db_user) DISC_DB_USER="$v" ;;
    realm) DISC_REALM="$v" ;;
    keytab) DISC_KEYTAB="$v" ;;
  esac
done <<< "$DISC"

SSH_HOST="${SSH_HOST:-${DISC_HOST_IP:-$DISC_HOST}}"
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

ambari_host_ip() {
  ambari_json GET \
    "/api/v1/clusters/${CLUSTER_NAME}/hosts?fields=Hosts/host_name,Hosts/ip" \
    | python3 -c '
import json, sys
want = sys.argv[1]
short = want.split(".")[0]
for item in json.load(sys.stdin).get("items") or []:
    host = item.get("Hosts") or {}
    name = str(host.get("host_name") or "")
    if name == want or name.split(".")[0] == short:
        print(str(host.get("ip") or ""))
        break
' "$1"
}

mysqld_listening() {
  "${ssh_base[@]}" "${SSH_USER}@$1" \
    "ss -lnt 2>/dev/null | awk '\$4 ~ /:3306$/ {found=1} END {exit !found}'"
}

if ! "${ssh_base[@]}" "${SSH_USER}@${SSH_HOST}" \
    "getent hosts '${HUE_DB_HOST}' >/dev/null 2>&1"; then
  if mysqld_listening "$SSH_HOST"; then
    echo "[WARN] DB host ${HUE_DB_HOST} does not resolve; using local MySQL at 127.0.0.1"
    HUE_DB_HOST=127.0.0.1
  else
    die "DB host ${HUE_DB_HOST} does not resolve and no local MySQL is listening"
  fi
fi

# The metastore usually lives on another node than HUE_SERVER, so the database
# and grants have to be created where mysqld actually runs. Ambari reports the
# metastore by host name; prefer the IP it reports for that host.
case "$HUE_DB_HOST" in
  127.0.0.1|localhost)
    DB_SSH_HOST="$SSH_HOST"
    ;;
  *)
    DB_SSH_HOST="$(ambari_host_ip "$HUE_DB_HOST" || true)"
    [[ -n "$DB_SSH_HOST" ]] || DB_SSH_HOST="$HUE_DB_HOST"
    ;;
esac

if ! mysqld_listening "$DB_SSH_HOST"; then
  if [[ "$DB_SSH_HOST" != "$SSH_HOST" ]] && mysqld_listening "$SSH_HOST"; then
    echo "[WARN] no MySQL on ${HUE_DB_HOST} (${DB_SSH_HOST}); using the local"
    echo "[WARN] server on the Hue host instead"
    HUE_DB_HOST=127.0.0.1
    DB_SSH_HOST="$SSH_HOST"
  else
    die "no MySQL listening on ${HUE_DB_HOST} (${DB_SSH_HOST}) or the Hue host;
start the metastore server, or point HUE_DB_HOST at the host that runs it"
  fi
fi
echo "    metastore server: ${HUE_DB_HOST} (ssh ${DB_SSH_HOST})"

# MySQL authorizes by the address a client connects FROM, so the Hue host needs
# its own grants; granting only on the database host locks Hue out.
HUE_CLIENT_ORIGINS="localhost 127.0.0.1"
for origin in "$SSH_HOST" "$DISC_HOST" "${DISC_HOST%%.*}" "$HUE_DB_HOST"; do
  [[ -n "$origin" ]] || continue
  [[ " $HUE_CLIENT_ORIGINS " == *" $origin "* ]] && continue
  HUE_CLIENT_ORIGINS="${HUE_CLIENT_ORIGINS} ${origin}"
done

echo "[INFO] persisting MySQL metastore settings in Ambari hue-desktop-site"
python3 - "$AMBARI_BASE_URL" "$AMBARI_USER" "$AMBARI_PASSWORD" "$CLUSTER_NAME" \
  "$HUE_DB_HOST" "$HUE_DB_NAME" "$HUE_DB_USER" "$HUE_DB_PASS" <<'PY'
import base64
import json
import sys
import time
import urllib.parse
import urllib.request

base, user, password, cluster, db_host, db_name, db_user, db_password = sys.argv[1:9]
auth = base64.b64encode(f"{user}:{password}".encode()).decode()


def req(method, path, body=None):
    request = urllib.request.Request(base.rstrip("/") + path, method=method)
    request.add_header("Authorization", "Basic " + auth)
    request.add_header("X-Requested-By", "ambari")
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        request.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(request, data=data, timeout=120) as response:
        raw = response.read()
        return json.loads(raw) if raw.strip() else {}


qc = urllib.parse.quote(cluster, safe="")
desired = req(
    "GET", f"/api/v1/clusters/{qc}?fields=Clusters/desired_configs"
)["Clusters"]["desired_configs"]
ctype = "hue-desktop-site"
tag = desired[ctype]["tag"]
items = req(
    "GET",
    f"/api/v1/clusters/{qc}/configurations"
    f"?type={urllib.parse.quote(ctype)}&tag={urllib.parse.quote(tag)}",
).get("items") or []
if not items:
    raise RuntimeError("Ambari hue-desktop-site configuration not found")
props = dict(items[0].get("properties") or {})
props.update(
    {
        "DB_FLAVOUR": "mysql",
        "db_host": db_host,
        "db_port": "3306",
        "db_name": db_name,
        "db_user": db_user,
        "db_password": db_password,
        "db_password_script": "",
        "db_options": props.get("db_options") or "{}",
    }
)
new_tag = "version" + str(int(time.time() * 1000))
req(
    "PUT",
    f"/api/v1/clusters/{qc}",
    {
        "Clusters": {
            "desired_config": [
                {
                    "type": ctype,
                    "tag": new_tag,
                    "properties": props,
                    "service_config_version_note": (
                        "setup-hue.sh: configure MySQL metastore"
                    ),
                }
            ]
        }
    },
)
print("[OK] updated hue-desktop-site DB_FLAVOUR=mysql")
PY

echo "[INFO] MySQL grants + database on ${DB_SSH_HOST}"
"${ssh_base[@]}" "${SSH_USER}@${DB_SSH_HOST}" "bash -s" <<REMOTEDB
set -euo pipefail
DB_NAME='${HUE_DB_NAME}'
DB_USER='${HUE_DB_USER}'
DB_PASS='${HUE_DB_PASS}'
CLIENTS='${HUE_CLIENT_ORIGINS}'

MYSQL_ADMIN=(sudo mysql)
if ! "\${MYSQL_ADMIN[@]}" -e 'SELECT 1' >/dev/null 2>&1; then
  # A packaged server may listen on TCP while the client looks for a socket
  # path that this distribution does not use.
  if sudo mysql -h 127.0.0.1 -e 'SELECT 1' >/dev/null 2>&1; then
    MYSQL_ADMIN=(sudo mysql -h 127.0.0.1)
  else
    echo "[FAIL] cannot open an admin MySQL session on \$(hostname -s)"
    sudo mysql -e 'SELECT 1' 2>&1 | tail -n 3 || true
    exit 2
  fi
fi

# MariaDB rejects "IDENTIFIED WITH mysql_native_password BY"; MySQL 8 defaults
# to caching_sha2, which the Hue client cannot use.
if "\${MYSQL_ADMIN[@]}" -N -B -e 'SELECT VERSION()' 2>/dev/null | grep -qi mariadb; then
  AUTH="IDENTIFIED BY"
else
  AUTH="IDENTIFIED WITH mysql_native_password BY"
fi

{
  echo "CREATE DATABASE IF NOT EXISTS \${DB_NAME} CHARACTER SET utf8 COLLATE utf8_general_ci;"
  for origin in \$CLIENTS; do
    echo "CREATE USER IF NOT EXISTS '\${DB_USER}'@'\${origin}' \${AUTH} '\${DB_PASS}';"
    echo "ALTER USER '\${DB_USER}'@'\${origin}' \${AUTH} '\${DB_PASS}';"
    echo "GRANT ALL PRIVILEGES ON \${DB_NAME}.* TO '\${DB_USER}'@'\${origin}';"
  done
  echo "FLUSH PRIVILEGES;"
} | "\${MYSQL_ADMIN[@]}"
echo "[OK] database \${DB_NAME} and grants for: \${CLIENTS}"
REMOTEDB

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

if ! getent hosts "\$HUE_DB_HOST" >/dev/null 2>&1; then
  # Already validated from this host before the grants ran, so a failure here
  # means DNS changed underneath us; switching hosts now would use a database
  # that was never granted to.
  echo "[FAIL] DB host \$HUE_DB_HOST no longer resolves from \$(hostname -s)"
  exit 2
fi

HUE_BIN=""
for CANDIDATE in \
  /usr/odp/current/hue/build/env/bin/hue \
  /usr/odp/current/hue-server/build/env/bin/hue \
  /usr/lib/hue/build/env/bin/hue \
  /usr/odp/*/hue/build/env/bin/hue; do
  if [[ -x "\$CANDIDATE" ]]; then
    HUE_BIN="\$CANDIDATE"
    break
  fi
done
[[ -n "\$HUE_BIN" ]] || {
  echo "[FAIL] Hue launcher not found under /usr/odp/current, /usr/lib, or stack version"
  exit 2
}
echo "[INFO] Hue launcher: \$HUE_BIN"

# Hue talks to MySQL through its own Python driver; the CLI is only used here to
# prove the grants cover this host, so its absence is not fatal.
if command -v mysql >/dev/null 2>&1; then
  HAVE_MYSQL_CLI=1
else
  HAVE_MYSQL_CLI=0
  echo "[WARN] no mysql client on \$(hostname -s); skipping the connectivity check"
fi

if [[ "\$HAVE_MYSQL_CLI" == "1" ]]; then
  echo "[INFO] MySQL connectivity from the Hue host"
  if ! mysql -u"\${HUE_DB_USER}" -p"\${HUE_DB_PASS}" -h"\${HUE_DB_HOST}" \
      -e "USE \${HUE_DB_NAME}; SELECT DATABASE(), USER();" >/dev/null; then
    echo "[FAIL] \${HUE_DB_USER}@\${HUE_DB_HOST} refused the connection from \$(hostname -s)"
    echo "[FAIL] the grant must name the address this host connects from"
    exit 2
  fi
  echo "[OK] MySQL connectivity"
fi

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
  if command -v kadmin.local >/dev/null 2>&1 && {
      systemctl is-active --quiet krb5kdc 2>/dev/null \
        || systemctl is-active --quiet krb5-kdc 2>/dev/null;
    }; then
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
  sudo -u hue env HUE_CONF_DIR=/etc/hue/conf DESKTOP_LOG_DIR=/var/log/hue \
    bash -c 'cd /var/log/hue && "\$1" syncdb --noinput && "\$1" migrate --noinput' \
    _ "\$HUE_BIN"
  if [[ "\$HAVE_MYSQL_CLI" == "1" ]]; then
    cnt=\$(mysql -u"\${HUE_DB_USER}" -p"\${HUE_DB_PASS}" -h"\${HUE_DB_HOST}" -N -e \
      "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='\${HUE_DB_NAME}';")
    echo "[OK] hue schema tables=\$cnt"
  else
    echo "[OK] hue syncdb + migrate finished"
  fi
fi

echo "[INFO] ensure form-login user \$HUE_USER"
sudo -u hue env HUE_CONF_DIR=/etc/hue/conf DESKTOP_LOG_DIR=/var/log/hue \
  HUE_SMOKE_USER="\$HUE_USER" HUE_SMOKE_PASSWORD="\$HUE_PASSWORD" \
  bash -c 'cd /var/log/hue && "\$1" shell' _ "\$HUE_BIN" <<'PY'
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
  echo "[INFO] verify Hue process and HTTP port"
  code=""
  healthy=0
  for i in $(seq 1 15); do
    if "${ssh_base[@]}" "${SSH_USER}@${SSH_HOST}" \
        "test -s /var/run/hue/hue-server.pid && sudo kill -0 \$(cat /var/run/hue/hue-server.pid) 2>/dev/null"; then
      code="$("${ssh_base[@]}" "${SSH_USER}@${SSH_HOST}" \
        "curl -sS -o /dev/null -w '%{http_code}' --max-time 10 http://127.0.0.1:8888/ || true")"
      if [[ "$code" =~ ^(200|302)$ ]]; then
        healthy=1
        break
      fi
    fi
    echo "    [$i] waiting for Hue HTTP port 8888"
    sleep 3
  done
  if [[ "$healthy" != "1" ]]; then
    "${ssh_base[@]}" "${SSH_USER}@${SSH_HOST}" \
      "tail -n 40 /var/log/hue/error.log /var/log/hue/runcpserver.out /var/log/hue/kt_renewer.out 2>/dev/null" \
      || true
    die "Ambari START completed but Hue is not healthy on port 8888"
  fi
  echo "[OK] Hue supervisor active; HTTP ${code}"
fi

echo "OK: Hue setup finished. Next: ./hue-sample-smoke.sh"
