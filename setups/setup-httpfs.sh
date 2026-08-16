#!/usr/bin/env bash
#
# Setup helper: make Ambari HttpFS startable on Kerberized ODP.
#
# Fixes the common post-install gap:
#   Ambari HttpFS START fails with:
#     Kerberos initialization failed ... httpfs/<host>@REALM
#     from keytab /etc/security/keytabs/httpfs.service.keytab
#     (Unable to obtain password from user)
#   Ambari surfaces that as: Cannot set priority of httpfs process
#
# When Hue (or other clients) need HttpFS, create the missing service
# principal/keytab, then Ambari START HTTPFS.
#
# Not a smoke test. Prefer ./httpfs-sample-smoke.sh afterward.
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   HTTPFS_ENV_FILE / HTTPFS_CONFIG_FILE  default <repo>/configs/httpfs.env
#   SSH_USER / SSH_KEY / SSH_HOST
#   HTTPFS_PRINCIPAL / HTTPFS_KEYTAB
#   HTTPFS_SKIP_KEYTAB=1 / HTTPFS_SKIP_START=1
#
# Usage:
#   ./setups/setup-httpfs.sh
#   SSH_KEY=$HOME/Downloads/usdc.pem ./setups/setup-httpfs.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${REPO_DIR}/configs/ambari.env}"
HTTPFS_ENV_FILE="${HTTPFS_ENV_FILE:-${HTTPFS_CONFIG_FILE:-${REPO_DIR}/configs/httpfs.env}}"

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
      HTTPFS_*|SSH_*)
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
load_env_file "$HTTPFS_ENV_FILE"
[[ -f "$AMBARI_CONFIG_FILE" ]] && load_env_file "$AMBARI_CONFIG_FILE"

AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://127.0.0.1:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-admin}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-admin}}"
CLUSTER_NAME="${CLUSTER_NAME:-}"
SSH_USER="${SSH_USER:-acceldata}"
SSH_KEY="${SSH_KEY:-${HOME}/Downloads/usdc.pem}"
HTTPFS_KEYTAB="${HTTPFS_KEYTAB:-/etc/security/keytabs/httpfs.service.keytab}"
HTTPFS_SKIP_KEYTAB="${HTTPFS_SKIP_KEYTAB:-0}"
HTTPFS_SKIP_START="${HTTPFS_SKIP_START:-0}"

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
    items = get(
        f"/api/v1/clusters/{qc}/configurations?type={urllib.parse.quote(ctype)}&tag={urllib.parse.quote(tag)}"
    ).get("items") or []
    return (items[0].get("properties") or {}) if items else {}

site = props("httpfs-site")
kenv = props("kerberos-env")
hc = get(
    f"/api/v1/clusters/{qc}/host_components"
    f"?HostRoles/component_name=HTTPFS_GATEWAY"
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
realm = (kenv.get("realm") or site.get("kerberos.realm") or "ADSRE.COM").strip()
keytab = (site.get("httpfs.hadoop.authentication.kerberos.keytab") or
          "/etc/security/keytabs/httpfs.service.keytab").strip()
princ_tmpl = (site.get("httpfs.hadoop.authentication.kerberos.principal") or
              "httpfs/_HOST@${realm}").strip()
print("host=%s" % host)
print("realm=%s" % realm)
print("keytab=%s" % keytab)
print("princ_tmpl=%s" % princ_tmpl)
PY
)"

DISC_HOST=""
DISC_REALM=""
DISC_KEYTAB=""
DISC_PRINC_TMPL=""
while IFS='=' read -r k v; do
  case "$k" in
    host) DISC_HOST="$v" ;;
    realm) DISC_REALM="$v" ;;
    keytab) DISC_KEYTAB="$v" ;;
    princ_tmpl) DISC_PRINC_TMPL="$v" ;;
  esac
done <<< "$DISC"

SSH_HOST="${SSH_HOST:-$DISC_HOST}"
[[ -n "$SSH_HOST" ]] || die "No HTTPFS_GATEWAY host; set SSH_HOST"
HTTPFS_KEYTAB="${HTTPFS_KEYTAB:-${DISC_KEYTAB:-/etc/security/keytabs/httpfs.service.keytab}}"
# Build principal: httpfs/_HOST@REALM -> httpfs/<short-host>@REALM
SHORT_HOST="${SSH_HOST%%.*}"
PRINC_TMPL="${DISC_PRINC_TMPL:-httpfs/_HOST@${DISC_REALM:-ADSRE.COM}}"
PRINC_TMPL="${PRINC_TMPL//_HOST/$SHORT_HOST}"
PRINC_TMPL="${PRINC_TMPL//\$\{realm\}/${DISC_REALM:-ADSRE.COM}}"
HTTPFS_PRINCIPAL="${HTTPFS_PRINCIPAL:-$PRINC_TMPL}"

ssh_base=(ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=20)
[[ -f "$SSH_KEY" ]] && ssh_base+=(-i "$SSH_KEY")

echo "---- HttpFS setup ----"
echo "    Ambari: ${AMBARI_BASE_URL} cluster=${CLUSTER_NAME}"
echo "    SSH: ${SSH_USER}@${SSH_HOST}"
echo "    principal: ${HTTPFS_PRINCIPAL}"
echo "    keytab: ${HTTPFS_KEYTAB}"

if [[ "$HTTPFS_SKIP_KEYTAB" != "1" ]]; then
  "${ssh_base[@]}" "${SSH_USER}@${SSH_HOST}" "bash -s" <<REMOTE
set -euo pipefail
HTTPFS_PRINCIPAL='${HTTPFS_PRINCIPAL}'
HTTPFS_KEYTAB='${HTTPFS_KEYTAB}'

if command -v kadmin.local >/dev/null 2>&1 && sudo systemctl is-active krb5kdc >/dev/null 2>&1; then
  echo "[INFO] ensuring Kerberos principal/keytab \$HTTPFS_PRINCIPAL"
  sudo kadmin.local -q "addprinc -randkey \$HTTPFS_PRINCIPAL" >/dev/null 2>&1 || true
  sudo rm -f "\$HTTPFS_KEYTAB"
  sudo kadmin.local -q "ktadd -k \$HTTPFS_KEYTAB \$HTTPFS_PRINCIPAL" >/dev/null
  sudo chown httpfs:hadoop "\$HTTPFS_KEYTAB"
  sudo chmod 440 "\$HTTPFS_KEYTAB"
  sudo -u httpfs kinit -kt "\$HTTPFS_KEYTAB" "\$HTTPFS_PRINCIPAL"
  sudo -u httpfs kdestroy >/dev/null 2>&1 || true
  echo "[OK] keytab \$HTTPFS_KEYTAB"
else
  echo "[WARN] kadmin.local/KDC not on this host; skip keytab create"
  sudo test -f "\$HTTPFS_KEYTAB" || { echo "[FAIL] missing \$HTTPFS_KEYTAB"; exit 2; }
fi
REMOTE
fi

if [[ "$HTTPFS_SKIP_START" != "1" ]]; then
  echo "[INFO] Ambari START HTTPFS"
  out="$(ambari_json PUT "/api/v1/clusters/${CLUSTER_NAME}/services/HTTPFS" \
    "{\"RequestInfo\":{\"context\":\"Start HttpFS (setup-httpfs.sh)\"},\"ServiceInfo\":{\"state\":\"STARTED\"}}")"
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
  sleep 5
fi

echo "OK: HttpFS setup finished. Next: ./httpfs-sample-smoke.sh"
