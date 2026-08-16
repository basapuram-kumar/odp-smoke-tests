#!/usr/bin/env bash
#
# Fix Ranger Admin truststore so Knox HTTPS "Test Connection" works.
#
# Root cause: missing /etc/ranger/admin/conf/ranger-admin-keystore.jks causes
# Ranger to set javax.net.ssl.trustStorePassword=_ and SSLContext fails.
#
# Resolves KNOX_HOST in this order:
#   1) env / CLI: KNOX_HOST or $1
#   2) configs/knox.env (KNOX_HOST or host parsed from KNOX_URL)
#   3) Ambari curl: KNOX / KNOX_GATEWAY host + gateway.port
#
# OPTIONAL / one-shot fix. Do NOT run from the connection smoke test.
# Use only when Knox Test Connection fails with truststore password errors
# (Password verification failed / missing ranger-admin-keystore.jks).
#
# Usage (on Ranger Admin host, as root):
#   ./fix-ranger-knox-truststore.sh
#   ./fix-ranger-knox-truststore.sh upg127n1
#   KNOX_HOST=upg127n1 ./fix-ranger-knox-truststore.sh
#
# Everyday Knox/Ranger connection checks:
#   ./ranger-plugin-connection-smoke.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
KNOX_ENV_FILE="${KNOX_ENV_FILE:-${SCRIPT_DIR}/configs/knox.env}"

TS="${RANGER_TRUSTSTORE_FILE:-/etc/ranger/admin/conf/ranger-admin-keystore.jks}"
PASS="${RANGER_TRUSTSTORE_PASSWORD:-changeit}"
CACERTS="${JAVA_CACERTS:-}"
RANGER_SVC="${RANGER_ADMIN_SVC:-/usr/odp/current/ranger-admin/ews/ranger-admin-services.sh}"

die() { echo "[ERROR] $*" >&2; exit 1; }
info() { echo "[INFO] $*"; }

find_cacerts() {
  local c java_home
  if [[ -n "${CACERTS}" && -f "${CACERTS}" ]]; then
    printf '%s' "$CACERTS"
    return 0
  fi
  for c in \
    /etc/pki/java/cacerts \
    /etc/ssl/certs/java/cacerts \
    /etc/pki/ca-trust/extracted/java/cacerts
  do
    if [[ -f "$c" ]]; then
      printf '%s' "$c"
      return 0
    fi
  done
  java_home="${JAVA_HOME:-}"
  if [[ -z "$java_home" ]] && command -v java >/dev/null 2>&1; then
    java_home="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"
  fi
  for c in \
    "${java_home}/lib/security/cacerts" \
    "${java_home}/jre/lib/security/cacerts" \
    /usr/lib/jvm/java-1.8.0-openjdk/jre/lib/security/cacerts \
    /usr/lib/jvm/jre/lib/security/cacerts \
    /usr/lib/jvm/java/jre/lib/security/cacerts
  do
    if [[ -n "$c" && -f "$c" ]]; then
      printf '%s' "$c"
      return 0
    fi
  done
  return 1
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

load_kv_file() {
  local f="$1"
  [[ -f "$f" ]] || return 0
  local key val line
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
      AMBARI_BASE_URL|AMBARI_USER|AMBARI_PASSWORD|CLUSTER_NAME|KNOX_HOST|KNOX_PORT|KNOX_URL|KNOX_GATEWAY)
        [[ -n "${!key:-}" ]] || printf -v "$key" '%s' "$val"
        ;;
    esac
  done <"$f"
}

host_from_url() {
  # https://host:8443/gateway -> host
  local url="$1"
  url="${url#*://}"
  url="${url%%/*}"
  url="${url%%:*}"
  printf '%s' "$url"
}

port_from_url() {
  local url="$1" rest
  url="${url#*://}"
  rest="${url%%/*}"
  if [[ "$rest" == *:* ]]; then
    printf '%s' "${rest##*:}"
  else
    printf '%s' ""
  fi
}

ambari_get() {
  curl -sS -f -u "${AMBARI_USER}:${AMBARI_PASSWORD}" \
    -H "X-Requested-By: ambari" "$1"
}

discover_cluster() {
  if [[ -n "${CLUSTER_NAME:-}" ]]; then
    printf '%s' "$CLUSTER_NAME"
    return 0
  fi
  ambari_get "${AMBARI_BASE_URL%/}/api/v1/clusters" \
    | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['items'][0]['Clusters']['cluster_name'])"
}

discover_knox_host_ambari() {
  local cluster="$1"
  ambari_get "${AMBARI_BASE_URL%/}/api/v1/clusters/${cluster}/services/KNOX/components/KNOX_GATEWAY" \
    | python3 -c "
import json,sys
d=json.load(sys.stdin)
hosts=[h.get('HostRoles',{}).get('host_name') for h in d.get('host_components',[])]
hosts=[h for h in hosts if h]
if not hosts:
  sys.exit(1)
print(hosts[0])
"
}

discover_knox_port_ambari() {
  local cluster="$1" tag props
  tag="$(ambari_get "${AMBARI_BASE_URL%/}/api/v1/clusters/${cluster}?fields=Clusters/desired_configs" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['Clusters']['desired_configs']['gateway-site']['tag'])")"
  ambari_get "${AMBARI_BASE_URL%/}/api/v1/clusters/${cluster}/configurations?type=gateway-site&tag=${tag}" \
    | python3 -c "
import json,sys
p=json.load(sys.stdin)['items'][0]['properties']
print(p.get('gateway.port','8443'))
"
}

# --- load configs ---
load_kv_file "$AMBARI_CONFIG_FILE"
load_kv_file "$KNOX_ENV_FILE"

# CLI host wins if given
if [[ $# -ge 1 && -n "${1:-}" ]]; then
  KNOX_HOST="$1"
fi
if [[ $# -ge 2 && -n "${2:-}" ]]; then
  KNOX_PORT="$2"
fi

# Alias from knox.env
if [[ -z "${KNOX_HOST:-}" && -n "${KNOX_GATEWAY:-}" ]]; then
  KNOX_HOST="$KNOX_GATEWAY"
fi

# Parse KNOX_URL if host/port still missing
if [[ -n "${KNOX_URL:-}" ]]; then
  if [[ -z "${KNOX_HOST:-}" ]]; then
    KNOX_HOST="$(host_from_url "$KNOX_URL")"
  fi
  if [[ -z "${KNOX_PORT:-}" ]]; then
    KNOX_PORT="$(port_from_url "$KNOX_URL")"
  fi
fi

# Ambari discovery
if [[ -z "${KNOX_HOST:-}" || -z "${KNOX_PORT:-}" ]]; then
  [[ -n "${AMBARI_BASE_URL:-}" ]] || die "set AMBARI_BASE_URL in configs/ambari.env or set KNOX_HOST in configs/knox.env"
  [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]] || die "set AMBARI_USER/AMBARI_PASSWORD"
  info "Discovering Knox from Ambari ${AMBARI_BASE_URL}"
  CLUSTER_NAME="$(discover_cluster)"
  info "Cluster=${CLUSTER_NAME}"
  if [[ -z "${KNOX_HOST:-}" ]]; then
    KNOX_HOST="$(discover_knox_host_ambari "$CLUSTER_NAME")" \
      || die "no KNOX_GATEWAY host in Ambari; set KNOX_HOST in configs/knox.env"
  fi
  if [[ -z "${KNOX_PORT:-}" ]]; then
    KNOX_PORT="$(discover_knox_port_ambari "$CLUSTER_NAME" 2>/dev/null || echo 8443)"
  fi
fi

KNOX_PORT="${KNOX_PORT:-8443}"
ALIAS="knox-${KNOX_HOST}"

info "KNOX_HOST=${KNOX_HOST}"
info "KNOX_PORT=${KNOX_PORT}"
info "Truststore=${TS}"

CACERTS="$(find_cacerts)" || die "cacerts not found (tried /etc/pki/java/cacerts, JAVA_HOME, etc). Set JAVA_CACERTS=/path/to/cacerts"
info "Using cacerts=${CACERTS}"
[[ "$(id -u)" -eq 0 ]] || die "run as root (needed for truststore + ranger restart)"

# Ambari sometimes leaves ranger-admin-keystore.jks as a symlink to system
# cacerts. Never write through that symlink - replace it with a real copy.
if [[ -L "$TS" ]]; then
  info "Removing symlink $TS -> $(readlink "$TS" || true)"
  rm -f "$TS"
elif [[ -e "$TS" ]]; then
  info "Backing up existing truststore to ${TS}.bak.$$"
  cp -a "$TS" "${TS}.bak.$$" || true
fi

info "Creating truststore from cacerts"
cp -a "$CACERTS" "$TS"
chown ranger:ranger "$TS"
chmod 640 "$TS"

info "Fetching Knox cert from ${KNOX_HOST}:${KNOX_PORT}"
echo | openssl s_client -connect "${KNOX_HOST}:${KNOX_PORT}" -servername "${KNOX_HOST}" 2>/dev/null \
  | openssl x509 > /tmp/knox.crt
grep -q "BEGIN CERTIFICATE" /tmp/knox.crt || die "could not fetch Knox certificate"

info "Importing cert alias=${ALIAS}"
# delete old alias if present
keytool -delete -alias "$ALIAS" -keystore "$TS" -storepass "$PASS" >/dev/null 2>&1 || true
keytool -importcert -noprompt -alias "$ALIAS" \
  -file /tmp/knox.crt \
  -keystore "$TS" \
  -storepass "$PASS"

keytool -list -keystore "$TS" -storepass "$PASS" | grep -i "$ALIAS" || true

if [[ ! -x "$RANGER_SVC" ]]; then
  info "Ranger service script missing: $RANGER_SVC"
  info "Restart RANGER_ADMIN from Ambari, then Test Connection"
  exit 0
fi

info "Restarting Ranger Admin"
sudo -u ranger "$RANGER_SVC" stop || true
sleep 3
pkill -f proc_rangeradmin 2>/dev/null || true
sleep 2
sudo -u ranger bash -c "cd $(dirname "$RANGER_SVC") && ./ranger-admin-services.sh start"
sleep 8

PID="$(pgrep -f proc_rangeradmin | head -1 || true)"
[[ -n "$PID" ]] || die "Ranger Admin did not start"
info "Ranger Admin pid=${PID}"
sudo -u ranger jcmd "$PID" VM.system_properties 2>/dev/null \
  | grep -E 'javax.net.ssl.trustStore(Password)?=' || true

info "Done. knox.url should be: https://${KNOX_HOST}:${KNOX_PORT}/gateway/admin/api/v1/topologies"
info "Retest Knox in Ranger UI."
