#!/usr/bin/env bash
#
# Smoke: Apache Ozone CLI data path + HA roles + HTTP endpoints.
#
# Steps:
#   1) Ambari cluster name; discover OM / SCM / Recon / S3 Gateway hosts
#   2) Resolve JAVA_HOME and an OZONE_CONF_DIR that actually carries ozone.om.service.ids
#      (the shared /etc/hadoop-ozone/conf/ozone-site.xml is an empty template in ODP)
#   3) kinit as hdfs-<cluster> with the Ozone headless keytab
#   4) ozone admin om roles / scm roles - one LEADER expected per service
#   5) SPNEGO GET on OM, SCM, Recon and S3 Gateway web endpoints
#   6) Data path: volume + bucket create/info, key put, list, info, get + content
#      compare, ofs listing, then recursive cleanup
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   OZONE_ENV_FILE / OZONE_CONFIG_FILE  default <script-dir>/configs/ozone.env
#   OZONE_KEYTAB            default /etc/security/keytabs/ozone.headless.keytab
#                           (falls back to hdfs.headless.keytab)
#   OZONE_PRINCIPAL         default hdfs-<cluster>
#   OZONE_SKIP_KINIT        default 0
#   OZONE_CONF_DIR          default first /etc/hadoop-ozone/conf[/ozone.*] with a service id
#   OZONE_BIN               default ozone from PATH, else /usr/odp/current/ozone-client/bin/ozone
#   JAVA_HOME               default from ambari-agent, else /usr/lib/jvm/java-1.8.0-openjdk
#   OZONE_SERVICE_ID        default ozone.om.service.ids from the resolved conf
#   OZONE_VOLUME            default odpsmokevol<timestamp>
#   OZONE_BUCKET            default smokebucket
#   OZONE_KEY               default smoke-key
#   OZONE_KEEP_DATA         default 0 - set 1 to leave the volume behind
#   OZONE_SKIP_HTTP         default 0 - set 1 to skip the web endpoint checks
#   OZONE_SKIP_FS           default 0 - set 1 to skip the ofs listing
#   CURL_EXTRA_OPTS         e.g. -k
#
# Usage:
#   sudo ./ozone-sample-smoke.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
OZONE_ENV_FILE="${OZONE_ENV_FILE:-${OZONE_CONFIG_FILE:-${SCRIPT_DIR}/configs/ozone.env}}"

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

load_ambari_env_file() {
  local f="$1" key val line
  [[ -f "$f" ]] || return 1
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
    esac
  done <"$f"
  return 0
}

load_ozone_env_file() {
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
      OZONE_*|JAVA_HOME|CURL_EXTRA_OPTS)
        # Environment always wins over the file.
        [[ -n "${!key:-}" ]] || printf -v "$key" '%s' "$val"
        ;;
    esac
  done <"$f"
  return 0
}

xml_prop_value() {
  local file="$1" name="$2"
  [[ -r "$file" ]] || return 1
  python3 - "$file" "$name" <<'PY'
import sys, xml.etree.ElementTree as ET
path, want = sys.argv[1], sys.argv[2]
try:
    root = ET.parse(path).getroot()
except Exception:
    sys.exit(1)
for prop in root.iter("property"):
    n = prop.find("name")
    v = prop.find("value")
    if n is not None and v is not None and (n.text or "").strip() == want:
        print((v.text or "").strip())
        sys.exit(0)
sys.exit(1)
PY
}

# Port part of a host:port property; empty (and successful) when absent, so the
# caller can fall back to a default without tripping set -e / pipefail.
xml_prop_port() {
  local v
  v="$(xml_prop_value "$1" "$2" 2>/dev/null || true)"
  [[ -n "$v" ]] || return 0
  printf '%s' "${v##*:}"
}

resolve_java_home() {
  if [[ -n "${JAVA_HOME:-}" && -x "${JAVA_HOME}/bin/java" ]]; then
    return 0
  fi
  local candidate
  candidate="$(grep -h -o '"java_home"[[:space:]]*:[[:space:]]*"[^"]*"' \
    /var/lib/ambari-agent/data/command-*.json 2>/dev/null \
    | head -1 | sed 's/.*:[[:space:]]*"//; s/"$//')"
  if [[ -n "$candidate" && -x "${candidate}/bin/java" ]]; then
    export JAVA_HOME="$candidate"
    return 0
  fi
  for candidate in /usr/lib/jvm/java-1.8.0-openjdk /usr/lib/jvm/java-11-openjdk /usr/lib/jvm/java; do
    if [[ -x "${candidate}/bin/java" ]]; then
      export JAVA_HOME="$candidate"
      return 0
    fi
  done
  candidate="$(command -v java 2>/dev/null || true)"
  if [[ -n "$candidate" ]]; then
    export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$candidate")")")"
    return 0
  fi
  return 1
}

# ODP ships an empty shared ozone-site.xml; the usable client config lives in the
# per-role directories, so pick the first one that declares a service id.
resolve_ozone_conf_dir() {
  if [[ -n "${OZONE_CONF_DIR:-}" ]]; then
    return 0
  fi
  local d
  for d in /etc/hadoop-ozone/conf/ozone.om \
           /etc/hadoop-ozone/conf/ozone.scm \
           /etc/hadoop-ozone/conf/ozone.s3g \
           /etc/hadoop-ozone/conf/ozone.recon \
           /etc/hadoop-ozone/conf/ozone.datanode \
           /etc/hadoop-ozone/conf; do
    if [[ -r "${d}/ozone-site.xml" ]] \
      && xml_prop_value "${d}/ozone-site.xml" "ozone.om.service.ids" >/dev/null 2>&1; then
      export OZONE_CONF_DIR="$d"
      return 0
    fi
  done
  if [[ -r /etc/hadoop-ozone/conf/ozone-site.xml ]]; then
    export OZONE_CONF_DIR=/etc/hadoop-ozone/conf
    return 0
  fi
  return 1
}

ambari_component_hosts() {
  local service="$1" component="$2" url
  [[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]] || return 1
  url="${AMBARI_BASE_URL%/}/api/v1/clusters/${cluster}/services/${service}/components/${component}"
  curl -sS -f $CURL_EXTRA_OPTS -u "${AMBARI_USER}:${AMBARI_PASSWORD}" \
    -H "X-Requested-By: ambari" "$url" 2>/dev/null \
    | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(1)
hosts = [h.get('HostRoles', {}).get('host_name') for h in data.get('host_components', [])]
hosts = [h for h in hosts if h]
if not hosts:
    sys.exit(1)
print(' '.join(hosts))
"
}

pass=0
fail=0
skip=0
declare -a results=()

record_pass() {
  pass=$((pass + 1))
  results+=("PASS    $1")
}

record_fail() {
  fail=$((fail + 1))
  results+=("FAIL    $1")
  echo "    FAIL: $1" >&2
}

record_skip() {
  skip=$((skip + 1))
  results+=("SKIPPED $1")
}

# check <label> <command...>
check() {
  local label="$1"
  shift
  local out
  if out="$("$@" 2>&1)"; then
    record_pass "$label"
    printf '%s\n' "$out" | ozone_filter | sed 's/^/        /'
    return 0
  fi
  printf '%s\n' "$out" | ozone_filter | tail -15 | sed 's/^/        /' >&2
  record_fail "$label"
  return 1
}

# The Ozone CLI is chatty on stderr; keep only lines a human would care about.
ozone_filter() {
  grep -v -e 'NativeCodeLoader' \
          -e 'ClientTrustManager' \
          -e 'MetricsConfig' \
          -e 'MetricsSystemImpl' \
          -e 'XceiverClientMetrics' \
          -e '^$' || true
}

oz() {
  "$OZONE_BIN" "$@"
}

http_code_of() {
  curl -s -o /dev/null $CURL_EXTRA_OPTS --negotiate -u : -w '%{http_code}' "$1" 2>/dev/null || echo 000
}

http_check() {
  local label="$1" url="$2" code
  code="$(http_code_of "$url")"
  case "$code" in
    2??|3??)
      echo "        OK ${url} -> HTTP ${code}"
      record_pass "$label"
      ;;
    *)
      echo "        ${url} -> HTTP ${code}" >&2
      record_fail "${label} (HTTP ${code})"
      ;;
  esac
}

# The S3 Gateway serves the S3 REST API at "/", so GET / is ListBuckets and
# answers 403 without an AWS SigV4 signature - SPNEGO does not supply one.
# Probe the admin servlet instead, and fall back to treating 401/403 on "/"
# as proof the gateway is up and enforcing auth.
s3g_http_check() {
  local label="$1" base="$2" code
  code="$(http_code_of "${base}/jmx")"
  case "$code" in
    2??|3??)
      echo "        OK ${base}/jmx -> HTTP ${code}"
      record_pass "$label"
      return
      ;;
  esac

  code="$(http_code_of "${base}/")"
  case "$code" in
    2??|3??)
      echo "        OK ${base}/ -> HTTP ${code}"
      record_pass "$label"
      ;;
    401|403)
      echo "        OK ${base}/ -> HTTP ${code} (S3 API requires AWS SigV4; gateway up, auth enforced)"
      record_pass "$label"
      ;;
    *)
      echo "        ${base}/ -> HTTP ${code}" >&2
      record_fail "${label} (HTTP ${code})"
      ;;
  esac
}

need_cmd curl
need_cmd kinit
need_cmd python3

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""

# Defaults are applied after the env file so the file can actually override them.
load_ozone_env_file "$OZONE_ENV_FILE"
OZONE_BUCKET="${OZONE_BUCKET:-smokebucket}"
OZONE_KEY="${OZONE_KEY:-smoke-key}"
OZONE_KEEP_DATA="${OZONE_KEEP_DATA:-0}"
OZONE_SKIP_KINIT="${OZONE_SKIP_KINIT:-0}"
OZONE_SKIP_HTTP="${OZONE_SKIP_HTTP:-0}"
OZONE_SKIP_FS="${OZONE_SKIP_FS:-0}"
CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}"

if [[ -f "$AMBARI_CONFIG_FILE" ]]; then
  load_ambari_env_file "$AMBARI_CONFIG_FILE" || die "failed to read $AMBARI_CONFIG_FILE"
elif [[ -n "${CLUSTER_NAME:-}" ]]; then
  :
elif [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]]; then
  :
else
  die "Missing Ambari credentials. Create ${AMBARI_CONFIG_FILE} (copy from ${SCRIPT_DIR}/configs/ambari.env.example) or set AMBARI_USER and AMBARI_PASSWORD in the environment."
fi

AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://10.101.11.22:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"

if [[ -z "${CLUSTER_NAME:-}" ]]; then
  [[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]] || die "AMBARI_USER and AMBARI_PASSWORD must be set in ${AMBARI_CONFIG_FILE} or in the environment."
  clusters_url="${AMBARI_BASE_URL%/}/api/v1/clusters/"
  json="$(curl -sS -f $CURL_EXTRA_OPTS -u "${AMBARI_USER}:${AMBARI_PASSWORD}" \
    -H "X-Requested-By: ambari" "$clusters_url")" || die "failed to GET $clusters_url"
  cluster="$(
    printf '%s\n' "$json" | python3 -c "
import json, sys
data = json.load(sys.stdin)
items = data.get('items') or []
if not items:
    sys.exit('no clusters in Ambari response')
name = (items[0].get('Clusters') or {}).get('cluster_name')
if not name:
    sys.exit('could not parse cluster_name from Ambari response')
print(name)
"
  )" || die "could not parse cluster name from Ambari JSON"
else
  cluster="$CLUSTER_NAME"
fi

OZONE_BIN="${OZONE_BIN:-$(command -v ozone 2>/dev/null || echo /usr/odp/current/ozone-client/bin/ozone)}"
[[ -x "$OZONE_BIN" ]] || die "ozone CLI not executable: $OZONE_BIN"

resolve_java_home || die "could not resolve JAVA_HOME; set it explicitly"
resolve_ozone_conf_dir || die "could not find an ozone-site.xml; set OZONE_CONF_DIR"

OZONE_SITE="${OZONE_CONF_DIR}/ozone-site.xml"
OZONE_SERVICE_ID="${OZONE_SERVICE_ID:-$(xml_prop_value "$OZONE_SITE" "ozone.om.service.ids" 2>/dev/null || true)}"
OZONE_VOLUME="${OZONE_VOLUME:-odpsmokevol$(date +%s)}"

echo "---- Ozone sample smoke ----"
echo "    cluster:      ${cluster}"
echo "    ozone bin:    ${OZONE_BIN}"
echo "    JAVA_HOME:    ${JAVA_HOME}"
echo "    conf dir:     ${OZONE_CONF_DIR}"
echo "    om service:   ${OZONE_SERVICE_ID:-<none, non-HA>}"
echo "    volume:       ${OZONE_VOLUME}"
echo "    bucket/key:   ${OZONE_BUCKET}/${OZONE_KEY}"

export JAVA_HOME OZONE_CONF_DIR

om_hosts="$(ambari_component_hosts OZONE OZONE_MANAGER 2>/dev/null || true)"
scm_hosts="$(ambari_component_hosts OZONE OZONE_STORAGE_CONTAINER_MANAGER 2>/dev/null || true)"
recon_hosts="$(ambari_component_hosts OZONE OZONE_RECON 2>/dev/null || true)"
s3g_hosts="$(ambari_component_hosts OZONE OZONE_S3_GATEWAY 2>/dev/null || true)"
echo "    OM hosts:     ${om_hosts:-<unknown>}"
echo "    SCM hosts:    ${scm_hosts:-<unknown>}"
echo "    Recon hosts:  ${recon_hosts:-<unknown>}"
echo "    S3G hosts:    ${s3g_hosts:-<unknown>}"

if [[ "$OZONE_SKIP_KINIT" != "1" ]]; then
  keytab="${OZONE_KEYTAB:-/etc/security/keytabs/ozone.headless.keytab}"
  if [[ ! -r "$keytab" ]]; then
    keytab="/etc/security/keytabs/hdfs.headless.keytab"
  fi
  [[ -r "$keytab" ]] || die "no readable Ozone keytab; set OZONE_KEYTAB or OZONE_SKIP_KINIT=1"
  principal="${OZONE_PRINCIPAL:-hdfs-${cluster}}"
  echo "---- kinit ----"
  echo "    keytab=${keytab} principal=${principal}"
  kinit -kt "$keytab" "$principal" || die "kinit failed for ${principal}"
fi

echo ""
echo "---- HA roles ----"
if [[ -n "$OZONE_SERVICE_ID" ]]; then
  check "om roles" oz admin om roles --service-id="$OZONE_SERVICE_ID" || true
else
  check "om roles" oz admin om roles || true
fi
check "scm roles" oz admin scm roles || true

if [[ "$OZONE_SKIP_HTTP" == "1" ]]; then
  record_skip "http endpoints"
else
  echo ""
  echo "---- web endpoints (SPNEGO) ----"
  om_http_port="$(xml_prop_port "$OZONE_SITE" "ozone.om.http-address")"
  scm_http_port="$(xml_prop_port "$OZONE_SITE" "ozone.scm.http-address")"
  recon_port="$(xml_prop_port "$OZONE_SITE" "ozone.recon.http-address")"
  s3g_http_port="$(xml_prop_port "$OZONE_SITE" "ozone.s3g.http-address")"

  for h in ${om_hosts:-}; do
    http_check "om web ${h}" "http://${h}:${om_http_port:-9874}/"
  done
  for h in ${scm_hosts:-}; do
    http_check "scm web ${h}" "http://${h}:${scm_http_port:-9876}/"
  done
  for h in ${s3g_hosts:-}; do
    s3g_http_check "s3g web ${h}" "http://${h}:${s3g_http_port:-9878}"
  done
  for h in ${recon_hosts:-}; do
    http_check "recon web ${h}" "http://${h}:${recon_port:-9898}/"
    http_check "recon clusterState ${h}" "http://${h}:${recon_port:-9898}/api/v1/clusterState"
  done
fi

echo ""
echo "---- data path ----"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ozone-smoke.XXXXXX")"
cleanup_done=0
cleanup() {
  [[ "$cleanup_done" == "1" ]] && return 0
  cleanup_done=1
  if [[ "$OZONE_KEEP_DATA" != "1" && "${volume_created:-0}" == "1" ]]; then
    echo ""
    echo "---- cleanup ----"
    # key delete moves the key to the bucket trash, so the bucket needs -r.
    oz sh bucket delete -r -y "/${OZONE_VOLUME}/${OZONE_BUCKET}" 2>&1 | ozone_filter | sed 's/^/        /' || true
    oz sh volume delete "/${OZONE_VOLUME}" 2>&1 | ozone_filter | sed 's/^/        /' || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT

src="${work_dir}/in.txt"
dst="${work_dir}/out.txt"
{
  echo "odp ozone smoke ${cluster}"
  echo "generated at $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "volume=${OZONE_VOLUME} bucket=${OZONE_BUCKET} key=${OZONE_KEY}"
} >"$src"

volume_created=0
if check "volume create" oz sh volume create "/${OZONE_VOLUME}"; then
  volume_created=1
fi

if (( volume_created == 1 )); then
  check "volume info" oz sh volume info "/${OZONE_VOLUME}" >/dev/null || true
  check "bucket create" oz sh bucket create "/${OZONE_VOLUME}/${OZONE_BUCKET}" || true
  check "bucket info" oz sh bucket info "/${OZONE_VOLUME}/${OZONE_BUCKET}" >/dev/null || true
  check "key put" oz sh key put "/${OZONE_VOLUME}/${OZONE_BUCKET}/${OZONE_KEY}" "$src" || true

  if key_list="$(oz sh key list "/${OZONE_VOLUME}/${OZONE_BUCKET}" 2>&1)"; then
    if printf '%s' "$key_list" | grep -q "\"name\" : \"${OZONE_KEY}\""; then
      echo "        found key ${OZONE_KEY} in listing"
      record_pass "key list"
    else
      record_fail "key list (${OZONE_KEY} missing)"
    fi
  else
    printf '%s\n' "$key_list" | ozone_filter | tail -10 | sed 's/^/        /' >&2
    record_fail "key list"
  fi

  check "key info" oz sh key info "/${OZONE_VOLUME}/${OZONE_BUCKET}/${OZONE_KEY}" >/dev/null || true

  if oz sh key get "/${OZONE_VOLUME}/${OZONE_BUCKET}/${OZONE_KEY}" "$dst" >/dev/null 2>&1 \
    && cmp -s "$src" "$dst"; then
    echo "        content matches ($(wc -c <"$src" | tr -d ' ') bytes)"
    record_pass "key get + content compare"
  else
    record_fail "key get + content compare"
  fi

  if [[ "$OZONE_SKIP_FS" == "1" ]]; then
    record_skip "ofs listing"
  elif [[ -n "$OZONE_SERVICE_ID" ]]; then
    check "ofs listing" oz fs -ls "ofs://${OZONE_SERVICE_ID}/${OZONE_VOLUME}/${OZONE_BUCKET}/" || true
  else
    record_skip "ofs listing (no ozone.om.service.ids)"
  fi

  check "key delete" oz sh key delete "/${OZONE_VOLUME}/${OZONE_BUCKET}/${OZONE_KEY}" || true
else
  record_skip "volume info"
  record_skip "bucket create"
  record_skip "bucket info"
  record_skip "key put"
  record_skip "key list"
  record_skip "key info"
  record_skip "key get + content compare"
  record_skip "ofs listing"
  record_skip "key delete"
fi

cleanup

echo ""
echo "---- summary ----"
for r in "${results[@]}"; do
  echo "    $r"
done
echo "    PASS=${pass} FAIL=${fail} SKIPPED=${skip}"

if (( fail > 0 )); then
  die "Ozone sample smoke had ${fail} failing check(s)."
fi
echo "OK: Ozone sample smoke finished (PASS=${pass} SKIPPED=${skip})."
