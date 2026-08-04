#!/usr/bin/env bash
#
# Smoke: Ranger KMS - key lifecycle + sample encryption-zone operations.
#
# Steps:
#   1) Ambari discovery of RANGER_KMS_SERVER hosts, kms_port, and SSL flag
#      (or use KMS_PROVIDER / core-site hadoop.security.key.provider.path)
#   2) Optional HTTP probe of each KMS /kms/v1/keys/names endpoint
#   3) kinit as rangerkms/<FQDN> (Ranger maps this to keyadmin, which has
#      CREATE_KEY / GET_KEYS). Falls back to hdfs-<cluster> when the
#      rangerkms keytab is not on this host.
#   4) hadoop key create / list / describe / roll
#   5) Optional HDFS encryption zone:
#        - hdfs-<cluster>: mkdir, createZone, chmod
#        - Ranger Admin: temporary KMS policy granting ambari-qa DECRYPT_EEK
#          (hdfs is blacklisted for DECRYPT_EEK in dbks-site by design)
#        - ambari-qa-<cluster>: put / get / content compare, listZones
#   6) Cleanup: drop temp Ranger policy, remove EZ path, delete smoke key
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   KMS_ENV_FILE / KMS_CONFIG_FILE   default <script-dir>/configs/ranger-kms.env
#   RANGER_ENV_FILE / RANGER_CONFIG_FILE  default <script-dir>/configs/ranger.env
#   KMS_PROVIDER                     skip discovery when set (kms://http@host:9292/kms)
#   KMS_HOSTS                        space/comma list; used with KMS_PORT when provider unset
#   KMS_PORT                         default 9292 (or from Ambari kms-env)
#   KMS_SSL                          0|1 - https scheme when building provider (default from Ambari)
#   KMS_KEYTAB                       default rangerkms.service.keytab if readable, else hdfs headless
#   KMS_PRINCIPAL                    default principal from KMS_KEYTAB (rangerkms/<FQDN> or hdfs-<cluster>)
#   KMS_HDFS_KEYTAB                  default /etc/security/keytabs/hdfs.headless.keytab (EZ create)
#   KMS_HDFS_PRINCIPAL               default hdfs-<cluster>
#   KMS_CLIENT_KEYTAB                default /etc/security/keytabs/smokeuser.headless.keytab (EZ put/get)
#   KMS_CLIENT_PRINCIPAL             default ambari-qa-<cluster>
#   KMS_CLIENT_USER                  Ranger/KMS short user for DECRYPT_EEK (default ambari-qa)
#   RANGER_BASE_URL / RANGER_USER / RANGER_PASSWORD
#                                    used to create a temporary KMS decrypt policy for the smoke key
#   KMS_SKIP_RANGER_POLICY           default 0 - set 1 to skip creating the temp policy
#   KMS_POLICY_WAIT_SECONDS          default 45 - wait/retry for KMS policy cache refresh
#   KMS_SKIP_KINIT                   default 0
#   KMS_KEY_NAME                     default odp_kms_smoke_<timestamp>
#   KMS_KEY_SIZE                     default 256
#   KMS_CIPHER                       default AES/CTR/NoPadding
#   KMS_EZ_PATH                      default /tmp/odp_kms_smoke_ez_<timestamp>
#   KMS_SKIP_HTTP                    default 0
#   KMS_SKIP_EZ                      default 0 - set 1 to skip encryption-zone checks
#   KMS_SKIP_ROLL                    default 0
#   KMS_KEEP_KEY                     default 0 - set 1 to leave the key (and EZ) behind
#   CURL_EXTRA_OPTS                  e.g. -k for self-signed TLS
#
# Usage:
#   # Prefer a RANGER_KMS_SERVER host (has rangerkms.service.keytab).
#   # For EZ put/get, also set configs/ranger.env (RANGER_PASSWORD) so the script
#   # can grant ambari-qa DECRYPT_EEK on the smoke key:
#   sudo ./ranger-kms-sample-smoke.sh
#
#   # Key ops only (no encryption zone):
#   KMS_SKIP_EZ=1 sudo -E ./ranger-kms-sample-smoke.sh
#
#   # Explicit provider (no Ambari):
#   CLUSTER_NAME=odp2007 KMS_PROVIDER=kms://http@kms-host:9292/kms sudo -E ./ranger-kms-sample-smoke.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
KMS_ENV_FILE="${KMS_ENV_FILE:-${KMS_CONFIG_FILE:-${SCRIPT_DIR}/configs/ranger-kms.env}}"
RANGER_ENV_FILE="${RANGER_ENV_FILE:-${RANGER_CONFIG_FILE:-${SCRIPT_DIR}/configs/ranger.env}}"

pass=0
fail=0
skip=0
results=()

die() {
  echo "ERROR: $*" >&2
  exit 1
}

record_pass() {
  results+=("PASS: $1")
  pass=$((pass + 1))
}

record_fail() {
  results+=("FAIL: $1")
  fail=$((fail + 1))
}

record_skip() {
  results+=("SKIPPED: $1")
  skip=$((skip + 1))
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

# Prefer a principal whose host part matches this node (or a known KMS host).
# Do not assume a fixed klist column: MIT krb5 layouts vary by version/locale.
principal_from_keytab() {
  local keytab="$1" hosts_csv="${2:-}" fqdn short
  [[ -r "$keytab" ]] || return 1
  command -v klist >/dev/null 2>&1 || return 1
  fqdn="$(hostname -f 2>/dev/null || hostname)"
  short="$(hostname -s 2>/dev/null || hostname)"
  klist -kt "$keytab" 2>/dev/null | awk -v fqdn="$fqdn" -v short="$short" -v hosts="$hosts_csv" '
    BEGIN { n_hosts = split(hosts, host_list, /[ ,]+/) }
    {
      for (i = 1; i <= NF; i++) {
        p = $i
        gsub(/^\(+/, "", p)
        gsub(/\)+$/, "", p)
        if (p !~ /@/) continue
        if (seen[p]++) continue
        order[++n] = p
        if (index(p, "/" fqdn "@") && !exact) exact = p
        if (index(p, "/" short "@") && !partial) partial = p
      }
    }
    END {
      if (exact) { print exact; exit 0 }
      if (partial) { print partial; exit 0 }
      for (h = 1; h <= n_hosts; h++) {
        if (host_list[h] == "") continue
        for (i = 1; i <= n; i++) {
          if (index(order[i], "/" host_list[h] "@")) { print order[i]; exit 0 }
        }
      }
      if (n) { print order[1]; exit 0 }
      exit 1
    }'
}

authz_hint() {
  local out="$1"
  if printf '%s\n' "$out" | grep -Eqi "not allowed to do 'DECRYPT_EEK'|not allowed to do \"DECRYPT_EEK\""; then
    echo "        HINT: DECRYPT_EEK was denied. dbks-site blacklists hdfs for DECRYPT_EEK by design;" >&2
    echo "              EZ put/get must run as a client user (default ambari-qa), with a Ranger KMS" >&2
    echo "              policy granting that user decrypteek on the key. Set configs/ranger.env" >&2
    echo "              (RANGER_PASSWORD) so this script can create a temporary policy, or add one" >&2
    echo "              manually in Ranger for user ${KMS_CLIENT_USER:-ambari-qa} on key ${KMS_KEY_NAME:-<key>}." >&2
  elif printf '%s\n' "$out" | grep -Eqi 'AuthorizationException|not allowed to do'; then
    echo "        HINT: Ranger KMS denied this call. Default policies grant CREATE_KEY/GET_KEYS to keyadmin" >&2
    echo "              (rangerkms/<FQDN> via rangerkms.service.keytab on a KMS host), not to hdfs." >&2
    echo "              Re-run on a RANGER_KMS_SERVER host, or set KMS_KEYTAB/KMS_PRINCIPAL, or add" >&2
    echo "              CREATE_KEY/GET_KEYS/ROLL_KEY/DELETE_KEY for your principal in Ranger KMS." >&2
  fi
}

do_kinit() {
  local keytab="$1" principal="$2" label="${3:-kinit}"
  [[ -r "$keytab" ]] || die "keytab not readable: $keytab"
  echo "    keytab=${keytab} principal=${principal}"
  kinit -kt "$keytab" "$principal" || die "kinit failed for ${principal}"
  record_pass "${label} ${principal}"
}

resolve_kms_admin_identity() {
  # Prefer rangerkms (maps to keyadmin in Ranger). hdfs typically cannot CREATE_KEY/GET_KEYS.
  local kt p
  if [[ -n "${KMS_KEYTAB:-}" ]]; then
    kt="$KMS_KEYTAB"
  elif [[ -r /etc/security/keytabs/rangerkms.service.keytab ]]; then
    kt=/etc/security/keytabs/rangerkms.service.keytab
  else
    kt="${HDFS_KEYTAB:-/etc/security/keytabs/hdfs.headless.keytab}"
  fi
  if [[ -n "${KMS_PRINCIPAL:-}" ]]; then
    p="$KMS_PRINCIPAL"
  elif [[ "$kt" == *rangerkms* ]]; then
    p="$(principal_from_keytab "$kt" "${KMS_HOSTS:-}" || true)"
    [[ -n "$p" ]] || p="rangerkms/$(hostname -f)"
  else
    p="hdfs-${cluster}"
  fi
  KMS_ADMIN_KEYTAB="$kt"
  KMS_ADMIN_PRINCIPAL="$p"
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

load_kms_env_file() {
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
      KMS_*|CURL_EXTRA_OPTS|CLUSTER_NAME|HDFS_KEYTAB|RANGER_*)
        [[ -n "${!key:-}" ]] || printf -v "$key" '%s' "$val"
        ;;
    esac
  done <"$f"
  return 0
}

load_ranger_env_file() {
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
      RANGER_USER|RANGER_PASSWORD|RANGER_BASE_URL)
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

ambari_get() {
  # shellcheck disable=SC2086
  curl -sS -f ${CURL_EXTRA_OPTS:-} -u "${AMBARI_USER}:${AMBARI_PASSWORD}" \
    -H "X-Requested-By: ambari" "$1"
}

ambari_component_hosts() {
  local service="$1" component="$2" url
  [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]] || return 1
  url="${AMBARI_BASE_URL%/}/api/v1/clusters/${cluster}/services/${service}/components/${component}"
  ambari_get "$url" 2>/dev/null | python3 -c "
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

ambari_config_prop() {
  local conf_type="$1" prop="$2"
  [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]] || return 1
  ambari_get "${AMBARI_BASE_URL%/}/api/v1/clusters/${cluster}?fields=Clusters/desired_configs" 2>/dev/null \
    | python3 - "$AMBARI_BASE_URL" "$cluster" "$conf_type" "$prop" "${AMBARI_USER}" "${AMBARI_PASSWORD}" "${CURL_EXTRA_OPTS:-}" <<'PY'
import json, os, shlex, subprocess, sys, urllib.parse

ambari, cluster, conf_type, prop, user, pw, curl_extra = sys.argv[1:8]
qc = urllib.parse.quote(cluster, safe="")

def curl_json(url):
    cmd = ["curl", "-sS", "-f", "-u", f"{user}:{pw}", "-H", "X-Requested-By: ambari"]
    extra = shlex.split(curl_extra.strip()) if curl_extra.strip() else []
    cmd += extra + [url]
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if r.returncode != 0:
        sys.exit(1)
    return json.loads(r.stdout)

j = curl_json(f"{ambari.rstrip('/')}/api/v1/clusters/{qc}?fields=Clusters/desired_configs")
dc = (j.get("Clusters") or {}).get("desired_configs") or {}
entry = dc.get(conf_type) or {}
tag = entry.get("tag")
if not tag:
    sys.exit(1)
j2 = curl_json(
    f"{ambari.rstrip('/')}/api/v1/clusters/{qc}/configurations"
    f"?type={urllib.parse.quote(conf_type)}&tag={urllib.parse.quote(tag)}"
)
items = j2.get("items") or []
if not items:
    sys.exit(1)
props = items[0].get("properties") or {}
val = (props.get(prop) or "").strip()
if not val:
    sys.exit(1)
print(val)
PY
}

build_provider() {
  local scheme="$1" hosts="$2" port="$3" first h
  first=1
  printf 'kms://%s@' "$scheme"
  for h in $hosts; do
    if (( first )); then
      first=0
    else
      printf ';'
    fi
    printf '%s' "$h"
  done
  printf ':%s/kms' "$port"
}

run_hadoop_key() {
  # Prefer explicit -provider when we have one so the CLI does not depend on
  # a local core-site that may be missing the KMS URI.
  if [[ -n "${KMS_PROVIDER:-}" ]]; then
    hadoop key "$@" -provider "$KMS_PROVIDER"
  else
    hadoop key "$@"
  fi
}

discover_ranger_base_url() {
  CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}" \
    AMBARI_USER="$AMBARI_USER" AMBARI_PASSWORD="$AMBARI_PASSWORD" python3 - "$AMBARI_BASE_URL" "$cluster" <<'PY'
import json, os, shlex, subprocess, sys
from urllib.parse import urlparse, quote

def curl_json(url):
    user, pw = os.environ["AMBARI_USER"], os.environ["AMBARI_PASSWORD"]
    extra = shlex.split(os.environ.get("CURL_EXTRA_OPTS", "").strip() or "")
    cmd = ["curl", "-sS", "-f", "-u", f"{user}:{pw}", "-H", "X-Requested-By: ambari"] + extra + [url]
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if r.returncode != 0:
        sys.exit(r.returncode or 1)
    return json.loads(r.stdout)

ambari, cluster = sys.argv[1].rstrip("/"), sys.argv[2]
qc = quote(cluster, safe="")
j = curl_json(f"{ambari}/api/v1/clusters/{qc}?fields=Clusters/desired_configs")
dc = (j.get("Clusters") or {}).get("desired_configs") or {}
ra = dc.get("ranger-admin-site") or {}
tag = ra.get("tag")
props = {}
if tag:
    j2 = curl_json(f"{ambari}/api/v1/clusters/{qc}/configurations?type=ranger-admin-site&tag={quote(tag)}")
    items = j2.get("items") or []
    if items:
        props = items[0].get("properties") or {}
pm = (props.get("policymgr_external_url") or "").strip()
if pm:
    raw = pm if "://" in pm else "http://" + pm
    p = urlparse(raw)
    if p.scheme and p.netloc:
        print(f"{p.scheme}://{p.netloc}".rstrip("/"))
        sys.exit(0)
port = (props.get("ranger.service.http.port") or "6080").strip()
hj = curl_json(
    f"{ambari}/api/v1/clusters/{qc}/host_components"
    f"?HostRoles/component_name=RANGER_ADMIN&fields=HostRoles/host_name,HostRoles/public_host_name"
)
for it in hj.get("items") or []:
    hr = it.get("HostRoles") or {}
    host = hr.get("public_host_name") or hr.get("host_name")
    if host:
        print(f"http://{host}:{port}".rstrip("/"))
        sys.exit(0)
sys.exit(2)
PY
}

# Create (or reuse) a temporary Ranger KMS policy so the client user can
# DECRYPT_EEK the smoke key. Prints policy id on success.
ensure_kms_decrypt_policy() {
  local key_name="$1" client_user="$2" hdfs_user="${3:-hdfs}"
  [[ -n "${RANGER_BASE_URL:-}" && -n "${RANGER_PASSWORD:-}" ]] || return 1
  RANGER_USER="${RANGER_USER:-admin}" \
  CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}" \
  RANGER_KMS_SERVICE_NAME="${RANGER_KMS_SERVICE_NAME:-}" \
  python3 - "$RANGER_BASE_URL" "$RANGER_USER" "$RANGER_PASSWORD" "$key_name" "$client_user" "$hdfs_user" <<'PY'
import json, os, shlex, subprocess, sys
from urllib.parse import quote

base, user, password, key_name, client_user, hdfs_user = sys.argv[1:7]
base = base.rstrip("/")
extra = shlex.split(os.environ.get("CURL_EXTRA_OPTS", "").strip() or "")
forced_svc = (os.environ.get("RANGER_KMS_SERVICE_NAME") or "").strip()
policy_name = "odp-kms-smoke-" + key_name

def curl(method, path, data=None):
    url = base + path
    cmd = ["curl", "-sS", "-w", "\n%{http_code}", "-u", "%s:%s" % (user, password),
           "-H", "Accept: application/json", "-X", method] + extra
    body = None
    if data is not None:
        body = json.dumps(data)
        cmd += ["-H", "Content-Type: application/json", "--data-binary", body]
    cmd.append(url)
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = r.stdout.decode("utf-8", "replace")
    lines = out.rsplit("\n", 1)
    raw, code_s = (lines[0], lines[1]) if len(lines) == 2 else (out, "0")
    try:
        code = int(code_s.strip() or "0")
    except ValueError:
        code, raw = 0, out
    try:
        j = json.loads(raw) if raw.strip() else {}
    except Exception:
        j = {"raw": raw[:500]}
    return code, j

def accesses(*names):
    return [{"type": n, "isAllowed": True} for n in names]

code, svcs = curl("GET", "/service/public/v2/api/service")
if code != 200 or not isinstance(svcs, list):
    sys.stderr.write("failed to list Ranger services HTTP=%s\n" % code)
    sys.exit(1)

svc_name = forced_svc
if not svc_name:
    for s in svcs:
        if (s.get("type") or "").lower() == "kms" and s.get("isEnabled", True):
            svc_name = s.get("name")
            break
if not svc_name:
    sys.stderr.write("no enabled Ranger service of type kms; set RANGER_KMS_SERVICE_NAME\n")
    sys.exit(1)

payload = {
    "service": svc_name,
    "name": policy_name,
    "policyType": 0,
    "isEnabled": True,
    "isAuditEnabled": True,
    "resources": {
        "keyname": {
            "values": [key_name],
            "isExcludes": False,
            "isRecursive": False,
        }
    },
    "policyItems": [
        {
            "accesses": accesses("getmetadata", "generateeek"),
            "users": [hdfs_user],
            "groups": [],
            "roles": [],
            "conditions": [],
            "delegateAdmin": False,
        },
        {
            "accesses": accesses("getmetadata", "generateeek", "decrypteek"),
            "users": [client_user],
            "groups": [],
            "roles": [],
            "conditions": [],
            "delegateAdmin": False,
        },
    ],
}

# Reuse existing smoke policy for this key if present.
code, found = curl(
    "GET",
    "/service/public/v2/api/service/%s/policy?policyName=%s"
    % (quote(svc_name, safe=""), quote(policy_name, safe="")),
)
existing = None
if code == 200:
    if isinstance(found, list) and found:
        existing = found[0]
    elif isinstance(found, dict) and found.get("id"):
        existing = found

if existing and existing.get("id"):
    payload["id"] = existing["id"]
    payload["guid"] = existing.get("guid")
    code, out = curl("PUT", "/service/public/v2/api/policy/%s" % existing["id"], payload)
else:
    code, out = curl("POST", "/service/public/v2/api/policy", payload)

if code not in (200, 201) or not isinstance(out, dict) or not out.get("id"):
    sys.stderr.write("failed to upsert KMS policy HTTP=%s body=%s\n" % (code, out))
    sys.exit(1)
print("%s %s" % (out["id"], svc_name))
PY
}

delete_kms_decrypt_policy() {
  local policy_id="$1"
  [[ -n "$policy_id" && -n "${RANGER_BASE_URL:-}" && -n "${RANGER_PASSWORD:-}" ]] || return 0
  # shellcheck disable=SC2086
  curl -sS -o /dev/null ${CURL_EXTRA_OPTS:-} -u "${RANGER_USER:-admin}:${RANGER_PASSWORD}" \
    -X DELETE "${RANGER_BASE_URL%/}/service/public/v2/api/policy/${policy_id}" >/dev/null 2>&1 || true
}

need_cmd curl
need_cmd kinit
need_cmd hadoop
need_cmd hdfs
need_cmd python3

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""

load_kms_env_file "$KMS_ENV_FILE"
load_ranger_env_file "$RANGER_ENV_FILE"

KMS_SKIP_KINIT="${KMS_SKIP_KINIT:-0}"
KMS_SKIP_HTTP="${KMS_SKIP_HTTP:-0}"
KMS_SKIP_EZ="${KMS_SKIP_EZ:-0}"
KMS_SKIP_ROLL="${KMS_SKIP_ROLL:-0}"
KMS_KEEP_KEY="${KMS_KEEP_KEY:-0}"
KMS_SKIP_RANGER_POLICY="${KMS_SKIP_RANGER_POLICY:-0}"
KMS_POLICY_WAIT_SECONDS="${KMS_POLICY_WAIT_SECONDS:-45}"
KMS_KEY_SIZE="${KMS_KEY_SIZE:-256}"
KMS_CIPHER="${KMS_CIPHER:-AES/CTR/NoPadding}"
CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}"
KMS_PORT="${KMS_PORT:-}"
KMS_SSL="${KMS_SSL:-}"
KMS_PROVIDER="${KMS_PROVIDER:-}"
KMS_HOSTS="${KMS_HOSTS:-}"
KMS_CLIENT_USER="${KMS_CLIENT_USER:-ambari-qa}"
RANGER_USER="${RANGER_USER:-admin}"
RANGER_PASSWORD="${RANGER_PASSWORD:-}"
RANGER_BASE_URL="${RANGER_BASE_URL:-}"
RANGER_KMS_SERVICE_NAME="${RANGER_KMS_SERVICE_NAME:-}"

stamp="$(date +%s)"
KMS_KEY_NAME="${KMS_KEY_NAME:-odp_kms_smoke_${stamp}}"
KMS_EZ_PATH="${KMS_EZ_PATH:-/tmp/odp_kms_smoke_ez_${stamp}}"
RANGER_SMOKE_POLICY_ID=""

if [[ -n "${CLUSTER_NAME:-}" && -n "${KMS_PROVIDER:-}" ]]; then
  :
elif [[ -f "$AMBARI_CONFIG_FILE" ]]; then
  load_ambari_env_file "$AMBARI_CONFIG_FILE" || die "failed to read $AMBARI_CONFIG_FILE"
elif [[ -n "${CLUSTER_NAME:-}" ]]; then
  :
elif [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]]; then
  :
elif [[ -n "${KMS_PROVIDER:-}" ]]; then
  :
else
  die "Missing Ambari / KMS context. Create ${AMBARI_CONFIG_FILE}, set CLUSTER_NAME, or set KMS_PROVIDER."
fi

AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://127.0.0.1:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"

if [[ -z "${CLUSTER_NAME:-}" && -z "${KMS_PROVIDER:-}" ]]; then
  [[ -n "$AMBARI_USER" && -n "$AMBARI_PASSWORD" ]] \
    || die "AMBARI_USER and AMBARI_PASSWORD required when CLUSTER_NAME and KMS_PROVIDER are unset."
fi

if [[ -n "${CLUSTER_NAME:-}" ]]; then
  cluster="$CLUSTER_NAME"
elif [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]]; then
  clusters_url="${AMBARI_BASE_URL%/}/api/v1/clusters/"
  json="$(ambari_get "$clusters_url")" || die "failed to GET $clusters_url"
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
  # Provider-only runs still need a principal; require an explicit override.
  cluster="${CLUSTER_NAME:-}"
  [[ -n "$cluster" || -n "${KMS_PRINCIPAL:-}" ]] \
    || die "Set CLUSTER_NAME or KMS_PRINCIPAL when Ambari is not used."
  cluster="${cluster:-local}"
fi

# Resolve provider URI: explicit > Ambari build > core-site / hdfs-site.
if [[ -z "${KMS_PROVIDER:-}" ]]; then
  if [[ -z "${KMS_HOSTS:-}" && -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]]; then
    KMS_HOSTS="$(ambari_component_hosts RANGER_KMS RANGER_KMS_SERVER 2>/dev/null || true)"
  fi
  if [[ -z "${KMS_PORT}" && -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]]; then
    KMS_PORT="$(ambari_config_prop kms-env kms_port 2>/dev/null || true)"
  fi
  if [[ -z "${KMS_SSL}" && -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]]; then
    ssl_val="$(ambari_config_prop ranger-kms-site ranger.service.https.attrib.ssl.enabled 2>/dev/null || true)"
    case "$ssl_val" in
      [Tt][Rr][Uu][Ee]) KMS_SSL=1 ;;
      *) KMS_SSL=0 ;;
    esac
  fi
  KMS_PORT="${KMS_PORT:-9292}"
  KMS_SSL="${KMS_SSL:-0}"

  if [[ -n "${KMS_HOSTS:-}" ]]; then
    KMS_HOSTS="$(printf '%s' "$KMS_HOSTS" | tr ',;' ' ')"
    scheme="http"
    [[ "$KMS_SSL" == "1" ]] && scheme="https"
    KMS_PROVIDER="$(build_provider "$scheme" "$KMS_HOSTS" "$KMS_PORT")"
  else
    for conf in \
      /etc/hadoop/conf/core-site.xml \
      /usr/odp/current/hadoop-client/conf/core-site.xml \
      /etc/hadoop/conf/hdfs-site.xml \
      /usr/odp/current/hadoop-client/conf/hdfs-site.xml; do
      if [[ -z "${KMS_PROVIDER:-}" ]]; then
        KMS_PROVIDER="$(xml_prop_value "$conf" "hadoop.security.key.provider.path" 2>/dev/null || true)"
      fi
      if [[ -z "${KMS_PROVIDER:-}" ]]; then
        KMS_PROVIDER="$(xml_prop_value "$conf" "dfs.encryption.key.provider.uri" 2>/dev/null || true)"
      fi
      [[ -n "${KMS_PROVIDER:-}" ]] && break
    done
  fi
fi

[[ -n "${KMS_PROVIDER:-}" ]] || die "Could not resolve KMS provider URI. Set KMS_PROVIDER or install RANGER_KMS in Ambari."

# Derive host list for HTTP probes when not already set.
if [[ -z "${KMS_HOSTS:-}" ]]; then
  # kms://http@host1;host2:9292/kms
  KMS_HOSTS="$(
    printf '%s' "$KMS_PROVIDER" | python3 -c "
import re, sys
u = sys.stdin.read().strip()
m = re.match(r'^kms://(?:https?)@([^/]+)/kms$', u)
if not m:
    sys.exit(0)
authority = m.group(1)
# strip trailing :port from last host only when shared port form
if ';' in authority:
    hosts_part, _, port = authority.rpartition(':')
    if port.isdigit():
        authority = hosts_part
    print(authority.replace(';', ' '))
else:
    host, _, port = authority.rpartition(':')
    print(host if port.isdigit() else authority)
"
  )"
fi
if [[ -z "${KMS_PORT}" ]]; then
  KMS_PORT="$(
    printf '%s' "$KMS_PROVIDER" | python3 -c "
import re, sys
u = sys.stdin.read().strip()
m = re.search(r':([0-9]+)/kms$', u)
print(m.group(1) if m else '9292')
"
  )"
fi
if [[ -z "${KMS_SSL}" ]]; then
  if [[ "$KMS_PROVIDER" == kms://https@* ]]; then
    KMS_SSL=1
  else
    KMS_SSL=0
  fi
fi

scheme="http"
[[ "$KMS_SSL" == "1" ]] && scheme="https"

echo "---- Ranger KMS sample smoke ----"
echo "    cluster:    ${cluster}"
echo "    provider:   ${KMS_PROVIDER}"
echo "    kms hosts:  ${KMS_HOSTS:-<none>}"
echo "    port/ssl:   ${KMS_PORT} / ${KMS_SSL}"
echo "    key name:   ${KMS_KEY_NAME}"
echo "    key size:   ${KMS_KEY_SIZE}"
echo "    ez path:    ${KMS_EZ_PATH}"

key_created=0
ez_created=0
cleanup_done=0
current_identity=""
cleanup() {
  [[ "$cleanup_done" == "1" ]] && return 0
  cleanup_done=1
  if [[ "$KMS_KEEP_KEY" == "1" ]]; then
    echo ""
    echo "---- cleanup skipped (KMS_KEEP_KEY=1) ----"
    return 0
  fi
  echo ""
  echo "---- cleanup ----"
  if [[ -n "${RANGER_SMOKE_POLICY_ID:-}" ]]; then
    delete_kms_decrypt_policy "$RANGER_SMOKE_POLICY_ID"
    echo "    deleted Ranger KMS policy id=${RANGER_SMOKE_POLICY_ID}"
    RANGER_SMOKE_POLICY_ID=""
  fi
  if [[ "$ez_created" == "1" ]]; then
    if [[ "$KMS_SKIP_KINIT" != "1" && -n "${KMS_HDFS_KEYTAB:-}" && -r "${KMS_HDFS_KEYTAB}" ]]; then
      kinit -kt "$KMS_HDFS_KEYTAB" "$KMS_HDFS_PRINCIPAL" >/dev/null 2>&1 || true
      current_identity="hdfs"
    fi
    set +e
    hdfs dfs -rm -r -skipTrash "$KMS_EZ_PATH" >/dev/null 2>&1
    set -e
    echo "    removed EZ path ${KMS_EZ_PATH}"
  fi
  if [[ "$key_created" == "1" ]]; then
    if [[ "$KMS_SKIP_KINIT" != "1" && -n "${KMS_ADMIN_KEYTAB:-}" && -r "${KMS_ADMIN_KEYTAB}" ]]; then
      kinit -kt "$KMS_ADMIN_KEYTAB" "$KMS_ADMIN_PRINCIPAL" >/dev/null 2>&1 || true
      current_identity="kms-admin"
    fi
    set +e
    del_out="$(run_hadoop_key delete "$KMS_KEY_NAME" -f 2>&1)"
    del_rc=$?
    set -e
    if (( del_rc == 0 )); then
      echo "    deleted key ${KMS_KEY_NAME}"
    else
      echo "    WARN: key delete failed (rc=${del_rc}): ${del_out}" >&2
      authz_hint "$del_out"
    fi
  fi
}
trap cleanup EXIT

# ---- HTTP reachability ----
if [[ "$KMS_SKIP_HTTP" == "1" ]]; then
  record_skip "kms http probe"
elif [[ -z "${KMS_HOSTS:-}" ]]; then
  record_skip "kms http probe (no hosts)"
else
  echo ""
  echo "---- kms http ----"
  for h in $KMS_HOSTS; do
    url="${scheme}://${h}:${KMS_PORT}/kms/v1/keys/names"
    # shellcheck disable=SC2086
    code="$(curl -s -o /dev/null ${CURL_EXTRA_OPTS:-} -w '%{http_code}' --connect-timeout 5 --max-time 15 "$url" 2>/dev/null || echo 000)"
    case "$code" in
      2??|401|403)
        # 401/403 still means the KMS servlet is up and enforcing auth.
        echo "        OK ${url} -> HTTP ${code}"
        record_pass "kms http ${h}"
        ;;
      *)
        echo "        ${url} -> HTTP ${code}" >&2
        record_fail "kms http ${h} (HTTP ${code})"
        ;;
    esac
  done
fi

# ---- Kerberos (keyadmin / rangerkms for key lifecycle) ----
KMS_HDFS_KEYTAB="${KMS_HDFS_KEYTAB:-${HDFS_KEYTAB:-/etc/security/keytabs/hdfs.headless.keytab}}"
KMS_HDFS_PRINCIPAL="${KMS_HDFS_PRINCIPAL:-hdfs-${cluster}}"
KMS_CLIENT_KEYTAB="${KMS_CLIENT_KEYTAB:-/etc/security/keytabs/smokeuser.headless.keytab}"
KMS_CLIENT_PRINCIPAL="${KMS_CLIENT_PRINCIPAL:-ambari-qa-${cluster}}"
resolve_kms_admin_identity

if [[ -z "${RANGER_BASE_URL:-}" && -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" && "$KMS_SKIP_EZ" != "1" && "$KMS_SKIP_RANGER_POLICY" != "1" ]]; then
  RANGER_BASE_URL="$(discover_ranger_base_url 2>/dev/null || true)"
fi
[[ -n "${RANGER_BASE_URL:-}" ]] && RANGER_BASE_URL="${RANGER_BASE_URL%/}"

if [[ "$KMS_SKIP_KINIT" == "1" ]]; then
  record_skip "kinit"
else
  echo ""
  echo "---- kinit (kms admin / keyadmin) ----"
  if [[ "$KMS_ADMIN_KEYTAB" != *rangerkms* && "$KMS_ADMIN_PRINCIPAL" == hdfs-* ]]; then
    echo "    WARN: using ${KMS_ADMIN_PRINCIPAL}; stock Ranger KMS policies deny CREATE_KEY/GET_KEYS for hdfs." >&2
    echo "    WARN: run on a RANGER_KMS_SERVER host (rangerkms.service.keytab) or set KMS_KEYTAB/KMS_PRINCIPAL." >&2
  fi
  do_kinit "$KMS_ADMIN_KEYTAB" "$KMS_ADMIN_PRINCIPAL" "kinit kms-admin"
  current_identity="kms-admin"
fi

# ---- Key lifecycle ----
echo ""
echo "---- hadoop key list (before create) ----"
set +e
list_before="$(run_hadoop_key list 2>&1)"
list_before_rc=$?
set -e
printf '%s\n' "$list_before" | sed 's/^/        /'
if (( list_before_rc == 0 )); then
  record_pass "hadoop key list"
else
  record_fail "hadoop key list (exit=${list_before_rc})"
  authz_hint "$list_before"
fi

echo ""
echo "---- hadoop key create ${KMS_KEY_NAME} ----"
set +e
create_out="$(run_hadoop_key create "$KMS_KEY_NAME" -size "$KMS_KEY_SIZE" -cipher "$KMS_CIPHER" 2>&1)"
create_rc=$?
set -e
printf '%s\n' "$create_out" | sed 's/^/        /'
if (( create_rc == 0 )); then
  key_created=1
  record_pass "hadoop key create ${KMS_KEY_NAME}"
elif printf '%s\n' "$create_out" | grep -Eqi 'already exists|KeyAlreadyExistsException'; then
  key_created=1
  record_pass "hadoop key create ${KMS_KEY_NAME} (already existed)"
else
  record_fail "hadoop key create ${KMS_KEY_NAME} (exit=${create_rc})"
  authz_hint "$create_out"
fi

echo ""
echo "---- hadoop key list (verify create) ----"
set +e
list_after="$(run_hadoop_key list 2>&1)"
list_after_rc=$?
set -e
printf '%s\n' "$list_after" | sed 's/^/        /'
if (( list_after_rc == 0 )) && printf '%s\n' "$list_after" | grep -Fq "$KMS_KEY_NAME"; then
  record_pass "key listed: ${KMS_KEY_NAME}"
elif (( key_created == 1 )); then
  record_fail "key not listed after create: ${KMS_KEY_NAME}"
else
  record_skip "key list verify (create failed)"
fi

echo ""
echo "---- hadoop key list -metadata ----"
set +e
meta_out="$(run_hadoop_key list -metadata 2>&1)"
meta_rc=$?
set -e
# Metadata listing can be long; show lines mentioning the smoke key.
printf '%s\n' "$meta_out" | grep -F "$KMS_KEY_NAME" | sed 's/^/        /' || printf '%s\n' "$meta_out" | tail -n 20 | sed 's/^/        /'
if (( meta_rc == 0 )) && [[ "$key_created" == "1" ]]; then
  record_pass "hadoop key list -metadata"
elif [[ "$key_created" != "1" ]]; then
  record_skip "key metadata (create failed)"
else
  record_fail "hadoop key list -metadata (exit=${meta_rc})"
fi

if [[ "$KMS_SKIP_ROLL" == "1" ]]; then
  record_skip "hadoop key roll"
elif [[ "$key_created" != "1" ]]; then
  record_skip "hadoop key roll (create failed)"
else
  echo ""
  echo "---- hadoop key roll ${KMS_KEY_NAME} ----"
  set +e
  roll_out="$(run_hadoop_key roll "$KMS_KEY_NAME" 2>&1)"
  roll_rc=$?
  set -e
  printf '%s\n' "$roll_out" | sed 's/^/        /'
  if (( roll_rc == 0 )); then
    record_pass "hadoop key roll ${KMS_KEY_NAME}"
  else
    record_fail "hadoop key roll ${KMS_KEY_NAME} (exit=${roll_rc})"
    authz_hint "$roll_out"
  fi
fi

# ---- Encryption zone sample ----
# createZone as hdfs; put/get as ambari-qa because dbks-site blacklists hdfs for DECRYPT_EEK.
if [[ "$KMS_SKIP_EZ" == "1" ]]; then
  record_skip "encryption zone"
elif [[ "$key_created" != "1" ]]; then
  record_skip "encryption zone (create failed)"
else
  echo ""
  echo "---- encryption zone ${KMS_EZ_PATH} ----"
  ez_ready=1
  if [[ "$KMS_SKIP_KINIT" != "1" ]]; then
    if [[ ! -r "$KMS_HDFS_KEYTAB" ]]; then
      record_skip "encryption zone (hdfs keytab not readable: ${KMS_HDFS_KEYTAB})"
      ez_ready=0
    elif [[ "$current_identity" != "hdfs" ]]; then
      echo "---- kinit (hdfs for createZone) ----"
      do_kinit "$KMS_HDFS_KEYTAB" "$KMS_HDFS_PRINCIPAL" "kinit hdfs"
      current_identity="hdfs"
    fi
  fi

  if [[ "$ez_ready" == "1" ]]; then
    set +e
    hdfs dfs -mkdir -p "$KMS_EZ_PATH" 2>&1 | sed 's/^/        /'
    mkdir_rc=${PIPESTATUS[0]}
    set -e
    if (( mkdir_rc != 0 )); then
      record_fail "hdfs mkdir ${KMS_EZ_PATH}"
      ez_ready=0
    else
      set +e
      ez_out="$(hdfs crypto -createZone -keyName "$KMS_KEY_NAME" -path "$KMS_EZ_PATH" 2>&1)"
      ez_rc=$?
      set -e
      printf '%s\n' "$ez_out" | sed 's/^/        /'
      if (( ez_rc == 0 )) || printf '%s\n' "$ez_out" | grep -Eqi 'AlreadyExistsException|already an encryption zone'; then
        ez_created=1
        record_pass "hdfs crypto -createZone"
        # Client user (not hdfs) needs write access for the put/get path.
        set +e
        hdfs dfs -chmod 777 "$KMS_EZ_PATH" 2>&1 | sed 's/^/        /'
        set -e
      else
        record_fail "hdfs crypto -createZone (exit=${ez_rc})"
        authz_hint "$ez_out"
        ez_ready=0
      fi
    fi
  fi

  if [[ "$ez_ready" == "1" && "$ez_created" == "1" ]]; then
    # Temporary Ranger KMS policy: ambari-qa needs DECRYPT_EEK; hdfs needs GENERATE_EEK.
    if [[ "$KMS_SKIP_RANGER_POLICY" == "1" ]]; then
      record_skip "ranger kms decrypt policy (KMS_SKIP_RANGER_POLICY=1)"
    elif [[ -z "${RANGER_PASSWORD:-}" ]]; then
      record_skip "ranger kms decrypt policy (set RANGER_PASSWORD in ${RANGER_ENV_FILE})"
      echo "        WARN: without a Ranger KMS policy, put/get as ${KMS_CLIENT_USER} will likely fail DECRYPT_EEK." >&2
    elif [[ -z "${RANGER_BASE_URL:-}" ]]; then
      record_skip "ranger kms decrypt policy (set RANGER_BASE_URL or Ambari credentials)"
    else
      echo "---- ranger kms policy for ${KMS_CLIENT_USER} DECRYPT_EEK ----"
      echo "    Ranger: ${RANGER_BASE_URL}"
      set +e
      policy_info="$(ensure_kms_decrypt_policy "$KMS_KEY_NAME" "$KMS_CLIENT_USER" "hdfs" 2>&1)"
      policy_rc=$?
      set -e
      if (( policy_rc == 0 )); then
        RANGER_SMOKE_POLICY_ID="$(printf '%s' "$policy_info" | awk '{print $1}')"
        echo "        OK policy id=${RANGER_SMOKE_POLICY_ID} service=$(printf '%s' "$policy_info" | awk '{print $2}')"
        record_pass "ranger kms decrypt policy"
      else
        printf '%s\n' "$policy_info" | sed 's/^/        /' >&2
        record_fail "ranger kms decrypt policy"
      fi
    fi

    if [[ "$KMS_SKIP_KINIT" != "1" ]]; then
      if [[ ! -r "$KMS_CLIENT_KEYTAB" ]]; then
        record_skip "ez put/get (client keytab not readable: ${KMS_CLIENT_KEYTAB})"
        ez_ready=0
      else
        echo "---- kinit (${KMS_CLIENT_USER} for ez put/get; hdfs is DECRYPT_EEK-blacklisted) ----"
        do_kinit "$KMS_CLIENT_KEYTAB" "$KMS_CLIENT_PRINCIPAL" "kinit client"
        current_identity="client"
      fi
    fi
  fi

  if [[ "$ez_ready" == "1" && "$ez_created" == "1" && "$current_identity" == "client" ]]; then
    work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ranger-kms-smoke.XXXXXX")"
    src="${work_dir}/hosts"
    dst="${work_dir}/hosts.out"
    cp /etc/hosts "$src"

    echo "---- hdfs put into encryption zone (retry up to ${KMS_POLICY_WAIT_SECONDS}s for policy cache) ----"
    put_rc=1
    put_out=""
    deadline=$((SECONDS + KMS_POLICY_WAIT_SECONDS))
    while (( SECONDS <= deadline )); do
      set +e
      put_out="$(hdfs dfs -put -f "$src" "${KMS_EZ_PATH}/hosts" 2>&1)"
      put_rc=$?
      set -e
      if (( put_rc == 0 )); then
        break
      fi
      if ! printf '%s\n' "$put_out" | grep -Eqi 'DECRYPT_EEK|not allowed to do'; then
        break
      fi
      echo "        waiting for KMS policy refresh..."
      sleep 5
    done
    printf '%s\n' "$put_out" | sed 's/^/        /'
    if (( put_rc == 0 )); then
      record_pass "hdfs put into encryption zone"
    else
      record_fail "hdfs put into encryption zone (exit=${put_rc})"
      authz_hint "$put_out"
    fi

    set +e
    get_out="$(hdfs dfs -get -f "${KMS_EZ_PATH}/hosts" "$dst" 2>&1)"
    get_rc=$?
    set -e
    printf '%s\n' "$get_out" | sed 's/^/        /'
    if (( get_rc == 0 )) && cmp -s "$src" "$dst"; then
      record_pass "hdfs get + content compare"
    elif (( get_rc == 0 )); then
      record_fail "hdfs get content mismatch"
    else
      record_fail "hdfs get from encryption zone (exit=${get_rc})"
      authz_hint "$get_out"
    fi

    set +e
    zones_out="$(hdfs crypto -listZones 2>&1)"
    zones_rc=$?
    set -e
    printf '%s\n' "$zones_out" | grep -F "$KMS_EZ_PATH" | sed 's/^/        /' \
      || printf '%s\n' "$zones_out" | sed 's/^/        /'
    if (( zones_rc == 0 )) && printf '%s\n' "$zones_out" | grep -Fq "$KMS_EZ_PATH"; then
      record_pass "hdfs crypto -listZones"
    else
      # listZones often needs hdfs; try once as hdfs if client failed.
      if [[ "$KMS_SKIP_KINIT" != "1" && -r "$KMS_HDFS_KEYTAB" ]]; then
        kinit -kt "$KMS_HDFS_KEYTAB" "$KMS_HDFS_PRINCIPAL" >/dev/null 2>&1 || true
        current_identity="hdfs"
        set +e
        zones_out="$(hdfs crypto -listZones 2>&1)"
        zones_rc=$?
        set -e
      fi
      if (( zones_rc == 0 )) && printf '%s\n' "$zones_out" | grep -Fq "$KMS_EZ_PATH"; then
        record_pass "hdfs crypto -listZones"
      else
        record_fail "hdfs crypto -listZones missing ${KMS_EZ_PATH}"
      fi
    fi

    rm -rf "$work_dir"
  fi
fi

echo ""
echo "---- summary ----"
for r in "${results[@]}"; do
  echo "    $r"
done
echo "    PASS=${pass} FAIL=${fail} SKIPPED=${skip}"

if (( fail > 0 )); then
  die "Ranger KMS sample smoke had ${fail} failing check(s)."
fi
echo "OK: Ranger KMS sample smoke finished (PASS=${pass} SKIPPED=${skip})."
