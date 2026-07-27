#!/usr/bin/env bash
#
# Smoke: Ambari Infra Solr - SolrCloud liveness, cluster state, and an optional
# create / index / query / delete round trip.
#
# Steps:
#   1) Ambari discovery of the INFRA_SOLR hosts, infra_solr_port,
#      infra_solr_ssl_enabled, infra_solr_znode and the Kerberos keytab
#   2) kinit with the Infra Solr service keytab (the HTTP endpoints run SPNEGO)
#   3) Per host: GET /solr/admin/info/system - Solr version, mode, ZK host
#   4) CLUSTERSTATUS - live node count matches the INFRA_SOLR host count and
#      every shard and replica of every collection is active
#   5) LIST - the collections that exist
#   6) Write path: CREATE a throwaway collection, index documents with a commit,
#      query them back, then DELETE (a trap removes it even if a check fails)
#
# The write path read-back needs the Solr "read" permission. On a stock ODP
# security.json that permission is granted to the "dev" role only, and the
# infra-solr principal holds "admin" alone - it can create, index into and drop
# a collection but cannot /select from one. The script therefore falls back to a
# second keytab whose identity carries "dev" (rangeradmin, logsearch or atlas in
# the stock user-role map) for the query, and reports the query SKIPPED when no
# such keytab is readable, since that is an authorization policy rather than a
# Solr fault. The query also has to go to the node that hosts the replica - see
# the comment on the write path.
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   INFRA_SOLR_ENV_FILE        default <script-dir>/configs/infra-solr.env
#   INFRA_SOLR_HOSTS           space-separated, default the Ambari INFRA_SOLR hosts
#   INFRA_SOLR_PORT            default infra_solr_port from Ambari, else 8886
#   INFRA_SOLR_SSL             default infra_solr_ssl_enabled from Ambari, else 0
#   INFRA_SOLR_ZNODE           default infra_solr_znode from Ambari, else /infra-solr
#   INFRA_SOLR_KEYTAB          default infra_solr_kerberos_keytab from Ambari
#   INFRA_SOLR_PRINCIPAL       default the keytab entry matching this host
#   INFRA_SOLR_SKIP_KINIT      default 0 - set 1 to run unauthenticated
#   INFRA_SOLR_READ_KEYTAB     default the first readable INFRA_SOLR_READ_CANDIDATES entry
#   INFRA_SOLR_READ_PRINCIPAL  default the read keytab entry matching this host
#   INFRA_SOLR_READ_CANDIDATES keytabs to try for the "dev" role, in order
#   INFRA_SOLR_SKIP_WRITE      default 0 - set 1 for read-only checks
#   INFRA_SOLR_COLLECTION      default odp_solr_smoke_<timestamp>
#   INFRA_SOLR_CONFIGSET       default _default, else the first configset listed
#   INFRA_SOLR_DOC_COUNT       default 3
#   INFRA_SOLR_KEEP_COLLECTION default 0 - set 1 to leave the collection behind
#   CURL_EXTRA_OPTS            e.g. -k when infra_solr_ssl_enabled is true
#
# Usage:
#   sudo ./infra-solr-sample-smoke.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
INFRA_SOLR_ENV_FILE="${INFRA_SOLR_ENV_FILE:-${SCRIPT_DIR}/configs/infra-solr.env}"

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
  local f="$1" pattern="$2" key val line
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
    esac
    case "$key" in
      $pattern)
        # Environment always wins over the file.
        [[ -n "${!key:-}" ]] || printf -v "$key" '%s' "$val"
        ;;
    esac
  done <"$f"
  return 0
}

ambari_get() {
  curl -sS -f -u "${AMBARI_USER}:${AMBARI_PASSWORD}" \
    -H "X-Requested-By: ambari" "$1" 2>/dev/null
}

ambari_component_hosts() {
  local service="$1" component="$2"
  [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]] || return 1
  ambari_get "${AMBARI_BASE_URL%/}/api/v1/clusters/${cluster}/services/${service}/components/${component}" \
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
print(' '.join(sorted(hosts)))
"
}

solr_config_json=""
load_solr_config() {
  [[ -n "$solr_config_json" ]] && return 0
  [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]] || return 1
  solr_config_json="$(ambari_get "${AMBARI_BASE_URL%/}/api/v1/clusters/${cluster}/configurations/service_config_versions?service_name=AMBARI_INFRA_SOLR&is_current=true")" || return 1
  [[ -n "$solr_config_json" ]]
}

solr_prop() {
  load_solr_config || return 1
  printf '%s' "$solr_config_json" | python3 -c "
import json, sys
want = sys.argv[1]
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(1)
for item in data.get('items', []):
    for conf in item.get('configurations', []):
        props = conf.get('properties') or {}
        if want in props:
            print(str(props[want]).strip())
            sys.exit(0)
sys.exit(1)
" "$1"
}

# The Infra Solr keytab holds host-scoped principals (<primary>/<fqdn>@REALM),
# so the usable one depends on where the script runs. Ambari only stores the
# _HOST template, which kinit will not expand.
principal_from_keytab() {
  local keytab="$1" fqdn short
  [[ -r "$keytab" ]] || return 1
  fqdn="$(hostname -f 2>/dev/null || hostname)"
  short="$(hostname -s 2>/dev/null || hostname)"
  klist -kt "$keytab" 2>/dev/null | awk -v fqdn="$fqdn" -v short="$short" '
    NF >= 4 && $4 ~ /@/ {
      if (seen[$4]++) next
      order[++n] = $4
      if ($4 ~ "/" fqdn "@" && !exact) exact = $4
      if ($4 ~ "/" short "@" && !partial) partial = $4
    }
    END {
      if (exact) { print exact; exit }
      if (partial) { print partial; exit }
      if (n) { print order[1] }
    }'
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
  echo "        FAIL: $1"
}

record_skip() {
  skip=$((skip + 1))
  results+=("SKIPPED $1")
}

# solr_as <ccache> <url> [curl args...] - sets resp_body / resp_code.
solr_as() {
  local cc="$1" url="$2" raw
  shift 2
  raw="$(
    [[ -n "$cc" ]] && export KRB5CCNAME="$cc"
    curl -sS $CURL_EXTRA_OPTS $SOLR_AUTH_OPTS -w $'\n%{http_code}' "$@" "$url" 2>/dev/null || true
  )"
  resp_code="${raw##*$'\n'}"
  resp_body="${raw%$'\n'*}"
  [[ "$resp_code" == 2* ]]
}

solr() {
  solr_as "$SOLR_CCACHE" "$@"
}

need_cmd curl
need_cmd python3

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""

load_env_file "$INFRA_SOLR_ENV_FILE" 'INFRA_SOLR_*|CURL_EXTRA_OPTS'
INFRA_SOLR_SKIP_KINIT="${INFRA_SOLR_SKIP_KINIT:-0}"
INFRA_SOLR_SKIP_WRITE="${INFRA_SOLR_SKIP_WRITE:-0}"
INFRA_SOLR_KEEP_COLLECTION="${INFRA_SOLR_KEEP_COLLECTION:-0}"
INFRA_SOLR_DOC_COUNT="${INFRA_SOLR_DOC_COUNT:-3}"
# Identities the stock Ambari Infra security.json puts in the "dev" role, which
# is the only role granted the "read" permission.
INFRA_SOLR_READ_CANDIDATES="${INFRA_SOLR_READ_CANDIDATES:-/etc/security/keytabs/rangeradmin.service.keytab /etc/security/keytabs/logsearch.service.keytab /etc/security/keytabs/atlas.service.keytab}"
CURL_EXTRA_OPTS="${CURL_EXTRA_OPTS:-}"

cluster="${CLUSTER_NAME:-}"
if [[ -f "$AMBARI_CONFIG_FILE" ]]; then
  load_env_file "$AMBARI_CONFIG_FILE" 'AMBARI_*'
fi
AMBARI_BASE_URL="${AMBARI_BASE_URL:-${_cfg_AMBARI_BASE_URL:-http://10.101.11.22:8080}}"
AMBARI_USER="${AMBARI_USER:-${_cfg_AMBARI_USER:-}}"
AMBARI_PASSWORD="${AMBARI_PASSWORD:-${_cfg_AMBARI_PASSWORD:-}}"

if [[ -z "$cluster" && -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]]; then
  cluster="$(ambari_get "${AMBARI_BASE_URL%/}/api/v1/clusters/" | python3 -c "
import json, sys
data = json.load(sys.stdin)
items = data.get('items') or []
if not items:
    sys.exit('no clusters in Ambari response')
print((items[0].get('Clusters') or {}).get('cluster_name') or sys.exit('no cluster_name'))
" 2>/dev/null || true)"
fi

if [[ -z "${INFRA_SOLR_HOSTS:-}" ]]; then
  [[ -n "$cluster" ]] || die "Set INFRA_SOLR_HOSTS, or provide Ambari credentials in ${AMBARI_CONFIG_FILE}."
  INFRA_SOLR_HOSTS="$(ambari_component_hosts AMBARI_INFRA_SOLR INFRA_SOLR 2>/dev/null || true)"
  [[ -n "$INFRA_SOLR_HOSTS" ]] || die "no INFRA_SOLR host in Ambari; set INFRA_SOLR_HOSTS"
fi

INFRA_SOLR_PORT="${INFRA_SOLR_PORT:-$(solr_prop infra_solr_port 2>/dev/null || echo 8886)}"
INFRA_SOLR_ZNODE="${INFRA_SOLR_ZNODE:-$(solr_prop infra_solr_znode 2>/dev/null || echo /infra-solr)}"
if [[ -z "${INFRA_SOLR_SSL:-}" ]]; then
  case "$(solr_prop infra_solr_ssl_enabled 2>/dev/null || echo false)" in
    true|True|TRUE|1) INFRA_SOLR_SSL=1 ;;
    *) INFRA_SOLR_SSL=0 ;;
  esac
fi
if [[ "$INFRA_SOLR_SSL" == "1" ]]; then
  scheme="https"
else
  scheme="http"
fi

solr_client_hosts="$(ambari_component_hosts AMBARI_INFRA_SOLR INFRA_SOLR_CLIENT 2>/dev/null || true)"
host_count="$(printf '%s\n' $INFRA_SOLR_HOSTS | grep -c .)"
first_host="${INFRA_SOLR_HOSTS%% *}"
base_url="${scheme}://${first_host}:${INFRA_SOLR_PORT}/solr"

SOLR_CCACHE=""
SOLR_AUTH_OPTS=""
kinit_reason=""
if [[ "$INFRA_SOLR_SKIP_KINIT" == "1" ]]; then
  kinit_reason="INFRA_SOLR_SKIP_KINIT=1"
elif ! command -v kinit >/dev/null 2>&1; then
  kinit_reason="kinit not installed"
else
  INFRA_SOLR_KEYTAB="${INFRA_SOLR_KEYTAB:-$(solr_prop infra_solr_kerberos_keytab 2>/dev/null || echo /etc/security/keytabs/ambari-infra-solr.service.keytab)}"
  if [[ ! -r "$INFRA_SOLR_KEYTAB" ]]; then
    kinit_reason="keytab not readable: ${INFRA_SOLR_KEYTAB} (run with sudo)"
  else
    INFRA_SOLR_PRINCIPAL="${INFRA_SOLR_PRINCIPAL:-$(principal_from_keytab "$INFRA_SOLR_KEYTAB" || true)}"
    if [[ -z "${INFRA_SOLR_PRINCIPAL:-}" ]]; then
      kinit_reason="no principal in ${INFRA_SOLR_KEYTAB}"
    fi
  fi
fi

echo "---- Ambari Infra Solr sample smoke ----"
echo "    cluster:      ${cluster:-<unknown>}"
echo "    hosts:        ${INFRA_SOLR_HOSTS}"
echo "    clients:      ${solr_client_hosts:-<none>}"
echo "    port / ssl:   ${INFRA_SOLR_PORT} / ${INFRA_SOLR_SSL}"
echo "    znode:        ${INFRA_SOLR_ZNODE}"
echo "    base url:     ${base_url}"
echo "    keytab:       ${INFRA_SOLR_KEYTAB:-<none>}"
echo "    principal:    ${INFRA_SOLR_PRINCIPAL:-<none>}"

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/infra-solr-smoke.XXXXXX")"
collection_created=0
cleanup_done=0
cleanup() {
  [[ "$cleanup_done" == "1" ]] && return 0
  cleanup_done=1
  if [[ "$INFRA_SOLR_KEEP_COLLECTION" != "1" && "$collection_created" == "1" ]]; then
    echo ""
    echo "---- cleanup ----"
    if solr "${base_url}/admin/collections?action=DELETE&name=${INFRA_SOLR_COLLECTION}&wt=json"; then
      echo "        deleted collection ${INFRA_SOLR_COLLECTION}"
    else
      echo "        WARN: could not delete ${INFRA_SOLR_COLLECTION} (HTTP ${resp_code})"
    fi
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT

echo ""
echo "---- kinit ----"
if [[ -n "$kinit_reason" ]]; then
  echo "        WARN: running unauthenticated (${kinit_reason})"
else
  SOLR_CCACHE="${work_dir}/krb5cc"
  if kinit_out="$(KRB5CCNAME="$SOLR_CCACHE" kinit -kt "$INFRA_SOLR_KEYTAB" "$INFRA_SOLR_PRINCIPAL" 2>&1)"; then
    echo "        kinit ${INFRA_SOLR_PRINCIPAL} OK"
    SOLR_AUTH_OPTS="--negotiate -u :"
  else
    SOLR_CCACHE=""
    kinit_reason="kinit failed for ${INFRA_SOLR_PRINCIPAL}"
    echo "        WARN: ${kinit_reason}"
    printf '%s\n' "$kinit_out" | sed 's/^/        /'
  fi
fi

echo ""
echo "---- per-host liveness ----"
for h in $INFRA_SOLR_HOSTS; do
  host_url="${scheme}://${h}:${INFRA_SOLR_PORT}/solr"
  if solr "${host_url}/admin/info/system?wt=json"; then
    info="$(printf '%s' "$resp_body" | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
lucene = d.get('lucene') or {}
print('%s|%s|%s' % (lucene.get('solr-spec-version', '?'), d.get('mode', '?'), d.get('zkHost', '')))
" 2>/dev/null || echo '?|?|')"
    echo "        ${h}:${INFRA_SOLR_PORT} -> Solr ${info%%|*} mode=$(printf '%s' "$info" | cut -d'|' -f2)"
    record_pass "system info ${h}"
    zk_host="$(printf '%s' "$info" | cut -d'|' -f3)"
    if [[ "$h" == "$first_host" ]]; then
      if [[ -n "$zk_host" && "$zk_host" == *"${INFRA_SOLR_ZNODE}" ]]; then
        echo "        zkHost ${zk_host}"
        record_pass "SolrCloud znode ${INFRA_SOLR_ZNODE}"
      else
        record_fail "SolrCloud znode ${INFRA_SOLR_ZNODE} (zkHost=${zk_host:-<none>})"
      fi
    fi
  elif [[ "$resp_code" == "401" && -z "$SOLR_AUTH_OPTS" ]]; then
    record_skip "system info ${h} (SPNEGO required, ${kinit_reason})"
  else
    record_fail "system info ${h} (HTTP ${resp_code})"
  fi
done

echo ""
echo "---- cluster status ----"
live_count=0
if solr "${base_url}/admin/collections?action=CLUSTERSTATUS&wt=json"; then
  cs_out="$(printf '%s' "$resp_body" | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit('CLUSTERSTATUS response is not JSON')
c = d.get('cluster') or {}
live = sorted(c.get('live_nodes') or [])
bad = []
lines = ['live nodes (%d): %s' % (len(live), ', '.join(live) or '<none>')]
colls = c.get('collections') or {}
for name in sorted(colls):
    coll = colls[name]
    shards = coll.get('shards') or {}
    replicas = 0
    for sname in sorted(shards):
        shard = shards[sname]
        if shard.get('state') != 'active':
            bad.append('%s/%s shard state=%s' % (name, sname, shard.get('state')))
        for rname in sorted(shard.get('replicas') or {}):
            rep = (shard.get('replicas') or {})[rname]
            replicas += 1
            if rep.get('state') != 'active':
                bad.append('%s/%s/%s state=%s' % (name, sname, rname, rep.get('state')))
            elif rep.get('node_name') not in live:
                bad.append('%s/%s/%s on node %s which is not live' % (name, sname, rname, rep.get('node_name')))
    lines.append('%s: %d shard(s), %d replica(s), health=%s' % (name, len(shards), replicas, coll.get('health', '?')))
if not colls:
    lines.append('no collections')
for b in bad:
    lines.append('BAD ' + b)
lines.append('LIVE_COUNT=%d' % len(live))
print('\n'.join(lines))
sys.exit(1 if bad else 0)
" 2>&1)" && cs_ok=1 || cs_ok=0
  printf '%s\n' "$cs_out" | grep -v '^LIVE_COUNT=' | sed 's/^/        /'
  live_count="$(printf '%s\n' "$cs_out" | sed -n 's/^LIVE_COUNT=//p')"
  live_count="${live_count:-0}"
  if (( cs_ok == 1 )); then
    record_pass "all shards and replicas active"
  else
    record_fail "all shards and replicas active"
  fi
  if [[ "$live_count" == "$host_count" ]]; then
    record_pass "live nodes match INFRA_SOLR hosts (${live_count}/${host_count})"
  else
    record_fail "live nodes match INFRA_SOLR hosts (${live_count}/${host_count})"
  fi
else
  record_fail "CLUSTERSTATUS (HTTP ${resp_code})"
  record_skip "live nodes match INFRA_SOLR hosts (CLUSTERSTATUS failed)"
fi

echo ""
echo "---- collections ----"
existing_collections=""
if solr "${base_url}/admin/collections?action=LIST&wt=json"; then
  existing_collections="$(printf '%s' "$resp_body" | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(' '.join(d.get('collections') or []))
" 2>/dev/null || true)"
  echo "        ${existing_collections:-<none>}"
  record_pass "collections LIST"
else
  record_fail "collections LIST (HTTP ${resp_code})"
fi

configsets=""
if solr "${base_url}/admin/configs?action=LIST&wt=json"; then
  configsets="$(printf '%s' "$resp_body" | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(' '.join(d.get('configSets') or []))
" 2>/dev/null || true)"
  echo "        configsets: ${configsets:-<none>}"
  record_pass "configsets LIST"
else
  record_fail "configsets LIST (HTTP ${resp_code})"
fi

declare -a write_labels=(
  "collection CREATE"
  "index documents"
  "query read-back"
  "collection DELETE"
)

skip_write() {
  local reason="$1" label
  for label in "${write_labels[@]}"; do
    record_skip "${label} (${reason})"
  done
}

if [[ "$INFRA_SOLR_SKIP_WRITE" == "1" ]]; then
  skip_write "INFRA_SOLR_SKIP_WRITE=1"
elif [[ -z "$configsets" ]]; then
  skip_write "no usable configset"
else
  if [[ -z "${INFRA_SOLR_CONFIGSET:-}" ]]; then
    for cs in $configsets; do
      if [[ "$cs" == "_default" ]]; then
        INFRA_SOLR_CONFIGSET="$cs"
        break
      fi
    done
    INFRA_SOLR_CONFIGSET="${INFRA_SOLR_CONFIGSET:-${configsets%% *}}"
  fi
  INFRA_SOLR_COLLECTION="${INFRA_SOLR_COLLECTION:-odp_solr_smoke_$(date +%s)}"

  echo ""
  echo "---- write path ----"
  echo "        collection ${INFRA_SOLR_COLLECTION} configset ${INFRA_SOLR_CONFIGSET}"

  if solr "${base_url}/admin/collections?action=CREATE&name=${INFRA_SOLR_COLLECTION}&numShards=1&replicationFactor=1&collection.configName=${INFRA_SOLR_CONFIGSET}&wt=json"; then
    collection_created=1
    record_pass "collection CREATE"
  else
    record_fail "collection CREATE (HTTP ${resp_code})"
    printf '%s\n' "$resp_body" | tail -5 | sed 's/^/        /'
  fi

  if (( collection_created == 1 )); then
    # A request for a collection whose replica lives on another node is proxied
    # there, and the proxied hop re-authenticates as the Solr node identity
    # (infra-solr, role "admin") rather than the caller. That identity has no
    # "read" permission, so /select answers 403 whenever the replica did not
    # land on the node being asked. Talk to the replica's own base_url instead.
    coll_url="$base_url"
    if solr "${base_url}/admin/collections?action=CLUSTERSTATUS&collection=${INFRA_SOLR_COLLECTION}&wt=json"; then
      replica_url="$(printf '%s' "$resp_body" | python3 -c "
import json, sys
d = json.load(sys.stdin)
colls = ((d.get('cluster') or {}).get('collections') or {})
for coll in colls.values():
    for shard in (coll.get('shards') or {}).values():
        for rep in (shard.get('replicas') or {}).values():
            if rep.get('state') == 'active' and rep.get('base_url'):
                print(rep['base_url'])
                sys.exit(0)
sys.exit(1)
" 2>/dev/null || true)"
      [[ -n "$replica_url" ]] && coll_url="${replica_url%/}"
    fi
    echo "        replica hosted at ${coll_url}"

    docs_file="${work_dir}/docs.json"
    python3 -c "
import json, sys
n = int(sys.argv[1])
tag = sys.argv[2]
print(json.dumps([{'id': '%s-%d' % (tag, i), 'smoke_s': tag} for i in range(n)]))
" "$INFRA_SOLR_DOC_COUNT" "$INFRA_SOLR_COLLECTION" >"$docs_file"

    if solr "${coll_url}/${INFRA_SOLR_COLLECTION}/update?commit=true&wt=json" \
      -H "Content-Type: application/json" --data-binary "@${docs_file}"; then
      echo "        indexed ${INFRA_SOLR_DOC_COUNT} document(s) with commit"
      record_pass "index documents"
    else
      record_fail "index documents (HTTP ${resp_code})"
    fi

    query_url="${coll_url}/${INFRA_SOLR_COLLECTION}/select?q=smoke_s:${INFRA_SOLR_COLLECTION}&rows=0&wt=json"
    query_done=0
    query_identity="${INFRA_SOLR_PRINCIPAL:-<anonymous>}"
    if solr "$query_url"; then
      query_done=1
    elif [[ "$resp_code" == "403" && -n "$SOLR_AUTH_OPTS" ]]; then
      echo "        ${query_identity} is not in the Solr \"dev\" role; retrying as a read identity"
      read_keytab="${INFRA_SOLR_READ_KEYTAB:-}"
      if [[ -z "$read_keytab" ]]; then
        for cand in $INFRA_SOLR_READ_CANDIDATES; do
          if [[ -r "$cand" ]]; then
            read_keytab="$cand"
            break
          fi
        done
      fi
      if [[ -n "$read_keytab" && -r "$read_keytab" ]]; then
        read_principal="${INFRA_SOLR_READ_PRINCIPAL:-$(principal_from_keytab "$read_keytab" || true)}"
        read_ccache="${work_dir}/krb5cc_read"
        if [[ -n "$read_principal" ]] \
          && KRB5CCNAME="$read_ccache" kinit -kt "$read_keytab" "$read_principal" >/dev/null 2>&1; then
          query_identity="$read_principal"
          if solr_as "$read_ccache" "$query_url"; then
            query_done=1
          fi
        fi
      fi
      if (( query_done == 0 )); then
        record_skip "query read-back (no keytab for an identity in the Solr \"dev\" role)"
      fi
    else
      record_fail "query read-back (HTTP ${resp_code})"
    fi

    if (( query_done == 1 )); then
      num_found="$(printf '%s' "$resp_body" | python3 -c "
import json, sys
d = json.load(sys.stdin)
print((d.get('response') or {}).get('numFound', -1))
" 2>/dev/null || echo -1)"
      if [[ "$num_found" == "$INFRA_SOLR_DOC_COUNT" ]]; then
        echo "        numFound=${num_found} as ${query_identity}"
        record_pass "query read-back"
      else
        record_fail "query read-back (numFound=${num_found}, expected ${INFRA_SOLR_DOC_COUNT})"
      fi
    fi
  else
    record_skip "index documents (collection CREATE failed)"
    record_skip "query read-back (collection CREATE failed)"
  fi

  if (( collection_created == 1 )); then
    if [[ "$INFRA_SOLR_KEEP_COLLECTION" == "1" ]]; then
      record_skip "collection DELETE (INFRA_SOLR_KEEP_COLLECTION=1)"
    elif solr "${base_url}/admin/collections?action=DELETE&name=${INFRA_SOLR_COLLECTION}&wt=json"; then
      collection_created=0
      echo "        deleted collection ${INFRA_SOLR_COLLECTION}"
      record_pass "collection DELETE"
    else
      record_fail "collection DELETE (HTTP ${resp_code})"
    fi
  else
    record_skip "collection DELETE (collection CREATE failed)"
  fi
fi

cleanup

echo ""
echo "---- summary ----"
for r in "${results[@]}"; do
  echo "    $r"
done
echo "    PASS=${pass} FAIL=${fail} SKIPPED=${skip}"

if (( fail > 0 )); then
  die "Ambari Infra Solr sample smoke had ${fail} failing check(s)."
fi
echo "OK: Ambari Infra Solr sample smoke finished (PASS=${pass} SKIPPED=${skip})."
