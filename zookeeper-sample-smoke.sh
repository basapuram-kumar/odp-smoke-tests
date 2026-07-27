#!/usr/bin/env bash
#
# Smoke: Apache ZooKeeper ensemble - per-server liveness, quorum roles, replica
# consistency, the AdminServer HTTP endpoint, and a zkCli.sh data path.
#
# Steps:
#   1) Ambari discovery of the ZOOKEEPER_SERVER hosts, clientPort, admin.serverPort
#      and the 4lw.commands.whitelist
#   2) Per-server: client port open, four-letter word "ruok" answers "imok"
#   3) Quorum roles from "srvr": exactly one leader, every other server a follower
#   4) Running membership from "conf" matches the Ambari host list
#   5) Per-server "mntr" metrics; on the leader zk_synced_followers must match the
#      number of followers the roles check found
#   6) Ensemble consistency: zk_znode_count within tolerance across all servers
#   7) AdminServer /commands/ruok on each server
#   8) Data path via zkCli.sh against the full connection string: create, get,
#      set, get, ls the parent, delete, with a cleanup trap
#   9) Top-level service znodes that other components depend on
#
# The four-letter words and the AdminServer are both optional in ZooKeeper, so a
# command that is not in 4lw.commands.whitelist or an AdminServer that is not
# listening is reported SKIPPED with the reason. A server that answers but is in
# the wrong state - no leader, a split ensemble, a znode value that does not
# round-trip - is a FAIL.
#
# Environment (optional):
#   AMBARI_CONFIG_FILE, AMBARI_BASE_URL, AMBARI_USER, AMBARI_PASSWORD, CLUSTER_NAME
#   ZOOKEEPER_ENV_FILE       default <script-dir>/configs/zookeeper.env
#   ZOOKEEPER_HOSTS          default the ZOOKEEPER_SERVER hosts from Ambari
#   ZOOKEEPER_CLIENT_PORT    default clientPort from zoo.cfg, else 2181
#   ZOOKEEPER_CONNECT        default <host>:<port>,... built from the two above
#   ZOOKEEPER_4LW_WHITELIST  default 4lw.commands.whitelist from zoo.cfg
#   ZOOKEEPER_ADMIN_PORT     default admin.serverPort from zoo.cfg, else 8080
#   ZOOKEEPER_SKIP_ADMIN     default 0 - set 1 to skip the AdminServer checks
#   ZOOKEEPER_SKIP_CLI       default 0 - set 1 to skip the zkCli.sh data path
#   ZOOKEEPER_SKIP_KINIT     default 0
#   ZOOKEEPER_KEYTAB         default /etc/security/keytabs/smokeuser.headless.keytab
#   ZOOKEEPER_PRINCIPAL      default ambari-qa-<cluster>
#   ZOOKEEPER_CLI_BIN        default /usr/odp/current/zookeeper-client/bin/zkCli.sh
#   JAVA_HOME                default from ambari-agent, else /usr/lib/jvm/...
#   ZOOKEEPER_ZNODE          default /odp_zk_smoke_<timestamp>
#   ZOOKEEPER_KEEP_ZNODE     default 0 - set 1 to leave the smoke znode behind
#   ZOOKEEPER_EXPECTED_ZNODES  space-separated top-level znodes that must exist
#   ZOOKEEPER_ZNODE_COUNT_TOLERANCE  default 25
#   ZOOKEEPER_4LW_TIMEOUT    default 5 (seconds)
#   ZOOKEEPER_CLI_TIMEOUT    default 90 (seconds)
#   CURL_EXTRA_OPTS
#
# Usage:
#   sudo ./zookeeper-sample-smoke.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AMBARI_CONFIG_FILE="${AMBARI_CONFIG_FILE:-${SCRIPT_DIR}/configs/ambari.env}"
ZOOKEEPER_ENV_FILE="${ZOOKEEPER_ENV_FILE:-${SCRIPT_DIR}/configs/zookeeper.env}"

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
  curl -sS -f $CURL_EXTRA_OPTS -u "${AMBARI_USER}:${AMBARI_PASSWORD}" \
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
print(' '.join(hosts))
"
}

zk_config_json=""
load_zk_config() {
  [[ -n "$zk_config_json" ]] && return 0
  [[ -n "${AMBARI_USER:-}" && -n "${AMBARI_PASSWORD:-}" ]] || return 1
  zk_config_json="$(ambari_get "${AMBARI_BASE_URL%/}/api/v1/clusters/${cluster}/configurations/service_config_versions?service_name=ZOOKEEPER&is_current=true")" || return 1
  [[ -n "$zk_config_json" ]]
}

zk_prop() {
  load_zk_config || return 1
  printf '%s' "$zk_config_json" | python3 -c "
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

# The Kerberos half of zoo.cfg (authProvider.1, jaasLoginRenew,
# requireClientAuthScheme) is injected by the stack when security is on and is
# not part of the Ambari zoo.cfg config type, so the rendered file on disk is the
# only place it can be read from. Ambari stays the fallback for hosts that do not
# have a ZooKeeper package installed.
zk_local_cfg=""
for _f in /usr/odp/current/zookeeper-server/conf/zoo.cfg \
          /usr/odp/current/zookeeper-client/conf/zoo.cfg \
          /etc/zookeeper/conf/zoo.cfg; do
  if [[ -r "$_f" ]]; then
    zk_local_cfg="$_f"
    break
  fi
done

zk_local_prop() {
  [[ -n "$zk_local_cfg" ]] || return 1
  local v
  v="$(awk -v k="$1" '
    /^[[:space:]]*#/ { next }
    {
      idx = index($0, "=")
      if (idx == 0) next
      key = substr($0, 1, idx - 1)
      val = substr($0, idx + 1)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", key)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", val)
      if (key == k) { print val; exit }
    }' "$zk_local_cfg")"
  [[ -n "$v" ]] || return 1
  printf '%s' "$v"
}

zk_cfg_prop() {
  zk_local_prop "$1" 2>/dev/null || zk_prop "$1" 2>/dev/null
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
  echo "        FAIL: $1" >&2
}

record_skip() {
  skip=$((skip + 1))
  results+=("SKIPPED $1")
}

tcp_open() {
  local host="$1" port="$2"
  timeout 5 bash -c "echo > /dev/tcp/${host}/${port}" 2>/dev/null
}

# zk_4lw <host> <port> <command> - the server closes the socket right after the
# reply, so a plain read to EOF is enough. Passed through the environment rather
# than interpolated into the -c string.
zk_4lw() {
  local host="$1" port="$2" cmd="$3" out
  out="$(ZK_4LW_HOST="$host" ZK_4LW_PORT="$port" ZK_4LW_CMD="$cmd" \
    timeout "$ZOOKEEPER_4LW_TIMEOUT" bash -c '
exec 3<>"/dev/tcp/${ZK_4LW_HOST}/${ZK_4LW_PORT}" || exit 1
printf "%s" "$ZK_4LW_CMD" >&3
cat <&3
' 2>/dev/null)" || return 1
  [[ -n "$out" ]] || return 1
  printf '%s\n' "$out"
}

lw_allowed() {
  local cmd="$1"
  [[ -z "$zk_4lw_whitelist" ]] && return 0
  [[ "$zk_4lw_whitelist" == "*" ]] && return 0
  [[ ",${zk_4lw_whitelist//[[:space:]]/}," == *",${cmd},"* ]]
}

# A non-whitelisted command still gets a reply, just a refusal, so the text has
# to be inspected instead of the connection result.
lw_refused() {
  [[ "$1" == *"is not executed because it is not in the whitelist"* ]]
}

mntr_value() {
  printf '%s\n' "$1" | awk -F'\t' -v k="$2" '$1 == k { print $2; exit }'
}

# zkCli.sh prints a banner, the log4j appender and the watcher events to the same
# stream as the command result, and it exits 0 for several server-side errors, so
# every assertion below is made on the surviving text rather than on $?.
zk_cli_filter() {
  grep -vE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}' \
    | grep -vE '^(Connecting to |WATCHER::|WatchedEvent |Welcome to ZooKeeper|JLine support is|log4j:)' \
    | grep -vE '^[[:space:]]*$' || true
}

zk_cli() {
  local out
  out="$(timeout "$ZOOKEEPER_CLI_TIMEOUT" "$ZOOKEEPER_CLI_BIN" \
    -server "$ZOOKEEPER_CONNECT" "$@" 2>&1 || true)"
  printf '%s\n' "$out" | zk_cli_filter
}

need_cmd curl
need_cmd python3
need_cmd timeout
need_cmd awk

_cfg_AMBARI_BASE_URL=""
_cfg_AMBARI_USER=""
_cfg_AMBARI_PASSWORD=""

load_env_file "$ZOOKEEPER_ENV_FILE" 'ZOOKEEPER_*|JAVA_HOME|CURL_EXTRA_OPTS'
ZOOKEEPER_SKIP_ADMIN="${ZOOKEEPER_SKIP_ADMIN:-0}"
ZOOKEEPER_SKIP_CLI="${ZOOKEEPER_SKIP_CLI:-0}"
ZOOKEEPER_SKIP_KINIT="${ZOOKEEPER_SKIP_KINIT:-0}"
ZOOKEEPER_KEEP_ZNODE="${ZOOKEEPER_KEEP_ZNODE:-0}"
ZOOKEEPER_EXPECTED_ZNODES="${ZOOKEEPER_EXPECTED_ZNODES:-}"
ZOOKEEPER_ZNODE_COUNT_TOLERANCE="${ZOOKEEPER_ZNODE_COUNT_TOLERANCE:-25}"
ZOOKEEPER_4LW_TIMEOUT="${ZOOKEEPER_4LW_TIMEOUT:-5}"
ZOOKEEPER_CLI_TIMEOUT="${ZOOKEEPER_CLI_TIMEOUT:-90}"
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

zk_hosts_src="Ambari"
if [[ -z "${ZOOKEEPER_HOSTS:-}" ]]; then
  [[ -n "$cluster" ]] || die "Set ZOOKEEPER_HOSTS, or provide Ambari credentials in ${AMBARI_CONFIG_FILE}."
  ZOOKEEPER_HOSTS="$(ambari_component_hosts ZOOKEEPER ZOOKEEPER_SERVER 2>/dev/null || true)"
  [[ -n "$ZOOKEEPER_HOSTS" ]] || die "no ZOOKEEPER_SERVER host in Ambari; set ZOOKEEPER_HOSTS"
else
  zk_hosts_src="ZOOKEEPER_HOSTS"
fi
ZOOKEEPER_HOSTS="${ZOOKEEPER_HOSTS//,/ }"
read -r -a zk_hosts <<<"$ZOOKEEPER_HOSTS"

zk_client_hosts="$(ambari_component_hosts ZOOKEEPER ZOOKEEPER_CLIENT 2>/dev/null || true)"

ZOOKEEPER_CLIENT_PORT="${ZOOKEEPER_CLIENT_PORT:-$(zk_cfg_prop "clientPort" || echo 2181)}"
ZOOKEEPER_ADMIN_PORT="${ZOOKEEPER_ADMIN_PORT:-$(zk_cfg_prop "admin.serverPort" || echo 8080)}"
zk_4lw_whitelist="${ZOOKEEPER_4LW_WHITELIST:-$(zk_cfg_prop "4lw.commands.whitelist" || true)}"
zk_admin_enabled="$(zk_cfg_prop "admin.enableServer" || echo true)"
zk_data_dir="$(zk_cfg_prop "dataDir" || true)"
zk_tick_time="$(zk_cfg_prop "tickTime" || true)"
zk_init_limit="$(zk_cfg_prop "initLimit" || true)"
zk_sync_limit="$(zk_cfg_prop "syncLimit" || true)"
zk_auth_provider="$(zk_cfg_prop "authProvider.1" || true)"
zk_require_auth="$(zk_cfg_prop "requireClientAuthScheme" || true)"
zk_jaas_renew="$(zk_cfg_prop "jaasLoginRenew" || true)"
zk_user_cfg="$(zk_prop "zk_user" 2>/dev/null || true)"
zk_keytab_cfg="$(zk_prop "zookeeper_keytab_path" 2>/dev/null || true)"
zk_principal_cfg="$(zk_prop "zookeeper_principal_name" 2>/dev/null || true)"

if [[ -z "${ZOOKEEPER_CONNECT:-}" ]]; then
  ZOOKEEPER_CONNECT=""
  for h in "${zk_hosts[@]}"; do
    ZOOKEEPER_CONNECT="${ZOOKEEPER_CONNECT:+${ZOOKEEPER_CONNECT},}${h}:${ZOOKEEPER_CLIENT_PORT}"
  done
fi

ZOOKEEPER_CLI_BIN="${ZOOKEEPER_CLI_BIN:-$(command -v zkCli.sh 2>/dev/null || echo /usr/odp/current/zookeeper-client/bin/zkCli.sh)}"
ZOOKEEPER_ZNODE="${ZOOKEEPER_ZNODE:-/odp_zk_smoke_$(date +%s)}"

echo "---- ZooKeeper sample smoke ----"
echo "    cluster:      ${cluster:-<unknown>}"
echo "    servers:      ${ZOOKEEPER_HOSTS} (from ${zk_hosts_src})"
echo "    clients:      ${zk_client_hosts:-<unknown>}"
echo "    client port:  ${ZOOKEEPER_CLIENT_PORT}"
echo "    connect:      ${ZOOKEEPER_CONNECT}"
echo "    admin server: enabled=${zk_admin_enabled} port=${ZOOKEEPER_ADMIN_PORT}"
echo "    4lw allowed:  ${zk_4lw_whitelist:-<unset, server default>}"
echo "    zoo.cfg:      ${zk_local_cfg:-<not on this host, using Ambari>}"
echo "    dataDir:      ${zk_data_dir:-<unknown>}"
echo "    timing:       tickTime=${zk_tick_time:-?} initLimit=${zk_init_limit:-?} syncLimit=${zk_sync_limit:-?}"
echo "    authProvider: ${zk_auth_provider:-<none>}"
echo "    require auth: ${zk_require_auth:-<unset, SASL accepted but not required>} jaasLoginRenew=${zk_jaas_renew:-<unset>}"
echo "    zk user:      ${zk_user_cfg:-<unknown>}"
echo "    keytab:       ${zk_keytab_cfg:-<unknown>} (${zk_principal_cfg:-<unknown>})"
echo "    znode:        ${ZOOKEEPER_ZNODE}"

echo ""
echo "---- server liveness ----"
declare -a live_hosts=()
for h in "${zk_hosts[@]}"; do
  if tcp_open "$h" "$ZOOKEEPER_CLIENT_PORT"; then
    echo "        ${h}:${ZOOKEEPER_CLIENT_PORT} accepting connections"
    record_pass "client port open ${h}"
    live_hosts+=("$h")
  else
    record_fail "client port open (${h}:${ZOOKEEPER_CLIENT_PORT})"
  fi
done

if lw_allowed ruok; then
  for h in "${live_hosts[@]}"; do
    out="$(zk_4lw "$h" "$ZOOKEEPER_CLIENT_PORT" ruok 2>/dev/null || true)"
    out="${out//[[:space:]]/}"
    if lw_refused "$out"; then
      record_skip "ruok ${h} (not in 4lw.commands.whitelist)"
    elif [[ "$out" == "imok" ]]; then
      echo "        ${h} ruok -> imok"
      record_pass "ruok ${h}"
    else
      record_fail "ruok ${h} (got '${out:-<no reply>}')"
    fi
  done
else
  for h in "${live_hosts[@]}"; do
    record_skip "ruok ${h} (ruok not in 4lw.commands.whitelist)"
  done
fi

echo ""
echo "---- quorum roles ----"
leaders=""
followers=""
observers=""
roles_known=0
if lw_allowed srvr; then
  for h in "${live_hosts[@]}"; do
    out="$(zk_4lw "$h" "$ZOOKEEPER_CLIENT_PORT" srvr 2>/dev/null || true)"
    if lw_refused "$out"; then
      record_skip "srvr ${h} (not in 4lw.commands.whitelist)"
      continue
    fi
    if [[ -z "$out" ]]; then
      record_fail "srvr ${h} (no reply)"
      continue
    fi
    mode="$(printf '%s\n' "$out" | awk -F': *' '/^Mode:/ { print tolower($2); exit }')"
    nodes="$(printf '%s\n' "$out" | awk -F': *' '/^Node count:/ { print $2; exit }')"
    zxid="$(printf '%s\n' "$out" | awk -F': *' '/^Zxid:/ { print $2; exit }')"
    ver="$(printf '%s\n' "$out" | awk -F': *' '/^Zookeeper version:/ { print $2; exit }')"
    if [[ -z "$mode" ]]; then
      record_fail "srvr ${h} (no Mode line in reply)"
      continue
    fi
    roles_known=1
    echo "        ${h} mode=${mode} nodes=${nodes:-?} zxid=${zxid:-?} version=${ver%%,*}"
    record_pass "srvr ${h} (${mode})"
    case "$mode" in
      leader) leaders="${leaders} ${h}" ;;
      follower) followers="${followers} ${h}" ;;
      observer) observers="${observers} ${h}" ;;
      standalone) leaders="${leaders} ${h}" ;;
    esac
  done
else
  for h in "${live_hosts[@]}"; do
    record_skip "srvr ${h} (srvr not in 4lw.commands.whitelist)"
  done
fi

leader_count=$(printf '%s' "$leaders" | wc -w | tr -d ' ')
follower_count=$(printf '%s' "$followers" | wc -w | tr -d ' ')
observer_count=$(printf '%s' "$observers" | wc -w | tr -d ' ')

if (( roles_known == 0 )); then
  record_skip "exactly one leader (no server reported a Mode)"
elif (( leader_count == 1 && follower_count == ${#live_hosts[@]} - 1 - observer_count )); then
  echo "        leader=${leaders# } followers=${followers# }${observers:+ observers=${observers# }}"
  record_pass "exactly one leader, ${follower_count} follower(s)"
elif (( leader_count == 0 )); then
  record_fail "exactly one leader (no leader elected - the ensemble has no quorum)"
elif (( leader_count > 1 )); then
  record_fail "exactly one leader (split ensemble, leaders:${leaders})"
else
  record_fail "exactly one leader (leader${leaders}, but only ${follower_count} of $(( ${#live_hosts[@]} - 1 - observer_count )) expected follower(s))"
fi

echo ""
echo "---- ensemble membership ----"
# "conf" reports the membership the server is actually running with, which can
# lag the Ambari host list if a ZOOKEEPER_SERVER was added without a restart.
if ! lw_allowed conf; then
  record_skip "ensemble membership matches Ambari (conf not in 4lw.commands.whitelist)"
elif (( ${#live_hosts[@]} == 0 )); then
  record_skip "ensemble membership matches Ambari (no server answered)"
else
  conf_out="$(zk_4lw "${live_hosts[0]}" "$ZOOKEEPER_CLIENT_PORT" conf 2>/dev/null || true)"
  if lw_refused "$conf_out" || [[ -z "$conf_out" ]]; then
    record_skip "ensemble membership matches Ambari (conf gave no membership)"
  else
    members="$(printf '%s\n' "$conf_out" \
      | awk -F'=' '/^server\.[0-9]+=/ { split($2, a, ":"); split(a[1], b, "."); print b[1] }' \
      | sort)"
    if [[ -z "$members" ]]; then
      record_skip "ensemble membership matches Ambari (conf gave no membership)"
    else
      expected="$(printf '%s\n' ${ZOOKEEPER_HOSTS} | sed 's/\..*//' | sort)"
      echo "        running: $(printf '%s' "$members" | tr '\n' ' ')"
      if [[ "$members" == "$expected" ]]; then
        record_pass "ensemble membership matches Ambari"
      else
        echo "        Ambari:  $(printf '%s' "$expected" | tr '\n' ' ')" >&2
        record_fail "ensemble membership matches Ambari (running set differs)"
      fi
    fi
  fi
fi

echo ""
echo "---- server metrics ----"
declare -a znode_counts=()
leader_host="${leaders# }"
leader_host="${leader_host%% *}"
synced_checked=0
if lw_allowed mntr; then
  for h in "${live_hosts[@]}"; do
    out="$(zk_4lw "$h" "$ZOOKEEPER_CLIENT_PORT" mntr 2>/dev/null || true)"
    if lw_refused "$out"; then
      record_skip "mntr ${h} (not in 4lw.commands.whitelist)"
      continue
    fi
    state="$(mntr_value "$out" zk_server_state)"
    if [[ -z "$state" ]]; then
      record_fail "mntr ${h} (no zk_server_state in reply)"
      continue
    fi
    conns="$(mntr_value "$out" zk_num_alive_connections)"
    znodes="$(mntr_value "$out" zk_znode_count)"
    watches="$(mntr_value "$out" zk_watch_count)"
    echo "        ${h} state=${state} connections=${conns:-?} znodes=${znodes:-?} watches=${watches:-?}"
    [[ -n "$znodes" ]] && znode_counts+=("${h}=${znodes}")
    record_pass "mntr ${h}"

    if [[ "$h" == "$leader_host" && "$state" == "leader" ]]; then
      lf="$(mntr_value "$out" zk_followers)"
      sf="$(mntr_value "$out" zk_synced_followers)"
      echo "        ${h} followers=${lf:-?} synced_followers=${sf:-?}"
      synced_checked=1
      if [[ -z "$sf" ]]; then
        record_skip "leader synced followers (zk_synced_followers not reported)"
      elif (( ${sf%%.*} == follower_count )); then
        record_pass "leader synced followers (${sf%%.*}/${follower_count})"
      else
        record_fail "leader synced followers (${sf%%.*} synced, ${follower_count} follower(s) in the ensemble)"
      fi
    fi
  done
  if (( synced_checked == 0 )); then
    record_skip "leader synced followers (no leader answered mntr)"
  fi
else
  for h in "${live_hosts[@]}"; do
    record_skip "mntr ${h} (mntr not in 4lw.commands.whitelist)"
  done
  record_skip "leader synced followers (mntr not in 4lw.commands.whitelist)"
fi

if (( ${#znode_counts[@]} < 2 )); then
  record_skip "ensemble znode count consistency (need mntr from at least 2 servers)"
else
  min=""
  max=""
  for entry in "${znode_counts[@]}"; do
    v="${entry#*=}"
    if [[ -z "$min" ]] || (( v < min )); then min="$v"; fi
    if [[ -z "$max" ]] || (( v > max )); then max="$v"; fi
  done
  spread=$(( max - min ))
  echo "        znode counts: ${znode_counts[*]} (spread ${spread})"
  if (( spread <= ZOOKEEPER_ZNODE_COUNT_TOLERANCE )); then
    record_pass "ensemble znode count consistency (spread ${spread} <= ${ZOOKEEPER_ZNODE_COUNT_TOLERANCE})"
  else
    record_fail "ensemble znode count consistency (spread ${spread} > ${ZOOKEEPER_ZNODE_COUNT_TOLERANCE}; replicas diverged)"
  fi
fi

echo ""
echo "---- admin server ----"
if [[ "$ZOOKEEPER_SKIP_ADMIN" == "1" ]]; then
  record_skip "admin server ruok (ZOOKEEPER_SKIP_ADMIN=1)"
elif [[ "$(printf '%s' "$zk_admin_enabled" | tr 'A-Z' 'a-z')" == "false" ]]; then
  record_skip "admin server ruok (admin.enableServer=false)"
else
  for h in "${live_hosts[@]}"; do
    if ! tcp_open "$h" "$ZOOKEEPER_ADMIN_PORT"; then
      record_skip "admin server ruok ${h} (nothing listening on :${ZOOKEEPER_ADMIN_PORT})"
      continue
    fi
    body="$(curl -sS -m 10 $CURL_EXTRA_OPTS "http://${h}:${ZOOKEEPER_ADMIN_PORT}/commands/ruok" 2>/dev/null || true)"
    if printf '%s' "$body" | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
sys.exit(0 if d.get('command') == 'ruok' and d.get('error') is None else 1)
" 2>/dev/null; then
      echo "        ${h}:${ZOOKEEPER_ADMIN_PORT}/commands/ruok -> ok"
      record_pass "admin server ruok ${h}"
    else
      record_fail "admin server ruok ${h} (unexpected reply on :${ZOOKEEPER_ADMIN_PORT})"
    fi
  done
fi

echo ""
echo "---- data path ----"
znode_created=0
cleanup_done=0
cleanup() {
  [[ "$cleanup_done" == "1" ]] && return 0
  cleanup_done=1
  if [[ "$ZOOKEEPER_KEEP_ZNODE" != "1" && "$znode_created" == "1" ]]; then
    echo ""
    echo "---- cleanup ----"
    zk_cli delete "$ZOOKEEPER_ZNODE" >/dev/null 2>&1 || true
    if [[ -z "$(zk_cli get "$ZOOKEEPER_ZNODE" 2>/dev/null | grep -v 'NoNode' || true)" ]]; then
      echo "        removed ${ZOOKEEPER_ZNODE}"
    else
      echo "        WARN: ${ZOOKEEPER_ZNODE} may still exist"
    fi
  fi
}

cli_possible=1
cli_reason=""
if [[ "$ZOOKEEPER_SKIP_CLI" == "1" ]]; then
  cli_possible=0
  cli_reason="ZOOKEEPER_SKIP_CLI=1"
elif [[ ! -x "$ZOOKEEPER_CLI_BIN" ]]; then
  cli_possible=0
  cli_reason="zkCli.sh not executable at ${ZOOKEEPER_CLI_BIN}"
elif ! resolve_java_home; then
  cli_possible=0
  cli_reason="could not resolve JAVA_HOME"
fi

if (( cli_possible == 1 )); then
  export JAVA_HOME
  echo "        zkCli:     ${ZOOKEEPER_CLI_BIN}"
  echo "        JAVA_HOME: ${JAVA_HOME}"

  # The shipped client JAAS section uses useTicketCache=true, so the CLI needs a
  # ticket in the cache. authProvider.1 is SASL but requireClientAuthScheme is
  # unset here, so a missing ticket only downgrades to an unauthenticated session
  # rather than failing outright - hence SKIPPED, not FAIL, when no keytab works.
  if [[ "$ZOOKEEPER_SKIP_KINIT" == "1" ]]; then
    record_skip "kinit (ZOOKEEPER_SKIP_KINIT=1)"
  else
    keytab="${ZOOKEEPER_KEYTAB:-/etc/security/keytabs/smokeuser.headless.keytab}"
    principal="${ZOOKEEPER_PRINCIPAL:-ambari-qa-${cluster}}"
    if ! command -v kinit >/dev/null 2>&1; then
      record_skip "kinit (kinit not installed)"
    elif [[ ! -r "$keytab" ]]; then
      record_skip "kinit (${keytab} not readable - run under sudo or set ZOOKEEPER_KEYTAB)"
    elif kinit -kt "$keytab" "$principal" 2>/dev/null; then
      echo "        kinit ${principal} from ${keytab}"
      record_pass "kinit ${principal}"
    else
      record_fail "kinit ${principal} from ${keytab}"
    fi
  fi

  trap cleanup EXIT
  znode_value="odp-zk-smoke-$(date -u '+%Y%m%dT%H%M%SZ')-v1"
  znode_value2="${znode_value%-v1}-v2"
  znode_name="${ZOOKEEPER_ZNODE##*/}"
  znode_parent="${ZOOKEEPER_ZNODE%/*}"
  [[ -z "$znode_parent" ]] && znode_parent="/"

  out="$(zk_cli create "$ZOOKEEPER_ZNODE" "$znode_value")"
  if printf '%s' "$out" | grep -q "^Created ${ZOOKEEPER_ZNODE}\$"; then
    znode_created=1
    echo "        created ${ZOOKEEPER_ZNODE}"
    record_pass "znode create"
  else
    printf '%s\n' "$out" | tail -5 | sed 's/^/        /' >&2
    record_fail "znode create"
  fi

  if (( znode_created == 1 )); then
    got="$(zk_cli get "$ZOOKEEPER_ZNODE" | tail -1)"
    if [[ "$got" == "$znode_value" ]]; then
      echo "        get -> ${got}"
      record_pass "znode get round-trip"
    else
      record_fail "znode get round-trip (expected '${znode_value}', got '${got}')"
    fi

    zk_cli set "$ZOOKEEPER_ZNODE" "$znode_value2" >/dev/null
    got="$(zk_cli get "$ZOOKEEPER_ZNODE" | tail -1)"
    if [[ "$got" == "$znode_value2" ]]; then
      echo "        set + get -> ${got}"
      record_pass "znode set round-trip"
    else
      record_fail "znode set round-trip (expected '${znode_value2}', got '${got}')"
    fi

    listing="$(zk_cli ls "$znode_parent" | tail -1)"
    if [[ ",${listing//[][ ]/}," == *",${znode_name},"* ]]; then
      echo "        ls ${znode_parent} lists ${znode_name}"
      record_pass "znode listed under ${znode_parent}"
    else
      record_fail "znode listed under ${znode_parent} (${znode_name} missing)"
    fi

    zk_cli delete "$ZOOKEEPER_ZNODE" >/dev/null
    got="$(zk_cli get "$ZOOKEEPER_ZNODE" || true)"
    if printf '%s' "$got" | grep -q 'NoNodeException'; then
      echo "        delete -> NoNode on re-read"
      record_pass "znode delete"
      znode_created=0
    else
      record_fail "znode delete (still readable: '${got}')"
    fi
  else
    record_skip "znode get round-trip (create failed)"
    record_skip "znode set round-trip (create failed)"
    record_skip "znode listed under ${znode_parent} (create failed)"
    record_skip "znode delete (create failed)"
  fi

  echo ""
  echo "---- service znodes ----"
  root_listing="$(zk_cli ls / | tail -1)"
  root_entries="${root_listing//[][]/}"
  root_entries="$(printf '%s' "${root_entries//,/ }" | tr -s ' ')"
  if [[ -z "${root_entries//[[:space:]]/}" ]]; then
    record_fail "top-level znode listing (ls / returned nothing)"
  else
    echo "        ls / -> ${root_entries}"
    record_pass "top-level znode listing"
    known="hbase-secure hbase-unsecure atsv2-hbase-secure infra-solr brokers kafka3 hiveserver2 hive rmstore druid nifi zookeeper"
    found=""
    for k in $known; do
      [[ " ${root_entries} " == *" ${k} "* ]] && found="${found} ${k}"
    done
    echo "        service znodes present:${found:- <none>}"
  fi

  if [[ -z "${ZOOKEEPER_EXPECTED_ZNODES//[[:space:]]/}" ]]; then
    record_skip "expected znodes present (ZOOKEEPER_EXPECTED_ZNODES not set)"
  else
    missing=""
    for want in ${ZOOKEEPER_EXPECTED_ZNODES//,/ }; do
      w="${want#/}"
      [[ " ${root_entries} " == *" ${w} "* ]] || missing="${missing} /${w}"
    done
    if [[ -z "$missing" ]]; then
      record_pass "expected znodes present (${ZOOKEEPER_EXPECTED_ZNODES})"
    else
      record_fail "expected znodes present (missing:${missing})"
    fi
  fi
else
  echo "        SKIPPED: ${cli_reason}"
  record_skip "kinit (${cli_reason})"
  record_skip "znode create (${cli_reason})"
  record_skip "znode get round-trip (${cli_reason})"
  record_skip "znode set round-trip (${cli_reason})"
  record_skip "znode listed under parent (${cli_reason})"
  record_skip "znode delete (${cli_reason})"
  record_skip "top-level znode listing (${cli_reason})"
  record_skip "expected znodes present (${cli_reason})"
fi

cleanup

echo ""
echo "---- summary ----"
for r in "${results[@]}"; do
  echo "    $r"
done
echo "    PASS=${pass} FAIL=${fail} SKIPPED=${skip}"

if (( fail > 0 )); then
  die "ZooKeeper sample smoke had ${fail} failing check(s)."
fi
echo "OK: ZooKeeper sample smoke finished (PASS=${pass} SKIPPED=${skip})."
