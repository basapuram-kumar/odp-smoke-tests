#!/usr/bin/env bash
# JupyterHub OS pre-reqs for RHEL 8 / 9 (Rocky / Alma / Oracle Linux)
#
# Docs (ODP 3.3.x / current):
#   https://docs.acceldata.io/odp/documentation/jupyter-prerequisites
#
# Matches ansible-rhel8/playbooks/roles/pre-reqs/tasks/jupyterhub.yml
# for the *-1 / *-3 release line (Python 3.11 + Node.js 20):
#   dnf install python3.11-devel
#   dnf module reset nodejs && enable nodejs:20 && install nodejs
#   npm install -g configurable-http-proxy
#
# For ODP *-2 (Python 3.8) set:
#   ODP_JHUB_PYTHON=3.8 sudo -E ./prereqs/install-jupyterhub-prereqs-rhel8.sh
#
# Idempotent. Run as root on the JupyterHub host:
#   sudo ./prereqs/install-jupyterhub-prereqs-rhel8.sh
set -euo pipefail

# Default to Python 3.11 per current docs; override with ODP_JHUB_PYTHON=3.8 for *-2.
ODP_JHUB_PYTHON="${ODP_JHUB_PYTHON:-3.11}"

log() { echo "[INFO] $*"; }
warn() { echo "[WARN] $*"; }
die() { echo "[ERROR] $*" >&2; exit 1; }

[[ "$(id -u)" -eq 0 ]] || die "Run as root (sudo)."

if [[ -f /etc/os-release ]]; then
  # shellcheck disable=SC1091
  . /etc/os-release
else
  die "Cannot detect OS (/etc/os-release missing)."
fi

case "${ID:-}" in
  rhel|rocky|almalinux|ol|centos) ;;
  *) die "This script is for RHEL-family only (got: ${ID:-unknown})." ;;
esac

OS_MAJOR="${VERSION_ID%%.*}"
case "${OS_MAJOR}" in
  8|9) ;;
  *) warn "Expected RHEL/Rocky/Alma 8 or 9; continuing on ${ID} ${VERSION_ID:-unknown}." ;;
esac

command -v dnf >/dev/null 2>&1 || die "dnf not found; this script requires RHEL 8+ with dnf."

case "${ODP_JHUB_PYTHON}" in
  3.11)
    PY_DEVEL_PKGS=(python3.11-devel)
    PY_BIN="python3.11"
    ;;
  3.8)
    PY_DEVEL_PKGS=(python38-devel python3.8-devel)
    PY_BIN="python3.8"
    ;;
  *)
    die "Unsupported ODP_JHUB_PYTHON=${ODP_JHUB_PYTHON} (use 3.11 or 3.8)."
    ;;
esac

log "Installing ${PY_DEVEL_PKGS[0]} (ODP_JHUB_PYTHON=${ODP_JHUB_PYTHON})..."
installed=0
for pkg in "${PY_DEVEL_PKGS[@]}"; do
  if dnf install -y "${pkg}"; then
    installed=1
    break
  fi
done
[[ "${installed}" -eq 1 ]] || die "Failed to install Python devel package(s): ${PY_DEVEL_PKGS[*]}"

if ! command -v "${PY_BIN}" >/dev/null 2>&1; then
  log "Installing ${PY_BIN} interpreter..."
  dnf install -y "${PY_BIN}" || true
fi

log "Resetting and enabling Node.js 20 module stream..."
dnf module reset nodejs -y || true
dnf module enable nodejs:20 -y

log "Installing Node.js 20 (includes npm)..."
dnf install -y nodejs
dnf install -y npm 2>/dev/null || true

command -v node >/dev/null || die "node not found after install."
command -v npm >/dev/null || die "npm not found after install."

NODE_MAJOR="$(node --version | sed -E 's/^v([0-9]+).*/\1/')"
[[ "${NODE_MAJOR}" -ge 20 ]] || die "Node.js 20+ required; got $(node --version)."
[[ "${NODE_MAJOR}" == "20" ]] || warn "Docs recommend Node.js 20.x; got $(node --version)."

log "Installing configurable-http-proxy globally..."
npm install -g configurable-http-proxy

if [[ -x "/usr/bin/${PY_BIN}" ]] && [[ ! -e "/usr/local/bin/${PY_BIN}" ]]; then
  ln -sf "/usr/bin/${PY_BIN}" "/usr/local/bin/${PY_BIN}"
  log "Created symlink /usr/local/bin/${PY_BIN}"
fi

if [[ -x /usr/bin/configurable-http-proxy ]] && [[ ! -e /usr/local/bin/configurable-http-proxy ]]; then
  ln -sf /usr/bin/configurable-http-proxy /usr/local/bin/configurable-http-proxy
elif command -v configurable-http-proxy >/dev/null && [[ ! -e /usr/local/bin/configurable-http-proxy ]]; then
  CHP_PATH="$(command -v configurable-http-proxy)"
  ln -sf "${CHP_PATH}" /usr/local/bin/configurable-http-proxy
fi

[[ -x /usr/local/bin/configurable-http-proxy ]] \
  || die "configurable-http-proxy missing under /usr/local/bin (Ambari expects this path)."

log "Verification"
echo "----"
"${PY_BIN}" --version 2>/dev/null || warn "${PY_BIN} not on PATH"
node --version
npm --version
configurable-http-proxy --version || true
ls -l "/usr/bin/${PY_BIN}" "/usr/local/bin/${PY_BIN}" /usr/local/bin/configurable-http-proxy 2>/dev/null || true
echo "----"
log "JupyterHub RHEL pre-reqs installed successfully."
log "Ref: https://docs.acceldata.io/odp/documentation/jupyter-prerequisites"
log "Next: install/start JupyterHub via Ambari, then run ../jupyterhub-sample-smoke.sh"
