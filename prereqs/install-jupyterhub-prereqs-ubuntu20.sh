#!/usr/bin/env bash
# JupyterHub OS pre-reqs for Ubuntu 20.04 / 22.04
#
# Docs (ODP 3.3.x / current):
#   https://docs.acceldata.io/odp/documentation/jupyter-prerequisites
#
# Matches ansible-rhel8/playbooks/roles/pre-reqs/tasks/jupyterhub.yml
# for the *-1 / *-3 release line (Python 3.11 + Node.js 20):
#   apt install -y python3.11-dev
#   apt remove -y nodejs npm
#   curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
#   apt install -y nodejs
#   npm install -g configurable-http-proxy
#
# For ODP *-2 (Python 3.8) set:
#   ODP_JHUB_PYTHON=3.8 sudo -E ./prereqs/install-jupyterhub-prereqs-ubuntu20.sh
#
# Idempotent. Run as root on the JupyterHub host:
#   sudo ./prereqs/install-jupyterhub-prereqs-ubuntu20.sh
set -euo pipefail

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

[[ "${ID:-}" == "ubuntu" ]] || die "This script is for Ubuntu only (got: ${ID:-unknown})."
case "${VERSION_ID:-}" in
  20.04|22.04) ;;
  *) warn "Expected Ubuntu 20.04 or 22.04; continuing on ${VERSION_ID:-unknown}." ;;
esac

case "${ODP_JHUB_PYTHON}" in
  3.11|3.8) ;;
  *) die "Unsupported ODP_JHUB_PYTHON=${ODP_JHUB_PYTHON} (use 3.11 or 3.8)." ;;
esac

PYTHON_MM="${ODP_JHUB_PYTHON}"
PY_BIN="python${PYTHON_MM}"

export DEBIAN_FRONTEND=noninteractive

log "Updating apt indexes..."
apt-get update -y

log "Installing Python ${PYTHON_MM} packages (dev/headers)..."
apt-get install -y \
  curl \
  ca-certificates \
  gnupg \
  "python${PYTHON_MM}" \
  "python${PYTHON_MM}-dev" \
  "python${PYTHON_MM}-venv" \
  build-essential

if [[ ! -e "/usr/local/bin/${PY_BIN}" ]]; then
  ln -sf "/usr/bin/${PY_BIN}" "/usr/local/bin/${PY_BIN}"
  log "Created symlink /usr/local/bin/${PY_BIN}"
fi

log "Installing Node.js 20 from NodeSource..."
apt-get remove -y nodejs npm 2>/dev/null || true
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt-get install -y nodejs

command -v node >/dev/null || die "node not found after install."
command -v npm >/dev/null || die "npm not found after install."

NODE_MAJOR="$(node --version | sed -E 's/^v([0-9]+).*/\1/')"
[[ "${NODE_MAJOR}" -ge 20 ]] || die "Node.js 20+ required; got $(node --version)."
[[ "${NODE_MAJOR}" == "20" ]] || warn "Docs recommend Node.js 20.x; got $(node --version)."

log "Installing configurable-http-proxy globally..."
npm install -g configurable-http-proxy

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
"${PY_BIN}" --version
node --version
npm --version
configurable-http-proxy --version || true
ls -l "/usr/bin/${PY_BIN}" "/usr/local/bin/${PY_BIN}" /usr/local/bin/configurable-http-proxy 2>/dev/null || true
echo "----"
log "JupyterHub pre-reqs installed successfully."
log "Ref: https://docs.acceldata.io/odp/documentation/jupyter-prerequisites"
log "Next: install/start JupyterHub via Ambari, then run ../jupyterhub-sample-smoke.sh"
