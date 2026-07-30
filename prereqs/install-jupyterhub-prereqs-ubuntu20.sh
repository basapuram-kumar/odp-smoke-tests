#!/usr/bin/env bash
# JupyterHub pre-reqs for Ubuntu 20.04 / 22.04 / ODP *-2 (Python 3.8 + Node.js 20)
#
# Matches ansible-rhel8/playbooks/roles/pre-reqs/tasks/jupyterhub.yml (*-2).
#
# Package-name notes vs older docs:
#   python38-dev  -> python3.8-dev
#   nodesource URL must be https://deb.nodesource.com/setup_20.x
#
# Idempotent. Run as root on the JupyterHub host:
#   sudo ./prereqs/install-jupyterhub-prereqs-ubuntu20.sh
set -euo pipefail

PYTHON_MM="3.8"
PY_BIN="python${PYTHON_MM}"

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

export DEBIAN_FRONTEND=noninteractive

log "Updating apt indexes..."
apt-get update -y

log "Installing Python ${PYTHON_MM}, headers, venv, and build tools..."
apt-get install -y \
  curl \
  ca-certificates \
  gnupg \
  "python${PYTHON_MM}" \
  "python${PYTHON_MM}-dev" \
  "python${PYTHON_MM}-venv" \
  build-essential

if [[ ! -e /usr/local/bin/python3.8 ]]; then
  ln -sf "/usr/bin/${PY_BIN}" /usr/local/bin/python3.8
  log "Created symlink /usr/local/bin/python3.8"
fi

log "Installing Node.js 20 from NodeSource..."
apt-get remove -y nodejs npm 2>/dev/null || true
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt-get install -y nodejs

command -v node >/dev/null || die "node not found after install."
command -v npm >/dev/null || die "npm not found after install."

NODE_MAJOR="$(node --version | sed -E 's/^v([0-9]+).*/\1/')"
[[ "${NODE_MAJOR}" == "20" ]] || warn "Expected Node.js 20.x; got $(node --version)."

log "Installing configurable-http-proxy..."
npm install -g configurable-http-proxy

if [[ -x /usr/bin/configurable-http-proxy ]] && [[ ! -e /usr/local/bin/configurable-http-proxy ]]; then
  ln -sf /usr/bin/configurable-http-proxy /usr/local/bin/configurable-http-proxy
elif command -v configurable-http-proxy >/dev/null && [[ ! -e /usr/local/bin/configurable-http-proxy ]]; then
  CHP_PATH="$(command -v configurable-http-proxy)"
  ln -sf "${CHP_PATH}" /usr/local/bin/configurable-http-proxy
fi

log "Verification"
echo "----"
"${PY_BIN}" --version
node --version
npm --version
configurable-http-proxy --version || true
ls -l /usr/bin/python3.8 /usr/local/bin/python3.8 /usr/local/bin/configurable-http-proxy 2>/dev/null || true
echo "----"
log "JupyterHub pre-reqs installed successfully."
log "Next: install/start JupyterHub via Ambari, then run ../jupyterhub-sample-smoke.sh"
