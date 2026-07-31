#!/usr/bin/env bash
# JupyterHub pre-reqs for RHEL 8 / 9 (and Rocky / Alma / Oracle Linux)
# ODP *-2 line (Python 3.8 + Node.js 20)
#
# Docs:
#   https://docs.acceldata.io/odp/odp-3.2.3.5-2/documentation/jupyter-prerequisites
#
# Matches ansible-rhel8/playbooks/roles/pre-reqs/tasks/jupyterhub.yml (*-2):
#   dnf install python38-devel
#   dnf groupinstall "Development Tools"
#   dnf module reset nodejs && enable nodejs:20 && install nodejs
#   npm install -g configurable-http-proxy
#
# Idempotent. Run as root on the JupyterHub host:
#   sudo ./prereqs/install-jupyterhub-prereqs-rhel8.sh
set -euo pipefail

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

if ! command -v dnf >/dev/null 2>&1; then
  die "dnf not found; this script requires RHEL 8+ with dnf."
fi

if command -v yum >/dev/null 2>&1; then
  YUM="yum"
else
  YUM="dnf"
fi

log "Installing python38-devel..."
# AppStream package name is typically python38-devel on RHEL 8/9.
dnf install -y python38-devel || dnf install -y python3.8-devel

# Ensure the python3.8 interpreter is present (devel sometimes assumes it).
if ! command -v python3.8 >/dev/null 2>&1; then
  log "Installing python3.8 interpreter..."
  dnf module enable python3.8 -y 2>/dev/null || true
  dnf install -y python3.8 || true
fi

log "Installing Development Tools group..."
"${YUM}" -y groupinstall "Development Tools" || dnf -y group install "Development Tools"

log "Resetting and enabling Node.js 20 module stream..."
# Docs install a default nodejs first, then reset to 20. Prefer enabling 20
# directly so the final node/npm versions match Node.js 20.
dnf module reset nodejs -y || true
dnf module enable nodejs:20 -y

log "Installing Node.js 20 (includes npm)..."
dnf install -y nodejs
# Some images ship npm as a separate package; ensure it is present.
"${YUM}" install -y npm 2>/dev/null || dnf install -y npm 2>/dev/null || true

command -v node >/dev/null || die "node not found after install."
command -v npm >/dev/null || die "npm not found after install."

NODE_MAJOR="$(node --version | sed -E 's/^v([0-9]+).*/\1/')"
[[ "${NODE_MAJOR}" == "20" ]] || warn "Expected Node.js 20.x; got $(node --version)."

log "Installing configurable-http-proxy..."
npm install -g configurable-http-proxy

# Symlinks Ambari / Ansible often expect under /usr/local/bin
if [[ -x /usr/bin/python3.8 ]] && [[ ! -e /usr/local/bin/python3.8 ]]; then
  ln -sf /usr/bin/python3.8 /usr/local/bin/python3.8
  log "Created symlink /usr/local/bin/python3.8"
fi

if [[ -x /usr/bin/configurable-http-proxy ]] && [[ ! -e /usr/local/bin/configurable-http-proxy ]]; then
  ln -sf /usr/bin/configurable-http-proxy /usr/local/bin/configurable-http-proxy
elif command -v configurable-http-proxy >/dev/null && [[ ! -e /usr/local/bin/configurable-http-proxy ]]; then
  CHP_PATH="$(command -v configurable-http-proxy)"
  ln -sf "${CHP_PATH}" /usr/local/bin/configurable-http-proxy
fi

log "Verification"
echo "----"
python3.8 --version 2>/dev/null || warn "python3.8 not on PATH"
node --version
npm --version
configurable-http-proxy --version || true
rpm -q python38-devel 2>/dev/null || rpm -q python3.8-devel 2>/dev/null || true
ls -l /usr/bin/python3.8 /usr/local/bin/python3.8 /usr/local/bin/configurable-http-proxy 2>/dev/null || true
echo "----"
log "JupyterHub RHEL pre-reqs installed successfully."
log "Ref: https://docs.acceldata.io/odp/odp-3.2.3.5-2/documentation/jupyter-prerequisites"
log "Next: install/start JupyterHub via Ambari, then run ../jupyterhub-sample-smoke.sh"
