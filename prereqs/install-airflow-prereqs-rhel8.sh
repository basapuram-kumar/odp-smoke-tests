#!/usr/bin/env bash
# Airflow pre-reqs for RHEL 8 / Rocky 8 / Alma 8 / Oracle Linux 8
# ODP *-2 line (Python 3.8)
#
# Docs:
#   https://docs.acceldata.io/odp/odp-3.2.3.5-2/documentation/single-node-installation
#
# Steps covered:
#   sudo dnf module enable python3.8 -y
#   sudo dnf install -y python3.8
#   sudo yum install python38-devel -y
#   sudo yum -y groupinstall "Development Tools"
#
# Idempotent. Run as root on the Airflow host:
#   sudo ./prereqs/install-airflow-prereqs-rhel8.sh
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
[[ "${OS_MAJOR}" == "8" ]] || warn "Expected RHEL/Rocky/Alma 8; continuing on ${ID} ${VERSION_ID:-unknown}."

if ! command -v dnf >/dev/null 2>&1; then
  die "dnf not found; this script requires RHEL 8+ with dnf."
fi

PKG="dnf"
if command -v yum >/dev/null 2>&1; then
  YUM="yum"
else
  YUM="dnf"
fi

log "Enabling Python 3.8 module stream..."
dnf module enable python3.8 -y

log "Installing python3.8..."
dnf install -y python3.8

log "Installing python38-devel..."
# Package name on AppStream is typically python38-devel
"${YUM}" install -y python38-devel || dnf install -y python3.8-devel

log "Installing Development Tools group..."
"${YUM}" -y groupinstall "Development Tools" || dnf -y group install "Development Tools"

# Symlink Ambari/Ansible often expects under /usr/local/bin
if [[ -x /usr/bin/python3.8 ]] && [[ ! -e /usr/local/bin/python3.8 ]]; then
  ln -sf /usr/bin/python3.8 /usr/local/bin/python3.8
  log "Created symlink /usr/local/bin/python3.8"
fi

# Ensure pip for python3.8 when available
if ! python3.8 -m pip --version >/dev/null 2>&1; then
  log "Installing pip for Python 3.8 (optional helper)..."
  dnf install -y python38-pip 2>/dev/null || dnf install -y python3-pip 2>/dev/null || true
fi

log "Verification"
echo "----"
python3.8 --version
which python3.8
python3.8 -c "import sqlite3; print('sqlite3=', sqlite3.sqlite_version)" || warn "sqlite3 import failed"
rpm -q python38-devel 2>/dev/null || rpm -q python3.8-devel 2>/dev/null || true
echo "----"
log "Airflow RHEL 8 pre-reqs installed successfully."
log "Ref: https://docs.acceldata.io/odp/odp-3.2.3.5-2/documentation/single-node-installation"
