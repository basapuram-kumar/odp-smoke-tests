#!/usr/bin/env bash
# Pinot pre-reqs for RHEL/CentOS/Rocky/Alma/Oracle: JDK 11 + Python requests
#
# Doc mapping:
#   sudo dnf install java-11-openjdk-devel -y
#   python -m pip install requests
#
# Idempotent. Run as root on the Pinot host:
#   sudo ./prereqs/install-pinot-prereqs-rhel.sh
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
  rhel|centos|rocky|almalinux|ol|fedora) ;;
  *) die "This script is for RHEL-family only (got: ${ID:-unknown})." ;;
esac

PKG=""
if command -v dnf >/dev/null 2>&1; then
  PKG="dnf"
elif command -v yum >/dev/null 2>&1; then
  PKG="yum"
else
  die "Neither dnf nor yum found."
fi

log "Installing OpenJDK 11 devel (${PKG})..."
"${PKG}" install -y java-11-openjdk-devel

log "Ensuring pip is available..."
"${PKG}" install -y python3-pip 2>/dev/null || "${PKG}" install -y python38-pip 2>/dev/null || true

PY_BIN=""
if command -v python3 >/dev/null 2>&1; then
  PY_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PY_BIN="python"
else
  die "Neither python3 nor python found."
fi

if ! "${PY_BIN}" -m pip --version >/dev/null 2>&1; then
  log "Bootstrapping pip for ${PY_BIN}..."
  curl -sS https://bootstrap.pypa.io/get-pip.py | "${PY_BIN}"
fi

log "Installing Python package: requests..."
"${PY_BIN}" -m pip install --upgrade requests

log "Verification"
echo "----"
java -version 2>&1 || true
javac -version 2>&1 || true
rpm -q java-11-openjdk-devel 2>/dev/null || true
"${PY_BIN}" --version
"${PY_BIN}" -m pip show requests | sed -n '1,3p'
echo "----"
log "Pinot pre-reqs installed successfully (RHEL-family)."
