#!/usr/bin/env bash
# Pinot pre-reqs for Ubuntu/Debian: JDK 11 + Python requests
#
# Doc mapping:
#   sudo apt install openjdk-11-jdk -y
#   python -m pip install requests
#
# Idempotent. Run as root on the Pinot host:
#   sudo ./prereqs/install-pinot-prereqs-ubuntu.sh
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
  ubuntu|debian) ;;
  *) die "This script is for Ubuntu/Debian only (got: ${ID:-unknown})." ;;
esac

export DEBIAN_FRONTEND=noninteractive

log "Updating apt indexes..."
apt-get update -y

log "Installing OpenJDK 11..."
apt-get install -y openjdk-11-jdk

log "Ensuring pip is available..."
apt-get install -y python3-pip python3-distutils || true

# Prefer python3; fall back to python if present
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
update-alternatives --display java 2>/dev/null | head -n 20 || true
"${PY_BIN}" --version
"${PY_BIN}" -m pip show requests | sed -n '1,3p'
echo "----"
log "Pinot pre-reqs installed successfully (Ubuntu/Debian)."
