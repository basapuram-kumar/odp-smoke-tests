#!/usr/bin/env bash
# Airflow pre-reqs for Ubuntu 20.04 (focal) / ODP *-2 (Python 3.8)
#
# RHEL 8 equivalents covered here:
#   dnf module enable python3.8          -> N/A (system Python on focal)
#   dnf install python3.8                -> apt install python3.8
#   yum install python38-devel           -> apt install python3.8-dev python3.8-venv
#   yum groupinstall "Development Tools" -> apt install build-essential
#
# Idempotent. Run as root on the Airflow host:
#   sudo ./prereqs/install-airflow-prereqs-ubuntu20.sh
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
[[ "${VERSION_ID:-}" == "20.04" ]] || warn "Expected Ubuntu 20.04; continuing on ${VERSION_ID:-unknown}."

export DEBIAN_FRONTEND=noninteractive

log "Updating apt indexes..."
apt-get update -y

log "Installing Python ${PYTHON_MM}, headers, venv, and build tools..."
apt-get install -y \
  curl \
  ca-certificates \
  "python${PYTHON_MM}" \
  "python${PYTHON_MM}-dev" \
  "python${PYTHON_MM}-venv" \
  python3-distutils \
  build-essential \
  libssl-dev \
  libffi-dev \
  zlib1g-dev \
  libbz2-dev \
  libsqlite3-dev \
  pkg-config

apt-get install -y "python${PYTHON_MM}-distutils" 2>/dev/null || true

command -v "${PY_BIN}" >/dev/null || die "${PY_BIN} not found after apt install."

if ! "${PY_BIN}" -m pip --version >/dev/null 2>&1; then
  log "Bootstrapping pip for ${PY_BIN} via get-pip.py..."
  curl -sS https://bootstrap.pypa.io/pip/3.8/get-pip.py | "${PY_BIN}"
fi

log "Upgrading pip/setuptools/wheel..."
"${PY_BIN}" -m pip install --upgrade pip setuptools wheel

if [[ ! -e /usr/local/bin/python3.8 ]]; then
  ln -sf "/usr/bin/${PY_BIN}" /usr/local/bin/python3.8
  log "Created symlink /usr/local/bin/python3.8"
fi

if [[ ! -x /usr/local/bin/pip3.8 ]]; then
  printf '#!/bin/sh\nexec /usr/bin/%s -m pip "$@"\n' "${PY_BIN}" >/usr/local/bin/pip3.8
  chmod 755 /usr/local/bin/pip3.8
  log "Created shim /usr/local/bin/pip3.8"
fi

log "Verification"
echo "----"
"${PY_BIN}" --version
"${PY_BIN}" -m pip --version
pip3.8 --version
"${PY_BIN}" -c "import sqlite3, venv; print('sqlite3=', sqlite3.sqlite_version); print('venv=OK')"
ls -l /usr/bin/python3.8* /usr/local/bin/python3.8 /usr/local/bin/pip3.8 2>/dev/null || true
echo "----"
log "Airflow Python ${PYTHON_MM} pre-reqs installed successfully."
log "Next: install/start Airflow via Ambari, then run ../airflow-sample-smoke.sh"
