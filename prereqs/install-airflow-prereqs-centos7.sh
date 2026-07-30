#!/usr/bin/env bash
# Airflow pre-reqs for CentOS 7 / RHEL 7
# Builds SQLite + Python 3.8.12 from source (ODP *-2)
#
# Docs:
#   https://docs.acceldata.io/odp/odp-3.2.3.5-2/documentation/single-node-installation#centos-7-setup
#
# Idempotent where practical. Run as root on the Airflow host:
#   sudo ./prereqs/install-airflow-prereqs-centos7.sh
#
# NOTE: This compiles Python from source and can take a long time.
set -euo pipefail

PYTHON_VERSION="3.8.12"
PYTHON_MM="3.8"
SQLITE_MIN_VERSION="3.31.0"
BUILD_ROOT="/opt"

log() { echo "[INFO] $*"; }
warn() { echo "[WARN] $*"; }
die() { echo "[ERROR] $*" >&2; exit 1; }

version_gte() {
  # Returns 0 (true) if $1 >= $2
  [[ "$(printf '%s\n' "$2" "$1" | sort -V | head -n1)" == "$2" ]]
}

[[ "$(id -u)" -eq 0 ]] || die "Run as root (sudo)."

if [[ -f /etc/os-release ]]; then
  # shellcheck disable=SC1091
  . /etc/os-release
elif [[ -f /etc/redhat-release ]]; then
  ID="centos"
  VERSION_ID="$(grep -oE '[0-9]+' /etc/redhat-release | head -1)"
else
  die "Cannot detect OS."
fi

case "${ID:-}" in
  centos|rhel|ol) ;;
  *) warn "Expected CentOS/RHEL 7; continuing on ${ID:-unknown} ${VERSION_ID:-}." ;;
esac

OS_MAJOR="${VERSION_ID%%.*}"
[[ "${OS_MAJOR}" == "7" ]] || warn "Expected major version 7; continuing on ${VERSION_ID:-unknown}."

command -v yum >/dev/null 2>&1 || die "yum not found."

# CentOS 7 mirrors often need vault after EOL
if ls /etc/yum.repos.d/*.repo >/dev/null 2>&1; then
  log "Pointing CentOS repos at vault.centos.org (EOL-safe)..."
  sed -i 's/mirror.centos.org/vault.centos.org/g' /etc/yum.repos.d/*.repo || true
  sed -i 's/^#.*baseurl=http/baseurl=http/g' /etc/yum.repos.d/*.repo || true
  sed -i 's/^mirrorlist=http/#mirrorlist=http/g' /etc/yum.repos.d/*.repo || true
fi

log "Installing build dependencies..."
yum install -y gcc openssl-devel wget curl bzip2-devel libffi-devel zlib-devel tcl make

# ---------------------------------------------------------------------------
# SQLite from source
# ---------------------------------------------------------------------------
need_sqlite=1
if command -v sqlite3 >/dev/null 2>&1; then
  SQLITE_VERSION="$(sqlite3 --version | awk '{print $1}')"
  if version_gte "${SQLITE_VERSION}" "${SQLITE_MIN_VERSION}" \
    && { [[ -f /usr/include/sqlite3.h ]] || [[ -f /usr/local/include/sqlite3.h ]]; }; then
    log "SQLite ${SQLITE_VERSION} already present with headers; skipping rebuild."
    need_sqlite=0
  fi
fi

if [[ "${need_sqlite}" -eq 1 ]]; then
  log "Building SQLite from source under ${BUILD_ROOT}..."
  mkdir -p "${BUILD_ROOT}"
  cd "${BUILD_ROOT}"

  if [[ ! -f sqlite.tar.gz ]]; then
    wget "https://www.sqlite.org/src/tarball/sqlite.tar.gz?r=release" \
      --no-check-certificate -O sqlite.tar.gz
  fi

  if [[ ! -d sqlite ]]; then
    tar xzf sqlite.tar.gz
  fi

  cd "${BUILD_ROOT}/sqlite"
  ./configure --prefix=/usr
  make
  make install
  ldconfig
fi

log "SQLite version: $(sqlite3 --version)"
echo "PATH=${PATH}"

# ---------------------------------------------------------------------------
# Python 3.8.12 from source
# ---------------------------------------------------------------------------
need_python=1
if command -v "python${PYTHON_MM}" >/dev/null 2>&1; then
  if "python${PYTHON_MM}" -c "import sqlite3" >/dev/null 2>&1; then
    log "python${PYTHON_MM} already installed with working sqlite3; skipping rebuild."
    need_python=0
  else
    warn "python${PYTHON_MM} exists but sqlite3 import failed; rebuilding."
  fi
fi

if [[ "${need_python}" -eq 1 ]]; then
  log "Building Python ${PYTHON_VERSION} from source under ${BUILD_ROOT}..."
  yum install -y gcc openssl-devel bzip2-devel libffi-devel zlib-devel

  mkdir -p "${BUILD_ROOT}"
  cd "${BUILD_ROOT}"

  if [[ ! -f "Python-${PYTHON_VERSION}.tgz" ]]; then
    curl -O "https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz"
  fi

  if [[ ! -d "Python-${PYTHON_VERSION}" ]]; then
    tar -zxvf "Python-${PYTHON_VERSION}.tgz"
  fi

  cd "${BUILD_ROOT}/Python-${PYTHON_VERSION}"
  if [[ -f Makefile ]]; then
    make clean || true
  fi

  export LDFLAGS="-L/usr/lib -L/usr/lib64"
  export CPPFLAGS="-I/usr/include"
  export LD_RUN_PATH="/usr/lib:/usr/lib64"

  ./configure --enable-shared LDFLAGS="${LDFLAGS}" CPPFLAGS="${CPPFLAGS}"
  make
  make install

  if ls ./libpython${PYTHON_MM}.so* >/dev/null 2>&1; then
    cp --no-clobber ./libpython${PYTHON_MM}.so* /lib64/ || true
    chmod 755 /lib64/libpython${PYTHON_MM}.so* || true
  fi
fi

# Paths / permissions (docs require both /usr/local/bin and /usr/bin)
if [[ -x "/usr/local/bin/python${PYTHON_MM}" ]] \
  && [[ ! -e "/usr/bin/python${PYTHON_MM}" ]]; then
  ln -s "/usr/local/bin/python${PYTHON_MM}" "/usr/bin/python${PYTHON_MM}"
  log "Created symlink /usr/bin/python${PYTHON_MM}"
fi

if [[ -d "/usr/local/lib/python${PYTHON_MM}" ]]; then
  chmod -R 755 "/usr/local/lib/python${PYTHON_MM}"
fi

BASHRC="${HOME}/.bashrc"
if [[ -f "${BASHRC}" ]] || [[ -w "$(dirname "${BASHRC}")" ]]; then
  touch "${BASHRC}"
  if ! grep -q '/usr/local/lib/' "${BASHRC}" 2>/dev/null; then
    echo 'export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}:/usr/local/lib/"' >> "${BASHRC}"
    log "Added LD_LIBRARY_PATH to ${BASHRC}"
  fi
fi
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}:/usr/local/lib/"
ldconfig

log "Installing Development Tools group..."
yum -y groupinstall "Development Tools" || true

log "Verification"
echo "----"
ls -l /usr/local/bin/python3.8* 2>/dev/null || warn "Missing /usr/local/bin/python3.8*"
ls -l /usr/bin/python3.8* 2>/dev/null || warn "Missing /usr/bin/python3.8*"
python3.8 --version
sqlite3 --version
python3.8 -c "import sqlite3; print('sqlite3=', sqlite3.sqlite_version)"
echo "----"
log "Airflow CentOS 7 pre-reqs installed successfully."
log "Ref: https://docs.acceldata.io/odp/odp-3.2.3.5-2/documentation/single-node-installation#centos-7-setup"
