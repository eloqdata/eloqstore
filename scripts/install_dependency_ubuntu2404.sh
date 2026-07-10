#!/bin/bash

set -ex

kernel_ge() {
  local need_major="$1" need_minor="$2"
  local rel ver major minor

  rel="$(uname -r)"          # e.g. 6.5.0-41-generic
  ver="${rel%%-*}"           # -> 6.5.0
  IFS=. read -r major minor _ <<<"$ver"

  major="${major:-0}"
  minor="${minor:-0}"

  (( major > need_major || (major == need_major && minor >= need_minor) ))
}

if ! kernel_ge 6 6; then
  echo "Kernel $(uname -r) < 6.6, exit." >&2
  exit 1
fi

echo "Kernel $(uname -r) >= 6.6, continue."

# Ensure noninteractive apt; keep TZ default
export DEBIAN_FRONTEND=noninteractive
export TZ=${TZ:-UTC}

needs_tz_config=false
if [ ! -f /etc/timezone ] || ! grep -qE '^(Etc/UTC|UTC)$' /etc/timezone; then
  needs_tz_config=true
fi
if [ ! -L /etc/localtime ] || [ "$(readlink -f /etc/localtime)" != "/usr/share/zoneinfo/Etc/UTC" ]; then
  needs_tz_config=true
fi

if $needs_tz_config; then
  echo 'tzdata tzdata/Areas select Etc' | sudo debconf-set-selections || true
  echo 'tzdata tzdata/Zones/Etc select UTC' | sudo debconf-set-selections || true
  echo 'Etc/UTC' | sudo tee /etc/timezone >/dev/null
  sudo ln -sf /usr/share/zoneinfo/Etc/UTC /etc/localtime
fi

# Install system packages
DEBIAN_FRONTEND=noninteractive sudo apt-get update
DEBIAN_FRONTEND=noninteractive sudo apt-get install -y --no-install-recommends \
    sudo curl wget ca-certificates gdb ccache rsync git \
    build-essential cmake pkg-config \
    libcurl4-openssl-dev libssl-dev libgflags-dev libzstd-dev \
    libboost-context-dev libc-ares-dev libprotobuf-dev libprotoc-dev protobuf-compiler \
    libjsoncpp-dev libleveldb-dev libsnappy-dev zlib1g-dev libzmq3-dev cppzmq-dev lcov

sudo ldconfig

echo "All dependencies have been installed successfully!" 
