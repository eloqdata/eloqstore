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

# Parse command line arguments
CHECK_IDEMPOTENCY=false
for arg in "$@"; do
  if [ "$arg" = "--check" ]; then
    CHECK_IDEMPOTENCY=true
    break
  fi
done

# arg 1: Library name (for display)  
# arg 2: Detection command (to check whether the core header file or library file exists)
need_install() {
  if [ "$CHECK_IDEMPOTENCY" = "false" ]; then
    return 0
  fi
  if eval "$2" >/dev/null 2>&1; then
    echo "--- [SKIP] $1 is already installed. ---"
    return 1
  fi
  return 0
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
    sudo curl ca-certificates gdb ccache rsync git \
    build-essential cmake pkg-config \
    libcurl4-openssl-dev libssl-dev libgflags-dev libzstd-dev \
    libboost-context-dev libc-ares-dev libprotobuf-dev libprotoc-dev protobuf-compiler \
    libjsoncpp-dev libleveldb-dev libsnappy-dev zlib1g-dev lcov

# Install glog
LIBGLOG_CHECK_CMD="ls /usr/local/lib/libglog.so 2>/dev/null"
if need_install "glog" "$LIBGLOG_CHECK_CMD"; then
    git clone https://github.com/eloqdata/glog.git glog
    cd glog
    cmake -S . -B build -G "Unix Makefiles"
    cmake --build build -j$(nproc)
    sudo cmake --build build --target install
    cd ../ && rm -rf glog
fi

# Install liburing
LIBURING_VERSION="2.6"
LIBURING_CHECK_CMD="ls /usr/lib/liburing.so.${LIBURING_VERSION} 2>/dev/null"
if need_install "liburing" "$LIBURING_CHECK_CMD"; then
    git clone https://github.com/axboe/liburing.git liburing
    cd liburing
    git checkout tags/liburing-${LIBURING_VERSION}
    ./configure --cc=gcc --cxx=g++
    make -j$(nproc) && sudo make install
    cd .. && rm -rf liburing
fi

# Install brpc
BRPC_CHECK_CMD="ls /usr/lib/libbrpc.so 2>/dev/null"
if need_install "brpc" "$BRPC_CHECK_CMD"; then
    git clone https://github.com/eloqdata/brpc.git brpc
    cd brpc
    mkdir build && cd build
    cmake .. \
        -DWITH_GLOG=ON \
        -DIO_URING_ENABLED=ON \
        -DBUILD_SHARED_LIBS=ON
    cmake --build . -j$(nproc)
    sudo cp -r ./output/include/* /usr/include/
    sudo cp ./output/lib/* /usr/lib/
    cd ../../ && rm -rf brpc
fi

# Install AWSSDK
AWS_SDK_VERSION="1.11.446"
AWS_SDK_VERSION_MAJOR=1
AWS_SDK_VERSION_MINOR=11
AWS_SDK_VERSION_PATCH=446
AWS_SDK_CORE_FILE="/usr/include/aws/core/VersionConfig.h"
AWS_SDK_CHECK_CMD="grep -q 'AWS_SDK_VERSION_MAJOR ${AWS_SDK_VERSION_MAJOR}' ${AWS_SDK_CORE_FILE} 2>/dev/null && \
                   grep -q 'AWS_SDK_VERSION_MINOR ${AWS_SDK_VERSION_MINOR}' ${AWS_SDK_CORE_FILE} 2>/dev/null && \
                   grep -q 'AWS_SDK_VERSION_PATCH ${AWS_SDK_VERSION_PATCH}' ${AWS_SDK_CORE_FILE} 2>/dev/null"
if need_install "AWS SDK (S3)" "$AWS_SDK_CHECK_CMD"; then
    git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp.git aws
    cd aws
    git checkout tags/${AWS_SDK_VERSION}
    mkdir bld && cd bld
    cmake .. \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DCMAKE_INSTALL_PREFIX=./output/ \
        -DENABLE_TESTING=OFF \
        -DBUILD_SHARED_LIBS=ON \
        -DFORCE_SHARED_CRT=OFF \
        -DBUILD_ONLY="s3"
    cmake --build . --config RelWithDebInfo -j$(nproc)
    cmake --install . --config RelWithDebInfo
    sudo cp -r ./output/include/* /usr/include/
    sudo cp -r ./output/lib/* /usr/lib/
    cd ../../ && rm -rf aws
fi

# Install Catch2
CATCH2_VERSION="3.3.2"
CATCH2_CHECK_CMD="pkg-config --exact-version=${CATCH2_VERSION} catch2-with-main"
if need_install "Catch2" "$CATCH2_CHECK_CMD"; then
    git clone -b v${CATCH2_VERSION} https://github.com/catchorg/Catch2.git
    cd Catch2 && mkdir bld && cd bld
    cmake .. \
        -DCMAKE_INSTALL_PREFIX=/usr/ \
        -DCATCH_BUILD_EXAMPLES=OFF \
        -DBUILD_TESTING=OFF
    cmake --build . -j4
    sudo cmake --install .
    cd ../../ && rm -rf Catch2
fi

echo "All dependencies have been installed successfully!" 
