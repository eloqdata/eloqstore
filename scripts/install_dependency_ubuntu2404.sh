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
    sudo curl ca-certificates gdb ccache rsync git \
    build-essential cmake pkg-config \
    libcurl4-openssl-dev libssl-dev libgflags-dev libzstd-dev \
    libboost-context-dev libc-ares-dev libprotobuf-dev libprotoc-dev protobuf-compiler \
    libjsoncpp-dev libleveldb-dev libsnappy-dev zlib1g-dev lcov

# Install glog
git clone https://github.com/eloqdata/glog.git glog
cd glog
cmake -S . -B build -G "Unix Makefiles"
cmake --build build -j$(nproc)
sudo cmake --build build --target install
cd ../ && rm -rf glog

# Install liburing
git clone https://github.com/axboe/liburing.git liburing
cd liburing
git checkout tags/liburing-2.6
./configure --cc=gcc --cxx=g++
make -j$(nproc) && sudo make install
cd .. && rm -rf liburing

# Install brpc
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

# Install AWSSDK
git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp.git aws
cd aws
git checkout tags/1.11.446
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

# Install Catch2
git clone -b v3.3.2 https://github.com/catchorg/Catch2.git
cd Catch2 && mkdir bld && cd bld
cmake .. \
    -DCMAKE_INSTALL_PREFIX=/usr/ \
    -DCATCH_BUILD_EXAMPLES=OFF \
    -DBUILD_TESTING=OFF
cmake --build . -j4
sudo cmake --install .
cd ../../ && rm -rf Catch2


echo "All dependencies have been installed successfully!" 
