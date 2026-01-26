# Static-linking dependencies into `eloqstore_combine.so`

This document explains how to bundle (most) third-party dependencies into a single shared library, `libeloqstore_combine.so`, to reduce reliance on system-installed shared libraries.

## Overview

With `STATIC_ALL_DEPS` enabled, CMake will:

1. Prefer static variants (`.a`) of dependencies when possible
2. Build a combined shared library `libeloqstore_combine.so`
3. Let Rust `build.rs` detect and link the combined library automatically

Note: when building a shared library, any static libraries linked into it **must be compiled with `-fPIC`**. Some system-provided static libraries (notably `libzstd.a` on some distros) may not be PIC, in which case we fall back to using the corresponding `.so` for that dependency.

## Usage

### 1) Ensure required libraries are installed

Ideally, your system should provide static libraries (`.a`) for:

- glog (`libglog.a`)
- jsoncpp (`libjsoncpp.a`)
- curl (`libcurl.a`)
- liburing (`liburing.a`)
- boost_context (`libboost_context.a`)
- AWS SDK (e.g. `libaws-cpp-sdk-s3.a`, `libaws-cpp-sdk-core.a`, …)

For `zstd`, you have two options:

- **Recommended**: provide the dynamic library (`libzstd.so*`) and let the combined library depend on it
- **Fully static**: rebuild `libzstd.a` with `-fPIC`

### 2) Build

Just build the Rust crate as usual:

```bash
cd rust-eloqstore/eloqstore-sys
cargo build
```

`build.rs` will:

- Configure CMake with `STATIC_ALL_DEPS=ON`
- Build `libeloqstore_combine.so`
- Link against the combined library rather than individually listing every dependency in Rust

### 3) Verify

After the build, inspect the produced shared library:

```bash
# Locate the library (path may vary by profile)
ls -lh target/**/build/**/out/build/libeloqstore_combine.so

# Inspect runtime dependencies (ideally only a small set of system libs + maybe libzstd.so)
ldd target/**/build/**/out/build/libeloqstore_combine.so
```

## Building dependencies from source (static)

If your system does not provide the needed static libraries, build them from source.

### glog

```bash
git clone https://github.com/eloqdata/glog.git
cd glog
cmake -S . -B build -DBUILD_SHARED_LIBS=OFF
cmake --build build
sudo cmake --build build --target install
```

### jsoncpp

```bash
git clone https://github.com/open-source-parsers/jsoncpp.git
cd jsoncpp
cmake -S . -B build -DBUILD_SHARED_LIBS=OFF -DCMAKE_BUILD_TYPE=Release
cmake --build build
sudo cmake --build build --target install
```

### curl

```bash
# Download and extract curl sources (adjust version)
wget https://curl.se/download/curl-8.x.x.tar.gz
tar -xzf curl-8.x.x.tar.gz
cd curl-8.x.x
./configure --disable-shared --enable-static --with-ssl
make
sudo make install
```

### zstd (PIC static library)

If you need `libzstd.a` to be linkable into a shared library, ensure it is built with PIC.
One simple approach is to build via CMake and enable PIC explicitly:

```bash
git clone https://github.com/facebook/zstd.git
cd zstd
cmake -S build/cmake -B build-pic -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=ON
cmake --build build-pic -j"$(nproc)"
sudo cmake --install build-pic
```

### liburing

```bash
git clone https://github.com/axboe/liburing.git
cd liburing
./configure --cc=gcc --cxx=g++
make
sudo make install
```

### boost_context

```bash
# Download Boost sources (adjust version)
wget https://boostorg.jfrog.io/artifactory/main/release/1.xx.x/source/boost_1_xx_x.tar.gz
tar -xzf boost_1_xx_x.tar.gz
cd boost_1_xx_x
./bootstrap.sh --with-libraries=context
./b2 link=static
sudo ./b2 install
```

### AWS SDK (S3 only)

```bash
git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp.git
cd aws-sdk-cpp
mkdir build && cd build
cmake .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SHARED_LIBS=OFF \
  -DENABLE_TESTING=OFF \
  -DBUILD_ONLY="s3"
cmake --build . -j"$(nproc)"
sudo cmake --install .
```

## Troubleshooting

### CMake cannot find a library

If CMake cannot locate headers/libs, try setting a prefix:

```bash
export CMAKE_PREFIX_PATH=/usr/local:$CMAKE_PREFIX_PATH
cargo build
```

### Relocation / `-fPIC` errors

If you see errors like:

```
relocation ... can not be used when making a shared object; recompile with -fPIC
```

then one of the static dependencies was not built with PIC. Rebuild that dependency with `-fPIC` (or use the `.so` variant).

### Disable static bundling

If needed, you can disable static bundling by changing `eloqstore-sys/build.rs` to pass:

```rust
.define("STATIC_ALL_DEPS", "OFF")
```

## Notes

- **Binary size**: statically bundling dependencies increases the size of the produced `.so`
- **Licensing**: verify license compatibility for all bundled dependencies
- **System libraries**: some libraries (e.g. `libc`, `libpthread`) remain dynamic on most Linux distributions
- **Testing**: always run integration tests after changing link mode

## Benefits

- **Safer**: reduces dependency on user-installed shared libraries and version drift
- **More portable**: easier deployment as a single primary `.so`
- **Simpler**: fewer moving parts for consumers
