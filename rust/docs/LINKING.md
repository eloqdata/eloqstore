# EloqStore Linking and Dependency Management

## Overview

EloqStore is an embedded key-value database written in C++, which exposes a Rust interface via Rust FFI. This document explains how dependencies are linked and managed in the build process.

**Core challenge**: The C++ project depends on multiple third-party libraries. The key problem is how to correctly link these libraries within Rust's build system.

---

## Current Approach: Combined Shared Library

To address the problems of "requiring end users to install many shared libraries themselves", version drift, and security concerns, `eloqstore-sys` now prefers a **combined shared library** approach:

- **CMake** builds `libeloqstore_combine.so`
- It tries to **statically link** as many third-party dependencies as possible into this `.so` (to reduce runtime external dependencies)
- **Rust** only links `eloqstore_combine` (plus a small set of system libraries)

If building the combined library fails, it falls back to the legacy mode: `libeloqstore.a` + Rust explicitly links a list of dependency libraries.

---

## Dependency Analysis

### Project Structure

```
rust-eloqstore/
├── eloqstore-sys/              # Rust FFI layer
│   ├── src/lib.rs              # FFI bindings
│   ├── build.rs                # C++ build script
│   └── vendor/                 # C++ source code (31MB)
│       ├── include/            # Header files
│       ├── src/                # Implementations
│       └── external/           # Third-party libraries
│           ├── abseil-cpp/      # 15MB - Google base library
│           ├── concurrentqueue/ # 15MB - Lock-free concurrent queue
│           └── inih/            # 308KB - INI file parser
```

### Dependency List

| Library             | Size           | Purpose                                          | Required                       |
| ------------------- | -------------- | ------------------------------------------------ | ------------------------------ |
| **abseil-cpp**      | 15 MB          | Strings, hash tables, synchronization primitives | ✅ Required                     |
| **concurrentqueue** | 15 MB          | Lock-free concurrent queue                       | ✅ Required                     |
| **inih**            | 308 KB         | INI configuration parsing                        | ⚠️ Optional                    |
| **glog**            | System library | Logging                                          | ✅ Required (system dependency) |
| **boost**           | System library | Context, filesystem                              | ✅ Required (system dependency) |
| **AWS SDK**         | System library | Cloud storage (S3)                               | ⚠️ Optional (cloud feature)    |

---

## Linking Strategy

### Combined Shared Library (Recommended)

The current implementation moves as much of the "dependency discovery / link-order complexity" as possible into CMake, so Rust only links a single combined library:

- **CMake (`eloqstore-sys/vendor/CMakeLists.txt`)**
  - With `BUILD_FOR_RUST=ON` and `STATIC_ALL_DEPS=ON`, it builds `eloqstore_combine` (shared) → `libeloqstore_combine.so`
  - For `glog/jsoncpp`: if the system only provides library files (e.g. `libglog.a`) but no CMake package target (like `glog::glog`), an IMPORTED target is created as a fallback
  - For `zstd`: if the system static library `libzstd.a` is not built with `-fPIC`, it prefers the dynamic library `libzstd.so*` (see Troubleshooting section)

- **Rust (`eloqstore-sys/build.rs`)**
  - Runs CMake with `STATIC_ALL_DEPS=ON`
  - If the combined artifact exists, it emits link directives:

```rust
// build.rs (conceptual)
println!("cargo:rustc-link-lib=dylib=eloqstore_combine");
println!("cargo:rustc-link-lib=pthread");
println!("cargo:rustc-link-lib=dl");
println!("cargo:rustc-link-lib=stdc++");
// For compatibility with a dynamic zstd dependency, link it once as well.
println!("cargo:rustc-link-lib=zstd");
```

### Static Linking vs Dynamic Linking (Legacy / Fallback)

If `libeloqstore_combine.so` cannot be built, we fall back to the legacy mode: build `libeloqstore.a`, and Rust explicitly links Abseil / system dependencies (more sensitive to link order).

### Build Pipeline

```
cargo build
    ↓
build.rs executes
    ↓
1. CMake configure/generate (`BUILD_FOR_RUST=ON`, `STATIC_ALL_DEPS=ON`)
2. CMake builds `libeloqstore_combine.so` (primary) or `libeloqstore.a` (fallback)
3. build.rs emits rustc link directives
4. Rust linker produces final artifact
```

### Link Order Issues

**Problem**: The linker is sensitive to library order.

**Solution**: In the primary path (combined shared library), link-order issues are mostly handled **inside CMake**, and Rust no longer needs to manually list many `cargo:rustc-link-lib` entries. In the fallback path, explicit ordering may still be needed.

```rust
// build.rs (fallback mode)
fn main() {
    // Link in dependency order
    // First: Abseil (lowest-level dependencies)
    println!("cargo:rustc-link-lib=static=absl_base");
    println!("cargo:rustc-link-lib=static=absl_strings");
    println!("cargo:rustc-link-lib=static=absl_container");
    
    // Then: EloqStore
    println!("cargo:rustc-link-lib=static=eloqstore");
    
    // Finally: system libraries
    println!("cargo:rustc-link-lib=dylib=glog");
    println!("cargo:rustc-link-lib=dylib=pthread");
    println!("cargo:rustc-link-lib=dylib=dl");
}
```

---

## Static Linking Dependencies into `libeloqstore_combine.so`

### Overview

With `STATIC_ALL_DEPS` enabled, CMake will:

1. Prefer static variants (`.a`) of dependencies when possible
2. Build a combined shared library `libeloqstore_combine.so`
3. Let Rust `build.rs` detect and link the combined library automatically

**Important Note**: When building a shared library, any static libraries linked into it **must be compiled with `-fPIC`**. Some system-provided static libraries (notably `libzstd.a` on some distros) may not be PIC, in which case we fall back to using the corresponding `.so` for that dependency.

### Usage

#### 1) Ensure required libraries are installed

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

#### 2) Build

Just build the Rust crate as usual:

```bash
cd rust-eloqstore/eloqstore-sys
cargo build
```

`build.rs` will:

- Configure CMake with `STATIC_ALL_DEPS=ON`
- Build `libeloqstore_combine.so`
- Link against the combined library rather than individually listing every dependency in Rust

#### 3) Verify

After the build, inspect the produced shared library:

```bash
# Locate the library (path may vary by profile)
ls -lh target/**/build/**/out/build/libeloqstore_combine.so

# Inspect runtime dependencies (ideally only a small set of system libs + maybe libzstd.so)
ldd target/**/build/**/out/build/libeloqstore_combine.so
```

---

## Building Dependencies from Source (Static)

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

---

## External Dependency Handling

### System Dependencies (CMake Find / Imported Targets)

In the current mode, dependency discovery is centralized in CMake:

- Prefer `find_package(...)` to obtain standard targets (e.g. `glog::glog`, `jsoncpp_lib`)
- If the system only provides library files but no corresponding targets, create IMPORTED targets as a fallback (to avoid "target not found")

Rust no longer enumerates dependency libraries via `pkg-config`; Rust links the combined library plus a small set of system libraries.

### Internal External Libraries (vendor Directory)

For libraries under `vendor/external/`:

#### Option A: Build as Submodules (Current Approach)

```cmake
# CMakeLists.txt
add_subdirectory(external/abseil-cpp)
add_subdirectory(external/concurrentqueue)

# Link to EloqStore
target_link_libraries(eloqstore PRIVATE absl::base absl::strings)
```

#### Option B: Use System-Installed Abseil

```cmake
# If abseil-dev is installed on the system
find_package(absl REQUIRED)
target_link_libraries(eloqstore PRIVATE absl::base absl::strings)
```

---

## Common Issues and Troubleshooting

### Incorrect Link Order

**Symptom**:

```
undefined reference to `absl::flat_hash_map'
```

**Solution**: Ensure dependency libraries appear *after* their users in the link order.

```rust
// Incorrect order
println!("cargo:rustc-link-lib=static=absl");  // Too early
println!("cargo:rustc-link-lib=static=eloqstore");

// Correct order
println!("cargo:rustc-link-lib=static=eloqstore");  // User first
println!("cargo:rustc-link-lib=static=absl");      // Dependency later
```

---

### Duplicate Symbols

**Symptom**:

```
multiple definition of `absl::base_internal::InitGoogleLogging()'
```

**Solution**: Ensure Abseil is linked only once.

```cmake
# Disable unnecessary Abseil components
abseil_cmake_configure(
    DISABLE_MSVC_WARNING_PREFIXES ON
    CXX_STANDARD 17
)
```

---

### Static Library Search Path Issues

**Symptom**:

```
cannot find -l:libabsl_base.a
```

**Solution**: Add library search paths in `build.rs`.

```rust
fn add_search_paths() {
    // Add library path under vendor directory
    let vendor_lib_dir = PathBuf::from("vendor/build");
    if vendor_lib_dir.exists() {
        println!("cargo:rustc-link-search=native={}", vendor_lib_dir.display());
    }
}
```

---

### `-fPIC` / Relocation Error When Building `libeloqstore_combine.so`

**Symptom** (typical example):

```
/usr/bin/ld: .../libzstd.a(...): relocation ... can not be used when making a shared object; recompile with -fPIC
```

**Cause**: Linking a **static library not compiled with `-fPIC`** (commonly the system-provided `libzstd.a`) into a shared object `.so` will fail.

**Solutions**:

1. **Use the dynamic version of the library** (recommended, easiest)  
   Our CMake logic already prefers `zstd` `.so` in this case. Ensure `libzstd.so*` exists on the system (common Ubuntu path: `/usr/lib/x86_64-linux-gnu/`).

2. **Rebuild the static library from source with PIC enabled** (if you want "fully static")  
   Rebuild the zstd static library with `-fPIC`, then point CMake to your PIC-enabled `libzstd.a`.

3. **Accept that the combined library may keep a small number of dynamic dependencies**  
   In practice, `libeloqstore_combine.so` still bundles most dependencies, while `zstd` (or a few others) may remain dynamically linked. This is still safer and more controllable than requiring users to install many dependencies manually.

---

### CMake Cannot Find a Library

If CMake cannot locate headers/libs, try setting a prefix:

```bash
export CMAKE_PREFIX_PATH=/usr/local:$CMAKE_PREFIX_PATH
cargo build
```

---

### Disable Static Bundling

If needed, you can disable static bundling by changing `eloqstore-sys/build.rs` to pass:

```rust
.define("STATIC_ALL_DEPS", "OFF")
```

---

## Optimization Strategies

### Reducing Package Size

| Strategy                  | Effect                       | Risk                                  |
| ------------------------- | ---------------------------- | ------------------------------------- |
| Remove cloud storage code | Reduce ~50KB source          | Lose cloud storage feature            |
| Use system Abseil         | Reduce 15MB                  | Cross-platform compatibility issues   |
| Prebuilt binaries         | Significantly smaller source | Requires CI for multi-platform builds |

---

### Optional Cloud Storage Feature

Controlled via feature flags:

```toml
# eloqstore-sys/Cargo.toml
[features]
default = ["cloud"]
cloud = []
```

```rust
// build.rs
fn main() {
    if cfg!(feature = "cloud") {
        // Include cloud storage code
        println!("cargo:rustc-cfg=feature=\"cloud\"");
    } else {
        // Exclude cloud storage code
        println!("cargo:exclude=src/storage/object_store.cpp");
    }
}
```

---

## Distribution Strategy

### Strategy Comparison

| Strategy                    | Source Size | User Experience          | Use Case                       |
| --------------------------- | ----------- | ------------------------ | ------------------------------ |
| **Source-only (crates.io)** | 31MB        | Requires C++ compilation | ❌ Not recommended (size limit) |
| **Prebuilt binaries**       | <1MB        | Ready to use             | ✅ Recommended                  |
| **Private Git repository**  | 31MB        | Requires C++ compilation | Internal use                   |
| **Local path**              | -           | Developer debugging      | Development stage              |

---

### Recommended Approach: Prebuilt Binaries + Rust FFI

```
Release artifacts:
├── eloqstore-sys-0.1.0.crate      # Rust FFI source (<100KB)
├── libeloqstore-x86_64-unknown-linux-gnu.tar.gz  # Prebuilt libs (~2MB)
│   ├── libeloqstore.so
│   ├── libabsl_*.a
│   └── libconcurrentqueue.a
└── README.md
```

User installation:

```toml
# Cargo.toml
[dependencies]
eloqstore = "0.1"
eloqstore-sys = { version = "0.1", features = ["prebuilt"] }
```

---

## Notes and Benefits

### Notes

- **Binary size**: statically bundling dependencies increases the size of the produced `.so`
- **Licensing**: verify license compatibility for all bundled dependencies
- **System libraries**: some libraries (e.g. `libc`, `libpthread`) remain dynamic on most Linux distributions
- **Testing**: always run integration tests after changing link mode

### Benefits

- **Safer**: reduces dependency on user-installed shared libraries and version drift
- **More portable**: easier deployment as a single primary `.so`
- **Simpler**: fewer moving parts for consumers

---

## Summary

### Core Principles

1. **Layered handling**
   * System dependencies: detected via pkg-config or CMake
   * Vendor dependencies: built as submodules
   * Proprietary code: statically linked

2. **Explicit link order**
   * User library → Third-party libraries → System libraries

3. **On-demand compilation**
   * Use feature flags to control optional functionality
   * Cloud storage, metrics, etc. as optional components

---

### Current Status

| Item                        | Status                     |
| --------------------------- | -------------------------- |
| C++ source compilation      | ✅ Working                  |
| Combined shared library     | ✅ Working (primary path)   |
| Fallback static linking     | ✅ Working (secondary path) |
| PIC-related edge cases      | ⚠️ Some static libs (e.g. zstd) may fail without `-fPIC` |
