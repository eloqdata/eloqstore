# Vendor Directory Guide

This directory is used for Rust FFI build, reusing source code from repository root via **soft links** to avoid duplication.

## Directory Structure

```
vendor/
├── CMakeLists.txt          # Rust FFI dedicated build configuration
├── external -> ../../../external  # Soft link: external dependencies (submodules + tool files)
├── src -> ../../../src            # Soft link: C++ core source code
├── include -> ../../../include    # Soft link: C++ header files
```

## Design Principles

1. **Minimize Duplication**: `vendor/` stores only Rust-specific build files
2. **Soft Link Reuse**: `src/`, `include/`, `external/` are all soft-linked to repository root
3. **Shared FFI Boundary**: Rust and Python both build against the repository root `ffi/` directory

## Maintenance Guide

### Modify Core Business Code
Modify directly in **repository root**, no need to sync to vendor:
- Modify `/src/*.cpp` → vendor automatically (soft link)
- Modify `/include/*.h` → vendor automatically (soft link)
- Modify `/external/*` → vendor automatically (soft link)

### Modify FFI-Specific Code
Modify in repository root:
- `ffi/src/eloqstore_capi.cpp`
- `ffi/include/eloqstore_capi.h`

### Update Submodule
Execute in repository root:
```bash
git submodule update --init --recursive
```
Or directly run `cargo build` (build.rs will automatically execute)

## Build Instructions

`build.rs` will:
1. Automatically execute `git submodule update --init --recursive`
2. Use CMake to build `vendor/` directory
3. Automatically use the latest source code from repository root via soft links and shared `ffi/`

No need to manually sync files!
