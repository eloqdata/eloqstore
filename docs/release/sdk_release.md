# SDK Release Guide

## Python SDK

The Python package lives under `python/` and is built with Hatchling plus a
custom build hook.

The hook does two things during `wheel` builds:

1. runs CMake against the repository root
2. copies `libeloqstore_capi.so` into the wheel under `eloqstore/.libs/`

### Build locally

```bash
cd python
python -m pip install build
python -m build --wheel
```

### Publish

```bash
python -m pip install twine
python -m twine upload dist/*
```

Expected consumer flow:

```bash
pip install eloqstore
```

No manual `ELOQSTORE_PY_LIB` override should be required when using the wheel.

### GitHub Actions

Recommended:

- use trusted publishing to PyPI
- trigger the workflow manually after reviewing the built wheel

The repository workflow is:

- `.github/workflows/sdk-checks.yml`
- `.github/workflows/sdk-release.yml`

## Rust SDK

The Rust SDK is split into:

- `eloqstore-sys`: native FFI crate
- `eloqstore`: safe higher-level wrapper

### Package checks

```bash
cd rust
cargo package -p eloqstore-sys --allow-dirty
cargo package -p eloqstore --allow-dirty
```

### Publish order

```bash
cd rust
cargo publish -p eloqstore-sys
cargo publish -p eloqstore
```

`eloqstore` depends on the published `eloqstore-sys` version, while local
workspace development continues to use the path dependency.

### GitHub Actions

Recommended:

- publish `eloqstore-sys` first
- wait for crates.io index propagation
- publish `eloqstore`

The release workflow already encodes that ordering.

## Current scope

- Python wheel bundling is currently implemented for Linux shared libraries
  (`libeloqstore_capi.so`)
- Rust packaging metadata and package contents are prepared for `cargo package`
- Cross-platform wheel production and CI publishing should be added next
