# SDK Release Guide

The release workflow is `.github/workflows/sdk-release.yml` (named "SDK Release").

## How it works

1. **Single source of truth**: Only `VERSION` file at repo root determines the release version.
2. **Trigger**: Push VERSION change to `main`, or manual `workflow_dispatch`.
3. **CI auto-syncs**: Workflow reads `VERSION`, writes version to `pyproject.toml` and `Cargo.toml`, commits back to main with `[skip ci]`.
4. **Build + test**: Python wheel (retagged to `manylinux`), pytest, Rust integration tests.
5. **Publish**: Python to PyPI (OIDC trusted publishing), Rust to crates.io (`--no-verify --allow-dirty`).
6. **Git tag + GitHub Release**: Changelog generated from git history, tag pushed, release created.

## Release flow

```
Developer: edits VERSION → commits → PR → merge to main
                                           ↓
CI: reads VERSION → syncs versions → pushes commit [skip ci]
          → builds → tests → publishes → tags → GitHub Release
```

## Idempotent releases

- PyPI: `skip-existing: true` (already-published versions are skipped)
- crates.io: `grep -q "already exists"` catches duplicate publish attempts

## Python SDK

### Build locally

```bash
cd python
python -m pip install build
python -m build --wheel
```

### Publish (manual)

```bash
python -m twine upload dist/*
```

### Consumer

```bash
pip install eloqstore
```

## Rust SDK

The Rust SDK is a workspace with:

- `eloqstore-sys`: native FFI crate (vendors C++ sources)
- `eloqstore`: safe higher-level wrapper

### Package checks

```bash
cd rust
cargo package -p eloqstore-sys --allow-dirty --no-verify
```

### Publish

Automated by CI. Manual order:

```bash
cd rust
cargo publish -p eloqstore-sys --no-verify --allow-dirty
cargo publish -p eloqstore --no-verify --allow-dirty
```

## Current scope

- Python wheel: Linux `manylinux_2_31_x86_64` platform
- Rust: crates.io ready with self-contained FFI sources
- Cross-platform builds: future work
