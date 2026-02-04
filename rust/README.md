# EloqStore Rust SDK (FFI)

Rust bindings for [EloqStore](https://github.com/eloqdata/eloqstore), a high-performance embedded key-value database written in C++.

This repository is a Cargo workspace with:

- `eloqstore-sys`: low-level Rust FFI bindings + CMake build of the C++ sources
- `eloqstore`: high-level Rust API (RocksDB-style methods + request/response trait API)

## Quick links

### Getting started

- **🚀 Run the example**: `cargo run --example basic_usage` (see [eloqstore/examples/basic_usage.rs](eloqstore/examples/basic_usage.rs))
- **Quick Start**: [docs/QUICK_START.md](docs/QUICK_START.md)
  - Local Ubuntu 24.04 setup (dependency install script)
  - CI Docker image workflow: [eloqdata/eloq-dev-ci-ubuntu2404](https://hub.docker.com/r/eloqdata/eloq-dev-ci-ubuntu2404)
  - Cargo git dependency example + runnable minimal app

### Documentation (index)

- **API overview & examples**: [docs/API.md](docs/API.md)
- **Linking model & troubleshooting**: [docs/LINKING.md](docs/LINKING.md)

## Quick example

**Want to see it in action?** Run:
```bash
cargo run --example basic_usage
```

Or check out the [full example code](eloqstore/examples/basic_usage.rs).

For a minimal snippet, the core flow looks like this:

```rust
use eloqstore::{EloqStore, Options, TableIdentifier};

fn main() -> Result<(), eloqstore::KvError> {
    let mut opts = Options::new()?;
    opts.set_num_threads(1)?;
    opts.add_store_path("tmp/eloqstore_demo")?;

    let mut store = EloqStore::new(&opts)?;
    store.start()?;

    let table = TableIdentifier::new("demo_table", 0)?;
    store.put(&table, b"hello", b"world", 1)?;
    let v = store.get(&table, b"hello")?.unwrap();
    println!("GET hello -> {}", String::from_utf8_lossy(&v));

    store.stop();
    Ok(())
}
```

## Build & test (repository checkout)

```bash
# build the high-level crate
cargo build -p eloqstore

# build the example executable (debug)
cargo build --example basic_usage

# build the example executable (release, optimized)
cargo build --release --example basic_usage

# build fully static executable (no external .so needed)
ELOQSTORE_STATIC_EXE=1 cargo build --release --example basic_usage

# run the integration test (recommended quick validation)
cargo test -p eloqstore --test integration_test
```

### Executable locations

After building, executables are located at:

- **Debug**: `target/debug/examples/basic_usage`
- **Release**: `target/release/examples/basic_usage`

### Static vs Dynamic Linking

#### Dynamic Linking (Default)

**How it works**:
- Requires `libeloqstore_combine.so` to be available at runtime
- The dynamic linker (ld.so) loads the `.so` file before your program's `main()` runs
- The `.so` file must exist as a **separate file** on the filesystem

**Characteristics**:
- Smaller executable size
- Better for multi-process scenarios (shared .so in memory)
- Use: `cargo build --example basic_usage`
- **Deployment**: Place `libeloqstore_combine.so` in a standard library path (e.g., `/usr/local/lib`) or set `LD_LIBRARY_PATH`

#### Static Linking (Recommended for Self-Contained Executables)

**How it works**:
- Link all dependencies **statically** into the executable
- No external `.so` files needed
- Link `libeloqstore.a` (static library) directly into the executable
- Link all Abseil libraries and system dependencies statically

**How to enable**:
```bash
export ELOQSTORE_STATIC_EXE=1
cargo build --release --example basic_usage
```

**Characteristics**:
- Most dependencies baked into the executable
- No `libeloqstore_combine.so` needed
- **Note**: Still requires some system dynamic libraries (libc, libpthread, libzstd, etc.) - these are standard system libraries available in our [CI Docker image](https://hub.docker.com/r/eloqdata/eloq-dev-ci-ubuntu2404) or any modern Linux distribution
- Larger executable size (~30-50MB)
- Longer link time
- **Best for**: Single-process deployments, quick and stable deployment

**Verify it's static**:
```bash
# Check that eloqstore_combine.so is NOT a dependency
ldd target/release/examples/basic_usage | grep eloqstore
# Should show nothing (or "not a dynamic executable")

# Test: remove .so and verify it still works
mv target/release/libeloqstore_combine.so target/release/libeloqstore_combine.so.bak
./target/release/examples/basic_usage  # Should still work!
mv target/release/libeloqstore_combine.so.bak target/release/libeloqstore_combine.so
```

**Trade-offs**:
- ✅ **Pros**: No `libeloqstore_combine.so` needed, easier single-process deployment, more stable
- ❌ **Cons**: Larger executable size (~30-50MB), longer link time, still needs system libs
- ⚠️ **Multi-process scenario**: If running multiple eloqstore processes, dynamic linking is more memory-efficient (shared .so in memory)

**Why .so can't be "embedded"**:
- Dynamic libraries (.so) must be loaded by the system's dynamic linker (ld.so)
- The dynamic linker runs **before** your `main()` function
- It needs the .so as a **separate file** on the filesystem
- You **cannot** embed a .so "inside" an executable in a way that the dynamic linker can use it directly

**Recommendation**:
- **Single process, quick deployment**: Use static linking (`ELOQSTORE_STATIC_EXE=1`)
- **Multiple processes on same device**: Use dynamic linking and place `libeloqstore_combine.so` in a shared library path (e.g., `/usr/local/lib`) to save memory

## License

EloqStore Rust SDK is licensed under the same license as EloqStore. See the upstream [EloqStore repository](https://github.com/eloqdata/eloqstore) for details.
