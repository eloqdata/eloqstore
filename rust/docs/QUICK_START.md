# Quick Start (Rust SDK)

This guide shows how to quickly build and run an application using the EloqStore Rust SDK.

You have two options for the build environment:

1. **Local setup** on Ubuntu 24.04 (install dependencies yourself)
2. **Pre-configured CI Docker image** (dependencies already installed)

After the environment is ready, you will:

- Add the SDK to your project via a **Cargo git dependency**
- Run a minimal example application

## Option A: Local environment (Ubuntu 24.04)

The repository provides an opinionated installer script:

- `scripts/install_dependency_ubuntu2404.sh`

It installs OS packages (cmake/ninja, compilers, headers, etc.) and builds/installs several third-party dependencies (e.g. glog, liburing, AWS SDK, …) into the system.

From the repository root:

```bash
bash scripts/install_dependency_ubuntu2404.sh
```

Notes:

- The script uses `sudo` and installs into system locations (e.g. `/usr/include`, `/usr/lib`).
- It may take a while (downloads + builds).

## Option B: Use the CI Docker image (recommended for a fast start)

We provide a pre-configured Ubuntu 24.04 build image with dependencies already installed:

- Docker Hub: [`eloqdata/eloq-dev-ci-ubuntu2404`](https://hub.docker.com/r/eloqdata/eloq-dev-ci-ubuntu2404)

Example usage:

```bash
docker pull eloqdata/eloq-dev-ci-ubuntu2404:latest

# From your workspace/repo root
docker run -it --security-opt seccomp=unconfined eloqdata/eloq-dev-ci-ubuntu2404:latest /bin/bash
```

Inside the container, you can build your app with `cargo` as usual.

## Add EloqStore to your Cargo project (git dependency)

Create a new Rust application:

```bash
cargo new my-eloqstore-app
cd my-eloqstore-app
```

Edit `Cargo.toml` and add the dependency (replace `<REPO_URL>` with your git URL; pin with `rev` for reproducibility):

```toml
[dependencies]
eloqstore = { git = "<REPO_URL>", rev = "<COMMIT_SHA>" }
```

Examples:

```toml
[dependencies]
# Using HTTPS
eloqstore = { git = "https://github.com/<org>/<repo>.git", rev = "<COMMIT_SHA>" }

# Using SSH
# eloqstore = { git = "ssh://git@github.com/<org>/<repo>.git", rev = "<COMMIT_SHA>" }
```

## Run a minimal example

Replace `src/main.rs` with:

```rust
use eloqstore::{EloqStore, Options, TableIdentifier};
use std::time::{SystemTime, UNIX_EPOCH};

fn timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn main() -> Result<(), eloqstore::KvError> {
    // 1) Configure
    let mut opts = Options::new()?;
    opts.set_num_threads(1)?;
    opts.add_store_path("/tmp/eloqstore_demo")?;

    // 2) Start store
    let mut store = EloqStore::new(&opts)?;
    store.start()?;

    // 3) Use a table
    let table = TableIdentifier::new("demo_table", 0)?;
    let ts = timestamp_ms();

    // 4) Put/Get
    store.put(&table, b"hello", b"world", ts)?;
    let v = store.get(&table, b"hello")?.expect("value not found");
    println!("GET hello -> {}", String::from_utf8_lossy(&v));

    // 5) Stop
    store.stop();
    Ok(())
}
```

Build and run:

```bash
cargo run
```

## Building Static Executables (Optional)

For self-contained executables that don't require `libeloqstore_combine.so` at runtime, use static linking:

```bash
# Build a static executable
export ELOQSTORE_STATIC_EXE=1
cargo build --release --example basic_usage

# Verify it doesn't depend on .so
ldd target/release/examples/basic_usage | grep eloqstore
# Should show nothing

# Run it - no .so file needed!
./target/release/examples/basic_usage
```

**Note**: Static executables still require some system dynamic libraries (libc, libpthread, libzstd, etc.), but these are standard libraries available in our [CI Docker image](https://hub.docker.com/r/eloqdata/eloq-dev-ci-ubuntu2404) or any modern Linux distribution.

**When to use static linking**:
- Single-process deployments
- Quick and stable deployment
- When you want a self-contained executable

**When to use dynamic linking**:
- Multiple processes on the same device (saves memory)
- Smaller executable size is important

## Run the SDK smoke test (optional)

```bash
cd rust-eloqstore
cargo test -p eloqstore --test integration_test
```
