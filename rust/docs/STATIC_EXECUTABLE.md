# Building Fully Static Executables (No External .so Required)

## Why Not Embed .so Directly?

The question "why can't we embed .so into the executable?" comes up often. Here's the technical explanation:

### The Problem with Dynamic Libraries (.so)

When you use `cargo:rustc-link-lib=dylib=eloqstore_combine`, the Rust linker:
1. **At link time**: Records that the executable depends on `libeloqstore_combine.so`
2. **At runtime**: The dynamic linker (ld.so) must find and load the .so file **before** your program's `main()` runs

This means:
- The .so file must exist as a **separate file** on the filesystem
- The dynamic linker searches for it in standard paths (`/usr/lib`, `LD_LIBRARY_PATH`, rpath, etc.)
- You **cannot** embed a .so "inside" an executable in a way that the dynamic linker can use it directly

### Solutions

#### Option 1: Fully Static Linking (Recommended for Self-Contained Executables)

Link all dependencies **statically** into the executable. No external .so files needed.

**How to enable:**

```bash
# Set environment variable to enable static linking
export ELOQSTORE_STATIC_EXE=1
cargo build --release --example basic_usage
```

This will:
- Link `libeloqstore.a` (static library) directly into the executable
- Link all Abseil libraries statically
- Link all system dependencies (glog, curl, jsoncpp, AWS SDK, etc.) statically
- Result: No `libeloqstore_combine.so` dependency (but still needs system libs like libc, libpthread, libzstd, etc.)

**Important Note**: This is **not fully static** - it still requires some system dynamic libraries (libc, libpthread, libzstd, etc.). These are standard system libraries available in our [CI Docker image](https://hub.docker.com/r/eloqdata/eloq-dev-ci-ubuntu2404) or any modern Linux distribution.

**Verify it's static:**
```bash
# Check that eloqstore_combine.so is NOT a dependency
ldd target/release/examples/basic_usage | grep eloqstore
# Should show nothing (or "not a dynamic executable")
```

**Trade-offs:**
- ✅ **Pros**: No `libeloqstore_combine.so` needed, easier single-process deployment, more stable
- ❌ **Cons**: Larger executable size (~30-50MB), longer link time, still needs system libs
- ⚠️ **Multi-process scenario**: If running multiple eloqstore processes, dynamic linking is more memory-efficient (shared .so in memory)

#### Option 2: Embed .so and Extract at Runtime (Current Implementation)

We've implemented a hybrid approach:
- Embed the .so content into the executable using `include_bytes!`
- Extract it to a temp directory at runtime
- Use `dlopen()` to load it before FFI calls

**How it works:**
1. `build.rs` copies `libeloqstore_combine.so` to `OUT_DIR`
2. `embedded_lib.rs` uses `include_bytes!` to embed it at compile time
3. At runtime, `ensure_library_loaded()` extracts it to `/tmp/eloqstore_libs/`
4. Uses `dlopen()` to load it explicitly

**Current status:**
- ✅ Works with `cargo run` (because `ensure_library_loaded()` is called before FFI)
- ⚠️ Direct execution still fails (linker tries to load before our code runs)

#### Option 3: Copy .so to Executable Directory + rpath

Simpler approach: Copy .so to `target/debug/` and set rpath.

**Current implementation:**
- `build.rs` copies .so to `target/debug/libeloqstore_combine.so`
- Sets rpath: `$ORIGIN/..` (for examples) and `$ORIGIN/../..` (for binaries)
- Still requires the .so file to exist alongside the executable

## Recommendation

For **truly self-contained executables**, use **Option 1 (Static Linking)**:

```bash
export ELOQSTORE_STATIC_EXE=1
cargo build --release --example basic_usage
./target/release/examples/basic_usage  # No .so file needed!
```

This produces a single executable with all dependencies baked in.

**Verification:**
```bash
# Check that eloqstore_combine.so is NOT in the dependency list
ldd target/release/examples/basic_usage | grep eloqstore
# Should return nothing

# Test: remove .so and verify it still works
mv target/release/libeloqstore_combine.so target/release/libeloqstore_combine.so.bak
./target/release/examples/basic_usage  # Should still work!
mv target/release/libeloqstore_combine.so.bak target/release/libeloqstore_combine.so
```

## Summary

**Why .so can't be "embedded" in the traditional sense:**
- Dynamic libraries (.so) must be loaded by the system's dynamic linker (ld.so)
- The dynamic linker runs **before** your `main()` function
- It needs the .so as a **separate file** on the filesystem

**Solution: Static Linking**
- Instead of using a .so, link all code directly into the executable
- Result: One file, no external dependencies (except system libs)
- Trade-off: Larger file size, but much better portability
