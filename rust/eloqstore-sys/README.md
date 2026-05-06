# eloqstore-sys

Low-level Rust FFI bindings for EloqStore.

This crate builds the native EloqStore sources with CMake and exposes the
flattened C API to Rust. It is intended to be used by the higher-level
`eloqstore` crate.

## Build behavior

- The crate builds native sources via `build.rs`
- The native shared library is linked automatically
- The packaged crate contains the vendored source tree needed for `cargo package`

## Publishing order

1. `cargo publish -p eloqstore-sys`
2. `cargo publish -p eloqstore`
