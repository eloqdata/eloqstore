# EloqStore Rust Implementation

## Quick Reference
**Goal**: Port EloqStore C++ to Rust, maintaining exact functionality
**Rule**: Follow C++ implementation closely - no new features (except I/O abstraction)
**C++ Code**: Located in `../` (read-only reference)

Notice that in C++ version, a shard is single threaded and no need to handle multiple thread synchronization, therefore, we can avoid lock protection when processing requests. We will implement the same mechanism in Rust. 


### 🚧 Known Limitations:
- io_uring disabled (tokio-uring thread safety)
- Archive cron partial (in background_write)
- No WAL or dirty page tracking (writes sync immediately)

## 🏗️ Architecture Notes

### I/O Abstraction (Our Only Innovation)
Created to solve tokio-uring thread safety:
- Trait: `IoBackend`
- Implementations: sync, tokio, thread-pool, io_uring
- Location: `src/io/backend/`

## ⚡ Quick Commands
```bash
# Build
cargo build

# Test
cargo test

# Check compilation
cargo check

# Run with tokio backend
cargo run -- --io-backend tokio
``` 