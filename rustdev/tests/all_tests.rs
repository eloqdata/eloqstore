//! Main test suite for EloqStore
//!
//! Run with:
//!   cargo test                     # Run all tests
//!   cargo test --test all_tests    # Run this specific test suite
//!   cargo test unit::              # Run unit tests only
//!   cargo test stress::            # Run stress tests only
//!   cargo test -- --nocapture      # Show println output

// Common test utilities
#[path = "common/mod.rs"]
mod common;

// Unit tests
#[path = "unit/mod.rs"]
mod unit;

// Integration tests
#[path = "integration/mod.rs"]
mod integration;

// Stress tests
#[path = "stress/mod.rs"]
mod stress;

// Re-export for convenience
pub use common::*;

#[cfg(test)]
mod test_suite {
    use super::*;

    #[test]
    fn test_suite_loads() {
        println!("EloqStore test suite loaded successfully");
    }
}