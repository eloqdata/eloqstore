//! Main EloqStore implementation

use crate::Result;
use crate::config::Config;

/// Main store structure
pub struct EloqStore {
    config: Config,
}

impl EloqStore {
    /// Create a new EloqStore instance
    pub fn new(config: Config) -> Result<Self> {
        Ok(Self { config })
    }

    /// Start the store
    pub async fn start(&self) -> Result<()> {
        // TODO: Implement store startup
        Ok(())
    }

    /// Stop the store
    pub async fn stop(&self) -> Result<()> {
        // TODO: Implement store shutdown
        Ok(())
    }
}