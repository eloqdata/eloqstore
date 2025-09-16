//! In-memory index management

pub mod index_page;
pub mod index_page_manager;
pub mod root_meta;

pub use index_page::{MemIndexPage, IndexPageIter};
pub use index_page_manager::IndexPageManager;
pub use root_meta::{RootMeta, CowRootMeta};

/// Configuration for IndexPageManager
#[derive(Debug, Clone)]
pub struct IndexPageManagerConfig {
    /// Maximum number of index pages in memory
    pub max_pages: usize,
}

impl Default for IndexPageManagerConfig {
    fn default() -> Self {
        Self {
            max_pages: 1000,
        }
    }
}