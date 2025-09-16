//! Page management - data pages, index pages, overflow pages

pub mod page;
pub mod data_page;
pub mod data_page_builder;
pub mod overflow_page;
pub mod page_mapper;
pub mod page_pool;
pub mod page_cache;
pub mod page_builder;

pub use page::{Page, PageHeader};
pub use data_page::{DataPage, DataPageIterator};
pub use data_page_builder::DataPageBuilder;
pub use overflow_page::{OverflowPage, OverflowChainBuilder, OverflowChainReader};
pub use page_mapper::{PageMapper, MappingSnapshot, PageMapping};
pub use page_pool::{PagePool, GlobalPagePool, LocalPageAllocator, PoolStats};
pub use page_cache::{PageCache, PageCacheConfig, CacheStats};
pub use page_builder::PageBuilder;