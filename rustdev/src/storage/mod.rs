//! Storage backend abstraction and implementations

pub mod file_manager;
pub mod async_file_manager;
pub mod manifest;
pub mod traits;

pub use file_manager::{FileManager, FileMetadata};
pub use async_file_manager::{AsyncFileManager, AsyncFileMetadata};
pub use manifest::{ManifestWriter, ManifestReader, find_latest_manifest};
pub use traits::*;