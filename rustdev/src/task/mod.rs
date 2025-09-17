//! Task system for coordinating storage operations

pub mod traits;
pub mod scheduler;
pub mod read;
pub mod write;
// Removed write_simple - following C++ implementation exactly
pub mod scan;
pub mod background_write;
pub mod file_gc;

pub use traits::{Task, TaskResult, TaskPriority, TaskType, TaskContext};
pub use scheduler::{TaskScheduler, TaskHandle};

// Export task implementations
pub use read::ReadTask;
pub use write::{WriteTask, BatchWriteTask, DeleteTask};
pub use scan::{ScanTask, ScanIterator};
pub use background_write::{BackgroundWriteTask, BackgroundStats};
pub use file_gc::{FileGarbageCollector, FileGcTask, get_retained_files};