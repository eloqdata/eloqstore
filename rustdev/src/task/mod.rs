//! Task system for coordinating storage operations

pub mod traits;
// TODO: Update these modules to use new I/O abstraction instead of UringManager
// pub mod read;
// pub mod write;
// pub mod scan;
// pub mod background;
pub mod scheduler;

pub use traits::{Task, TaskResult, TaskPriority, TaskType, TaskContext};
// pub use read::ReadTask;
// pub use write::{WriteTask, BatchWriteTask};
// pub use scan::ScanTask;
// pub use background::{CompactionTask, GarbageCollectionTask};
pub use scheduler::{TaskScheduler, TaskHandle};