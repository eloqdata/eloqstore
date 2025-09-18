//! Comprehensive tests for I/O backends
//!
//! This module tests all I/O backend implementations to ensure
//! they provide consistent behavior across different implementations.

#[cfg(test)]
mod backend_tests {
    use std::sync::Arc;
    use tempfile::TempDir;

    use crate::io::backend::{IoBackend, IoBackendFactory, IoBackendType, FileHandle};
    use crate::io::backend::factory::IoBackendConfig;
    use crate::Result;

    /// Test basic file operations
    async fn test_basic_operations(backend: Arc<dyn IoBackend>) -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.dat");

        // Test file creation
        let file = backend.open_file(&file_path, true).await?;

        // Test writing
        let data = b"Hello, Backend!";
        let written = file.write_at(0, data).await?;
        assert_eq!(written, data.len());

        // Test reading
        let read_data = file.read_at(0, data.len()).await?;
        assert_eq!(read_data.as_ref(), data);

        // Test file size
        let size = file.file_size().await?;
        assert_eq!(size, data.len() as u64);

        // Test sync
        file.sync().await?;
        file.sync_data().await?;

        // Test truncate
        file.truncate(5).await?;
        let new_size = file.file_size().await?;
        assert_eq!(new_size, 5);

        Ok(())
    }

    /// Test directory operations
    async fn test_directory_operations(backend: Arc<dyn IoBackend>) -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().join("test_dir");

        // Create directory
        backend.create_dir(&dir_path).await?;

        // Check existence
        let exists = backend.file_exists(&dir_path).await?;
        assert!(exists);

        // Get metadata
        let metadata = backend.metadata(&dir_path).await?;
        assert!(metadata.is_dir);
        assert!(!metadata.is_file);

        // Create nested directories
        let nested_path = dir_path.join("nested/deep/path");
        backend.create_dir_all(&nested_path).await?;
        assert!(backend.file_exists(&nested_path).await?);

        Ok(())
    }

    /// Test concurrent operations
    async fn test_concurrent_operations(backend: Arc<dyn IoBackend>) -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut handles = Vec::new();

        // Spawn multiple concurrent file operations
        for i in 0..10 {
            let backend = backend.clone();
            let file_path = temp_dir.path().join(format!("concurrent_{}.dat", i));

            let handle = tokio::spawn(async move {
                let file = backend.open_file(&file_path, true).await.unwrap();

                // Write unique data
                let data = format!("File {}", i);
                file.write_at(0, data.as_bytes()).await.unwrap();

                // Read it back
                let read_data = file.read_at(0, data.len()).await.unwrap();
                assert_eq!(read_data.as_ref(), data.as_bytes());

                // Sync
                file.sync().await.unwrap();
            });

            handles.push(handle);
        }

        // Wait for all operations to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all files exist
        for i in 0..10 {
            let file_path = temp_dir.path().join(format!("concurrent_{}.dat", i));
            assert!(backend.file_exists(&file_path).await?);
        }

        Ok(())
    }

    /// Test large file operations
    async fn test_large_file_operations(backend: Arc<dyn IoBackend>) -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("large.dat");

        let file = backend.open_file(&file_path, true).await?;

        // Write 1MB of data in chunks
        let chunk_size = 4096;
        let chunk_data = vec![0xAB; chunk_size];
        let total_size = 1024 * 1024; // 1MB

        for offset in (0..total_size).step_by(chunk_size) {
            file.write_at(offset as u64, &chunk_data).await?;
        }

        // Verify file size
        let size = file.file_size().await?;
        assert_eq!(size, total_size as u64);

        // Read random chunks and verify
        for offset in [0, chunk_size, total_size - chunk_size] {
            let data = file.read_at(offset as u64, chunk_size).await?;
            assert_eq!(data.len(), chunk_size);
            assert_eq!(data[0], 0xAB);
        }

        Ok(())
    }

    /// Test file renaming
    async fn test_rename_operations(backend: Arc<dyn IoBackend>) -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let old_path = temp_dir.path().join("old.dat");
        let new_path = temp_dir.path().join("new.dat");

        // Create file
        let file = backend.open_file(&old_path, true).await?;
        file.write_at(0, b"test").await?;
        drop(file);

        // Rename
        backend.rename(&old_path, &new_path).await?;

        // Verify
        assert!(!backend.file_exists(&old_path).await?);
        assert!(backend.file_exists(&new_path).await?);

        // Open renamed file and verify content
        let file = backend.open_file(&new_path, false).await?;
        let data = file.read_at(0, 4).await?;
        assert_eq!(data.as_ref(), b"test");

        Ok(())
    }

    /// Test reading directory contents
    async fn test_read_dir(backend: Arc<dyn IoBackend>) -> Result<()> {
        let temp_dir = TempDir::new().unwrap();

        // Create some files
        for i in 0..5 {
            let file_path = temp_dir.path().join(format!("file_{}.dat", i));
            backend.open_file(&file_path, true).await?;
        }

        // Create some directories
        for i in 0..3 {
            let dir_path = temp_dir.path().join(format!("dir_{}", i));
            backend.create_dir(&dir_path).await?;
        }

        // Read directory
        let entries = backend.read_dir(temp_dir.path()).await?;

        // Should have 8 entries
        assert_eq!(entries.len(), 8);

        // Check that all expected entries are present
        for i in 0..5 {
            let name = format!("file_{}.dat", i);
            assert!(entries.contains(&name));
        }
        for i in 0..3 {
            let name = format!("dir_{}", i);
            assert!(entries.contains(&name));
        }

        Ok(())
    }

    /// Test error handling
    async fn test_error_handling(backend: Arc<dyn IoBackend>) -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("nonexistent.dat");

        // Opening non-existent file without create should fail
        assert!(backend.open_file(&file_path, false).await.is_err());

        // Deleting non-existent file should fail
        assert!(backend.delete_file(&file_path).await.is_err());

        // Creating directory over existing file should fail
        let file_path = temp_dir.path().join("file.dat");
        backend.open_file(&file_path, true).await?;
        assert!(backend.create_dir(&file_path).await.is_err());

        Ok(())
    }

    /// Run all tests for a given backend
    async fn test_backend(backend_type: IoBackendType) -> Result<()> {

        let backend = IoBackendFactory::create_default(backend_type)?;

        // Run all test cases
        test_basic_operations(backend.clone()).await?;
        test_directory_operations(backend.clone()).await?;
        test_concurrent_operations(backend.clone()).await?;
        test_large_file_operations(backend.clone()).await?;
        test_rename_operations(backend.clone()).await?;
        test_read_dir(backend.clone()).await?;
        test_error_handling(backend.clone()).await?;

        // Check statistics
        let stats = backend.stats();
        assert!(stats.reads > 0);
        assert!(stats.writes > 0);
        assert!(stats.bytes_read > 0);
        assert!(stats.bytes_written > 0);

        // Shutdown
        backend.shutdown().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_backend() {
        test_backend(IoBackendType::Sync).await.unwrap();
    }

    #[tokio::test]
    async fn test_tokio_backend() {
        test_backend(IoBackendType::Tokio).await.unwrap();
    }

    #[tokio::test]
    async fn test_thread_pool_backend() {
        test_backend(IoBackendType::ThreadPool).await.unwrap();
    }

    // Uncomment when io_uring backend is fixed
    // #[cfg(target_os = "linux")]
    // #[tokio::test]
    // async fn test_io_uring_backend() {
    //     test_backend(IoBackendType::IoUring).await.unwrap();
    // }

    /// Test backend factory
    #[test]
    fn test_backend_factory() {
        // Test available backends
        let backends = IoBackendFactory::available_backends();
        assert!(backends.contains(&IoBackendType::Sync));
        assert!(backends.contains(&IoBackendType::Tokio));
        assert!(backends.contains(&IoBackendType::ThreadPool));

        // Test backend type parsing
        assert_eq!(IoBackendType::from_str("sync"), Some(IoBackendType::Sync));
        assert_eq!(IoBackendType::from_str("tokio"), Some(IoBackendType::Tokio));
        assert_eq!(IoBackendType::from_str("thread_pool"), Some(IoBackendType::ThreadPool));
        assert_eq!(IoBackendType::from_str("invalid"), None);

        // Test recommended backend
        let recommended = IoBackendFactory::recommended_backend();
        assert!(backends.contains(&recommended));
    }

    /// Test configuration
    #[tokio::test]
    async fn test_backend_configuration() {
        let config = IoBackendConfig {
            backend_type: IoBackendType::ThreadPool,
            thread_pool_size: Some(8),
            queue_depth: Some(512),
            direct_io: false,
            sync_io: false,
            alignment: 4096,
        };

        let backend = IoBackendFactory::create(config).unwrap();
        assert_eq!(backend.backend_type(), IoBackendType::ThreadPool);
        assert!(backend.is_async());
    }
}