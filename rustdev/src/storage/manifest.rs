//! Manifest for persisting metadata and page mappings

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write, BufReader, BufWriter};
use std::path::{Path, PathBuf};

use serde::{Serialize, Deserialize};
use bincode;

use crate::types::{TableIdent, FileId, PageId, FilePageId};
use crate::page::{MappingSnapshot, PageMapping};
use crate::Result;
use crate::error::Error;

/// Manifest version
const MANIFEST_VERSION: u32 = 1;

/// Magic bytes for manifest files
const MAGIC_BYTES: &[u8; 8] = b"ELOQMFST";

/// Manifest header
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManifestHeader {
    /// Magic bytes
    magic: [u8; 8],
    /// Version
    version: u32,
    /// Checksum of the manifest
    checksum: u64,
    /// Creation timestamp
    created_at: u64,
    /// Table identifier
    table_id: String,
}

/// Manifest entry types
#[derive(Debug, Clone, Serialize, Deserialize)]
enum ManifestEntry {
    /// Page mapping entry
    PageMapping {
        page_id: PageId,
        file_page_id: FilePageId,
        file_id: FileId,
    },
    /// File creation
    FileCreated {
        file_id: FileId,
        path: String,
        created_at: u64,
    },
    /// File deletion
    FileDeleted {
        file_id: FileId,
        deleted_at: u64,
    },
    /// Snapshot marker
    Snapshot {
        version: u64,
        page_count: usize,
        timestamp: u64,
    },
    /// Compaction record
    Compaction {
        input_files: Vec<FileId>,
        output_files: Vec<FileId>,
        timestamp: u64,
    },
}

/// Manifest writer
pub struct ManifestWriter {
    /// Path to manifest file
    path: PathBuf,
    /// File writer
    writer: BufWriter<File>,
    /// Current version
    version: u64,
    /// Entry count
    entry_count: u64,
}

impl ManifestWriter {
    /// Create a new manifest writer
    pub fn new(dir: impl AsRef<Path>, table_id: &TableIdent) -> Result<Self> {
        let filename = format!("{}_manifest_{}.mfst", table_id.to_string(),
                              chrono::Utc::now().timestamp_millis());
        let path = dir.as_ref().join(filename);

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| Error::Io(e))?;

        let mut writer = BufWriter::new(file);

        // Write header
        let header = ManifestHeader {
            magic: *MAGIC_BYTES,
            version: MANIFEST_VERSION,
            checksum: 0, // Will be updated on close
            created_at: chrono::Utc::now().timestamp_millis() as u64,
            table_id: table_id.to_string(),
        };

        let header_bytes = bincode::serialize(&header)
            .map_err(|e| Error::Serialization(e.to_string()))?;

        // Write header size first
        let header_size = header_bytes.len() as u64;
        writer.write_all(&header_size.to_le_bytes())
            .map_err(|e| Error::Io(e))?;

        // Write header
        writer.write_all(&header_bytes)
            .map_err(|e| Error::Io(e))?;

        Ok(Self {
            path,
            writer,
            version: 0,
            entry_count: 0,
        })
    }

    /// Write a page mapping
    pub fn write_mapping(&mut self, mapping: &PageMapping) -> Result<()> {
        let entry = ManifestEntry::PageMapping {
            page_id: mapping.page_id,
            file_page_id: mapping.file_page_id,
            file_id: mapping.file_id,
        };

        self.write_entry(&entry)
    }

    /// Write a snapshot
    pub fn write_snapshot(&mut self, snapshot: &MappingSnapshot) -> Result<()> {
        // Write snapshot marker
        let marker = ManifestEntry::Snapshot {
            version: snapshot.version(),
            page_count: snapshot.len(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
        };

        self.write_entry(&marker)?;

        // Write all mappings
        for mapping in snapshot.all_mappings().values() {
            self.write_mapping(mapping)?;
        }

        self.version = snapshot.version();

        Ok(())
    }

    /// Write a file creation entry
    pub fn write_file_created(&mut self, file_id: FileId, path: &str) -> Result<()> {
        let entry = ManifestEntry::FileCreated {
            file_id,
            path: path.to_string(),
            created_at: chrono::Utc::now().timestamp_millis() as u64,
        };

        self.write_entry(&entry)
    }

    /// Write a file deletion entry
    pub fn write_file_deleted(&mut self, file_id: FileId) -> Result<()> {
        let entry = ManifestEntry::FileDeleted {
            file_id,
            deleted_at: chrono::Utc::now().timestamp_millis() as u64,
        };

        self.write_entry(&entry)
    }

    /// Write an entry
    fn write_entry(&mut self, entry: &ManifestEntry) -> Result<()> {
        let entry_bytes = bincode::serialize(entry)
            .map_err(|e| Error::Serialization(e.to_string()))?;

        // Write entry length
        let len = entry_bytes.len() as u32;
        let len_bytes = len.to_le_bytes();

        self.writer.write_all(&len_bytes)
            .map_err(|e| Error::Io(e))?;

        // Write entry
        self.writer.write_all(&entry_bytes)
            .map_err(|e| Error::Io(e))?;

        self.entry_count += 1;

        Ok(())
    }

    /// Sync to disk
    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()
            .map_err(|e| Error::Io(e))?;

        self.writer.get_mut().sync_all()
            .map_err(|e| Error::Io(e))?;

        Ok(())
    }

    /// Close the manifest
    pub fn close(mut self) -> Result<()> {
        self.sync()?;
        Ok(())
    }
}

/// Manifest reader
pub struct ManifestReader {
    /// Reader
    reader: BufReader<File>,
    /// Header
    header: ManifestHeader,
}

impl ManifestReader {
    /// Open a manifest file
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)
            .map_err(|e| Error::Io(e))?;

        let mut reader = BufReader::new(file);

        // Read header size first
        let mut size_bytes = [0u8; 8];
        reader.read_exact(&mut size_bytes)
            .map_err(|e| Error::Io(e))?;
        let header_size = u64::from_le_bytes(size_bytes) as usize;

        // Read header
        let mut header_bytes = vec![0u8; header_size];
        reader.read_exact(&mut header_bytes)
            .map_err(|e| Error::Io(e))?;

        let header: ManifestHeader = bincode::deserialize(&header_bytes)
            .map_err(|e| Error::Serialization(e.to_string()))?;

        // Verify magic bytes
        if header.magic != *MAGIC_BYTES {
            return Err(Error::Corruption("Invalid manifest magic bytes".to_string()));
        }

        // Verify version
        if header.version != MANIFEST_VERSION {
            return Err(Error::NotSupported(
                format!("Unsupported manifest version: {}", header.version)
            ));
        }

        Ok(Self {
            reader,
            header,
        })
    }

    /// Read all entries
    pub fn read_all(&mut self) -> Result<Vec<ManifestEntry>> {
        let mut entries = Vec::new();

        loop {
            match self.read_entry()? {
                Some(entry) => entries.push(entry),
                None => break,
            }
        }

        Ok(entries)
    }

    /// Read next entry
    pub fn read_entry(&mut self) -> Result<Option<ManifestEntry>> {
        // Read entry length
        let mut len_bytes = [0u8; 4];
        match self.reader.read_exact(&mut len_bytes) {
            Ok(_) => {},
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(Error::Io(e)),
        }

        let len = u32::from_le_bytes(len_bytes) as usize;

        // Read entry
        let mut entry_bytes = vec![0u8; len];
        self.reader.read_exact(&mut entry_bytes)
            .map_err(|e| Error::Io(e))?;

        let entry = bincode::deserialize(&entry_bytes)
            .map_err(|e| Error::Serialization(e.to_string()))?;

        Ok(Some(entry))
    }

    /// Reconstruct snapshot from manifest
    pub fn reconstruct_snapshot(&mut self) -> Result<MappingSnapshot> {
        let entries = self.read_all()?;

        let mut mappings = std::collections::BTreeMap::new();
        let mut version = 0u64;

        for entry in entries {
            match entry {
                ManifestEntry::PageMapping { page_id, file_page_id, file_id } => {
                    mappings.insert(page_id, PageMapping {
                        page_id,
                        file_page_id,
                        file_id,
                    });
                }
                ManifestEntry::Snapshot { version: v, .. } => {
                    version = v;
                }
                _ => {} // Ignore other entries for snapshot reconstruction
            }
        }

        Ok(MappingSnapshot::from_mappings(mappings, version))
    }

    /// Get table ID from manifest
    pub fn table_id(&self) -> Result<TableIdent> {
        TableIdent::from_string(&self.header.table_id)
            .ok_or_else(|| Error::InvalidArgument("Invalid table ID in manifest".to_string()))
    }
}

/// Find the latest manifest file for a table
pub fn find_latest_manifest(dir: impl AsRef<Path>, table_id: &TableIdent) -> Result<Option<PathBuf>> {
    let prefix = format!("{}_manifest_", table_id.to_string());

    let mut manifests = Vec::new();

    let entries = std::fs::read_dir(dir)
        .map_err(|e| Error::Io(e))?;

    for entry in entries {
        let entry = entry.map_err(|e| Error::Io(e))?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        if name_str.starts_with(&prefix) && name_str.ends_with(".mfst") {
            manifests.push(entry.path());
        }
    }

    manifests.sort();
    Ok(manifests.into_iter().last())
}

// Add chrono dependency for timestamps
use chrono;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_manifest_write_read() {
        let temp_dir = TempDir::new().unwrap();
        let table_id = TableIdent::new("test", 1);

        // Write manifest
        {
            let mut writer = ManifestWriter::new(temp_dir.path(), &table_id).unwrap();

            writer.write_file_created(1, "/path/to/file1").unwrap();

            let mapping = PageMapping {
                page_id: 10,
                file_page_id: 100,
                file_id: 1,
            };
            writer.write_mapping(&mapping).unwrap();

            writer.close().unwrap();
        }

        // Find and read manifest
        let manifest_path = find_latest_manifest(temp_dir.path(), &table_id)
            .unwrap()
            .unwrap();

        let mut reader = ManifestReader::open(manifest_path).unwrap();
        let entries = reader.read_all().unwrap();

        assert_eq!(entries.len(), 2);

        match &entries[0] {
            ManifestEntry::FileCreated { file_id, .. } => {
                assert_eq!(*file_id, 1);
            }
            _ => panic!("Expected FileCreated entry"),
        }

        match &entries[1] {
            ManifestEntry::PageMapping { page_id, file_page_id, .. } => {
                assert_eq!(*page_id, 10);
                assert_eq!(*file_page_id, 100);
            }
            _ => panic!("Expected PageMapping entry"),
        }
    }

    #[test]
    fn test_manifest_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let table_id = TableIdent::new("test", 1);

        // Create a snapshot
        let mut mappings = std::collections::BTreeMap::new();
        for i in 0..10 {
            mappings.insert(i, PageMapping {
                page_id: i,
                file_page_id: i as FilePageId * 10,
                file_id: 1,
            });
        }

        let snapshot = MappingSnapshot::from_mappings(mappings, 42);

        // Write snapshot to manifest
        {
            let mut writer = ManifestWriter::new(temp_dir.path(), &table_id).unwrap();
            writer.write_snapshot(&snapshot).unwrap();
            writer.close().unwrap();
        }

        // Read and reconstruct
        let manifest_path = find_latest_manifest(temp_dir.path(), &table_id)
            .unwrap()
            .unwrap();

        let mut reader = ManifestReader::open(manifest_path).unwrap();
        let reconstructed = reader.reconstruct_snapshot().unwrap();

        assert_eq!(reconstructed.version(), snapshot.version());
        assert_eq!(reconstructed.len(), snapshot.len());

        for i in 0..10 {
            assert_eq!(reconstructed.get(i), Some(i as FilePageId * 10));
        }
    }
}