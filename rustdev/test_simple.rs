use std::path::PathBuf;
use eloqstore::{EloqStore, Result};
use eloqstore::api::request::{BatchWriteRequest, WriteEntry, WriteOp};
use eloqstore::config::KvOptions;
use eloqstore::types::{TableIdent};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    println!("Test program starting");

    // Create configuration
    let mut options = KvOptions::default();
    options.num_threads = 1;
    options.data_dirs = vec![PathBuf::from("/tmp/test_simple")];
    options.data_page_size = 4096;

    println!("Creating store...");
    let mut store = EloqStore::new(options)?;

    println!("Starting store...");
    store.start().await?;
    println!("Store started");

    let table_id = TableIdent::new("test_table", 0);

    // Single write
    println!("Writing single key...");
    let write_req = BatchWriteRequest {
        shard_id: 0,
        table_ident: table_id.clone(),
        entries: vec![
            WriteEntry {
                key: b"key1".to_vec().into(),
                value: b"value1".to_vec().into(),
                op: WriteOp::Upsert,
                ttl: None,
            }
        ],
    };

    match store.batch_write(write_req).await {
        Ok(_) => println!("Write successful!"),
        Err(e) => println!("Write failed: {:?}", e),
    }

    println!("Test complete");
    Ok(())
}