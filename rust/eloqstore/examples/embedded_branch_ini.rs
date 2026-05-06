use eloqstore::{EloqStore, Options, TableIdentifier};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

fn timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn main() -> Result<(), eloqstore::KvError> {
    let mut root = std::env::temp_dir();
    root.push(format!("eloqstore-rust-example-{}", timestamp_ms()));
    let store_path = root.join("data");
    fs::create_dir_all(&store_path).expect("failed to create store path");

    let ini_path = root.join("eloqstore.ini");
    fs::write(
        &ini_path,
        "[run]\nnum_threads = 1\nbuffer_pool_size = 4MB\n\n[permanent]\ndata_page_size = 4KB\n",
    )
    .expect("failed to write ini");

    let mut opts = Options::new()?;
    opts.load_from_ini(&ini_path)?;
    opts.add_store_path(&store_path)?;

    let mut store = EloqStore::new(&opts)?;
    store.start_with_branch("feature-x", 7, 3)?;

    let table = TableIdentifier::new("demo_table", 0)?;
    store.put(&table, b"hello", b"world", timestamp_ms())?;
    println!(
        "GET hello -> {}",
        String::from_utf8_lossy(&store.get(&table, b"hello")?.unwrap())
    );

    store.stop();
    Ok(())
}
