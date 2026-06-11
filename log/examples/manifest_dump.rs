//! Debug tool: dump the latest slatedb manifest of a local-FS store.
//!
//! Usage: cargo run -p opendata-log --example manifest_dump -- <fs-root> <db-path>
//! e.g.   cargo run -p opendata-log --example manifest_dump -- /tmp/opendata-scan-debug bench-data

use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let fs_root = args.next().expect("fs root arg");
    let db_path = args.next().expect("db path arg");

    let store = Arc::new(slatedb::object_store::local::LocalFileSystem::new_with_prefix(&fs_root)?);
    let admin = slatedb::admin::AdminBuilder::new(db_path, store).build();
    let manifest = admin
        .read_manifest(None)
        .await?
        .expect("no manifest found");
    println!("{:#?}", manifest);

    // Compaction history with statuses — which specs ran, which are stuck.
    match admin.read_compactions(None).await {
        Ok(compactions) => println!("COMPACTIONS: {:#?}", compactions),
        Err(e) => println!("COMPACTIONS unavailable: {e}"),
    }
    Ok(())
}
