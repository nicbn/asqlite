use futures::{AsyncReadExt, AsyncWriteExt};
use std::time::Instant;

async fn run(size: u64) {
    let mut conn = asqlite::Connection::builder()
        .write(true)
        .create(true)
        .open_memory("")
        .await
        .unwrap();

    conn.execute("CREATE TABLE test (value BLOB)", ())
        .await
        .unwrap();
    let id = conn
        .insert(
            "INSERT INTO test (value) VALUES (?)",
            asqlite::params!(asqlite::ZeroBlob(size)),
        )
        .await
        .unwrap();

    let data_to_write: Vec<u8> = (0..size).map(|byte| byte as u8).collect();

    let write_start = Instant::now();

    let mut blob = conn
        .open_blob(
            "main",
            "test",
            "value",
            id,
            asqlite::BlobOpenMode::ReadWrite,
        )
        .await
        .unwrap();
    blob.write_all(&data_to_write).await.unwrap();
    blob.close().await.unwrap();
    println!(
        "{} bytes written in {} ms",
        data_to_write.len(),
        write_start.elapsed().as_millis()
    );

    let read_start = Instant::now();

    let mut blob = conn
        .open_blob("main", "test", "value", id, asqlite::BlobOpenMode::ReadOnly)
        .await
        .unwrap();
    let mut data = Vec::new();
    blob.read_to_end(&mut data).await.unwrap();
    blob.close().await.unwrap();
    println!(
        "{} bytes read in {} ms",
        data.len(),
        read_start.elapsed().as_millis()
    );

    conn.close().await.unwrap();
}

#[tokio::main]
async fn main() {
    run(1 << 20).await;
    run(16 << 20).await;
    // run(128 << 20).await; // FIXME: reading is too slow
}
