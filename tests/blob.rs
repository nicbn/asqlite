use asqlite::{params, BlobOpenMode};
use futures_lite::{AsyncReadExt, StreamExt};
use std::io::SeekFrom;

#[tokio::test]
async fn read() {
    let mut conn = asqlite::Connection::builder()
        .write(true)
        .create(true)
        .open_memory(":memory")
        .await
        .unwrap();

    let blob_value: Vec<u8> = (0..=255).collect();

    conn.execute("CREATE TABLE test (key INTEGER, value BLOB)", ())
        .await
        .unwrap();
    conn.insert(
        "INSERT INTO test (key, value) VALUES (?, ?)",
        params!(1, blob_value),
    )
    .await
    .unwrap();

    {
        let id = conn
            .query::<i64>("SELECT rowid FROM test WHERE key = ?", params!(1))
            .next()
            .await
            .unwrap()
            .unwrap();

        let mut blob = conn
            .open_blob("main", "test", "value", id, BlobOpenMode::ReadOnly)
            .await
            .unwrap();

        assert_eq!(blob.size(), 256);

        let mut buf = [0; 8];
        blob.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, [0, 1, 2, 3, 4, 5, 6, 7]);
        blob.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, [8, 9, 10, 11, 12, 13, 14, 15]);
        assert_eq!(blob.seek(SeekFrom::Start(0)).unwrap(), 0);
        blob.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, [0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(blob.seek(SeekFrom::Current(100)).unwrap(), 108);
        blob.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, [108, 109, 110, 111, 112, 113, 114, 115]);
        assert_eq!(blob.seek(SeekFrom::End(-50)).unwrap(), 206);
        blob.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, [206, 207, 208, 209, 210, 211, 212, 213]);
    }

    conn.close().await.unwrap();
}

#[tokio::test]
async fn write() {
    let mut conn = asqlite::Connection::builder()
        .write(true)
        .create(true)
        .open_memory(":memory")
        .await
        .unwrap();

    conn.execute("CREATE TABLE test (key INTEGER, value BLOB)", ())
        .await
        .unwrap();
    conn.insert(
        "INSERT INTO test (key, value) VALUES (?, ?)",
        params!(1, asqlite::ZeroBlob(16)),
    )
    .await
    .unwrap();

    {
        let id = conn
            .query::<i64>("SELECT rowid FROM test WHERE key = ?", params!(1))
            .next()
            .await
            .unwrap()
            .unwrap();

        let mut blob = conn
            .open_blob("main", "test", "value", id, BlobOpenMode::ReadWrite)
            .await
            .unwrap();

        assert_eq!(blob.size(), 16);

        blob.write(&[1]).await.unwrap();
        blob.seek(SeekFrom::Start(5)).unwrap();
        blob.write(&[10]).await.unwrap();
        blob.flush().await.unwrap();

        drop(blob);

        let data: Vec<u8> = conn
            .query("SELECT value FROM test WHERE key = ?", params!(1))
            .next()
            .await
            .unwrap()
            .unwrap();

        assert_eq!(data, [1, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    }

    conn.close().await.unwrap();
}
