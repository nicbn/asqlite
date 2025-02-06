use asqlite::params;
use futures_lite::StreamExt;

#[tokio::test]
async fn query() {
    let mut conn = asqlite::Connection::builder()
        .write(true)
        .create(true)
        .open_memory(":memory")
        .await
        .unwrap();

    conn.execute("CREATE TABLE test (key INTEGER, value INTEGER)", ())
        .await
        .unwrap();
    conn.insert("INSERT INTO test (key, value) VALUES (?, ?)", params!(1, 1))
        .await
        .unwrap();
    conn.insert("INSERT INTO test (key, value) VALUES (?, ?)", params!(2, 2))
        .await
        .unwrap();
    conn.insert("INSERT INTO test (key, value) VALUES (?, ?)", params!(2, 3))
        .await
        .unwrap();

    {
        let mut rows = conn.query::<i64>("SELECT value FROM test WHERE key = ?", params!(2));

        assert_eq!(rows.next().await.transpose().unwrap(), Some(2));
        assert_eq!(rows.next().await.transpose().unwrap(), Some(3));
        assert_eq!(rows.next().await.transpose().unwrap(), None);
    }

    conn.close().await.unwrap();
}

#[tokio::test]
async fn update() {
    let mut conn = asqlite::Connection::builder()
        .write(true)
        .create(true)
        .open_memory(":memory")
        .await
        .unwrap();

    conn.execute("CREATE TABLE test (value INTEGER)", ())
        .await
        .unwrap();
    conn.insert("INSERT INTO test (value) VALUES (?)", params!(1))
        .await
        .unwrap();
    let value = conn
        .query::<i64>("SELECT value FROM test", ())
        .next()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, 1);
    conn.insert("UPDATE test SET value = ?", params!(2))
        .await
        .unwrap();
    let value = conn
        .query::<i64>("SELECT value FROM test", ())
        .next()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, 2);

    conn.close().await.unwrap();
}
