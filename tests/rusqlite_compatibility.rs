use asqlite::params;
use futures_lite::StreamExt;
use std::{
    env::temp_dir,
    future::Future,
    path::PathBuf,
    process,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::fs;

async fn with_tmp_file<F, T>(f: F) -> T::Output
where
    F: FnOnce(PathBuf) -> T,
    T: Future,
{
    static FILE_ID: AtomicUsize = AtomicUsize::new(0);

    let path = temp_dir().join(format!(
        "asqlite_{}_{}.sqlite3",
        process::id(),
        FILE_ID.fetch_add(1, Ordering::Relaxed),
    ));
    let _ = fs::remove_file(&path).await;
    let result = f(path.clone()).await;
    let _ = fs::remove_file(&path).await;
    result
}

#[tokio::test]
async fn create_with_rusqlite_query_with_asqlite() {
    with_tmp_file(|file| async move {
        let conn = rusqlite::Connection::open(&file).unwrap();
        conn.execute("CREATE TABLE test (key INTEGER, value INTEGER)", ())
            .unwrap();
        conn.execute("INSERT INTO test (key, value) VALUES (?, ?)", (1, 1))
            .unwrap();
        conn.execute("INSERT INTO test (key, value) VALUES (?, ?)", (2, 2))
            .unwrap();
        conn.execute("INSERT INTO test (key, value) VALUES (?, ?)", (2, 3))
            .unwrap();

        conn.close().unwrap();

        let mut conn = asqlite::Connection::builder().open(&file).await.unwrap();

        {
            let mut rows = conn.query::<i64>("SELECT value FROM test WHERE key = ?", params!(2));

            assert_eq!(rows.next().await.transpose().unwrap(), Some(2));
            assert_eq!(rows.next().await.transpose().unwrap(), Some(3));
            assert_eq!(rows.next().await.transpose().unwrap(), None);
        }

        conn.close().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn create_with_asqlite_query_with_rusqlite() {
    with_tmp_file(|file| async move {
        let mut conn = asqlite::Connection::builder()
            .write(true)
            .create(true)
            .open(&file)
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

        conn.close().await.unwrap();

        let conn = rusqlite::Connection::open(&file).unwrap();

        {
            let mut statement = conn
                .prepare("SELECT value FROM test WHERE key = ?")
                .unwrap();
            let mut rows = statement.query((2,)).unwrap();

            assert_eq!(
                rows.next()
                    .unwrap()
                    .map(|row| row.get::<_, i64>(0).unwrap()),
                Some(2)
            );
            assert_eq!(
                rows.next()
                    .unwrap()
                    .map(|row| row.get::<_, i64>(0).unwrap()),
                Some(3)
            );
            assert_eq!(
                rows.next()
                    .unwrap()
                    .map(|row| row.get::<_, i64>(0).unwrap()),
                None
            );
        }

        conn.close().unwrap();
    })
    .await;
}
