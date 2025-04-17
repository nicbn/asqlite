use asqlite::params;
use futures_lite::StreamExt;
use std::sync::{atomic::AtomicBool, Arc};
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
        FILE_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_file(&path).await;
    let result = f(path.clone()).await;
    let _ = fs::remove_file(&path).await;
    result
}

#[tokio::test]
async fn query_file() {
    with_tmp_file(|file| async move {
        let mut conn = asqlite::Connection::builder()
            .write(true)
            .create(true)
            .open(file)
            .await
            .unwrap();

        conn.execute("CREATE TABLE test (key INTEGER, value TEXT)", ())
            .await
            .unwrap();
        conn.insert(
            "INSERT INTO test (key, value) VALUES (?, ?)",
            params!("one", 1),
        )
        .await
        .unwrap();
        conn.insert(
            "INSERT INTO test (key, value) VALUES (?, ?)",
            params!("two_or_three", 2),
        )
        .await
        .unwrap();
        conn.insert(
            "INSERT INTO test (key, value) VALUES (?, ?)",
            params!("two_or_three", 3),
        )
        .await
        .unwrap();

        {
            let mut rows = conn.query::<i64>(
                "SELECT value FROM test WHERE key = ?",
                params!("two_or_three"),
            );

            assert_eq!(rows.next().await.transpose().unwrap(), Some(2));
            assert_eq!(rows.next().await.transpose().unwrap(), Some(3));
            assert_eq!(rows.next().await.transpose().unwrap(), None);
        }

        conn.close().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn update_file() {
    with_tmp_file(|file| async move {
        let mut conn = asqlite::Connection::builder()
            .write(true)
            .create(true)
            .open(file)
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
    })
    .await;
}

#[tokio::test]
async fn busy_handler() {
    with_tmp_file(|file| async move {
        let busy = Arc::new(AtomicBool::new(false));

        let mut conn1 = asqlite::Connection::builder()
            .write(true)
            .create(true)
            .open(&file)
            .await
            .unwrap();

        let mut conn2 = asqlite::Connection::builder()
            .write(true)
            .create(false)
            .open(file)
            .await
            .unwrap();

        {
            let busy = busy.clone();

            conn2
                .set_busy_handler(move |_| {
                    busy.store(true, Ordering::SeqCst);
                    false
                })
                .unwrap();
        }

        conn1.execute("BEGIN EXCLUSIVE", ()).await.unwrap();

        assert_eq!(
            conn2
                .execute("BEGIN EXCLUSIVE", ())
                .await
                .unwrap_err()
                .kind(),
            asqlite::ErrorKind::DatabaseBusy
        );

        assert!(busy.load(Ordering::SeqCst));

        conn1.close().await.unwrap();
        conn2.close().await.unwrap();
    })
    .await;
}
