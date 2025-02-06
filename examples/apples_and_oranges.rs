use futures::StreamExt;
use std::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    // Create an in-memory database connection with the name :memory
    let mut conn = asqlite::Connection::builder()
        .create(true) // create if it does not exists
        .write(true) // read and write
        .open_memory(":memory") // to open a file, use .open(path)
        .await?;

    // Create a table
    conn.execute(
        "CREATE TABLE fruit (name TEXT, color TEXT)",
        (), // no parameters
    )
    .await?;

    // Populate the table
    let _apple_id = conn
        .insert(
            "INSERT INTO fruit (name, color) VALUES (?, ?)",
            asqlite::params!("apple", "red"),
        )
        .await?;

    let _cherry_id = conn
        .insert(
            "INSERT INTO fruit (name, color) VALUES (?, ?)",
            asqlite::params!("cherry", "red"),
        )
        .await?;

    let _orange_id = conn
        .insert(
            "INSERT INTO fruit (name, color) VALUES (?, ?)",
            asqlite::params!("orange", "orange"),
        )
        .await?;

    // Check all red fruits
    let mut rows = conn.query(
        "SELECT name FROM fruit WHERE color = ?",
        asqlite::params!("red"),
    );

    // Iterate rows
    while let Some(row) = rows.next().await {
        let name: String = row?;

        println!("{} is red", name);
    }

    drop(rows);

    // Check all orange fruits
    let mut rows = conn.query(
        "SELECT name FROM fruit WHERE color = ?",
        asqlite::params!("orange"),
    );

    // Iterate rows
    while let Some(row) = rows.next().await {
        let name: String = row?;

        println!("{} is orange", name);
    }

    Ok(())
}
