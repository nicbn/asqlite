# asqlite - Async SQLite wrapper for Rust

This crate provides an API for accessing SQLite databases using
async Rust.

It wraps the `libsqlite3` library.

See the official documentation.

> :warning: **DISCLAIMER**: this crate is not associated with the mantainers or
  trademark owners of the official SQLite3 library.

## Example

```rs
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

// ...

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
```

[See the entire example](examples/apples_and_oranges.rs)

## Minimum Supported Rust Version

Currently the Minimum Supported Rust Version (MSRV) is 1.72. This version may
be increased in the future with a minor release bump.

## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
