use crate::{
    convert::{IntoSql, Sql, SqlRef}, params, Error, ErrorKind, Result, ZeroBlob
};
use std::borrow::Cow;

#[test]
fn positional() {
    let params = params!(10, 20);
    let items = params.items.unwrap();
    assert_eq!(items[0].name, None);
    assert!(matches!(items[0].param, Sql::Int(10)));
    assert_eq!(items[1].name, None);
    assert!(matches!(items[1].param, Sql::Int(20)));
    assert_eq!(items.len(), 2);
}

#[test]
fn named() {
    let params = params!("a" => 10, "y" => 20);
    let items = params.items.unwrap();
    assert_eq!(items[0].name, Some(Cow::Borrowed("a")));
    assert!(matches!(items[0].param, Sql::Int(10)));
    assert_eq!(items[1].name, Some(Cow::Borrowed("y")));
    assert!(matches!(items[1].param, Sql::Int(20)));
    assert_eq!(items.len(), 2);
}

#[test]
fn positional_and_named() {
    let params = params!(10, "a" => 20);
    let items = params.items.unwrap();
    assert_eq!(items[0].name, None);
    assert!(matches!(items[0].param, Sql::Int(10)));
    assert_eq!(items[1].name, Some(Cow::Borrowed("a")));
    assert!(matches!(items[1].param, Sql::Int(20)));
    assert_eq!(items.len(), 2);
}

#[test]
fn empty() {
    let params = params!();
    let items = params.items.unwrap();
    assert!(items.is_empty());
}

#[test]
fn different_types() {
    let params = params!(
        "i64" => 10,
        "f64" => 0.1,
        "t" => "string",
        "b" => [0x1, 0x1],
        "z" => ZeroBlob(10),
        "x" => (),
    );
    let items = params.items.unwrap();

    assert_eq!(items[0].name, Some(Cow::Borrowed("i64")));
    assert!(matches!(items[0].param, Sql::Int(10)));

    assert_eq!(items[1].name, Some(Cow::Borrowed("f64")));
    assert!(matches!(items[1].param, Sql::Float(0.1)));

    assert_eq!(items[2].name, Some(Cow::Borrowed("t")));
    assert!(matches!(items[2].param.borrow(), SqlRef::Text("string")));

    assert_eq!(items[3].name, Some(Cow::Borrowed("b")));
    assert!(matches!(items[3].param.borrow(), SqlRef::Blob(&[0x1, 0x1])));

    assert_eq!(items[4].name, Some(Cow::Borrowed("z")));
    assert!(matches!(items[4].param, Sql::ZeroBlob(ZeroBlob(10))));

    assert_eq!(items[5].name, Some(Cow::Borrowed("x")));
    assert!(matches!(items[5].param, Sql::Null));

    assert_eq!(items.len(), 6);
}

#[test]
fn error() {
    struct ErrorValue;

    impl IntoSql for ErrorValue {
        fn into_sql(self) -> Result<Sql> {
            Err(Error::from(ErrorKind::Generic))
        }
    }

    let params = params!(ErrorValue);
    let e = params.items.unwrap_err();

    assert_eq!(e.kind(), ErrorKind::Generic);
}
