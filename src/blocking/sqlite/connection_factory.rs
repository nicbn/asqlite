use crate::{
    blocking::{self, sqlite::Connection},
    Result,
};
use std::ffi::CStr;

#[derive(Clone, Copy)]
pub(crate) struct ConnectionFactory;

impl blocking::ConnectionFactory for ConnectionFactory {
    type Connection = Connection;

    #[inline]
    fn open(&self, path: &CStr, flags: i32) -> Result<Connection> {
        Connection::open(path, flags)
    }
}
