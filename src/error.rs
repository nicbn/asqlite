use std::{fmt, io};

/// Library error.
#[derive(Clone, Debug)]
pub struct Error {
    kind: ErrorKind,
    message: String,
}

impl Error {
    pub(crate) fn from_code(code: i32, message: Option<String>) -> Self {
        Self {
            kind: ErrorKind::from_code(code),
            message: message.unwrap_or_else(|| ErrorKind::from_code(code).to_string()),
        }
    }

    pub(crate) fn new(kind: ErrorKind, message: String) -> Self {
        Self {
            kind,
            message,
        }
    }

    pub(crate) fn background_task_failed() -> Self {
        Self::new(ErrorKind::Generic, String::from("background task failed"))
    }

    /// Returns the kind of the error.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns the message of the error.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sqlite error: {} ({})", self.kind(), self.message())
    }
}

impl std::error::Error for Error {}

impl From<Error> for io::Error {
    fn from(v: Error) -> io::Error {
        io::Error::new(v.kind.into(), v)
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self {
            kind,
            message: kind.to_string(),
        }
    }
}

macro_rules! error_kind {
    (
        $(
            $(#[doc = $doc:expr])*
            #[message = $message:expr]
            $(#[io = $error_kind:ident])?
            $variant:ident $(= $code:ident)?,
        )*
    ) => {
        /// Library error kind.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum ErrorKind {
            $(
                $(#[doc = $doc])*
                $variant,
            )*
        }

        impl ErrorKind {
            /// Converts the error kind from a SQLite error code.
            pub const fn from_code(code: i32) -> Self {
                match code & 0xFF {
                    $(
                        $(libsqlite3_sys::$code => Self::$variant,)?
                    )*
                    _ => Self::Generic,
                }
            }

            /// Converts the error kind into a SQLite error code.
            pub const fn code(self) -> Option<i32> {
                match self {
                    $(
                        $(Self::$variant => Some(libsqlite3_sys::$code),)?
                    )*
                    _ => None,
                }
            }
        }

        impl fmt::Display for ErrorKind {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                match self {
                    $(
                        Self::$variant => f.write_str($message),
                    )*
                }
            }
        }

        impl From<ErrorKind> for io::ErrorKind {
            fn from(v: ErrorKind) -> io::ErrorKind {
                match v {
                    $(
                        $(ErrorKind::$variant => io::ErrorKind::$error_kind,)?
                    )*
                    _ => io::ErrorKind::Other,
                }
            }
        }
    };
}

error_kind! {
    /// Generic error.
    #[message = "generic error"]
    Generic = SQLITE_ERROR,

    /// Internal library error.
    #[message = "internal malfunction"]
    InternalMalfunction = SQLITE_INTERNAL,

    /// Unable to access the database.
    #[message = "permission denied"]
    #[io = PermissionDenied]
    PermissionDenied = SQLITE_PERM,

    /// The operation has been aborted.
    #[message = "operation aborted"]
    #[io = Interrupted]
    OperationAborted = SQLITE_ABORT,

    /// Database is being used concurrently by the same process.
    #[message = "database busy"]
    DatabaseBusy = SQLITE_BUSY,

    /// Database is locked, usually because another process has opened it
    /// with read-write access.
    #[message = "database locked"]
    DatabaseLocked = SQLITE_LOCKED,

    /// Out of memory.
    #[message = "out of memory"]
    #[io = OutOfMemory]
    OutOfMemory = SQLITE_NOMEM,

    /// Tried to write to a database which is read only.
    #[message = "database is read only"]
    #[io = PermissionDenied]
    ReadOnly = SQLITE_READONLY,

    /// The operation has been interrupted.
    #[message = "operation interrupted"]
    #[io = Interrupted]
    OperationInterrupted = SQLITE_INTERRUPT,

    /// System I/O error.
    #[message = "i/o error"]
    SystemIoFailure = SQLITE_IOERR,

    /// Database is corrupted.
    #[message = "corrupted database"]
    #[io = InvalidData]
    DatabaseCorrupt = SQLITE_CORRUPT,

    /// Database was not found.
    #[message = "database not found"]
    #[io = NotFound]
    NotFound = SQLITE_NOTFOUND,

    /// Disk is full.
    #[message = "disk full"]
    DiskFull = SQLITE_FULL,

    /// Cannot open the database.
    #[message = "cannot open database"]
    CannotOpen = SQLITE_CANTOPEN,

    /// Error in the file locking protocol.
    #[message = "file locking protocol error"]
    FileLockingProtocolFailed = SQLITE_PROTOCOL,

    /// Schema has changed.
    #[message = "schema has changed"]
    #[io = InvalidData]
    SchemaChanged = SQLITE_SCHEMA,

    /// String or blob is too large.
    #[message = "string or blob is too large"]
    #[io = InvalidData]
    TooLarge = SQLITE_TOOBIG,

    /// Constraint violation.
    #[message = "constraint violation"]
    #[io = InvalidData]
    ConstraintViolation = SQLITE_CONSTRAINT,

    /// Datatype mismatch.
    #[message = "datatype mismatch"]
    #[io = InvalidData]
    DatatypeMismatch = SQLITE_MISMATCH,

    /// Library has been misused.
    #[message = "library misuse"]
    #[io = InvalidInput]
    Misuse = SQLITE_MISUSE,

    /// No support for large files.
    #[message = "lfs not supported"]
    #[io = Unsupported]
    LfsUnsupported = SQLITE_NOLFS,

    /// Statement is not authorized.
    #[message = "unauthorized statement"]
    #[io = PermissionDenied]
    Unauthorized = SQLITE_AUTH,

    /// Out of range.
    #[message = "out of range"]
    #[io = InvalidInput]
    OutOfRange = SQLITE_RANGE,

    /// Not a database.
    #[message = "not a database"]
    #[io = NotFound]
    NotADatabase = SQLITE_NOTADB,

    /// Invalid path.
    #[message = "invalid database path"]
    #[io = InvalidInput]
    InvalidPath,

    /// Database was closed already.
    #[message = "connection was closed"]
    #[io = BrokenPipe]
    ConnectionClosed,

    /// Too many statements.
    #[message = "too many statements"]
    #[io = InvalidInput]
    TooManyStatements,
}
