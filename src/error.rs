use std::fmt;

#[derive(Debug)]
pub enum NotaDbError {
    Io(std::io::Error),
    Corruption(String),
}

impl fmt::Display for NotaDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotaDbError::Io(e) => write!(f, "io error: {}", e),
            NotaDbError::Corruption(msg) => write!(f, "corruption: {}", msg),
        }
    }
}

impl From<std::io::Error> for NotaDbError {
    fn from(e: std::io::Error) -> Self {
        NotaDbError::Io(e)
    }
}

impl std::error::Error for NotaDbError {}

pub type Result<T> = std::result::Result<T, NotaDbError>;
