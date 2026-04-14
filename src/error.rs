use std::fmt;

#[derive(Debug)]
pub enum NotaDbError {
    // e.g. key not found, encoding issues, etc.
}

impl fmt::Display for NotaDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl std::error::Error for NotaDbError {}

pub type Result<T> = std::result::Result<T, NotaDbError>;
