use crate::Version;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidPatch(String),
    Precondition { path: Vec<String>, reason: String },
    Conflict { expected: Version, actual: Version },
    MergeConflict { path: Vec<String> },
    ForeignBatch,
    RootMustBeObject,
    InvalidVersion { expected: Version, actual: Version },
    VersionOverflow,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPatch(reason) => write!(f, "invalid patch: {reason}"),
            Self::Precondition { path, reason } => write!(f, "at {path:?}: {reason}"),
            Self::Conflict { expected, actual } => write!(
                f,
                "version conflict: based on {expected}, current is {actual}"
            ),
            Self::MergeConflict { path } => write!(
                f,
                "overlapping edits at {path:?}; keep them ordered in a batch"
            ),
            Self::ForeignBatch => write!(f, "batch belongs to another store"),
            Self::RootMustBeObject => write!(f, "snapshot root must be a JSON object"),
            Self::InvalidVersion { expected, actual } => {
                write!(f, "log version {actual}, expected {expected}")
            }
            Self::VersionOverflow => write!(f, "version counter exhausted"),
        }
    }
}
impl std::error::Error for Error {}
