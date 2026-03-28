use thiserror::Error;

#[derive(Debug, Error)]
pub enum HeimdallError {
    #[error("FST error: {0}")]
    Fst(#[from] fst::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Record store out of bounds: id={0}")]
    RecordOutOfBounds(u32),

    #[error("Invalid coordinate: lat={lat}, lon={lon}")]
    InvalidCoord { lat: f64, lon: f64 },

    #[error("Build error: {0}")]
    Build(String),

    #[error("Index not found at path: {0}")]
    IndexNotFound(String),
}
