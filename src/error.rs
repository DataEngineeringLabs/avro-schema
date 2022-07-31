#[derive(Debug, Clone, Copy)]
pub enum Error {
    OutOfSpec,
    /// When the file is compressed but the feature flag "compression" is not active.
    RequiresCompression,
}

impl From<std::io::Error> for Error {
    fn from(_: std::io::Error) -> Self {
        Error::OutOfSpec
    }
}
