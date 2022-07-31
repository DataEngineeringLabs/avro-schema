use crate::schema::Schema;

/// Avro file's Metadata
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct FileMetadata {
    /// Its schema, whose root is a [`Schema::Record`].
    pub schema: Schema,
    pub compression: Option<Compression>,
    pub marker: [u8; 16],
}

/// A compressed Avro block.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompressedBlock {
    /// The number of rows
    pub number_of_rows: usize,
    /// The compressed data
    pub data: Vec<u8>,
}

impl CompressedBlock {
    /// Creates a new CompressedBlock
    pub fn new(number_of_rows: usize, data: Vec<u8>) -> Self {
        Self {
            number_of_rows,
            data,
        }
    }
}

/// An uncompressed Avro block.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Block {
    /// The number of rows
    pub number_of_rows: usize,
    /// The uncompressed data
    pub data: Vec<u8>,
}

impl Block {
    /// Creates a new Block
    pub fn new(number_of_rows: usize, data: Vec<u8>) -> Self {
        Self {
            number_of_rows,
            data,
        }
    }
}

/// Valid compressions
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum Compression {
    /// Deflate
    Deflate,
    /// Snappy
    Snappy,
}
