use std::convert::TryInto;

use avro_schema::error::Error;
use avro_schema::file::{Block, Compression};
use avro_schema::read::fallible_streaming_iterator::FallibleStreamingIterator;
use avro_schema::schema::{Field, Record, Schema};

fn read_avro(mut data: &[u8]) -> Result<Vec<f32>, Error> {
    let metadata = avro_schema::read::read_metadata(&mut data)?;

    let mut blocks = avro_schema::read::BlockStreamingIterator::new(
        &mut data,
        metadata.compression,
        metadata.marker,
    );

    let mut values = vec![];
    while let Some(block) = blocks.next()? {
        let _fields = &metadata.record.fields;
        let length = block.number_of_rows;
        let mut block: &[u8] = block.data.as_ref();
        // at this point you can deserialize the block based on `_fields` according
        // to avro's specification. Note that `Block` is already decompressed.
        // for example, if there was a single field with f32, we would use
        for _ in 0..length {
            let (item, remaining) = block.split_at(4);
            block = remaining;
            let value = f32::from_le_bytes(item.try_into().unwrap());
            values.push(value)
            // if there were more fields, we would need to consume (or skip) the remaining
            // here. You can use `avro_schema::read::decode::zigzag_i64` for integers :D
        }
    }

    Ok(values)
}

fn write_avro(
    compression: Option<avro_schema::file::Compression>,
    array: &[f32],
) -> Result<Vec<u8>, Error> {
    let mut file = vec![];

    let record = Record::new("", vec![Field::new("value", Schema::Float)]);

    avro_schema::write::write_metadata(&mut file, record, compression)?;

    // we need to create a `Block`
    let mut data: Vec<u8> = vec![];
    for item in array.iter() {
        let bytes = item.to_le_bytes();
        data.extend(bytes);
    }
    let mut block = Block::new(array.len(), data);

    // once completed, we compress it
    let mut compressed_block = avro_schema::file::CompressedBlock::default();
    let _ = avro_schema::write::compress(&mut block, &mut compressed_block, compression)?;

    // and finally write it to the file
    avro_schema::write::write_block(&mut file, &compressed_block)?;

    Ok(file)
}

#[test]
fn round_trip() -> Result<(), Error> {
    let original = vec![0.1, 0.2];
    let file = write_avro(None, &original)?;
    let read = read_avro(&file)?;
    assert_eq!(read, original);
    Ok(())
}

#[test]
fn round_trip_deflate() -> Result<(), Error> {
    let original = vec![0.1, 0.2];
    let file = write_avro(Some(Compression::Deflate), &original)?;
    let read = read_avro(&file)?;
    assert_eq!(read, original);
    Ok(())
}

#[test]
fn round_trip_snappy() -> Result<(), Error> {
    let original = vec![0.1, 0.2];
    let file = write_avro(Some(Compression::Snappy), &original)?;
    let read = read_avro(&file)?;
    assert_eq!(read, original);
    Ok(())
}
