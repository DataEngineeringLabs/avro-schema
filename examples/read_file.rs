use std::convert::TryInto;
use std::fs::File;
use std::io::BufReader;

use avro_schema::error::Error;
use avro_schema::read::fallible_streaming_iterator::FallibleStreamingIterator;

fn main() -> Result<(), Error> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let path = &args[1];

    let file = &mut BufReader::new(File::open(path)?);

    let metadata = avro_schema::read::read_metadata(file)?;

    println!("{:#?}", metadata);

    let mut blocks =
        avro_schema::read::BlockStreamingIterator::new(file, metadata.compression, metadata.marker);

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
            let _value = f32::from_le_bytes(item.try_into().unwrap());
            // if there were more fields, we would need to consume (or skip) the remaining
            // here. You can use `avro_schema::read::decode::zigzag_i64` for integers :D
        }
    }

    Ok(())
}
