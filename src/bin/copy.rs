use std::{sync::Arc, fs::{self}};

use bytes::Bytes;
use parquet::{arrow::{ParquetFileArrowReader, ArrowReader, ArrowWriter}, file::{serialized_reader::SerializedFileReader}};

fn main() {
    let parquet_file = "./parquet/input.parquet";
    let parquet_file_content = fs::read(parquet_file).unwrap();
    let chunk = Bytes::from(parquet_file_content);
    let file_reader = SerializedFileReader::new(chunk).unwrap();
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

    let schema = arrow_reader.get_schema().unwrap();
    let mut output_buffer = vec![];
    let mut writer = ArrowWriter::try_new(&mut output_buffer, Arc::new(schema), None).unwrap();
    
    arrow_reader.get_record_reader(10)
        .unwrap()
        .for_each(|batch| {
            writer.write(&batch.unwrap()).unwrap()
        });
    
    writer.close().unwrap();
    fs::write("./parquet/output.parquet", output_buffer).unwrap();
}
