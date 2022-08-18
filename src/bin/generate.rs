use std::{sync::Arc, fs::{self}};
use arrow::{datatypes::{DataType, Field, Schema}};
use parquet::{arrow::{ArrowWriter}};

fn main() {
    let json_content = r#"
    {"maps":null}
    {"maps":{"fruit": "Pineapple"}}
    {"maps":{"fruit": "Mango"}}
    {"maps":{"fruit": "Banana"}}
    "#;
    let entries_struct_type = DataType::Struct(vec![
        Field::new("keys", DataType::Utf8, false),
        Field::new("values", DataType::Utf8, true),
    ]);
    let custom_field = Field::new(
        "maps",
        DataType::Map(
            Box::new(Field::new("entries", entries_struct_type, false)),
            false,
        ),
        true,
    );
    let schema = Arc::new(Schema::new(vec![custom_field]));
    let builder = arrow::json::ReaderBuilder::new()
        .with_schema(schema.clone())
        .with_batch_size(64);
    let reader = builder.build(std::io::Cursor::new(json_content)).unwrap();

    let mut output_buffer = vec![];
    let mut writer = ArrowWriter::try_new(&mut output_buffer, schema, None).unwrap();
    
    reader.for_each(|batch| {
        writer.write(&batch.unwrap()).unwrap()
    });
    
    writer.close().unwrap();
    fs::write("./parquet/input.parquet", output_buffer).unwrap();
}

