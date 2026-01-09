use super::Place;
use anyhow::{Ok, Result};
use arrow::{
    array::{ArrayRef, MapArray, StringArray, StructArray, UInt64Array},
    buffer::OffsetBuffer,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

pub struct ParquetWriter {
    schema: Arc<Schema>,
    writer: ArrowWriter<File>,
    places: Vec<Place>,
    num_tags: usize,
}

impl ParquetWriter {
    pub fn try_new(batch_size: usize, out: &Path) -> Result<ParquetWriter> {
        assert!(batch_size > 0);
        let file = File::create(out)?;
        let schema = Arc::new(make_schema());
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(15)?))
            .build();
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
        Ok(ParquetWriter {
            schema,
            writer,
            places: Vec::with_capacity(batch_size),
            num_tags: 0,
        })
    }

    pub fn write(&mut self, place: Place) -> Result<()> {
        if self.places.len() == self.places.capacity() {
            self.flush()?;
        }
        self.num_tags += place.tags.len();
        self.places.push(place);
        Ok(())
    }

    pub fn close(mut self) -> Result<()> {
        self.flush()?;
        self.writer.close()?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        let s2_cell_ids = Arc::new(UInt64Array::from_iter(
            self.places.iter().map(|p| p.s2_cell_id),
        ));
        let sources = Arc::new(StringArray::from_iter_values(
            self.places.iter().map(|p| p.source.as_str()),
        ));
        let tags = make_tags(&self.places, self.num_tags);
        let batch = RecordBatch::try_new(self.schema.clone(), vec![s2_cell_ids, sources, tags])?;
        self.writer.write(&batch)?;
        self.places.clear();
        self.num_tags = 0;
        Ok(())
    }
}

fn make_tags(places: &[Place], num_tags: usize) -> Arc<MapArray> {
    let mut keys = Vec::with_capacity(num_tags);
    let mut values = Vec::with_capacity(num_tags);
    let mut offsets = Vec::with_capacity(num_tags + 1);
    for place in places.iter() {
        offsets.push(keys.len() as i32);
        for (key, value) in place.tags.iter() {
            keys.push(key.as_ref());
            values.push(value.as_ref());
        }
    }
    offsets.push(keys.len() as i32);

    let keys = StringArray::from(keys);
    let values = StringArray::from(values);
    let offsets = OffsetBuffer::new(offsets.into());

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(keys) as ArrayRef,
        ),
        (
            Arc::new(Field::new("value", DataType::Utf8, false)),
            Arc::new(values) as ArrayRef,
        ),
    ]);

    let map_array = MapArray::new(
        Arc::new(Field::new(
            "key_value",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, false),
                ]
                .into(),
            ),
            false, // non-nullable
        )),
        offsets,
        struct_array,
        None, // no nulls
        true, // sorted
    );
    Arc::new(map_array)
}

fn make_schema() -> Schema {
    Schema::new(vec![
        Field::new("s2_cell_id", DataType::UInt64, /* nullable */ false),
        Field::new("source", DataType::Utf8, /* nullable */ false),
        Field::new(
            "tags",
            DataType::Map(
                Arc::new(Field::new(
                    "key_value",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, /* nullable */ false),
                            Field::new("value", DataType::Utf8, /* nullable */ false),
                        ]
                        .into(),
                    ),
                    /* nullable */ false,
                )),
                /* sorted */ true,
            ),
            false, // non-nullable
        ),
    ])
}
