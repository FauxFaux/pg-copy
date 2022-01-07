use std::io::Write;
use std::sync::mpsc::{SendError, SyncSender};
use std::thread::JoinHandle;

use anyhow::{anyhow, Context, Result};
use arrow2::datatypes::Field as ArrowField;
use arrow2::datatypes::Schema;
use arrow2::error::ArrowError;
use arrow2::io::parquet::write::{
    to_parquet_schema, write_file, Compression, Encoding, RowGroupIterator, Version, WriteOptions,
};
use arrow2::record_batch::RecordBatch;
use itertools::Itertools;
use log::info;

use crate::packit::table::{Kind, Table};

#[derive(Clone)]
pub struct Field {
    name: String,
    kind: Kind,
    nullable: bool,

    encoding: Encoding,
}

impl Field {
    pub fn new(name: impl ToString, kind: Kind, nullable: bool) -> Self {
        Field {
            name: name.to_string(),
            kind,
            nullable,
            encoding: Encoding::Plain,
        }
    }
}

pub struct Writer<W> {
    table: Table,
    schema: Box<[Field]>,
    thread: Option<OutThread<W>>,
}

struct OutThread<W> {
    tx: SyncSender<Result<RecordBatch, ArrowError>>,
    thread: JoinHandle<Result<W>>,
}

impl<W: Write + Send + 'static> Writer<W> {
    pub fn new(mut inner: W, schema: &[Field]) -> Result<Self> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);

        let arrow_schema = Schema::new(
            schema
                .iter()
                .map(|f| ArrowField {
                    name: f.name.to_string(),
                    data_type: f.kind.to_arrow(),
                    nullable: f.nullable,
                    dict_id: 0,
                    dict_is_ordered: false,
                    metadata: None,
                })
                .collect(),
        );

        let write_options = WriteOptions {
            write_statistics: true,
            compression: Compression::Zstd,
            version: Version::V2,
        };
        let encodings = schema.iter().map(|f| f.encoding).collect();

        let thread = std::thread::spawn(move || -> Result<W> {
            write_file(
                &mut inner,
                RowGroupIterator::try_new(rx.into_iter(), &arrow_schema, write_options, encodings)?,
                &arrow_schema,
                to_parquet_schema(&arrow_schema)?,
                write_options,
                None,
            )?;
            inner.flush()?;
            Ok(inner)
        });

        Ok(Self {
            schema: schema.to_vec().into_boxed_slice(),
            table: Table::with_capacity(&schema.iter().map(|f| f.kind).collect_vec(), 0),
            thread: Some(OutThread { thread, tx }),
        })
    }

    pub fn table(&mut self) -> &mut Table {
        &mut self.table
    }

    pub fn finish_row(&mut self) -> Result<()> {
        self.table.finish_row()?;

        let thread = self
            .thread
            .as_mut()
            .ok_or_else(|| anyhow!("already failed"))?;

        if self.table.mem_estimate() < 512 * 1024 * 1024 {
            return Ok(());
        }

        info!("dibatching patch");
        let result = RecordBatch::try_from_iter(
            self.table
                .take_batch()
                .into_iter()
                .zip(self.schema.iter())
                .map(|(arr, stat)| (stat.name.to_string(), arr)),
        );

        if let Err(SendError(_)) = thread.tx.send(result) {
            let thread = self.thread.take().expect("it was there a second ago");
            // the read half is gone; this is a fused, finished consumer
            drop(thread.tx);
            thread
                .thread
                .join()
                .expect("thread panic")
                .with_context(|| anyhow!("file writer had failed"))?;
        }

        Ok(())
    }

    pub fn finish(mut self) -> Result<W> {
        let thread = self
            .thread
            .take()
            .ok_or_else(|| anyhow!("already failed"))?;
        drop(thread.tx);
        thread.thread.join().expect("thread panic")
    }
}
