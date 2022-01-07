use std::any::Any;
use std::fs;
use std::io::Write;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use arrow2::array::{Array, MutableArray, MutableUtf8Array, TryPush};
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::io::parquet::write::{
    to_parquet_schema, write_file, Compression, Encoding, RowGroupIterator, Version, WriteOptions,
};
use arrow2::record_batch::RecordBatch;
use clap::ArgMatches;
use log::info;

mod unbin;

use crate::stats::Stats;
use unbin::Unbin;

pub fn cli(args: &ArgMatches) -> Result<()> {
    let state_dir = args.value_of("project").expect("required");
    let stats: Stats = super::pg2pg::read_state_file(state_dir, "stats.json")?;

    let mut packit_schema = Vec::new();

    for col in &stats.cols {
        match col.col_type {
            _ => packit_schema.push(PackItKind::String),
        }
    }

    let (tx, rx) = std::sync::mpsc::sync_channel(1);

    let mut file = fs::File::create("a.parquet")?;
    let arrow_schema = Schema::new(
        packit_schema
            .iter()
            .zip(stats.cols.iter())
            .map(|(packit, stats)| Field {
                name: stats.name.to_string(),
                data_type: packit.to_arrow(),
                nullable: stats.nullable,
                dict_id: 0,
                dict_is_ordered: false,
                metadata: None,
            })
            .collect(),
    );
    let write_options = WriteOptions {
        write_statistics: true,
        compression: Compression::Zstd,
        version: Version::V1,
    };

    let thread = std::thread::spawn(move || -> Result<()> {
        write_file(
            &mut file,
            RowGroupIterator::try_new(
                rx.into_iter(),
                &arrow_schema,
                write_options,
                vec![Encoding::Plain; arrow_schema.fields().len()],
            )?,
            &arrow_schema,
            to_parquet_schema(&arrow_schema)?,
            write_options,
            None,
        )?;
        file.flush()?;
        Ok(())
    });

    let mut pack = PackIt::with_capacity(&packit_schema, 4096);

    for input in args.values_of("FILES").expect("required") {
        let input = zstd::Decoder::new(fs::File::open(input)?)?;
        let mut input = Unbin::new(input, u16::try_from(stats.cols.len())?)?;
        while let Some(row) = input.next()? {
            for (i, col) in stats.cols.iter().enumerate() {
                let data = match row.get(u16::try_from(i)?) {
                    Some(data) => data,
                    None => {
                        pack.push_null(i)?;
                        continue;
                    }
                };
                match col.col_type {
                    _ => pack.push_str(i, Some(&String::from_utf8_lossy(data)))?,
                };
            }

            pack.finish_row()?;

            if pack.mem_used > 512 * 1024 * 1024 {
                info!("dibatching patch");
                tx.send(RecordBatch::try_from_iter(
                    pack.take_batch()
                        .into_iter()
                        .zip(stats.cols.iter())
                        .map(|(arr, stat)| (stat.name.to_string(), arr)),
                ))?;
            }
        }
    }

    info!("joining");
    drop(tx);
    thread.join().expect("thread panic")?;

    Ok(())
}

#[derive(Copy, Clone)]
enum PackItKind {
    String,
}

impl PackItKind {
    fn array_with_capacity(self, capacity: usize) -> VarArray {
        match self {
            PackItKind::String => VarArray::new(MutableUtf8Array::<i32>::with_capacity(capacity)),
        }
    }

    fn to_arrow(self) -> DataType {
        match self {
            PackItKind::String => DataType::Utf8,
        }
    }
}

struct VarArray {
    inner: Box<dyn MutableArray>,
}

impl VarArray {
    fn new<T: MutableArray + 'static>(array: T) -> Self {
        Self {
            inner: Box::new(array),
        }
    }

    fn downcast_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.inner.as_mut_any().downcast_mut()
    }

    // this moves, but has to be called from a mut ref?!
    fn as_arc(&mut self) -> Arc<dyn Array> {
        self.inner.as_arc()
    }
}

struct PackIt {
    schema: Box<[PackItKind]>,
    builders: Box<[VarArray]>,
    cap: usize,
    mem_used: usize,
}

fn make_builders(schema: &[PackItKind], cap: usize) -> Box<[VarArray]> {
    schema
        .iter()
        .map(|kind| kind.array_with_capacity(cap))
        .collect()
}

impl PackIt {
    fn with_capacity(schema: &[PackItKind], cap: usize) -> Self {
        Self {
            schema: schema.to_vec().into_boxed_slice(),
            builders: make_builders(schema, cap),
            cap,
            mem_used: 0,
        }
    }

    fn push_null(&mut self, i: usize) -> Result<()> {
        self.builders[i].inner.push_null();
        Ok(())
    }

    fn push_str(&mut self, i: usize, val: Option<&str>) -> Result<()> {
        let arr = &mut self.builders[i];
        if let Some(arr) = arr.downcast_mut::<MutableUtf8Array<i32>>() {
            self.mem_used += val.map(|val| val.len()).unwrap_or_default() + 4;
            arr.try_push(val)?;
            Ok(())
        } else {
            Err(anyhow!(
                "can't push a string to a {}",
                std::any::type_name_of_val(arr)
            ))
        }
    }

    fn finish_row(&mut self) -> Result<()> {
        Ok(())
    }

    fn take_batch(&mut self) -> Vec<Arc<dyn Array>> {
        let ret = self.builders.iter_mut().map(|arr| arr.as_arc()).collect();
        self.builders = make_builders(&self.schema, self.cap);
        self.mem_used = 0;
        ret
    }
}
