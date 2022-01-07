use std::any::Any;
use std::io::Write;
use std::str::from_utf8;
use std::sync::Arc;
use std::{fs, io};

use anyhow::{anyhow, bail, Result};
use arrow2::array::{
    Array, MutableArray, MutableBooleanArray, MutablePrimitiveArray, MutableUtf8Array, TryPush,
};
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::io::parquet::write::{
    to_parquet_schema, write_file, Compression, Encoding, RowGroupIterator, Version, WriteOptions,
};
use arrow2::record_batch::RecordBatch;
use arrow2::types::NativeType;
use clap::ArgMatches;
use log::info;

mod unbin;

use crate::stats::ColType;
use crate::stats::Stats;
use unbin::Unbin;

/// '2000-01-01' - '1970-01-01' to microseconds
const POSTGRES_UNIX_OFFSET: i64 = 946_684_800_000_000;

pub fn cli(args: &ArgMatches) -> Result<()> {
    let state_dir = args.value_of("project").expect("required");
    let stats: Stats = super::pg2pg::read_state_file(state_dir, "stats.json")?;

    let packit_schema = stats
        .cols
        .iter()
        .map(|col| {
            Ok(match col.col_type {
                ColType::Bool => PackItKind::Bool,
                ColType::Int4 => PackItKind::I32,
                ColType::Int8 => PackItKind::I64,
                ColType::Float8 => PackItKind::F64,
                // TODO: actually map timestamp columns
                ColType::TimeStampTz => PackItKind::I64,
                ColType::Text | ColType::JsonB | ColType::Json => PackItKind::String,
                other => bail!("unsupported column type {:?}", other),
            })
        })
        .collect::<Result<Vec<_>>>()?;

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
        version: Version::V2,
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
        let input = io::BufReader::new(zstd::Decoder::new(fs::File::open(input)?)?);
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
                    ColType::Bool => pack.push_bool(i, pg_to_bool(data)?)?,
                    ColType::Int4 => pack.push_primitive(i, pg_to_i32(data)?)?,
                    ColType::Int8 => pack.push_primitive(i, pg_to_i64(data)?)?,
                    ColType::TimeStampTz => {
                        pack.push_primitive(i, pg_to_i64(data)? + POSTGRES_UNIX_OFFSET)?
                    }
                    ColType::Float8 => pack.push_primitive(i, pg_to_f64(data)?)?,
                    ColType::Text => pack.push_str(i, Some(from_utf8(data)?))?,
                    ColType::JsonB => pack.push_str(i, Some(&from_utf8(data)?[1..]))?,
                    other => panic!(
                        "mismatch between schema and insertion, unsupported {:?}",
                        other
                    ),
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

fn pg_to_bool(slice: &[u8]) -> Result<bool> {
    if slice.len() != 1 {
        bail!("bools should be one byte, not {:?}", slice)
    }

    Ok(match slice[0] {
        0 => false,
        1 => true,
        other => bail!("bools should be 1/0, not {}", other),
    })
}

fn pg_to_i32(slice: &[u8]) -> Result<i32> {
    Ok(i32::from_be_bytes(slice.try_into()?))
}

fn pg_to_i64(slice: &[u8]) -> Result<i64> {
    Ok(i64::from_be_bytes(slice.try_into()?))
}

fn pg_to_f64(slice: &[u8]) -> Result<f64> {
    Ok(f64::from_be_bytes(slice.try_into()?))
}

#[derive(Copy, Clone)]
enum PackItKind {
    Bool,
    I32,
    I64,
    F64,
    String,
}

impl PackItKind {
    fn array_with_capacity(self, capacity: usize) -> VarArray {
        match self {
            PackItKind::Bool => VarArray::new(MutableBooleanArray::with_capacity(capacity)),
            PackItKind::I32 => VarArray::new(MutablePrimitiveArray::<i32>::with_capacity(capacity)),
            PackItKind::I64 => VarArray::new(MutablePrimitiveArray::<i64>::with_capacity(capacity)),
            PackItKind::F64 => VarArray::new(MutablePrimitiveArray::<f64>::with_capacity(capacity)),
            PackItKind::String => VarArray::new(MutableUtf8Array::<i32>::with_capacity(capacity)),
        }
    }

    fn to_arrow(self) -> DataType {
        match self {
            PackItKind::Bool => DataType::Boolean,
            PackItKind::I32 => DataType::Int32,
            PackItKind::I64 => DataType::Int64,
            PackItKind::F64 => DataType::Float64,
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
        // only off by a factor of about eight
        self.mem_used += 1;
        self.builders[i].inner.push_null();
        Ok(())
    }

    fn push_str(&mut self, i: usize, val: Option<&str>) -> Result<()> {
        let arr = &mut self.builders[i];
        if let Some(arr) = arr.downcast_mut::<MutableUtf8Array<i32>>() {
            self.mem_used +=
                val.map(|val| val.len()).unwrap_or_default() + std::mem::size_of::<i32>();
            arr.try_push(val)?;
            Ok(())
        } else {
            Err(anyhow!("can't push a string to this column"))
        }
    }

    fn push_bool(&mut self, i: usize, val: bool) -> Result<()> {
        let arr = &mut self.builders[i];
        if let Some(arr) = arr.downcast_mut::<MutableBooleanArray>() {
            // only off by a factor of about four
            self.mem_used += 1;
            arr.try_push(Some(val))?;
            Ok(())
        } else {
            Err(anyhow!("can't push a bool to this column"))
        }
    }

    fn push_primitive<T: NativeType>(&mut self, i: usize, val: T) -> Result<()> {
        let arr = &mut self.builders[i];
        if let Some(arr) = arr.downcast_mut::<MutablePrimitiveArray<T>>() {
            self.mem_used += std::mem::size_of::<T>();
            arr.try_push(Some(val))?;
            Ok(())
        } else {
            Err(anyhow!(
                "can't push an {} to this column",
                std::any::type_name::<T>()
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
