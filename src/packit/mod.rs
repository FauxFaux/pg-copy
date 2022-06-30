use std::io::Write;
use std::str::from_utf8;
use std::{fs, io};

use anyhow::{bail, Result};
use clap::ArgMatches;
use log::info;
use pack_it::{Kind, Packer, TableField};

mod unbin;

use crate::stats::ColType;
use crate::stats::Stats;
use unbin::Unbin;

/// '2000-01-01' - '1970-01-01' to microseconds
const POSTGRES_UNIX_OFFSET: i64 = 946_684_800_000_000;

pub fn cli(args: &ArgMatches) -> Result<()> {
    let state_dir = args.value_of("project").expect("required");
    let out_file = args.value_of("output").expect("required");
    let stats: Stats = super::pg2pg::read_state_file(state_dir, "stats.json")?;

    let packit_schema = stats
        .cols
        .iter()
        .map(|col| {
            let kind = match col.col_type {
                ColType::Bool => Kind::Bool,
                ColType::Int4 => Kind::I32,
                ColType::Int8 => Kind::I64,
                ColType::Float8 => Kind::F64,
                // TODO: actually map timestamp columns
                ColType::TimeStampTz => Kind::I64,
                ColType::Text | ColType::JsonB | ColType::Json => Kind::String,
                other => bail!("unsupported column type {:?}", other),
            };
            Ok(TableField::new(&col.name, kind, col.nullable))
        })
        .collect::<Result<Vec<_>>>()?;

    let file = fs::File::create(out_file)?;

    let mut writer = Packer::new(file, &packit_schema)?;

    for input in args.values_of("FILES").expect("required") {
        info!("processing {}...", input);
        let input = io::BufReader::new(zstd::Decoder::new(fs::File::open(input)?)?);
        let mut input = Unbin::new(input, u16::try_from(stats.cols.len())?)?;
        while let Some(row) = input.next()? {
            let pack = writer.table();

            for (i, col) in stats.cols.iter().enumerate() {
                let data = match row.get(u16::try_from(i)?) {
                    Some(data) => data,
                    None => {
                        pack.push_null(i)?;
                        continue;
                    }
                };
                match col.col_type {
                    ColType::Bool => pack.push_bool(i, Some(pg_to_bool(data)?))?,
                    ColType::Int4 => pack.push_primitive(i, Some(pg_to_i32(data)?))?,
                    ColType::Int8 => pack.push_primitive(i, Some(pg_to_i64(data)?))?,
                    ColType::TimeStampTz => {
                        pack.push_primitive(i, Some(pg_to_i64(data)? + POSTGRES_UNIX_OFFSET))?
                    }
                    ColType::Float8 => pack.push_primitive(i, Some(pg_to_f64(data)?))?,
                    ColType::Text => pack.push_str(i, Some(from_utf8(data)?))?,
                    ColType::JsonB => pack.push_str(i, Some(&from_utf8(data)?[1..]))?,
                    other => panic!(
                        "mismatch between schema and insertion, unsupported {:?}",
                        other
                    ),
                };
            }

            writer.consider_flushing()?;
        }
    }

    writer.finish()?.flush()?;

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

// should probably use postgres-types
fn pg_to_i32(slice: &[u8]) -> Result<i32> {
    Ok(i32::from_be_bytes(slice.try_into()?))
}

fn pg_to_i64(slice: &[u8]) -> Result<i64> {
    Ok(i64::from_be_bytes(slice.try_into()?))
}

fn pg_to_f64(slice: &[u8]) -> Result<f64> {
    Ok(f64::from_be_bytes(slice.try_into()?))
}
