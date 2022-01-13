use std::fs;
use std::io;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use arrow2::array::MutablePrimitiveArray;
use arrow2::io::parquet::write::Encoding;
use clap::ArgMatches;
use itertools::Itertools;
use log::info;
use pack_it::{Kind, TableField, Writer};
use postgres::fallible_iterator::FallibleIterator;
use postgres::types::Type;
use threadpool::ThreadPool;

use super::ranges::generate_wheres;
use crate::conn::{conn_string_from_env, pg};
use crate::pg2pg::pooler::run_all;
use crate::pg2pg::ranges::generate_ranges;
use crate::pg2pg::ProjectStatus;
use crate::stats::ColType;

pub fn cli(args: &ArgMatches) -> Result<()> {
    let state_dir = args.value_of("project").expect("required");
    let query = fs::read_to_string(args.value_of("query_file").expect("required"))?;
    go(state_dir, &query)
}

pub fn go(state_dir: impl AsRef<Path>, query: &str) -> Result<()> {
    let status: ProjectStatus = super::read_state_file(state_dir.as_ref(), "status.json")?;
    let ids: Vec<i64> = super::read_state_file(state_dir.as_ref(), "ids.json")?;
    let wheres = generate_ranges(&ids, status.id);
    let src = conn_string_from_env("PG_SRC")?;

    let mut client = pg(&src, "src")?;

    let stat = client.prepare_typed(query, &[Type::INT8, Type::INT8])?;
    let schema = stat
        .columns()
        .iter()
        .map(|col| {
            let col_type = ColType::from_str(col.type_().name())?;
            let kind = match col_type {
                ColType::Bool => Kind::Bool,
                ColType::Int4 => Kind::I32,
                ColType::Float4 => unimplemented!("f32"),
                ColType::Float8 => Kind::F64,
                ColType::Int8 => Kind::I64,
                ColType::TimeStampTz => Kind::TimestampSecsZ,
                ColType::Uuid => unimplemented!("uuid"),
                ColType::Text => Kind::String,
                ColType::Json => Kind::String,
                ColType::JsonB => Kind::String,
            };
            Ok(TableField::new(col.name(), kind, true))
        })
        .collect::<Result<Vec<_>>>()?;

    client.close()?;

    info!("{} total jobs", wheres.len());

    let pool = run_all(wheres.into_iter().map(|range| {
        let src = src.to_string();
        let table_name = status.table_name.to_string();
        let schema = schema.clone();

        || unimplemented!()
        // move || work(&src, &range, &table_name)
    }))?;

    while pool.still_running(Duration::from_secs(2))? {}

    info!("complete");

    Ok(())
}

fn work(src: &str, query: &str, range: (i64, i64), schema: &[TableField]) -> Result<()> {
    let mut src = pg(&src, "src")?;

    let mut iter = src.query_raw(query, &[range.0, range.1])?;

    let mut table = pack_it::Table::with_capacity(&schema.iter().map(|f| f.kind).collect_vec(), 16);

    while let Some(row) = iter.next()? {
        for (i, field) in schema.iter().enumerate() {
            match field.kind {
                Kind::Bool => table.push_bool(i, row.get(i)),
                Kind::U8 => unimplemented!(),
                Kind::I32 => table.push_primitive(i, row.get::<_, i32>(i)),
                Kind::I64 => table.push_primitive(i, row.get::<_, i64>(i)),
                Kind::F64 => table.push_primitive(i, row.get::<_, f32>(i)),
                Kind::String => table.push_str(i, Some(&row.get::<_, String>(i))),
                Kind::TimestampSecsZ => table.push_primitive(i, row.get::<_, i64>(i)),
            }?;
        }
    }

    unimplemented!("take batch and..");

    Ok(())
}
