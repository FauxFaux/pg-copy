use crate::conn::{conn_string_from_env, pg};

use anyhow::{bail, Context, Result};
use clap::ArgMatches;
use lazy_static::lazy_static;
use log::info;
use postgres::Client;
use regex::Regex;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Stats {
    pub est_rows: u64,
    pub cols: Vec<ColInfo>,
}

#[derive(Copy, Clone, Eq, PartialEq, Deserialize, Serialize)]
enum ColType {
    Bool,
    Int4,
    Float4,

    Float8,
    Int8,
    TimeStampTz,

    Text,
    Json,
    JsonB,
}

#[derive(Deserialize, Serialize)]
pub struct ColInfo {
    name: String,
    len: i16,
    nullable: bool,
    col_type: ColType,

    avg_width: i32,
    n_distinct: f32,
    // most_common_vals: Vec<String>,
}

pub fn export_stats(args: &ArgMatches) -> Result<()> {
    let table = args.value_of("table").expect("required parameter");
    let src = conn_string_from_env("PG_SRC")?;
    let mut client = pg(&src, "src")?;
    let stats = stats(&mut client, table)?;
    serde_json::to_writer_pretty(std::io::stdout().lock(), &stats)?;
    Ok(())
}

pub fn stats(client: &mut Client, table: &str) -> Result<Stats> {
    info!("{}: collecting schema...", table);
    let cols = client.query(
        concat!(
            "select att.attname,attlen,attndims,attnotnull,typ.typname,stat.avg_width,stat.n_distinct,stat.most_common_vals",
            " from pg_attribute att inner join pg_type typ on (atttypid=typ.oid)",
            " inner join pg_stats stat on (tablename=$1 and stat.attname = att.attname)",
            " where attrelid=(select relid from pg_stat_user_tables where relname=$1)",
            " and not attisdropped and attnum >= 1 order by attnum"
        ),
        &[&table.to_string()],
    )?.into_iter().map(|row| -> Result<ColInfo> {
        Ok(ColInfo {
            name: row.get("attname"),
            len: row.get("attlen"),
            nullable: row.get("attnotnull"),
            col_type: ColType::from_str(row.get("typname"))?,
            avg_width: row.get("avg_width"),
            n_distinct: row.get("n_distinct"),

            // anyarray type, no idea how to unpack
            // most_common_vals: row.get("most_common_vals")
        })
    }).collect::<Result<Vec<_>>>()?;

    info!("{}: estimating length...", table);
    lazy_static! {
        static ref ROWS: Regex = Regex::new("\\brows=(\\d+)\\b").unwrap();
    }

    // can't use n_live_tup as it's not available on replicas, despite the stats being there (9.6)
    let est_rows = client
        .query(&format!("explain select 1 from {}", table), &[])?
        .into_iter()
        .find_map(|row| {
            let line = row.get::<_, String>(0);
            ROWS.captures(&line)
                .and_then(|cap| cap.get(1))
                .and_then(|num| num.as_str().parse::<u64>().ok())
        })
        .with_context(|| "explain didn't return rows=?")?;

    Ok(Stats { est_rows, cols })
}

impl ColType {
    fn from_str(val: &str) -> Result<ColType> {
        Ok(match val {
            "bool" => ColType::Bool,
            "int4" => ColType::Int4,
            "float4" => ColType::Float4,
            "int8" => ColType::Int8,
            "float8" => ColType::Float8,
            "timestamptz" => ColType::TimeStampTz,

            "text" => ColType::Text,
            "json" => ColType::Json,
            "jsonb" => ColType::JsonB,

            _ => bail!("unsupported ColType: {:?}", val),
        })
    }
}
