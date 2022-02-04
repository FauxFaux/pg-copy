use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{env, fs};

use anyhow::Result;
use ciborium::value::Value;
use clap::ArgMatches;
use itertools::Itertools;
use log::info;
use num_format::{Locale, ToFormattedString};
use postgres::types::{FromSql, Type};
use postgres::Row;
use time::OffsetDateTime;

use crate::conn::{conn_string_from_env, pg};

pub fn cli(args: &ArgMatches) -> Result<()> {
    let ids: Vec<i64> =
        serde_json::from_reader(fs::File::open(args.value_of("id-list").expect("required"))?)?;
    let query = fs::read_to_string(args.value_of("query-file").expect("required"))?;
    let mut conn = pg(&conn_string_from_env("PG_SRC")?, "src")?;

    let stat = conn.prepare_typed(&query, &[Type::INT8_ARRAY])?;

    for id in ids {
        let mut out = Vec::new();
        for row in conn.query(&stat, &[&vec![id]])? {
            out.push(cbor_row(&row));
        }
        ciborium::ser::into_writer(&out, fs::File::create(format!("{id}.cbor"))?)?;
    }
    Ok(())
}

fn cbor_row(row: &Row) -> Vec<Value> {
    row.columns()
        .iter()
        .enumerate()
        .map(|(i, col)| match col.type_().name() {
            "int8" => or_null(&row, i, |v: i64| Value::Integer(v.into())),
            "int4" => or_null(&row, i, |v: i32| Value::Integer(v.into())),
            "float8" => or_null(&row, i, Value::Float),
            "text" => or_null(&row, i, Value::Text),
            "bool" => or_null(&row, i, Value::Bool),
            // discarding millis/nanos
            "timestamptz" => or_null(&row, i, |v: OffsetDateTime| {
                Value::Integer(v.unix_timestamp().into())
            }),
            other => unimplemented!("column type {:?}", other),
        })
        .collect()
}

fn or_null<'a, T: FromSql<'a>>(row: &'a Row, i: usize, f: impl FnOnce(T) -> Value) -> Value {
    match row.get::<_, Option<T>>(i) {
        Some(v) => f(v),
        None => Value::Null,
    }
}
