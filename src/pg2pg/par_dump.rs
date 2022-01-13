use std::fs;
use std::io;
use std::io::Write;
use std::path::Path;

use anyhow::Result;
use clap::ArgMatches;
use log::info;
use threadpool::ThreadPool;

use super::ranges::generate_wheres;
use crate::conn::{conn_string_from_env, pg};
use crate::pg2pg::ProjectStatus;

pub fn cli(args: &ArgMatches) -> Result<()> {
    go(args.value_of("project").expect("required"))
}

pub fn go(state_dir: impl AsRef<Path>) -> Result<()> {
    let status: ProjectStatus = super::read_state_file(state_dir.as_ref(), "status.json")?;
    let ids: Vec<i64> = super::read_state_file(state_dir.as_ref(), "ids.json")?;
    let wheres = generate_wheres(&ids, status.id);
    let src = conn_string_from_env("PG_SRC")?;

    info!("{} total jobs", wheres.len());

    let pool = ThreadPool::new(64);
    for (i, query) in wheres.into_iter().enumerate() {
        let src = src.to_string();
        let table_name = status.table_name.to_string();
        pool.execute(move || {
            work(&src, i, &query, &table_name).expect(&format!("{} {}", i, query));
        });
    }

    pool.join();

    info!("complete");

    Ok(())
}

fn work(src: &str, i: usize, query: &str, table_name: &str) -> Result<()> {
    let mut src = pg(&src, "src")?;

    let mut writer = zstd::Encoder::new(fs::File::create(&format!("{}.pg-copy-par.zstd", i))?, 3)?;

    let mut reader = src.copy_out(&format!(
        "COPY (SELECT * FROM {} WHERE {}) TO STDOUT WITH (FORMAT binary)",
        table_name, query
    ))?;

    info!("{}: COPY OUT command sent", i);

    let bytes = io::copy(&mut reader, &mut writer)?;

    writer.finish()?.flush()?;

    info!("{}: flushed uncompressed bytes: {}", i, bytes);

    Ok(())
}
