use std::collections::HashMap;
use std::env;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::ArgMatches;
use itertools::Itertools;
use log::info;
use num_format::{Locale, ToFormattedString};

use crate::conn::{conn_string_from_env, pg};
use crate::pg2pg::count::Counter;
use crate::pg2pg::pooler::run_all;
use crate::pg2pg::ProjectStatus;

#[derive(Clone)]
struct Config {
    src: String,
    dest: String,
    src_table: String,
    dest_table: String,
}

#[derive(Default, Debug)]
struct StatKeeper {
    observations: Counter,
    bytes: Counter,
    read_micros: Counter,
    write_micros: Counter,
    started: AtomicBool,
    finished: AtomicBool,
}

pub fn cli(args: &ArgMatches) -> Result<()> {
    go(args.value_of("project").expect("required"))
}

pub fn go(state_dir: impl AsRef<Path>) -> Result<()> {
    let status: ProjectStatus = super::read_state_file(state_dir.as_ref(), "status.json")?;

    let config = Config {
        src: conn_string_from_env("PG_SRC")?,
        dest: conn_string_from_env("PG_DEST")?,
        src_table: status.table_name.to_string(),
        dest_table: env::var("OUT_TABLE").unwrap_or_else(|_| status.table_name.to_string()),
    };

    let ids: Vec<i64> = super::read_state_file(state_dir.as_ref(), "ids.json")?;

    let wheres = super::ranges::generate_wheres(&ids, status.id);

    let mut stats = HashMap::new();

    let pool = run_all(wheres.into_iter().map(|query| {
        let config = config.clone();
        let stat = Arc::new(StatKeeper::default());
        stats.insert(query.to_string(), Arc::clone(&stat));

        || work(config, stat, query)
    }))?;

    let start = Instant::now();

    while pool.still_running(Duration::from_secs(2))? {
        summary_stats(start, &stats)?;
    }

    Ok(())
}

fn summary_stats(start: Instant, stats: &HashMap<String, Arc<StatKeeper>>) -> Result<()> {
    let all_stats = stats.values().collect_vec();

    let ord = Ordering::Relaxed;
    let read: u64 = all_stats.iter().map(|stats| stats.read_micros.get()).sum();
    let write: u64 = all_stats.iter().map(|stats| stats.write_micros.get()).sum();
    let observations: u64 = all_stats.iter().map(|stats| stats.observations.get()).sum();

    let bytes: u64 = all_stats.iter().map(|stats| stats.bytes.get()).sum();
    let live = all_stats
        .iter()
        .filter(|stats| stats.started.load(ord) && !stats.finished.load(ord))
        .count();
    let finished = all_stats
        .iter()
        .filter(|stats| stats.finished.load(ord))
        .count();

    let elapsed = start.elapsed();
    let obs_div = observations.max(1);
    println!(
        "{}s elapsed, {}us mean read, {}us mean write",
        elapsed.as_secs().to_formatted_string(&Locale::en),
        read / obs_div,
        write / obs_div,
    );

    println!(
        "Read {} bytes; {} running jobs, {} finished jobs.",
        bytes.to_formatted_string(&Locale::en),
        live,
        finished
    );
    println!();

    Ok(())
}

fn work(config: Config, stats: Arc<StatKeeper>, query: String) -> Result<()> {
    let mut src = pg(&config.src, "src")?;
    let mut dest = pg(&config.dest, "dest")?;
    let mut reader = src.copy_out(&format!(
        "COPY (SELECT * FROM {} WHERE {}) TO STDOUT WITH (FORMAT binary)",
        config.src_table, query
    ))?;
    info!("COPY OUT command sent");

    let mut writer = dest.copy_in(&format!(
        "COPY {} FROM STDIN WITH (FORMAT binary)",
        config.dest_table
    ))?;
    info!("COPY IN command sent");

    stats.started.store(true, Ordering::Relaxed);

    let mut buf = [0u8; 8 * 1024];
    loop {
        let found = stats.read_micros.time(|| reader.read(&mut buf))?;
        if 0 == found {
            break;
        }

        stats
            .bytes
            .add(u64::try_from(found).expect("8kB <= u64::MAX"));

        let buf = &buf[..found];
        stats.write_micros.time(|| writer.write_all(buf))?;

        stats.observations.inc();
    }

    drop(reader);
    src.close()?;

    writer.flush()?;
    writer.finish()?;
    dest.close()?;

    stats.finished.store(true, Ordering::Release);

    Ok(())
}
