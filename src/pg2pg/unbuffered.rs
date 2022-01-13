use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::bail;
use anyhow::Result;
use clap::ArgMatches;
use itertools::Itertools;
use log::info;
use num_format::{Locale, ToFormattedString};
use threadpool::ThreadPool;

use crate::conn::{conn_string_from_env, pg};
use crate::pg2pg::count::Counter;
use crate::pg2pg::ProjectStatus;

#[derive(Clone)]
struct Config {
    src: String,
    dest: String,
    table: String,
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
        table: status.table_name.to_string(),
    };

    let ids: Vec<i64> = super::read_state_file(state_dir.as_ref(), "ids.json")?;

    let wheres = generate_wheres(&ids, status.id);

    let pool = ThreadPool::new(64);
    let (tx, rx) = channel();

    let mut stats = HashMap::new();

    for query in wheres {
        let config = config.clone();
        let tx = tx.clone();
        let stat = Arc::new(StatKeeper::default());
        stats.insert(query.to_string(), Arc::clone(&stat));

        pool.execute(move || {
            tx.send(work(config, stat, query))
                .expect("main thread has gone away");
        });
    }

    let start = Instant::now();

    while pool.queued_count() > 0 || pool.active_count() > 0 {
        if let Ok(result) = rx.try_recv() {
            result?;
            continue;
        };

        summary_stats(start, &stats)?;

        if pool.panic_count() > 0 {
            bail!("some thread panic'd");
        }

        std::thread::sleep(Duration::from_secs(2));
    }

    Ok(())
}

fn generate_ranges(ids: &[i64], end: i64) -> Vec<(i64, i64)> {
    let ids_per_bucket = 32;

    let buckets = ids.len() / ids_per_bucket;

    if buckets <= 1 {
        return vec![(0, end)];
    }

    let middles = (1..buckets)
        .flat_map(|bucket| ids.get(bucket * ids_per_bucket).copied())
        .dedup()
        .collect_vec();

    let mut wheres = Vec::with_capacity(middles.len() + 2);
    wheres.push((0, middles[0]));
    wheres.extend(middles.iter().copied().tuple_windows::<(i64, i64)>());
    wheres.push((middles[middles.len() - 1], end));

    wheres
}

pub fn generate_wheres(ids: &[i64], end: i64) -> Vec<String> {
    generate_ranges(ids, end)
        .into_iter()
        .map(|(left, right)| format!("id > {} AND id <= {}", left, right))
        .collect()
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

fn time<T>(timer: &Counter, func: impl FnOnce() -> T) -> T {
    let start = Instant::now();
    let response = func();
    timer.add(u64::try_from(start.elapsed().as_micros()).expect("2^64 micros is 500,000 years"));
    response
}

fn work(config: Config, stats: Arc<StatKeeper>, query: String) -> Result<()> {
    let mut src = pg(&config.src, "src")?;
    let mut dest = pg(&config.dest, "dest")?;
    let mut reader = src.copy_out(&format!(
        "COPY (SELECT * FROM {} WHERE {}) TO STDOUT WITH (FORMAT binary)",
        config.table, query
    ))?;
    info!("COPY OUT command sent");

    let mut writer = dest.copy_in(&format!(
        "COPY {} FROM STDIN WITH (FORMAT binary)",
        config.table
    ))?;
    info!("COPY IN command sent");

    stats.started.store(true, Ordering::Relaxed);

    let mut buf = [0u8; 8 * 1024];
    loop {
        let found = time(&stats.read_micros, || reader.read(&mut buf))?;
        if 0 == found {
            break;
        }

        stats
            .bytes
            .add(u64::try_from(found).expect("8kB <= u64::MAX"));

        let buf = &buf[..found];
        time(&stats.write_micros, || writer.write_all(buf))?;

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

#[cfg(test)]
mod tests {
    use super::generate_ranges;
    use anyhow::Result;
    use itertools::Itertools;

    #[test]
    fn ranges_too_few_ids() -> Result<()> {
        for i in 0..64 {
            let ids = &(0..i).into_iter().collect_vec();
            assert_eq!(vec![(0, 555)], generate_ranges(ids, 555), "0..{}", i);
        }

        for i in 64..(64 + 32) {
            let ids = &(0..i).into_iter().collect_vec();
            assert_eq!(
                vec![(0, 32), (32, 555)],
                generate_ranges(ids, 555),
                "0..{}",
                i
            );
        }
        Ok(())
    }
}
