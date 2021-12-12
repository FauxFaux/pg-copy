use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::conn::{conn_string_from_env, pg};
use crate::pg2pg::ProjectStatus;
use anyhow::bail;
use anyhow::Result;
use clap::ArgMatches;
use itertools::Itertools;
use log::info;
use num_format::{Locale, ToFormattedString};
use threadpool::ThreadPool;

#[derive(Clone)]
struct Config {
    src: String,
    dest: String,
    table: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum State {
    NotStarted,
    Started,
    Finished(u64),
}

impl Default for State {
    fn default() -> Self {
        Self::NotStarted
    }
}

#[derive(Clone, Default, Debug)]
struct StatKeeper {
    observations: usize,
    bytes: u64,
    read_micros: u128,
    write_micros: u128,
    state: State,
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

    let buckets = 4096;
    let every_n = ids.len() as f32 / buckets as f32;

    let middles: Vec<i64> = (1..buckets)
        .map(|bucket| ids[(bucket as f32 * every_n).round() as usize])
        .dedup()
        .collect_vec();

    let mut queries = Vec::new();
    queries.push((0, middles[0]));
    queries.extend(middles.iter().copied().tuple_windows::<(i64, i64)>());
    queries.push((middles[middles.len() - 1], status.id));

    let queries = queries
        .iter()
        .map(|(left, right)| format!("id >= {} AND id < {}", left, right))
        .collect_vec();

    let pool = ThreadPool::new(64);
    let (tx, rx) = channel();

    let mut stats = HashMap::new();

    for query in queries {
        let config = config.clone();
        let tx = tx.clone();
        let stat = Arc::new(Mutex::new(StatKeeper::default()));
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

        // couldn't get this to collect()
        let mut all_stats = Vec::new();
        for (query, stat) in stats.iter() {
            let stat = match stat.lock() {
                Ok(stat) => stat,
                Err(e) => bail!("{}: poison! {:?}", query, e),
            };

            all_stats.push(stat.clone());
        }

        let read: u128 = all_stats.iter().map(|stats| stats.read_micros).sum();
        let write: u128 = all_stats.iter().map(|stats| stats.write_micros).sum();
        let observations: usize = all_stats.iter().map(|stats| stats.observations).sum();

        let bytes: u64 = all_stats.iter().map(|stats| stats.bytes).sum();
        let live = all_stats
            .iter()
            .filter(|stats| stats.state == State::Started)
            .count();
        let finished = all_stats
            .iter()
            .filter(|stats| match stats.state {
                State::Finished(_) => true,
                _ => false,
            })
            .count();

        let elapsed = start.elapsed();
        let obs_div = (observations as u128).max(1);
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

        if pool.panic_count() > 0 {
            bail!("some thread panic'd");
        }

        std::thread::sleep(Duration::from_secs(2));
    }

    Ok(())
}

fn work(config: Config, shared_stats: Arc<Mutex<StatKeeper>>, query: String) -> Result<()> {
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

    let mut stats = StatKeeper::default();
    stats.state = State::Started;

    let mut buf = [0u8; 8 * 1024];
    loop {
        let start = Instant::now();

        let found = reader.read(&mut buf)?;
        if 0 == found {
            break;
        }

        stats.bytes += found as u64;
        stats.read_micros += start.elapsed().as_micros();

        let begin_write = Instant::now();
        let buf = &buf[..found];
        writer.write_all(buf)?;

        stats.write_micros += begin_write.elapsed().as_micros();

        // silly optimisation, avoid locking in the loop
        // approximately every 500kB
        if stats.observations % 64 == 0 {
            let mut shared_stats = shared_stats
                .lock()
                .expect("the main thread would have panicked");
            *shared_stats = stats.clone();
        }

        stats.observations += 1;
    }

    drop(reader);
    src.close()?;

    writer.flush()?;
    let written = writer.finish()?;
    dest.close()?;

    stats.state = State::Finished(written);

    let mut shared = shared_stats
        .lock()
        .expect("the main thread would have panicked");
    *shared = stats;

    Ok(())
}
