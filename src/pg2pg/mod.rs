use std::collections::HashMap;
use std::env;
use std::fmt::Display;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::str::FromStr;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::conn::{conn_string_from_env, pg};
use crate::stats::Stats;
use anyhow::Context;
use anyhow::Result;
use anyhow::{anyhow, bail};
use clap::ArgMatches;
use itertools::Itertools;
use log::info;
use num_format::{Locale, ToFormattedString};
use postgres::Client;
use serde::{Deserialize, Serialize};
use threadpool::ThreadPool;

#[derive(Clone)]
struct Config {
    src: String,
    dest: String,
    table: String,
    table_sample: f32,
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

impl Config {
    fn file(&self, extension: impl Display) -> String {
        format!("pg-copy-state/{}.{}", self.table.to_lowercase(), extension)
    }
}

pub fn cli(args: &ArgMatches) -> Result<()> {
    match args.subcommand().expect("subcommand required") {
        ("prepare", args) => prepare(args),
        ("batched", args) => go(),
        _ => unimplemented!("subcommands should be covered"),
    }
}

#[derive(Serialize, Deserialize)]
enum ProjectState {
    NotStarted,
    InitialSync,
    UpToDate,
    Committing,
    Failed,
}

#[derive(Serialize, Deserialize)]
struct ProjectStatus {
    id: i64,
    state: ProjectState,
    table_name: String,
}

struct InputConfig {
    table_name: String,
    target_batch_rows: u64,
}

fn prepare(args: &ArgMatches) -> Result<()> {
    let src = conn_string_from_env("PG_SRC")?;
    let table_name = args.value_of("table").expect("required option");

    let mut client = pg(&src, "src")?;

    let dest = args.value_of("dest").expect("required option").to_string();
    fs::create_dir(&dest)
        .with_context(|| anyhow!("creating dest: {:?} from {:?}", &dest, env::current_dir()))?;

    let initial = compute_initial(
        &mut client,
        &InputConfig {
            table_name: table_name.to_string(),
            target_batch_rows: args.value_of("batch_rows").expect("has default").parse()?,
        },
    )?;

    write_file_in(
        &dest,
        "status.json",
        ProjectStatus {
            id: initial.max,
            state: ProjectState::NotStarted,
            table_name: table_name.to_string(),
        },
    )?;

    write_file_in(&dest, "stats.json", initial.stats)?;

    write_file_in(&dest, "ids.json", initial.ids)?;

    Ok(())
}

fn catchup() {
    // min batch size, max batch size?
    // generate batches
    // min threads? max threads

    // open transaction to grab snapshot
    // bulk copy in threads
    // commit everything together

    // commit asap, for initial mode? always?

    // how about a catchup-safe, which we're sure about whether it's committed or not (single writer transaction)
    // pretty sure yes
    // assume dense for catchup-safe, for batch calculation
}

pub fn go() -> Result<()> {
    let usage = concat!(
        "usage: SRC='host=foo user=bar password=baz dbname=quux' ",
        "DEST='host..' TABLE=table ./app [compute-initial|initial]"
    );

    let config = Config {
        src: env::var("SRC").with_context(|| usage)?,
        dest: env::var("DEST").with_context(|| usage)?,
        table: env::var("TABLE").with_context(|| usage)?,
        table_sample: env::var("TABLE_SAMPLE_FLOAT")
            .unwrap_or_else(|_| "0.01".to_string())
            .parse()?,
    };

    let max = fs::read_to_string(config.file("watermark"))
        .with_context(|| {
            anyhow!(
                "loading the watermark file written by the `compute-initial` command for {}",
                config.table
            )
        })?
        .parse()?;
    let mut ids: Vec<i64> = fs::read_to_string(config.file(format!("ids.{}", max)))?
        .split('\n')
        .map(|s| Ok(i64::from_str(s)?))
        .collect::<Result<_>>()?;

    let buckets = 4096;
    let every_n = ids.len() as f32 / buckets as f32;
    ids.sort();

    let middles: Vec<i64> = (1..buckets)
        .map(|bucket| ids[(bucket as f32 * every_n).round() as usize])
        .dedup()
        .collect_vec();

    let mut queries = Vec::new();
    queries.push((0, middles[0]));
    queries.extend(middles.iter().copied().tuple_windows::<(i64, i64)>());
    queries.push((middles[middles.len() - 1], max));

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

struct Initial {
    stats: Stats,
    max: i64,
    ids: Vec<i64>,
}

fn compute_initial(src: &mut Client, config: &InputConfig) -> Result<Initial> {
    let stats = crate::stats::stats(src, &config.table_name)?;

    let max = src
        .query_one(&format!("select max(id) from {}", config.table_name), &[])?
        .get::<_, i64>(0);

    let sample_rows_per_batch = 32.;
    let table_sample = 100. * (sample_rows_per_batch / (config.target_batch_rows as f32));

    info!(
        "found max ({}), est. count ({}), collecting sample at ({})",
        max, stats.est_rows, table_sample
    );

    let mut ids = src
        .query(
            &format!(
                "select id from {} tablesample system ({}) order by id",
                config.table_name, table_sample
            ),
            &[],
        )?
        .into_iter()
        .map(|row| row.get::<_, i64>(0))
        .collect_vec();
    info!("id samples: {}", ids.len());

    Ok(Initial { stats, max, ids })
}

fn write_file_in(path: impl AsRef<Path>, name: &str, val: impl Serialize) -> Result<()> {
    let mut dest = path.as_ref().to_path_buf();
    dest.push(name);
    let mut sponge = tempfile_fast::Sponge::new_for(dest)?;
    serde_json::to_writer(&mut sponge, &val)?;
    sponge.commit()?;
    Ok(())
}
