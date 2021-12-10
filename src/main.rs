use std::collections::HashMap;
use std::env;
use std::fmt::Display;
use std::fs;
use std::io::{BufRead, Read, Write};
use std::path;
use std::str::FromStr;
use std::sync::mpsc::{channel, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use anyhow::Result;
use anyhow::{anyhow, bail};
use itertools::Itertools;
use log::error;
use log::info;
use native_tls::TlsConnector;
use num_format::{Locale, ToFormattedString};
use postgres::Client;
use postgres_native_tls::MakeTlsConnector;
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

fn main() -> Result<()> {
    env_logger::init();

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

    let args = env::args().skip(1).collect_vec();
    match (args.get(0).map(|v| v.as_ref()), args.get(1)) {
        (Some("compute-initial"), None) => return compute_initial(&config),
        (Some("initial"), None) => (),
        _ => bail!(usage),
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

fn compute_initial(config: &Config) -> Result<()> {
    let mut src = pg(&config.src, "src")?;

    let max = src
        .query_one(&format!("select max(id) from {}", config.table), &[])?
        .get::<_, i64>(0);

    fs::create_dir_all("pg-copy-state")?;
    fs::write(config.file("watermark"), format!("{}\n", max))?;
    info!("found max ({}), collecting sample", max);

    let mut ids = src
        .query(
            &format!(
                "select id from {} tablesample system ({})",
                config.table, config.table_sample
            ),
            &[],
        )?
        .into_iter()
        .map(|row| row.get::<_, i64>(0))
        .collect_vec();
    info!("id samples: {}", ids.len());
    ids.sort();

    fs::write(
        config.file(format!("ids.{}", max)),
        ids.iter().map(|v| format!("{}\n", v)).join(""),
    )?;
    src.close()?;
    Ok(())
}

fn pg(db_connection_string: &str, logging_tag: &str) -> Result<Client> {
    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .build()?;
    let connector = MakeTlsConnector::new(connector);

    let mut client = Client::connect(&db_connection_string, connector)?;
    info!("{}: connected", logging_tag);

    client.execute("set time zone 'UTC'", &[])?;
    info!("{}: server responds", logging_tag);

    Ok(client)
}
