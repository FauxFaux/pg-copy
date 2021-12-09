use std::collections::HashMap;
use std::env;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::anyhow;
use anyhow::Context;
use anyhow::Result;
use itertools::Itertools;
use log::error;
use log::info;
use native_tls::TlsConnector;
use postgres::Client;
use postgres_native_tls::MakeTlsConnector;
use std_semaphore::Semaphore;

#[derive(Clone)]
struct Config {
    src: String,
    dest: String,
    table: String,
}

#[derive(Clone, Default, Debug)]
struct StatKeeper {
    observations: usize,
    bytes: u64,
    read_micros: u128,
    write_micros: u128,
    finished: bool,
}

fn main() -> Result<()> {
    env_logger::init();
    let usage =
        "usage: SRC='host=foo user=bar password=baz dbname=quux' DEST='host..' TABLE=table ./app";

    let config = Config {
        src: env::var("SRC").with_context(|| usage)?,
        dest: env::var("DEST").with_context(|| usage)?,
        table: env::var("TABLE").with_context(|| usage)?,
    };

    let mut src = pg(&config.src, "src")?;

    let max = src
        .query_one(&format!("select max(id) from {}", config.table), &[])?
        .get::<_, i64>(0);
    info!("max: {}", max);

    let mut ids = src
        .query(
            &format!("select id from {} tablesample system (0.01)", config.table),
            &[],
        )?
        .into_iter()
        .map(|row| row.get::<_, i64>(0))
        .collect_vec();
    info!("samples: {}", ids.len());

    drop(src);

    let buckets = 20;
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

    let mut threads = Vec::new();

    let semaphore = Arc::new(Semaphore::new(60));
    let mut stats = HashMap::new();

    work(
        config.clone(),
        Arc::new(Mutex::new(StatKeeper::default())),
        "id < 0".to_string(),
    )?;

    for query in queries {
        let config = config.clone();
        let semaphore = Arc::clone(&semaphore);
        let stat = Arc::new(Mutex::new(StatKeeper::default()));
        stats.insert(query.to_string(), Arc::clone(&stat));

        threads.push((
            query.to_string(),
            std::thread::spawn(move || -> Result<()> {
                let _guard = semaphore.access();
                work(config, stat, query)?;
                Ok(())
            }),
        ));
    }

    'outer: loop {
        let mut finished = 0;
        for (query, stat) in stats.iter() {
            let stat = match stat.lock() {
                Ok(stat) => stat,
                Err(_) => break 'outer,
            };

            if stat.finished {
                finished += 1;
            }

            println!("{} {:?}", query, stat);
        }

        if finished == stats.len() {
            break;
        }

        std::thread::sleep(Duration::from_secs(2));
    }

    let mut errors = Vec::new();
    for (name, thread) in threads {
        if let Err(e) = thread
            .join()
            .expect(&format!("thread panicked: '{}'", name))
            .with_context(|| anyhow!("joining thread '{}'", name))
        {
            error!("{}: thread failed: {:?}", name, e);
            errors.push((name, e));
        }
    }

    for (name, error) in &errors {
        error!("{}: thread failed, so failing build: {:?}", name, error);
    }

    if let Some((_, error)) = errors.into_iter().next() {
        Err(error)?;
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

        stats.observations += 1;

        // approximately every 2MB
        if stats.observations % 256 == 0 {
            let mut shared_stats = shared_stats
                .lock()
                .expect("the main thread would have panicked");
            *shared_stats = stats.clone();
        }
    }

    drop(reader);
    src.close()?;

    writer.flush()?;
    let written = writer.finish()?;

    shared_stats
        .lock()
        .expect("the main thread would have panicked")
        .finished = true;

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
