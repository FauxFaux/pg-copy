mod par_dump;
mod unbuffered;

use std::env;
use std::fs;
use std::path::Path;

use crate::conn::{conn_string_from_env, pg};
use crate::stats::Stats;
use anyhow::anyhow;
use anyhow::Context;
use anyhow::Result;
use clap::ArgMatches;
use itertools::Itertools;
use log::info;
use postgres::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub fn cli(args: &ArgMatches) -> Result<()> {
    match args.subcommand().expect("subcommand required") {
        ("prepare", args) => prepare(args),
        ("batched", args) => unbuffered::cli(args),
        ("par-dump", args) => par_dump::cli(args),
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
pub struct ProjectStatus {
    id: i64,
    state: ProjectState,
    table_name: String,
}

struct InputConfig {
    table_name: String,
    target_batch_rows: u64,
}

// min threads? max threads
// open transaction to grab snapshot

fn prepare(args: &ArgMatches) -> Result<()> {
    let src = conn_string_from_env("PG_SRC")?;
    let table_name = args.value_of("table").expect("required option");

    let mut client = pg(&src, "src")?;

    let dest = args
        .value_of("project")
        .expect("required option")
        .to_string();
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

    let ids = src
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

pub fn read_state_file<T: DeserializeOwned>(path: impl AsRef<Path>, name: &str) -> Result<T> {
    let mut path = path.as_ref().to_path_buf();
    path.push(name);
    Ok(serde_json::from_reader(
        fs::File::open(&path)
            .with_context(|| anyhow!("opening {:?} in {:?}", path, env::current_dir()))?,
    )
    .with_context(|| anyhow!("parsing {:?} in {:?}", path, env::current_dir()))?)
}
