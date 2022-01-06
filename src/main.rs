mod conn;
mod packit;
mod pg2pg;
mod stats;

use std::env;

use anyhow::{anyhow, Context, Result};
use clap::{App, Arg};
use log::info;

fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let table = Arg::new("table")
        .short('t')
        .long("table")
        .takes_value(true)
        .required(true);

    let project = Arg::new("project")
        .short('p')
        .long("project")
        .takes_value(true)
        .required(true);

    let matches = clap::app_from_crate!()
        .arg(
            Arg::new("env")
                .short('e')
                .long("env")
                .takes_value(true)
                .help(".env file to load"),
        )
        .subcommand(App::new("export-stats").arg(table.clone()))
        .subcommand(
            App::new("packit")
                .arg(Arg::new("FILES").multiple_values(true).required(true))
                .arg(project.clone()),
        )
        .subcommand(
            App::new("pg2pg")
                .subcommand(
                    App::new("prepare")
                        .arg(table.clone())
                        .arg(project.clone())
                        .arg(
                            Arg::new("batch_rows")
                                .long("batch-rows")
                                .default_value("1000000"),
                        ),
                )
                .subcommand(App::new("batched").arg(project.clone()))
                .subcommand(App::new("par-dump").arg(project.clone())),
        )
        .setting(clap::AppSettings::SubcommandRequiredElseHelp)
        .get_matches();

    if let Some(env) = matches.value_of("env") {
        load_env(env)?;
    }

    match matches.subcommand().expect("SubcommandRequiredElseHelp") {
        ("export-stats", args) => stats::export_stats(args)?,
        ("packit", args) => packit::cli(args)?,
        ("pg2pg", args) => pg2pg::cli(args)?,
        _ => unreachable!("uncovered subcommand"),
    }

    Ok(())
}

fn load_env(env: impl ToString) -> Result<()> {
    let mut env = env.to_string();
    if !env.ends_with(".env") {
        env.push_str(".env");
    }
    info!(
        "loaded env from {:?}",
        dotenv::from_filename(&env).with_context(|| anyhow!(
            "loading {:?} from {:?}",
            env,
            env::current_dir()
        ))?
    );

    Ok(())
}
