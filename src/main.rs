mod conn;
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

    let matches = clap::app_from_crate!()
        .arg(
            Arg::new("env")
                .short('e')
                .long("env")
                .takes_value(true)
                .help(".env file to load"),
        )
        .subcommand(
            App::new("export-stats").arg(
                Arg::new("table")
                    .short('t')
                    .long("table")
                    .takes_value(true)
                    .required(true),
            ),
        )
        .setting(clap::AppSettings::SubcommandRequiredElseHelp)
        .get_matches();

    if let Some(env) = matches.value_of("env") {
        load_env(env)?;
    }

    match matches.subcommand().expect("SubcommandRequiredElseHelp") {
        ("export-stats", args) => stats::export_stats(args)?,
        ("pg2pg", args) => pg2pg::cli(args)?,
        _ => unreachable!(),
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
