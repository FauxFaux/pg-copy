use anyhow::{anyhow, Context, Result};
use log::{debug, info};
use native_tls::TlsConnector;
use postgres::Client;
use postgres_native_tls::MakeTlsConnector;
use std::env;

pub fn pg(db_connection_string: &str, logging_tag: &str) -> Result<Client> {
    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .build()?;
    let connector = MakeTlsConnector::new(connector);

    let mut client = Client::connect(&db_connection_string, connector)?;
    info!("{}: connected", logging_tag);

    client.execute("set time zone 'UTC'", &[])?;
    debug!("{}: server responds", logging_tag);

    Ok(client)
}

pub fn conn_string_from_env(var: &str) -> Result<String> {
    env::var(var).with_context(|| anyhow!("reading env var: {:?}", var))
}
