use std::collections::HashMap;

use anyhow::Result;
use clap::ArgMatches;

struct Stats {
    id_max: String,
    id_values: Vec<String>,
    table_sample: f32,

    cols: HashMap<String, ColInfo>,
}

struct ColInfo {
    avg_width: i64,
    n_distinct: i64,

    most_common_vals: Vec<String>,
}

pub(crate) fn export_stats(args: &ArgMatches) -> Result<()> {
    unimplemented!()
}
