use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand};

mod coverage;
mod import_atp;
mod import_osm;
mod place;

use crate::coverage::build_coverage;
use crate::{import_atp::import_atp, import_osm::import_osm};

#[derive(Parser)]
#[command(name = "diffed-places")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    ImportAtp {
        #[arg(short, long, value_name = "alltheplaces.zip")]
        input: PathBuf,

        #[arg(short, long, value_name = "alltheplaces.parquet")]
        output: PathBuf,
    },
    BuildCoverage {
        #[arg(short, long, value_name = "alltheplaces.parquet")]
        places: PathBuf,

        #[arg(short, long, value_name = "coverage")]
        output: PathBuf,
    },
    ImportOsm {
        #[arg(long, value_name = "openstreetmap.pbf")]
        osm: PathBuf,

        #[arg(long, value_name = "coverage")]
        coverage: PathBuf,

        #[arg(short, long, value_name = "openstreetmap.parquet")]
        output: PathBuf,
    },
}

fn main() -> Result<()> {
    let args = Cli::parse();
    match &args.command {
        Some(Commands::ImportAtp { input, output }) => import_atp(input, output),
        Some(Commands::BuildCoverage { places, output }) => build_coverage(places, output),
        Some(Commands::ImportOsm {
            osm,
            coverage,
            output,
        }) => import_osm(osm, coverage, output),
        None => Err(anyhow!("no subcommand given")),
    }
}
