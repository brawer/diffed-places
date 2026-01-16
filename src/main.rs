use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

mod coverage;
mod import_atp;
mod place;

use crate::coverage::build_coverage;
use crate::import_atp::import_atp;

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

        #[arg(short, long, value_name = "spatial-coverage")]
        out_spatial_coverage: PathBuf,
    },
}

fn main() -> Result<()> {
    let args = Cli::parse();
    match &args.command {
        Some(Commands::ImportAtp { input, output }) => {
            import_atp(input, output)?;
        }
        Some(Commands::BuildCoverage {
            places,
            out_spatial_coverage,
        }) => {
            build_coverage(places, out_spatial_coverage)?;
        }
        None => {
            eprintln!("no subcommand given");
        }
    }
    Ok(())
}
