use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand};
use std::fs::create_dir;

use diffed_places::{build_coverage, import_atp, import_osm};

#[derive(Parser)]
#[command(name = "diffed-places")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Run {
        #[arg(short, long, value_name = "alltheplaces.zip")]
        atp: PathBuf,

        #[arg(long, value_name = "openstreetmap.pbf")]
        osm: PathBuf,

        #[arg(short, long, value_name = "workdir")]
        workdir: PathBuf,
    },
}

fn main() -> Result<()> {
    let args = Cli::parse();
    env_logger::init();
    match &args.command {
        Some(Commands::Run { atp, osm, workdir }) => {
            if !workdir.exists() {
                create_dir(workdir)?;
            }
            let atp = import_atp(atp, workdir)?;
            let coverage = build_coverage(&atp, workdir)?;
            import_osm(osm, &coverage, workdir)?;
            Ok(())
        }
        None => Err(anyhow!("no subcommand given")),
    }
}
