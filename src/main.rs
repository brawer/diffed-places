use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

mod import_atp;
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
        #[arg(short, long, value_name = "FILE.zip")]
        input: PathBuf,
    },
}

fn main() -> Result<()> {
    let args = Cli::parse();
    match &args.command {
        Some(Commands::ImportAtp { input }) => {
            import_atp(input)?;
        }
        None => {
            eprintln!("no subcommand given");
        }
    }
    Ok(())
}
