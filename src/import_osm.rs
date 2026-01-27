use anyhow::{Context, Ok, Result};
use std::fs::File;
use std::path::Path;

use crate::coverage::Coverage;

pub fn import_osm(pbf: &Path, coverage: &Path, _output: &Path) -> Result<()> {
    let _pbf = File::open(pbf).with_context(|| format!("could not open file `{:?}`", pbf))?;
    let _coverage = Coverage::load(coverage)
        .with_context(|| format!("could not open coverage file `{:?}`", coverage))?;
    Ok(())
}
