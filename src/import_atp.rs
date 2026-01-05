use anyhow::{Context, Ok, Result};
use memmap2::Mmap;
use piz::ZipArchive;
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;

pub fn import_atp(input: &PathBuf) -> Result<()> {
    let file = File::open(input).with_context(|| format!("could not open file `{:?}`", input))?;
    let mapping = unsafe { Mmap::map(&file).context("Couldn't mmap zip file")? };
    let archive = ZipArchive::with_prepended_data(&mapping)
        .context("Couldn't load archive")?
        .0;
    archive.entries().par_iter().try_for_each(|entry| {
        if entry.size > 0 {
            let reader = archive.read(entry)?;
            process_geojson(reader)?;
        }
        Ok(())
    })?;
    Ok(())
}

fn process_geojson<T: Read>(reader: T) -> Result<()> {
    let buffer = BufReader::new(reader);
    let mut spider_attrs = serde_json::from_str("{}")?;
    for line in buffer.lines() {
        let line = line?;
        if line.starts_with("{\"type\":\"FeatureCollection\"") {
            let mut json = String::from(line);
            json.push_str("]}");
            let val: serde_json::Value = serde_json::from_str(&json)?;
            if let Some(attrs) = val.get("dataset_attributes") {
                spider_attrs = attrs.clone();
            }
            continue;
        }

        if line == "]}" {
            continue;
        }

        let trimmed = if let Some((a, _)) = line.split_at_checked(line.len() - 1) {
            a
        } else {
            &line
        };
        println!("** {:?}", trimmed);
    }
    println!("{:?}", spider_attrs);
    Ok(())
}
