use super::BlobReader;
use anyhow::{Ok, Result};
use indicatif::MultiProgress;
use osm_pbf_iter::{Blob, Primitive, PrimitiveBlock};
use rayon::prelude::*;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::mpsc::sync_channel;
use std::thread;

use crate::coverage::{Coverage, is_wikidata_key, parse_wikidata_ids};
use crate::u64_table::U64Table;

struct Node {
    _id: u64,
}

struct Way {
    _id: u64,
    _nodes: Vec<u64>,
}

struct Relation {
    _id: u64,
}

pub fn filter_relations<R: Read + Seek + Send>(
    reader: &mut BlobReader<R>,
    blobs: (usize, usize),
    coverage: &Coverage,
    covered_relations: &U64Table,
    progress: &MultiProgress,
    workdir: &Path,
) -> Result<PathBuf> {
    let out = workdir.join("osm-filtered-relations");
    if out.exists() {
        return Ok(out);
    }

    let num_blobs = (blobs.1 - blobs.0) as u64;
    let mut num_results = 0;
    let progress_bar = super::make_progress_bar(progress, "osm.flt.r", num_blobs, "blobs");
    thread::scope(|s| {
        let num_workers = usize::from(thread::available_parallelism()?);
        let (blob_tx, blob_rx) = sync_channel::<Blob>(num_workers);
        let (rel_tx, rel_rx) = sync_channel::<Relation>(1024);
        let producer = s.spawn(|| super::read_blobs(reader, blobs, &progress_bar, blob_tx));
        let handler = s.spawn(move || {
            blob_rx.into_iter().par_bridge().try_for_each(|blob| {
                let rel_tx = rel_tx.clone();
                let data = blob.into_data(); // decompress
                let block = PrimitiveBlock::parse(&data);
                for primitive in block.primitives() {
                    if let Primitive::Relation(rel) = primitive
                        && filter(rel.id, rel.tags(), covered_relations, coverage)
                    {
                        rel_tx.send(Relation { _id: rel.id })?;
                    }
                }
                Ok(())
            })?;
            Ok(())
        });
        let consumer = s.spawn(|| {
            for _rel in rel_rx {
                num_results += 1;
            }
            Ok(())
        });
        producer
            .join()
            .expect("panic in filter_relations producer")
            .and(handler.join().expect("panic in filter_relations handler"))
            .and(consumer.join().expect("panic in filter_relations consumer"))
    })?;
    progress_bar.finish_with_message(format!("blobs → {} relations", num_results));

    Ok(out)
}

pub fn filter_ways<R: Read + Seek + Send>(
    reader: &mut BlobReader<R>,
    blobs: (usize, usize),
    coverage: &Coverage,
    covered_ways: &U64Table,
    progress: &MultiProgress,
    workdir: &Path,
) -> Result<PathBuf> {
    let out = workdir.join("osm-filtered-ways");
    if out.exists() {
        return Ok(out);
    }

    let num_blobs = (blobs.1 - blobs.0) as u64;
    let mut num_results = 0;
    let progress_bar = super::make_progress_bar(progress, "osm.flt.w", num_blobs, "blobs");
    thread::scope(|s| {
        let num_workers = usize::from(thread::available_parallelism()?);
        let (blob_tx, blob_rx) = sync_channel::<Blob>(num_workers);
        let (way_tx, way_rx) = sync_channel::<Way>(1024);
        let producer = s.spawn(|| super::read_blobs(reader, blobs, &progress_bar, blob_tx));
        let handler = s.spawn(move || {
            blob_rx.into_iter().par_bridge().try_for_each(|blob| {
                let way_tx = way_tx.clone();
                let data = blob.into_data(); // decompress
                let block = PrimitiveBlock::parse(&data);
                for primitive in block.primitives() {
                    if let Primitive::Way(way) = primitive
                        && filter(way.id, way.tags(), covered_ways, coverage)
                    {
                        let nodes = way.refs().filter(|&x| x > 0).map(|x| x as u64).collect();
                        way_tx.send(Way {
                            _id: way.id,
                            _nodes: nodes,
                        })?;
                    }
                }
                Ok(())
            })?;
            Ok(())
        });
        let consumer = s.spawn(|| {
            for _way in way_rx {
                num_results += 1;
            }
            Ok(())
        });
        producer
            .join()
            .expect("panic in filter_ways producer")
            .and(handler.join().expect("panic in filter_ways handler"))
            .and(consumer.join().expect("panic in filter_ways consumer"))
    })?;
    progress_bar.finish_with_message(format!("blobs → {} ways", num_results));

    Ok(out)
}

pub fn filter_nodes<R: Read + Seek + Send>(
    reader: &mut BlobReader<R>,
    blobs: (usize, usize),
    coverage: &Coverage,
    covered_nodes: &U64Table,
    progress: &MultiProgress,
    workdir: &Path,
) -> Result<PathBuf> {
    let out = workdir.join("osm-filtered-nodes");
    if out.exists() {
        return Ok(out);
    }

    let num_blobs = (blobs.1 - blobs.0) as u64;
    let mut num_results = 0;
    let progress_bar = super::make_progress_bar(progress, "osm.flt.n", num_blobs, "blobs");
    thread::scope(|s| {
        let num_workers = usize::from(thread::available_parallelism()?);
        let (blob_tx, blob_rx) = sync_channel::<Blob>(num_workers);
        let (node_tx, node_rx) = sync_channel::<Node>(1024);
        let producer = s.spawn(|| super::read_blobs(reader, blobs, &progress_bar, blob_tx));
        let handler = s.spawn(move || {
            blob_rx.into_iter().par_bridge().try_for_each(|blob| {
                let node_tx = node_tx.clone();
                let data = blob.into_data(); // decompress
                let block = PrimitiveBlock::parse(&data);
                for primitive in block.primitives() {
                    if let Primitive::Node(node) = primitive
                        && filter(node.id, node.tags.iter().copied(), covered_nodes, coverage)
                    {
                        node_tx.send(Node { _id: node.id })?;
                    }
                }
                Ok(())
            })?;
            Ok(())
        });
        let consumer = s.spawn(|| {
            for _node in node_rx {
                num_results += 1;
            }
            Ok(())
        });
        producer
            .join()
            .expect("panic in filter_nodes producer")
            .and(handler.join().expect("panic in filter_nodes handler"))
            .and(consumer.join().expect("panic in filter_nodes consumer"))
    })?;
    progress_bar.finish_with_message(format!("blobs → {} nodes", num_results));

    Ok(out)
}

fn filter<'a, I>(id: u64, tags: I, covered_ids: &U64Table, coverage: &Coverage) -> bool
where
    I: Iterator<Item = (&'a str, &'a str)>,
{
    let mut has_any_tags = false;
    for (key, value) in tags {
        has_any_tags = true;
        if is_wikidata_key(key) {
            for id in parse_wikidata_ids(value) {
                if coverage.contains_wikidata_item(id) {
                    return true;
                }
            }
        }
    }

    if covered_ids.contains(id) {
        return has_any_tags;
    }

    false
}
