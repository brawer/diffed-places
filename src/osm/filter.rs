use super::BlobReader;
use crate::coverage::{Coverage, is_wikidata_key, parse_wikidata_ids};
use crate::{u64_table, u64_table::U64Table};
use anyhow::{Ok, Result};
use indicatif::MultiProgress;
use osm_pbf_iter::{Blob, Primitive, PrimitiveBlock, RelationMemberType};
use rayon::prelude::*;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::mpsc::sync_channel;
use std::thread;

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

// TODO: Handle recursive relations.
// https://github.com/diffed-places/pipeline/issues/141
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

    let node_refs = workdir.join("node-refs-in-relations");
    let way_refs = workdir.join("way-refs-in-relations");
    let num_blobs = (blobs.1 - blobs.0) as u64;
    let mut num_results = 0;
    let mut num_node_refs = 0;
    let mut num_way_refs = 0;
    let progress_bar = super::make_progress_bar(progress, "osm.filter.r", num_blobs, "blobs");
    thread::scope(|s| {
        let num_workers = usize::from(thread::available_parallelism()?);
        let (blob_tx, blob_rx) = sync_channel::<Blob>(num_workers);
        let (rel_tx, rel_rx) = sync_channel::<Relation>(1024);
        let (node_ref_tx, node_ref_rx) = sync_channel::<u64>(8192);
        let (way_ref_tx, way_ref_rx) = sync_channel::<u64>(8192);
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
                        for (_name, member_id, member_type) in rel.members() {
                            match member_type {
                                RelationMemberType::Node => {
                                    node_ref_tx.send(member_id)?;
                                }
                                RelationMemberType::Way => {
                                    way_ref_tx.send(member_id)?;
                                }
                                _ => {}
                            }
                        }
                        rel_tx.send(Relation { _id: rel.id })?;
                    }
                }
                Ok(())
            })
        });

        let node_ref_writer = s.spawn(|| {
            num_node_refs = u64_table::create(node_ref_rx, workdir, &node_refs)?;
            Ok(())
        });

        let way_ref_writer = s.spawn(|| {
            num_way_refs = u64_table::create(way_ref_rx, workdir, &way_refs)?;
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
            .and(
                node_ref_writer
                    .join()
                    .expect("panic in filter_ways node_ref_writer"),
            )
            .and(
                way_ref_writer
                    .join()
                    .expect("panic in filter_ways way_ref_writer"),
            )
    })?;
    progress_bar.finish_with_message(format!(
        "blobs → {} relations with {} nodes and {} ways",
        num_results, num_node_refs, num_way_refs
    ));

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

    let node_refs = workdir.join("node-refs-in-ways");
    let num_blobs = (blobs.1 - blobs.0) as u64;
    let mut num_results = 0;
    let mut num_node_refs = 0;
    let progress_bar = super::make_progress_bar(progress, "osm.filter.w", num_blobs, "blobs");
    thread::scope(|s| {
        let num_workers = usize::from(thread::available_parallelism()?);
        let (blob_tx, blob_rx) = sync_channel::<Blob>(num_workers);
        let (way_tx, way_rx) = sync_channel::<Way>(4096);
        let (node_ref_tx, node_ref_rx) = sync_channel::<u64>(8192);
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
                        let nodes: Vec<u64> =
                            way.refs().filter(|&x| x > 0).map(|x| x as u64).collect();
                        for n in nodes.iter() {
                            node_ref_tx.send(*n)?;
                        }
                        way_tx.send(Way {
                            _id: way.id,
                            _nodes: nodes,
                        })?;
                    }
                }
                Ok(())
            })
        });

        let node_ref_writer = s.spawn(|| {
            num_node_refs = u64_table::create(node_ref_rx, workdir, &node_refs)?;
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
            .and(
                node_ref_writer
                    .join()
                    .expect("panic in filter_ways node_resf_writer"),
            )
    })?;
    progress_bar.finish_with_message(format!(
        "blobs → {} ways with {} nodes",
        num_results, num_node_refs
    ));

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
    let progress_bar = super::make_progress_bar(progress, "osm.filter.n", num_blobs, "blobs");
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
            })
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
