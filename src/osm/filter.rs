use super::BlobReader;
use crate::coverage::{Coverage, is_wikidata_key, parse_wikidata_ids};
use crate::{u64_table, u64_table::U64Table};
use anyhow::{Ok, Result};
use indicatif::MultiProgress;
use osm_pbf_iter::{Blob, Primitive, PrimitiveBlock, RelationMemberType};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::{File, remove_file, rename};
use std::io::{BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::sync_channel;
use std::thread;

struct Node {
    _id: u64,
}

#[derive(Deserialize, Serialize)]
struct Way {
    id: u64,
    nodes: Vec<u64>,
    tags: Vec<String>,
}

#[derive(Deserialize, Serialize)]
struct Relation {
    id: u64,
    tags: Vec<String>,
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
    let out_path = workdir.join("osm-filtered-relations");
    if out_path.exists() {
        return Ok(out_path);
    }

    let filtered_rels_data_path = workdir.join("osm-filtered-relations.data.tmp");
    let filtered_rels_offsets_path = workdir.join("osm-filtered-relations.offsets.tmp");
    let node_refs_path = workdir.join("osm-filtered-relations.node-refs.tmp");
    let way_refs_path = workdir.join("osm-filtered-relations.way-refs.tmp");

    let num_blobs = (blobs.1 - blobs.0) as u64;
    let mut num_relations = 0;
    let mut num_node_refs = 0;
    let mut num_way_refs = 0;
    let progress_bar =
        super::make_progress_bar(progress, "osm.filter.r", num_blobs, "blobs → relations");
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
                        let tags: Vec<String> = rel
                            .tags()
                            .flat_map(|(k, v)| [k.to_string(), v.to_string()])
                            .collect();
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
                        rel_tx.send(Relation { id: rel.id, tags })?;
                    }
                }
                Ok(())
            })
        });

        let rel_writer = s.spawn(|| {
            let mut serializer = rmp_serde::Serializer::new(Vec::<u8>::with_capacity(32768));
            let mut data_writer = BufWriter::new(File::create(&filtered_rels_data_path)?);
            let mut offsets_writer = BufWriter::new(File::create(&filtered_rels_offsets_path)?);
            let mut cur_offset = 0_u64;
            for rel in rel_rx {
                serializer.get_mut().clear();
                rel.serialize(&mut serializer)?;
                let buf = serializer.get_ref();
                data_writer.write_all(buf)?;
                offsets_writer.write_all(&cur_offset.to_le_bytes())?;
                cur_offset += buf.len() as u64;
                num_relations += 1;
            }
            data_writer.into_inner()?.sync_all()?;
            offsets_writer.into_inner()?.sync_all()?;
            Ok(())
        });

        let node_ref_writer = s.spawn(|| {
            num_node_refs = u64_table::create(node_ref_rx, workdir, &node_refs_path)?;
            Ok(())
        });

        let way_ref_writer = s.spawn(|| {
            num_way_refs = u64_table::create(way_ref_rx, workdir, &way_refs_path)?;
            Ok(())
        });

        producer
            .join()
            .expect("panic in filter_relations producer")
            .and(handler.join().expect("panic in filter_relations handler"))
            .and(
                rel_writer
                    .join()
                    .expect("panic in filter_relations rel_writer"),
            )
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

    // Assemble out output file "osm-filtered-relations" and clean up temporary intermediates.
    let mut tmp_out = PathBuf::from(&out_path);
    tmp_out.add_extension("tmp");
    filtered_file::write(
        None, // no nodes
        None, // no ways
        Some((
            &filtered_rels_data_path,
            &filtered_rels_offsets_path,
            &node_refs_path,
            &way_refs_path,
        )),
        &tmp_out,
    )?;
    remove_file(&filtered_rels_data_path)?;
    remove_file(&filtered_rels_offsets_path)?;
    remove_file(&node_refs_path)?;
    remove_file(&way_refs_path)?;
    rename(&tmp_out, &out_path)?;

    progress_bar.finish_with_message(format!(
        "blobs → {} relations referring to {} nodes and {} ways",
        num_relations, num_node_refs, num_way_refs
    ));

    Ok(out_path)
}

pub fn filter_ways<R: Read + Seek + Send>(
    reader: &mut BlobReader<R>,
    blobs: (usize, usize),
    coverage: &Coverage,
    covered_ways: &U64Table,
    progress: &MultiProgress,
    workdir: &Path,
) -> Result<PathBuf> {
    let out_path = workdir.join("osm-filtered-ways");
    if out_path.exists() {
        return Ok(out_path);
    }

    let filtered_ways_data_path = workdir.join("osm-filtered-ways.data.tmp");
    let filtered_ways_offsets_path = workdir.join("osm-filtered-ways.offsets.tmp");
    let node_refs_path = workdir.join("osm-filtered-ways.node-refs.tmp");

    let num_blobs = (blobs.1 - blobs.0) as u64;
    let mut num_ways = 0;
    let mut num_node_refs = 0;
    let progress_bar =
        super::make_progress_bar(progress, "osm.filter.w", num_blobs, "blobs → ways");
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
                        let tags: Vec<String> = way
                            .tags()
                            .flat_map(|(k, v)| [k.to_string(), v.to_string()])
                            .collect();
                        for n in nodes.iter() {
                            node_ref_tx.send(*n)?;
                        }
                        way_tx.send(Way {
                            id: way.id,
                            nodes,
                            tags,
                        })?;
                    }
                }
                Ok(())
            })
        });

        let way_writer = s.spawn(|| {
            let mut serializer = rmp_serde::Serializer::new(Vec::<u8>::with_capacity(32768));
            let mut data_writer = BufWriter::new(File::create(&filtered_ways_data_path)?);
            let mut offsets_writer = BufWriter::new(File::create(&filtered_ways_offsets_path)?);
            let mut cur_offset = 0_u64;
            for way in way_rx {
                serializer.get_mut().clear();
                way.serialize(&mut serializer)?;
                let buf = serializer.get_ref();
                data_writer.write_all(buf)?;
                offsets_writer.write_all(&cur_offset.to_le_bytes())?;
                cur_offset += buf.len() as u64;
                num_ways += 1;
            }
            data_writer.into_inner()?.sync_all()?;
            offsets_writer.into_inner()?.sync_all()?;
            Ok(())
        });

        let node_ref_writer = s.spawn(|| {
            num_node_refs = u64_table::create(node_ref_rx, workdir, &node_refs_path)?;
            Ok(())
        });

        producer
            .join()
            .expect("panic in filter_ways producer")
            .and(handler.join().expect("panic in filter_ways handler"))
            .and(way_writer.join().expect("panic in filter_ways way_writer"))
            .and(
                node_ref_writer
                    .join()
                    .expect("panic in filter_ways node_resf_writer"),
            )
    })?;

    // Assemble our output file "osm-filtered-ways" and clean up temporary intermediates.
    // For clean checkpointing, we first build "osm-filtered-ways.tmp" and then rename the file.
    // Other than writing/assembling the file piece by piece, renaming is an atomic operation.
    let mut tmp_out = PathBuf::from(&out_path);
    tmp_out.add_extension("tmp");
    filtered_file::write(
        None, // no nodes
        Some((
            &filtered_ways_data_path,
            &filtered_ways_offsets_path,
            &node_refs_path,
        )),
        None, // no relations
        &tmp_out,
    )?;
    remove_file(&filtered_ways_data_path)?;
    remove_file(&filtered_ways_offsets_path)?;
    remove_file(&node_refs_path)?;
    rename(&tmp_out, &out_path)?;

    progress_bar.finish_with_message(format!(
        "blobs → {} ways referring to {} nodes",
        num_ways, num_node_refs
    ));

    Ok(out_path)
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
    let progress_bar =
        super::make_progress_bar(progress, "osm.filter.n", num_blobs, "blobs → nodes");
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

mod filtered_file {
    use anyhow::{Ok, Result};
    use std::fs::File;
    use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
    use std::path::Path;

    const NUM_HEADERS: usize = 9;
    const BUFFER_SIZE: usize = 256 * 1024;

    pub fn write(
        nodes: Option<(&Path, &Path)>,
        ways: Option<(&Path, &Path, &Path)>,
        rels: Option<(&Path, &Path, &Path, &Path)>,
        out: &Path,
    ) -> Result<()> {
        let mut writer = BufWriter::with_capacity(BUFFER_SIZE, File::create(out)?);

        let (node_data, node_offsets) = if let Some((data, offsets)) = nodes {
            (Some(data), Some(offsets))
        } else {
            (None, None)
        };
        let (way_data, way_offsets, way_node_refs) = if let Some((data, offsets, node_refs)) = ways
        {
            (Some(data), Some(offsets), Some(node_refs))
        } else {
            (None, None, None)
        };
        let (rel_data, rel_offsets, rel_node_refs, rel_way_refs) =
            if let Some((data, offsets, node_refs, way_refs)) = rels {
                (Some(data), Some(offsets), Some(node_refs), Some(way_refs))
            } else {
                (None, None, None, None)
            };

        // Reserve space for file header.
        writer.write_all(b"diffed-places filtered\0\0")?;
        writer.write_all(&(NUM_HEADERS as u64).to_le_bytes())?;
        writer.write_all(&[0; 24 * NUM_HEADERS])?; // leave space for headers

        // Write data.
        let (node_data_start, node_data_len) =
            write_data(node_data, /* align */ 1, &mut writer)?;
        let (way_data_start, way_data_len) = write_data(way_data, /* align */ 1, &mut writer)?;
        let (rel_data_start, rel_data_len) = write_data(rel_data, /* align */ 1, &mut writer)?;

        // Write offsets into data.
        let (node_offsets_start, node_offsets_len) =
            write_data(node_offsets, /* align */ 8, &mut writer)?;
        let (way_offsets_start, way_offsets_len) =
            write_data(way_offsets, /* align */ 8, &mut writer)?;
        let (rel_offsets_start, rel_offsets_len) =
            write_data(rel_offsets, /* align */ 8, &mut writer)?;

        // Write a table for telling which nodes are referenced by ways.
        // The table is keyed by OpenStreetMap node ID, so it needs to be aligned for access as &[u64].
        let (way_node_refs_start, way_node_refs_len) =
            write_data(way_node_refs, /* align */ 8, &mut writer)?;

        // Write tables for telling which nodes and ways are referenced by relations.
        let (rel_node_refs_start, rel_node_refs_len) =
            write_data(rel_node_refs, /* align */ 8, &mut writer)?;
        let (rel_way_refs_start, rel_way_refs_len) =
            write_data(rel_way_refs, /* align */ 8, &mut writer)?;

        // Write file header.
        write_headers(
            &[
                ("nod_data", node_data_start, node_data_len),
                ("nod_offs", node_offsets_start, node_offsets_len),
                ("way_data", way_data_start, way_data_len),
                ("way_offs", way_offsets_start, way_offsets_len),
                ("way_rf_n", way_node_refs_start, way_node_refs_len),
                ("rel_data", rel_data_start, rel_data_len),
                ("rel_offs", rel_offsets_start, rel_offsets_len),
                ("rel_rf_n", rel_node_refs_start, rel_node_refs_len),
                ("rel_rf_w", rel_way_refs_start, rel_way_refs_len),
            ],
            &mut writer,
        )?;

        writer.into_inner()?.sync_all()?;
        Ok(())
    }

    fn append_file<W: Write + Seek>(file: &Path, out: &mut W) -> Result<u64> {
        let mut reader = BufReader::with_capacity(BUFFER_SIZE, File::open(file)?);
        std::io::copy(&mut reader, out)?;
        Ok(reader.stream_position()?)
    }

    fn write_padding<W: Write + Seek>(alignment: usize, out: &mut W) -> Result<()> {
        if alignment == 1 {
            return Ok(());
        }

        let pos = out.stream_position()?;
        let alignment = alignment as u64;
        let num_bytes = ((alignment - (pos % alignment)) % alignment) as usize;
        if num_bytes > 0 {
            let padding = vec![0; num_bytes];
            out.write_all(&padding)?;
        }

        Ok(())
    }

    fn write_headers<W: Write + Seek>(headers: &[(&str, u64, u64)], out: &mut W) -> Result<()> {
        out.seek(SeekFrom::Start(32))?;
        assert_eq!(headers.len(), NUM_HEADERS);
        for (id, pos, len) in headers {
            assert_eq!(
                id.len(),
                8,
                "header id must be 8 chars long but \"{:?}\" is not",
                id
            );
            out.write_all(id.as_bytes())?;
            out.write_all(&pos.to_le_bytes())?;
            out.write_all(&len.to_le_bytes())?;
        }
        out.flush()?;
        Ok(())
    }

    fn write_data<W: Write + Seek>(
        data: Option<&Path>,
        alignment: usize,
        out: &mut W,
    ) -> Result<(u64, u64)> {
        if let Some(path) = data {
            write_padding(alignment, out)?;
            let start = out.stream_position()?;
            let len = append_file(path, out)?;
            Ok((start, len))
        } else {
            Ok((0, 0))
        }
    }
}
