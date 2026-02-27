use super::{BlobReader, coords::Coords};
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

use filtered_file::FilteredFile;

#[derive(Deserialize, Serialize)]
struct Node {
    id: u64,
    tags: Vec<String>,
    lon_e7: i32,
    lat_e7: i32,
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
pub fn filter_relations<'a, R: Read + Seek + Send>(
    reader: &mut BlobReader<R>,
    blobs: (usize, usize),
    coverage: &Coverage,
    covered_relations: &U64Table,
    progress: &MultiProgress,
    workdir: &Path,
) -> Result<FilteredFile<'a>> {
    let out_path = workdir.join("osm-filtered-relations");
    if out_path.exists() {
        return Ok(FilteredFile::open(&out_path)?);
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

    let mut writer = filtered_file::Writer::create(&tmp_out)?;
    writer.write_features(&filtered_rels_data_path, &filtered_rels_offsets_path)?;
    writer.write_node_refs(&node_refs_path)?;
    writer.write_way_refs(&way_refs_path)?;
    writer.close()?;

    remove_file(&filtered_rels_data_path)?;
    remove_file(&filtered_rels_offsets_path)?;
    remove_file(&node_refs_path)?;
    remove_file(&way_refs_path)?;
    rename(&tmp_out, &out_path)?;

    progress_bar.finish_with_message(format!(
        "blobs → {} relations referring to {} nodes and {} ways",
        num_relations, num_node_refs, num_way_refs
    ));

    Ok(FilteredFile::open(&out_path)?)
}

pub fn filter_ways<'a, R: Read + Seek + Send>(
    reader: &mut BlobReader<R>,
    blobs: (usize, usize),
    coverage: &Coverage,
    covered_ways: &U64Table,
    filtered_relations: &FilteredFile,
    progress: &MultiProgress,
    workdir: &Path,
) -> Result<FilteredFile<'a>> {
    let out_path = workdir.join("osm-filtered-ways");
    if out_path.exists() {
        return Ok(FilteredFile::open(&out_path)?);
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
                    if let Primitive::Way(way) = primitive {
                        if filter(way.id, way.tags(), covered_ways, coverage) {
                            let nodes = collect_way_nodes(&way);
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
                        } else if filtered_relations.has_way_ref(way.id) {
                            // Also keep ways that aren't really “interesting” (relevant for our matching)
                            // by themselves, but that are part of an “interesting” relation. We’ll use
                            // such ways for constructing the relation’s geometry, so we need the coordinates
                            // of the nodes belonging to such ways. However, we don’t need
                            // the way’s tags since the way will not actually make it into our final output.
                            let nodes = collect_way_nodes(&way);
                            for n in nodes.iter() {
                                node_ref_tx.send(*n)?;
                            }
                            way_tx.send(Way {
                                id: way.id,
                                nodes,
                                tags: Vec::<String>::with_capacity(0),
                            })?;
                        }
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

    let mut writer = filtered_file::Writer::create(&tmp_out)?;
    writer.write_features(&filtered_ways_data_path, &filtered_ways_offsets_path)?;
    writer.write_node_refs(&node_refs_path)?;
    writer.close()?;

    remove_file(&filtered_ways_data_path)?;
    remove_file(&filtered_ways_offsets_path)?;
    remove_file(&node_refs_path)?;
    rename(&tmp_out, &out_path)?;

    progress_bar.finish_with_message(format!(
        "blobs → {} ways referring to {} nodes",
        num_ways, num_node_refs
    ));

    Ok(FilteredFile::open(&out_path)?)
}

/// Helper for `filter_ways()`.
fn collect_way_nodes(way: &osm_pbf_iter::Way) -> Vec<u64> {
    way.refs().filter(|&x| x > 0).map(|x| x as u64).collect()
}

#[allow(clippy::too_many_arguments)]
pub fn filter_nodes<'a, R: Read + Seek + Send>(
    reader: &mut BlobReader<R>,
    blobs: (usize, usize),
    coverage: &Coverage,
    covered_nodes: &U64Table,
    filtered_ways: &FilteredFile,
    filtered_relations: &FilteredFile,
    progress: &MultiProgress,
    workdir: &Path,
) -> Result<FilteredFile<'a>> {
    let out_path = workdir.join("osm-filtered-nodes");
    if out_path.exists() {
        return Ok(FilteredFile::open(&out_path)?);
    }

    let coord_keys_path = workdir.join("osm-filtered-nodes.coords.keys.tmp");
    let coord_data_path = workdir.join("osm-filtered-nodes.coords.data.tmp");
    let filtered_nodes_data_path = workdir.join("osm-filtered-nodes.data.tmp");
    let filtered_nodes_offsets_path = workdir.join("osm-filtered-nodes.offsets.tmp");

    let num_blobs = (blobs.1 - blobs.0) as u64;
    let mut num_coords = 0;
    let mut num_nodes = 0;

    let progress_bar =
        super::make_progress_bar(progress, "osm.filter.n", num_blobs, "blobs → nodes");
    thread::scope(|s| {
        let num_workers = usize::from(thread::available_parallelism()?);
        let (blob_tx, blob_rx) = sync_channel::<Blob>(num_workers);
        let (node_tx, node_rx) = sync_channel::<Node>(8192);
        let (coords_tx, coords_rx) = sync_channel::<Coords>(8192);
        let producer = s.spawn(|| super::read_blobs(reader, blobs, &progress_bar, blob_tx));
        let handler = s.spawn(move || {
            blob_rx.into_iter().par_bridge().try_for_each(|blob| {
                let coords_tx = coords_tx.clone();
                let node_tx = node_tx.clone();
                let data = blob.into_data(); // decompress
                let block = PrimitiveBlock::parse(&data);
                for primitive in block.primitives() {
                    if let Primitive::Node(node) = primitive {
                        if filter(node.id, node.tags.iter().copied(), covered_nodes, coverage) {
                            let tags: Vec<String> = node
                                .tags
                                .into_iter()
                                .flat_map(|(k, v)| [k.to_string(), v.to_string()])
                                .collect();
                            if let Some((lon_e7, lat_e7)) = round_coords(node.lon, node.lat) {
                                node_tx.send(Node {
                                    id: node.id,
                                    tags,
                                    lon_e7,
                                    lat_e7,
                                })?;
                            }
                        };

                        // If the current node is referenced by some way or relation, we’ll need its
                        // coordinates to build the complex (non-point) geometry. Note that this is
                        // independent of whether the node is “interesting” for our matching purposes.
                        if (filtered_ways.has_node_ref(node.id)
                            || filtered_relations.has_node_ref(node.id))
                            && let Some((lon_e7, lat_e7)) = round_coords(node.lon, node.lat)
                        {
                            coords_tx.send(Coords {
                                key: node.id,
                                lon_e7,
                                lat_e7,
                            })?;
                        }
                    }
                }
                Ok(())
            })
        });

        let coords_writer = s.spawn(|| {
            num_coords = super::coords::build_tables(
                coords_rx,
                workdir,
                &coord_keys_path,
                &coord_data_path,
            )?;
            Ok(())
        });

        let node_writer = s.spawn(|| {
            let mut serializer = rmp_serde::Serializer::new(Vec::<u8>::with_capacity(32768));
            let mut data_writer = BufWriter::new(File::create(&filtered_nodes_data_path)?);
            let mut offsets_writer = BufWriter::new(File::create(&filtered_nodes_offsets_path)?);
            let mut cur_offset = 0_u64;
            for node in node_rx {
                serializer.get_mut().clear();
                node.serialize(&mut serializer)?;
                let buf = serializer.get_ref();
                data_writer.write_all(buf)?;
                offsets_writer.write_all(&cur_offset.to_le_bytes())?;
                cur_offset += buf.len() as u64;
                num_nodes += 1;
            }
            data_writer.into_inner()?.sync_all()?;
            offsets_writer.into_inner()?.sync_all()?;
            Ok(())
        });

        producer
            .join()
            .expect("panic in filter_nodes producer")
            .and(handler.join().expect("panic in filter_nodes handler"))
            .and(
                coords_writer
                    .join()
                    .expect("panic in filter_nodes coords_writer"),
            )
            .and(
                node_writer
                    .join()
                    .expect("panic in filter_nodes node_writer"),
            )
    })?;

    let mut tmp_out = PathBuf::from(&out_path);
    tmp_out.add_extension("tmp");

    let mut writer = filtered_file::Writer::create(&tmp_out)?;
    writer.write_features(&filtered_nodes_data_path, &filtered_nodes_offsets_path)?;
    writer.write_coords(&coord_keys_path, &coord_data_path)?;
    writer.close()?;

    remove_file(&filtered_nodes_data_path)?;
    remove_file(&filtered_nodes_offsets_path)?;
    remove_file(&coord_keys_path)?;
    remove_file(&coord_data_path)?;

    rename(&tmp_out, &out_path)?;

    progress_bar.finish_with_message(format!(
        "blobs → {} coords, {} nodes",
        num_coords, num_nodes
    ));

    Ok(FilteredFile::open(&out_path)?)
}

/// Helper for `filter_nodes()`.
fn round_coords(lon: f64, lat: f64) -> Option<(i32, i32)> {
    if (-90.0..=90.0).contains(&lat) && (-180.0..=180.0).contains(&lon) {
        Some(((lon * 1e7).round() as i32, (lat * 1e7).round() as i32))
    } else {
        None
    }
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
    use anyhow::{Ok, Result, anyhow};
    use geo_types::Point;
    use memmap2::Mmap;
    use std::fs::File;
    use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
    use std::path::Path;

    const BUFFER_SIZE: usize = 256 * 1024;

    pub struct FilteredFile<'a> {
        /// Backing store for `mmap`.
        _file: File,

        /// Backing store for memory-mapped slices.
        _mmap: Mmap,

        #[allow(unused)] // TODO: Remove attribute once we use the features.
        feature_data: &'a [u8],

        #[allow(unused)] // TODO: Remove attribute once we use the features.
        feature_offsets: &'a [u64],

        #[allow(unused)] // TODO: Remove attribute once we use the coordinates.
        coord_keys: &'a [u64],

        #[allow(unused)] // TODO: Remove attribute once we use the coordinates.
        coord_data: &'a [i32],

        node_refs: &'a [u64],
        way_refs: &'a [u64],
    }

    impl<'a> FilteredFile<'a> {
        pub fn open(path: &Path) -> Result<FilteredFile<'a>> {
            let file = File::open(path)?;

            // SAFETY: We don’t truncate the file while it’s mapped into memory.
            let mmap = unsafe { Mmap::map(&file)? };

            Self::check_signature(&mmap)?;

            let coord_keys = {
                if let Some((offset, size)) =
                    Self::get_offset_size(b"coo_keys", &mmap, /* alignment */ 8)
                {
                    // SAFETY: Bounds and alignment checked by get_offset_size().
                    unsafe {
                        let ptr = mmap.as_ptr().add(offset) as *const u64;
                        std::slice::from_raw_parts(ptr, size / 8)
                    }
                } else {
                    &[]
                }
            };

            let coord_data = {
                if let Some((offset, size)) =
                    Self::get_offset_size(b"coo_data", &mmap, /* alignment */ 4)
                {
                    // SAFETY: Bounds and alignment checked by get_offset_size().
                    unsafe {
                        let ptr = mmap.as_ptr().add(offset) as *const i32;
                        std::slice::from_raw_parts(ptr, size / 4)
                    }
                } else {
                    &[]
                }
            };

            let feature_data = {
                if let Some((offset, size)) =
                    Self::get_offset_size(b"fea_data", &mmap, /* alignment */ 1)
                {
                    // SAFETY: Bounds checked by get_offset_size(); no alignment constraints.
                    unsafe { std::slice::from_raw_parts(mmap.as_ptr().add(offset), size) }
                } else {
                    &[]
                }
            };

            let node_refs = {
                if let Some((offset, size)) =
                    Self::get_offset_size(b"nod_refs", &mmap, /* alignment */ 8)
                {
                    // SAFETY: Bounds and alignment checked by get_offset_size().
                    unsafe {
                        let ptr = mmap.as_ptr().add(offset) as *const u64;
                        std::slice::from_raw_parts(ptr, size / 8)
                    }
                } else {
                    &[]
                }
            };

            let feature_offsets = {
                if let Some((offset, size)) =
                    Self::get_offset_size(b"fea_offs", &mmap, /* alignment */ 8)
                {
                    // SAFETY: Bounds and alignment checked by get_offset_size().
                    unsafe {
                        let ptr = mmap.as_ptr().add(offset) as *const u64;
                        std::slice::from_raw_parts(ptr, size / 8)
                    }
                } else {
                    &[]
                }
            };

            let way_refs = {
                if let Some((offset, size)) =
                    Self::get_offset_size(b"way_refs", &mmap, /* alignment */ 8)
                {
                    // SAFETY: Bounds and alignment checked by get_offset_size().
                    unsafe {
                        let ptr = mmap.as_ptr().add(offset) as *const u64;
                        std::slice::from_raw_parts(ptr, size / 8)
                    }
                } else {
                    &[]
                }
            };

            Ok(FilteredFile {
                _file: file,
                _mmap: mmap,
                coord_keys,
                coord_data,
                feature_data,
                feature_offsets,
                node_refs,
                way_refs,
            })
        }

        #[allow(unused)] // TODO: Remove attribute once we use the coordinates.
        pub fn get_coords(&self, node_id: u64) -> Option<Point> {
            let index = if cfg!(target_endian = "little") {
                self.coord_keys.binary_search(&node_id)
            } else {
                self.coord_keys
                    .binary_search_by(|x| x.swap_bytes().cmp(&node_id))
            };
            if let Result::Ok(i) = index {
                let lon = self.coord_data[i * 2] as f64 / 1e7;
                let lat = self.coord_data[i * 2 + 1] as f64 / 1e7;
                Some(Point::new(lon, lat))
            } else {
                None
            }
        }

        pub fn has_node_ref(&self, node_id: u64) -> bool {
            if cfg!(target_endian = "little") {
                self.node_refs.binary_search(&node_id).is_ok()
            } else {
                self.node_refs
                    .binary_search_by(|x| x.swap_bytes().cmp(&node_id))
                    .is_ok()
            }
        }

        pub fn has_way_ref(&self, way_id: u64) -> bool {
            if cfg!(target_endian = "little") {
                self.way_refs.binary_search(&way_id).is_ok()
            } else {
                self.way_refs
                    .binary_search_by(|x| x.swap_bytes().cmp(&way_id))
                    .is_ok()
            }
        }

        fn check_signature(data: &[u8]) -> Result<()> {
            if data.len() < 32 || &data[0..24] != b"diffed-places filtered\0\0" {
                return Err(anyhow!("malformed filtered file"));
            }
            Ok(())
        }

        fn get_offset_size(key: &[u8; 8], data: &[u8], alignment: usize) -> Option<(usize, usize)> {
            let header_pos = u64::from_le_bytes(data[24..32].try_into().ok()?);
            let p = usize::try_from(header_pos).ok()?;
            let num_headers_u64 = u64::from_le_bytes(data[p..p + 8].try_into().ok()?);
            let num_headers = usize::try_from(num_headers_u64).ok()?;
            for i in 0..num_headers {
                let o = p + 8 + i * 24;
                if o + 8 <= data.len() && *key == data[o..o + 8] {
                    let offset =
                        usize::try_from(u64::from_le_bytes(data[o + 8..o + 16].try_into().ok()?))
                            .ok()?;
                    let len =
                        usize::try_from(u64::from_le_bytes(data[o + 16..o + 24].try_into().ok()?))
                            .ok()?;
                    if offset + len <= data.len()
                        && offset.is_multiple_of(alignment)
                        && len.is_multiple_of(alignment)
                    {
                        return Some((offset, len));
                    };
                }
            }
            None
        }
    }

    pub struct Writer {
        writer: BufWriter<File>,
        headers: Vec<(&'static [u8; 8], u64, u64)>,
    }

    impl Writer {
        pub fn create(path: &Path) -> Result<Writer> {
            let mut writer = BufWriter::with_capacity(BUFFER_SIZE, File::create(path)?);
            // Reserve space for file header.
            writer.write_all(b"diffed-places filtered\0\0")?;
            writer.write_all(&[0; 8])?; // leave space to offset of header section
            Ok(Writer {
                writer,
                headers: Vec::with_capacity(10),
            })
        }

        pub fn write_features(&mut self, data: &Path, offsets: &Path) -> Result<()> {
            let (data_start, data_len) = self.write_file(data, /* alignment */ 1)?;
            let (offsets_start, offsets_len) = self.write_file(offsets, /* alignment */ 8)?;
            self.headers.push((b"fea_data", data_start, data_len));
            self.headers.push((b"fea_offs", offsets_start, offsets_len));
            Ok(())
        }

        pub fn write_coords(&mut self, keys: &Path, data: &Path) -> Result<()> {
            let (keys_start, keys_len) = self.write_file(keys, /* alignment */ 8)?;
            let (data_start, data_len) = self.write_file(data, /* alignment */ 4)?;
            self.headers.push((b"coo_keys", keys_start, keys_len));
            self.headers.push((b"coo_data", data_start, data_len));
            Ok(())
        }

        pub fn write_node_refs(&mut self, path: &Path) -> Result<()> {
            let (start, len) = self.write_file(path, /* alignment */ 8)?;
            self.headers.push((b"nod_refs", start, len));
            Ok(())
        }

        pub fn write_way_refs(&mut self, path: &Path) -> Result<()> {
            let (start, len) = self.write_file(path, /* alignment */ 8)?;
            self.headers.push((b"way_refs", start, len));
            Ok(())
        }

        pub fn close(mut self) -> Result<()> {
            self.write_headers()?;
            self.writer.flush()?;
            Ok(())
        }

        fn write_headers(&mut self) -> Result<()> {
            self.write_padding(/* alignment */ 8)?;
            let header_pos = self.writer.stream_position()?;

            let num_headers = self.headers.len() as u64;
            self.writer.write_all(&num_headers.to_le_bytes())?;

            for (id, pos, len) in self.headers.iter() {
                self.writer.write_all(*id)?;
                self.writer.write_all(&pos.to_le_bytes())?;
                self.writer.write_all(&len.to_le_bytes())?;
            }
            self.writer.seek(SeekFrom::Start(24))?;
            self.writer.write_all(&header_pos.to_le_bytes())?;
            Ok(())
        }

        fn write_file(&mut self, path: &Path, alignment: usize) -> Result<(u64, u64)> {
            self.write_padding(alignment)?;
            let start = self.writer.stream_position()?;
            let mut reader = BufReader::with_capacity(BUFFER_SIZE, File::open(path)?);
            std::io::copy(&mut reader, &mut self.writer)?;
            let len = reader.stream_position()?;
            Ok((start, len))
        }

        fn write_padding(&mut self, alignment: usize) -> Result<()> {
            if alignment > 1 {
                let pos = self.writer.stream_position()?;
                let alignment = alignment as u64;
                let num_bytes = ((alignment - (pos % alignment)) % alignment) as usize;
                if num_bytes > 0 {
                    let padding = vec![0; num_bytes];
                    self.writer.write_all(&padding)?;
                }
            }
            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::FilteredFile;

        #[test]
        fn test_check_signature() {
            assert!(
                FilteredFile::check_signature(b"diffed-places filtered\0\0\0\0\0\0\0\0\0\0")
                    .is_ok()
            );
            assert!(FilteredFile::check_signature(b"").is_err());
            assert!(FilteredFile::check_signature(b"foo").is_err());
            assert!(FilteredFile::check_signature(b"diffed-places coverage\0\x01").is_err());
            assert!(FilteredFile::check_signature(b"diffed-places coverage\0\0\0\0\0\0").is_err());
        }

        #[test]
        fn test_get_offset_size() {
            // Construct a coverage file with two keys in its header.
            let mut bytes = Vec::new();
            bytes.extend_from_slice(b"diffed-places filtered\0\0"); // [0..24]: signature
            bytes.extend_from_slice(&48_u64.to_le_bytes()); // [24..32]: header_pos
            bytes.extend_from_slice(&0xdeadbeefcafefeed_u64.to_le_bytes()); // [32..40]: data[0]
            bytes.extend_from_slice(&42_u64.to_le_bytes()); // [40..48]: data[1]
            bytes.extend_from_slice(&2_u64.to_le_bytes()); // [48..56]: num_headers
            assert!(FilteredFile::get_offset_size(b"some_key", &bytes, 1) == None);
            for (key, offset, size) in [(b"some_key", 88, 16), (b"otherkey", 91, 2)] {
                bytes.extend_from_slice(key as &[u8; 8]);
                bytes.extend_from_slice(&(offset as u64).to_le_bytes());
                bytes.extend_from_slice(&(size as u64).to_le_bytes());
            }

            let bytes = bytes.as_ref();

            // The file header has no entry for key "in-exist".
            assert_eq!(FilteredFile::get_offset_size(b"in-exist", bytes, 1), None);

            // The data for "some_key" starts at offset 88 and is 16 bytes, which is
            // correctly aligned for single-byte, 8-byte and 16-byte access.
            assert_eq!(
                FilteredFile::get_offset_size(b"some_key", bytes, 1),
                Some((88, 16))
            );
            assert_eq!(
                FilteredFile::get_offset_size(b"some_key", bytes, 2),
                Some((88, 16))
            );
            assert_eq!(
                FilteredFile::get_offset_size(b"some_key", bytes, 8),
                Some((88, 16))
            );

            // However, it would be unsafe (at least on RISC CPUs) to perform 32-byte
            // access, eg. loading a SIMD register, because 80 is not divisible by 32.
            // Also, the data size is too small to read 32-byte entitites anyway.
            assert_eq!(FilteredFile::get_offset_size(b"some_key", bytes, 32), None);

            // The data for "otherkey" starts at offset 91 and is 2 bytes long.
            // Access is only safe with single-byte alignment.
            assert_eq!(
                FilteredFile::get_offset_size(b"otherkey", bytes, 1),
                Some((91, 2))
            );
            assert_eq!(FilteredFile::get_offset_size(b"otherkey", bytes, 2), None);
            assert_eq!(FilteredFile::get_offset_size(b"otherkey", bytes, 8), None);
        }
    }

    #[test]
    fn test_coords() -> Result<()> {
        let tmp = tempfile::NamedTempFile::new()?;
        let mut writer = Writer::create(tmp.path())?;
        let keys_path = {
            let mut p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            p.push("tests/test_data/u64_le_0_2_7");
            p
        };
        let data = tempfile::NamedTempFile::new()?;
        {
            let mut file = File::create(data.path())?;
            for i in 1_i32..=6_i32 {
                file.write_all(&i.to_le_bytes())?;
            }
            file.sync_all()?;
        }
        writer.write_coords(&keys_path, &data.path())?;
        writer.close()?;
        let ff = FilteredFile::open(tmp.path())?;
        assert_eq!(ff.get_coords(0), Some(Point::new(0.0000001, 0.0000002)));
        assert_eq!(ff.get_coords(1), None);
        assert_eq!(ff.get_coords(2), Some(Point::new(0.0000003, 0.0000004)));
        assert_eq!(ff.get_coords(7), Some(Point::new(0.0000005, 0.0000006)));
        assert_eq!(ff.get_coords(99), None);
        Ok(())
    }

    #[test]
    fn test_node_refs() -> Result<()> {
        let tmp = tempfile::NamedTempFile::new()?;
        let mut writer = Writer::create(tmp.path())?;
        writer.write_node_refs(&{
            let mut p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            p.push("tests/test_data/u64_le_0_2_7");
            p
        })?;
        writer.close()?;
        let ff = FilteredFile::open(tmp.path())?;
        assert_eq!(ff.has_node_ref(0), true);
        assert_eq!(ff.has_node_ref(2), true);
        assert_eq!(ff.has_node_ref(7), true);
        assert_eq!(ff.has_node_ref(1), false);
        assert_eq!(ff.has_node_ref(8), false);
        assert_eq!(ff.has_node_ref(1234567890123456789), false);
        Ok(())
    }

    #[test]
    fn test_way_refs() -> Result<()> {
        let tmp = tempfile::NamedTempFile::new()?;
        let mut writer = Writer::create(tmp.path())?;
        writer.write_way_refs(&{
            let mut p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            p.push("tests/test_data/u64_le_0_2_7");
            p
        })?;
        writer.close()?;
        let ff = FilteredFile::open(tmp.path())?;
        assert_eq!(ff.has_way_ref(0), true);
        assert_eq!(ff.has_way_ref(2), true);
        assert_eq!(ff.has_way_ref(7), true);
        assert_eq!(ff.has_way_ref(1), false);
        assert_eq!(ff.has_way_ref(8), false);
        assert_eq!(ff.has_way_ref(1234567890123456789), false);
        Ok(())
    }

    #[test]
    fn test_empty() -> Result<()> {
        let tmp = tempfile::NamedTempFile::new()?;
        let writer = Writer::create(tmp.path())?;
        writer.close()?;
        let ff = FilteredFile::open(tmp.path())?;
        assert_eq!(ff.get_coords(5), None);
        assert_eq!(ff.has_node_ref(123), false);
        assert_eq!(ff.has_way_ref(789), false);
        Ok(())
    }
}
