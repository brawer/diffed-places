use anyhow::{Context, Ok, Result};
use arrow::datatypes::{DataType, Field, Schema};
use ext_sort::{buffer::mem::MemoryLimitedBufferBuilder, ExternalSorter, ExternalSorterBuilder};
use geo::algorithm::line_measures::Haversine;
use geo::{InteriorPoint, InterpolateLine, Point};
use geojson::GeoJson;
use memmap2::Mmap;
use piz::ZipArchive;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use wkb::writer::point_wkb_size;

#[derive(Serialize, Deserialize, Debug)]
struct Place {
    s2_cell_id: u64,
    point: geo::Point,
    tags: Vec<(String, String)>,
}

impl deepsize::DeepSizeOf for Place {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        8 + 16 + self.tags.deep_size_of_children(context)
    }
}

// Implement Eq (marker trait, no methods to implement)
impl Eq for Place {}

impl Ord for Place {
    fn cmp(&self, other: &Self) -> Ordering {
        //println!("cmp({}, {})", self.s2_cell_id, other.s2_cell_id);
        self.s2_cell_id
            .cmp(&other.s2_cell_id)
            .then(self.tags.cmp(&other.tags))
    }
}

impl PartialEq for Place {
    fn eq(&self, other: &Self) -> bool {
        self.s2_cell_id == other.s2_cell_id && self.tags == other.tags
    }
}

impl PartialOrd for Place {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub fn import_atp(input: &PathBuf) -> Result<()> {
    // To avoid deadlock, we must not use Rayon threads here.
    // https://dev.to/sgchris/scoped-threads-with-stdthreadscope-in-rust-163-48f9
    let (tx, rx) = sync_channel(50_000);
    let (ra, rb) = std::thread::scope(|s| {
        let r1 = s.spawn(|| process_places(rx));
        let r2 = s.spawn(|| process_zip(input, tx));
        (r1.join().unwrap(), r2.join().unwrap())
    });
    ra?;
    rb?;
    Ok(())
}

fn process_places(places: Receiver<Place>) -> Result<()> {
    let _schema = make_parquet_schema();
    let mut out = BufWriter::new(File::create("output.txt")?);
    let sorter: ExternalSorter<Place, std::io::Error, MemoryLimitedBufferBuilder> =
        ExternalSorterBuilder::new()
            .with_tmp_dir(Path::new("./"))
            .with_buffer(MemoryLimitedBufferBuilder::new(150_000_000))
            .build()?;

    let sorted = sorter.sort(places.iter().map(std::io::Result::Ok))?;
    let mut num_places = 0;
    for place in sorted {
        num_places += 1;
        out.write_all(format!("{}\n", place?.s2_cell_id).as_bytes())?;
    }
    println!("got {} places", num_places);
    Ok(())
}

fn process_zip(path: &PathBuf, channel: SyncSender<Place>) -> Result<()> {
    let file = File::open(path).with_context(|| format!("could not open file `{:?}`", path))?;
    let mapping = unsafe { Mmap::map(&file).context("Couldn't mmap zip file")? };
    let archive = ZipArchive::with_prepended_data(&mapping)
        .context("Couldn't load archive")?
        .0;
    archive.entries().par_iter().try_for_each(|entry| {
        if entry.size > 0 {
            let reader = archive.read(entry)?;
            process_geojson(reader, channel.clone())?;
        }
        Ok(())
    })?;
    Ok(())
}

fn process_geojson<T: Read>(reader: T, channel: SyncSender<Place>) -> Result<()> {
    let buffer = BufReader::new(reader);
    for line in buffer.lines() {
        let line = line?;

        // Handle start of FeatureCollection, first line in file.
        if line.starts_with("{\"type\":\"FeatureCollection\"") {
            let mut json = line;
            json.push_str("]}");
            let _parsed = json.parse::<geojson::FeatureCollection>()?;
            // TODO: Collect bounding box and attributes?
            continue;
        }

        // Handle end of FeatureCollection, last line in file.
        if line == "]}" {
            continue;
        }

        // Handle individual Features.
        let trimmed = if let Some((a, _)) = line.split_at_checked(line.len() - 1) {
            a
        } else {
            &line
        };
        let Some(place) = make_place(trimmed) else {
            continue;
        };
        channel.send(place)?;
    }
    Ok(())
}

fn make_place(geojson: &str) -> Option<Place> {
    let parsed = geojson.parse::<GeoJson>().ok()?;
    let point = find_point(&parsed)?;
    let s2_lat_lng = s2::latlng::LatLng::from_degrees(point.y(), point.x());
    let s2_cell_id = s2::cellid::CellID::from(s2_lat_lng).0;
    let GeoJson::Feature(feature) = parsed else {
        return None;
    };
    let tags = properties_to_tags(&feature.properties?);
    Some(Place {
        s2_cell_id,
        point,
        tags,
    })
}

fn properties_to_tags(
    properties: &serde_json::Map<String, serde_json::Value>,
) -> Vec<(String, String)> {
    let mut tags: Vec<(String, String)> = properties
        .iter()
        .filter_map(|(key, value)| {
            // Only keep properties where the value is a string.
            if let serde_json::Value::String(s) = value {
                Some((key.clone(), s.clone()))
            } else {
                None
            }
        })
        .collect();
    tags.sort();
    tags
}

/// Finds a representative point for a GeoJson feature.
fn find_point(geojson: &GeoJson) -> Option<Point> {
    let GeoJson::Feature(f) = geojson else {
        return None;
    };
    let Some(geometry) = &f.geometry else {
        return None;
    };
    let geom = TryInto::<geo::Geometry<f64>>::try_into(geometry).ok()?;
    match geom {
        geo::Geometry::LineString(line_string) => {
            Haversine.point_at_ratio_from_start(&line_string, 0.5)
        }
        geo::Geometry::MultiLineString(mls) => {
            if !mls.0.is_empty() {
                Haversine.point_at_ratio_from_start(&mls.0[0], 0.5)
            } else {
                None
            }
        }
        _ => geom.interior_point(),
    }
}

fn make_parquet_schema() -> Schema {
    use std::sync::Arc;

    // Metadata for GEOMETRY logical type
    let mut point_metadata = HashMap::new();
    point_metadata.insert(
        "ARROW:extension:metadata".to_string(),
        r#"{"logicalType": "GEOMETRY"}"#.to_string(),
    );
    let point_size = point_wkb_size(geo_traits::Dimensions::Xy);
    Schema::new(vec![
        Field::new("point", DataType::FixedSizeBinary(point_size as i32), false)
            .with_metadata(point_metadata),
        Field::new(
            "tags",
            DataType::Map(Arc::new(Field::new("entries", DataType::Utf8, true)), false),
            false,
        ),
        Field::new("spider", DataType::Utf8, false),
    ])
}

#[cfg(test)]
mod tests {
    use geo::Point;
    use geojson::GeoJson;

    // A point feature for testing.
    const PLAYGROUND: &str = r#"{
       "type": "Feature",
       "properties": {
           "leisure": "playground",
           "addr:street": "Hermann-G\u00f6tz-Strasse",
           "addr:city": "Winterthur",
           "operator": "Stadtgr\u00fcn Winterthur",
           "operator:wikidata": "Q56825906",
           "@spider": "winterthur_ch"
       },
       "geometry": {
           "type": "Point",
           "coordinates": [8.7339982, 47.5039168]
        }
    }"#;

    const BICYCLE_ROAD: &str = r#"{
       "type": "Feature",
       "properties": {
           "@spider": "bern_ch",
           "highway": "residential",
           "bicycle_road": "yes"
       },
       "geometry": {
           "type": "LineString",
           "coordinates": [
               [7.458535, 46.940702],
               [7.458746, 46.941164],
               [7.458778, 46.941229],
               [7.459291, 46.942315],
               [7.459298, 46.942329],
               [7.459647, 46.943080],
               [7.460838, 46.943692]
           ]
       }
    }"#;

    fn find_point(geojson: &str) -> Option<Point> {
        super::find_point(&geojson.parse::<GeoJson>().unwrap())
    }

    #[test]
    fn test_find_point_for_point() {
        let pt = find_point(PLAYGROUND).unwrap();
        assert!((pt.x() - 8.7339982).abs() < 1e-7);
        assert!((pt.y() - 47.5039168).abs() < 1e-7);
    }

    #[test]
    fn test_find_point_for_line_string() {
        let pt = find_point(&BICYCLE_ROAD).unwrap();
        assert!((pt.x() - 7.4593195).abs() < 1e-6);
        assert!((pt.y() - 46.9423753).abs() < 1e-6);
    }

    #[test]
    fn test_find_point_for_polygon() {
        let geojson = r#"{
           "type": "Feature",
           "geometry": {
               "type": "Polygon",
               "coordinates": [
                   [
                       [-80.190, 25.774],
                       [-66.118, 18.466],
                       [-64.757, 32.321]
                   ],
                   [
                       [-70.579, 28.745],
                       [-67.514, 29.570],
                       [-66.668, 27.339]
                   ]
               ]
           }
        }"#;
        let pt = find_point(&geojson).unwrap();
        assert!((pt.x() - -72.4474).abs() < 1e-3);
        assert!((pt.y() - 25.3935).abs() < 1e-3);
    }

    fn format_tags(tags: &Vec<(String, String)>) -> Vec<String> {
        tags.into_iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect()
    }

    #[test]
    fn test_make_place() {
        let place = super::make_place(PLAYGROUND).unwrap();
        assert_eq!(place.s2_cell_id, 5159605115004699013);
        assert!((place.point.x() - 8.7339982).abs() < 1e-7);
        assert!((place.point.y() - 47.5039168).abs() < 1e-7);
        assert_eq!(
            format_tags(&place.tags),
            [
                "@spider=winterthur_ch",
                "addr:city=Winterthur",
                "addr:street=Hermann-Götz-Strasse",
                "leisure=playground",
                "operator=Stadtgrün Winterthur",
                "operator:wikidata=Q56825906"
            ]
        );
    }

    #[test]
    fn test_make_place_for_line_string() {
        let place = super::make_place(BICYCLE_ROAD).unwrap();
        assert_eq!(place.s2_cell_id, 5156122231380170133);
        assert!((place.point.x() - 7.4593195).abs() < 1e-6);
        assert!((place.point.y() - 46.9423753).abs() < 1e-6);
        assert_eq!(
            format_tags(&place.tags),
            ["@spider=bern_ch", "bicycle_road=yes", "highway=residential"]
        );
    }
}
