use anyhow::{Context, Ok, Result};
use ext_sort::{ExternalSorter, ExternalSorterBuilder, buffer::mem::MemoryLimitedBufferBuilder};
use geo::algorithm::line_measures::Haversine;
use geo::{InteriorPoint, InterpolateLine, Point};
use geojson::GeoJson;
use memmap2::Mmap;
use piz::ZipArchive;
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

use crate::place::{ParquetWriter, Place};

pub fn import_atp(input: &Path, output: &Path) -> Result<()> {
    // To avoid deadlock, we must not use Rayon threads here.
    // https://dev.to/sgchris/scoped-threads-with-stdthreadscope-in-rust-163-48f9
    let (tx, rx) = sync_channel(50_000);
    let (ra, rb) = std::thread::scope(|s| {
        let r1 = s.spawn(|| process_places(rx, output));
        let r2 = s.spawn(|| process_zip(input, tx));
        (r1.join().unwrap(), r2.join().unwrap())
    });
    ra?;
    rb?;
    Ok(())
}

fn process_places(places: Receiver<Place>, out: &Path) -> Result<()> {
    let mut writer = ParquetWriter::try_new(/* batch size */ 64 * 1024, out)?;
    let sorter: ExternalSorter<Place, std::io::Error, MemoryLimitedBufferBuilder> =
        ExternalSorterBuilder::new()
            .with_tmp_dir(Path::new("./"))
            .with_buffer(MemoryLimitedBufferBuilder::new(150_000_000))
            .build()?;
    let sorted = sorter.sort(places.iter().map(std::io::Result::Ok))?;
    let mut num_places = 0;
    for place in sorted {
        num_places += 1;
        writer.write(place?)?;
    }
    writer.close()?;
    println!("got {} places", num_places);
    Ok(())
}

fn process_zip(path: &Path, channel: SyncSender<Place>) -> Result<()> {
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
    let GeoJson::Feature(feature) = parsed else {
        return None;
    };
    let properties = feature.properties?;

    let mut source: Option<String> = None;

    // We strip three properties ("nsi_id", "@spider", "@source_uri") from tags,
    // so we do not need to reserve space for them.
    let num_tags = properties.len();
    let mut tags =
        Vec::<(String, String)>::with_capacity(if num_tags > 3 { num_tags - 3 } else { num_tags });

    for (key, val) in properties {
        // All The Places inserts an "nsi_id" for any features
        // that are matches for the OSM Name Suggestion Index.
        // Since this is purely internal to debugging All The Places,
        // we strip off this property here.
        if key == "nsi_id" || key.is_empty() {
            continue;
        }

        let value = match val {
            serde_json::Value::String(v) => Some(v),
            serde_json::Value::Bool(v) => Some(v.to_string()),
            serde_json::Value::Number(v) => Some(v.to_string()),
            _ => None,
        };
        let Some(mut value) = value else {
            continue;
        };

        if key == "@spider" {
            value.insert_str(0, "atp/");
            source = Some(value);
            continue;
        }

        if key.starts_with('@') || value.is_empty() {
            continue;
        }

        tags.push((key, value));
    }
    Place::new(&point, source?, tags)
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

#[cfg(test)]
mod tests {
    use crate::place::Place;
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
           "bicycle_road": "yes",
           "@source_uri": "https://map.bern.ch/ogd/poi_velo/poi_velo_json.zip"
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

    fn tags<'a>(place: &'a Place) -> Vec<(&'a str, &'a str)> {
        place
            .tags
            .iter()
            .map(|(a, b)| (a.as_str(), b.as_str()))
            .collect()
    }

    #[test]
    fn test_make_place() {
        let place = super::make_place(PLAYGROUND).unwrap();
        assert_eq!(place.source, "atp/winterthur_ch");
        assert_eq!(
            tags(&place),
            [
                ("addr:city", "Winterthur"),
                ("addr:street", "Hermann-Götz-Strasse"),
                ("leisure", "playground"),
                ("operator", "Stadtgrün Winterthur"),
                ("operator:wikidata", "Q56825906"),
            ]
        );
    }

    #[test]
    fn test_make_place_for_line_string() {
        let place = super::make_place(BICYCLE_ROAD).unwrap();
        assert_eq!(place.source, "atp/bern_ch");
        assert_eq!(
            tags(&place),
            [("bicycle_road", "yes"), ("highway", "residential"),]
        );
    }
}
