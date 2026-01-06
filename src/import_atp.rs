use anyhow::{Context, Ok, Result};
use geo::algorithm::line_measures::Haversine;
use geo::{InteriorPoint, InterpolateLine, Point};
use geojson::GeoJson;
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

        // Handle start of FeatureCollection, first line in file.
        if line.starts_with("{\"type\":\"FeatureCollection\"") {
            let mut json = String::from(line);
            json.push_str("]}");
            let val: serde_json::Value = serde_json::from_str(&json)?;
            if let Some(attrs) = val.get("dataset_attributes") {
                spider_attrs = attrs.clone();
            }
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
        let feature = trimmed.parse::<GeoJson>()?;
        if let Some(pt) = find_point(&feature) {
            println!("*** {:?}", pt);
        }
    }
    println!("{:?}", spider_attrs);
    Ok(())
}

/// Finds a representative point for a GeoJson feature.
fn find_point(geojson: &GeoJson) -> Option<Point> {
    let GeoJson::Feature(f) = geojson else {
        return None;
    };
    let Some(geometry) = &f.geometry else {
        return None;
    };
    let Some(geom) = TryInto::<geo::Geometry<f64>>::try_into(geometry).ok() else {
        return None;
    };
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
	_ => geom.interior_point()
    }
}

#[cfg(test)]
mod tests {
    use geo::Point;
    use geojson::GeoJson;

    fn find_point(geojson: &str) -> Option<Point> {
        super::find_point(&geojson.parse::<GeoJson>().unwrap())
    }

    #[test]
    fn test_find_point_for_point() {
        let geojson = r#"{
           "type": "Feature",
           "geometry": {
               "type": "Point",
               "coordinates": [7.45, 46.95]
            }
        }"#;
        let pt = find_point(&geojson).unwrap();
        assert!((pt.x() - 7.45).abs() < 1e-7);
        assert!((pt.y() - 46.95).abs() < 1e-7);
    }

    #[test]
    fn test_find_point_for_linestring() {
        let geojson = r#"{
           "type": "Feature",
           "geometry": {
               "type": "LineString",
               "properties": {
                   "@spider": "bern_ch",
                   "highway": "residential",
                   "bicycle_road": "yes"
               },
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
        let pt = find_point(&geojson).unwrap();
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
}
