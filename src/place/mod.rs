use deepsize::DeepSizeOf;
use geo_types::Point;
use serde::{Deserialize, Serialize};

mod writer;
pub use writer::ParquetWriter;

#[derive(Debug, DeepSizeOf, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Place {
    pub s2_cell_id: u64,
    pub source: String,
    pub tags: Vec<(String, String)>,
}

impl Place {
    pub fn new(point: &Point, source: String, tags: Vec<(String, String)>) -> Option<Place> {
        let s2_lat_lng = s2::latlng::LatLng::from_degrees(point.y(), point.x());
        if !s2_lat_lng.is_valid() {
            return None;
        }

        let s2_cell_id = s2::cellid::CellID::from(s2_lat_lng).0;
        Some(Place {
            s2_cell_id,
            source,
            tags,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Place;
    use geo::Point;

    #[test]
    fn test_new() {
        let p = Point::new(7.447_812_3, 46.947_980_1);
        let source = "test/source".to_string();
        let tags = vec![
            ("building".to_string(), "tower".to_string()),
            ("name:gsw".to_string(), "Zytglogge".to_string()),
        ];
        let place = Place::new(&p, source, tags.clone()).unwrap();
        assert_eq!(place.s2_cell_id, 5156122125915201443);
        assert_eq!(place.source, "test/source");
        assert_eq!(place.tags, tags);
    }

    #[test]
    fn test_cmp() {
        let a = Place::new(
            &Point::new(7.4478123, 46.9479801),
            "test/source".to_string(),
            vec![],
        )
        .unwrap();
        let b = Place::new(
            &Point::new(-122.4630042, 37.8045878),
            "test/source".to_string(),
            vec![],
        )
        .unwrap();
        assert_eq!(a.eq(&b), false);
        assert_eq!(a.eq(&a), true);
        assert_eq!(a.cmp(&b), a.s2_cell_id.cmp(&b.s2_cell_id));
        assert_eq!(a.partial_cmp(&b), a.s2_cell_id.partial_cmp(&b.s2_cell_id));
    }
}
