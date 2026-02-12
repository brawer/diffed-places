mod coverage;
mod import_atp;
mod osm;
mod place;
mod u64_table;

// Re-exported for main.rs.
pub use coverage::build_coverage;
pub use import_atp::import_atp;
pub use osm::import_osm;

#[cfg(fuzzing)]
pub use import_atp::fuzz::fuzz_process_geojson;

const PROGRESS_BAR_STYLE: &str =
    "{prefix} [{elapsed_precise}] {bar:42.blue} {pos:>7}/{len:7} {msg}";
