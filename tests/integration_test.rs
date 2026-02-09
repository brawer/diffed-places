use anyhow::{Ok, Result};
use assert_cmd::{Command, cargo_bin};
use predicates;
use std::path::PathBuf;
use tempfile::{NamedTempFile, TempDir};

#[test]
fn test_pipeline() -> Result<()> {
    let mut test_data = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    test_data.push("tests/test_data");
    let workdir = TempDir::new()?;

    let mut alltheplaces_zip = test_data.clone();
    alltheplaces_zip.push("alltheplaces.zip");
    let alltheplaces_parquet = NamedTempFile::new()?;
    Command::new(cargo_bin!("diffed-places"))
        .arg("import-atp")
        .arg("--input")
        .arg(&alltheplaces_zip)
        .arg("--output")
        .arg(alltheplaces_parquet.path())
        .assert()
        .success();

    let coverage = NamedTempFile::new()?;
    Command::new(cargo_bin!("diffed-places"))
        .arg("build-coverage")
        .arg("--places")
        .arg(alltheplaces_parquet.path())
        .arg("--output")
        .arg(coverage.path())
        .assert()
        .success();

    let mut zugerland_osm_pbf = test_data.clone();
    zugerland_osm_pbf.push("zugerland.osm.pbf");
    Command::new(cargo_bin!("diffed-places"))
        .arg("import-osm")
        .arg("--osm")
        .arg(&zugerland_osm_pbf)
        .arg("--coverage")
        .arg(coverage.path())
        .arg("--workdir")
        .arg(workdir.path())
        .assert()
        .success();

    Ok(())
}

#[test]
fn test_no_subcommand() {
    Command::new(cargo_bin!("diffed-places"))
        .assert()
        .failure()
        .stderr(predicates::str::contains("no subcommand given"));
}

#[test]
fn test_import_atp_bad_input_path() -> Result<()> {
    let output = NamedTempFile::new()?;
    Command::new(cargo_bin!("diffed-places"))
        .arg("import-atp")
        .arg("--input")
        .arg("test/file/does-not-exist")
        .arg("--output")
        .arg(output.path())
        .assert()
        .failure()
        .stderr(predicates::str::contains("could not open file"))
        .stderr(predicates::str::contains("test/file/does-not-exist"));
    Ok(())
}

#[test]
fn test_build_coverage_bad_input_path() -> Result<()> {
    let output = NamedTempFile::new()?;
    Command::new(cargo_bin!("diffed-places"))
        .arg("build-coverage")
        .arg("--places")
        .arg("test/file/does-not-exist")
        .arg("--output")
        .arg(output.path())
        .assert()
        .failure()
        .stderr(predicates::str::contains("could not open file"))
        .stderr(predicates::str::contains("test/file/does-not-exist"));
    Ok(())
}

#[test]
fn test_import_osm_bad_input_path() -> Result<()> {
    let mut test_data = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    test_data.push("tests/test_data");
    let mut osm = test_data.clone();
    osm.push("zugerland.osm.pbf");
    let mut coverage = test_data.clone();
    coverage.push("alltheplaces.coverage");
    let workdir = TempDir::new()?;

    // OpenStreetMap file does not exist.
    Command::new(cargo_bin!("diffed-places"))
        .arg("import-osm")
        .arg("--osm")
        .arg("test/file/does-not-exist")
        .arg("--coverage")
        .arg(&coverage)
        .arg("--workdir")
        .arg(workdir.path())
        .assert()
        .failure()
        .stderr(predicates::str::contains("could not open file"))
        .stderr(predicates::str::contains("test/file/does-not-exist"));

    // Coverage file does not exist.
    Command::new(cargo_bin!("diffed-places"))
        .arg("import-osm")
        .arg("--osm")
        .arg(&osm)
        .arg("--coverage")
        .arg("test/coverage-file/does-not-exist")
        .arg("--workdir")
        .arg(workdir.path())
        .assert()
        .failure()
        .stderr(predicates::str::contains("could not open coverage file"))
        .stderr(predicates::str::contains(
            "test/coverage-file/does-not-exist",
        ));

    Ok(())
}
