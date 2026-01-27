use anyhow::{Ok, Result};
use assert_cmd::{Command, cargo_bin};
use predicates;
use std::path::PathBuf;
use tempfile::NamedTempFile;

#[test]
fn test_pipeline() -> Result<()> {
    let mut test_data = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    test_data.push("tests/test_data");

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

    Ok(())
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
