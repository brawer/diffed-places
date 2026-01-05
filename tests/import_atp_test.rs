use assert_cmd::{Command, cargo_bin};
use predicates;
use std::path::PathBuf;

#[test]
fn test_import_atp() {
    let mut cmd = Command::new(cargo_bin!("diffed-places"));
    let mut test_data = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    test_data.push("tests/test_data");
    let mut input_zip = test_data.clone();
    input_zip.push("alltheplaces.zip");
    cmd.arg("import-atp").arg("--input").arg(&input_zip);
    cmd.assert().success();
}

#[test]
fn test_import_atp_bad_input_path() {
    let mut cmd = Command::new(cargo_bin!("diffed-places"));
    cmd.arg("import-atp")
        .arg("--input")
        .arg("test/file/does-not-exist");
    cmd.assert()
        .failure()
        .stderr(predicates::str::contains("could not open file"))
        .stderr(predicates::str::contains("test/file/does-not-exist"));
}
