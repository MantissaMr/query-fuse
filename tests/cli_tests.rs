
use assert_cmd::Command;
use predicates::prelude::*;
use std::process::Command as ProcessCommand;

// Helper: Ensure the sample data exists before testing
fn setup_data() {
    let _ = ProcessCommand::new("python3")
        .arg("create_sample.py")
        .output();
    
    // Fallback check: verify file exists
    if !std::path::Path::new("sample.parquet").exists() {
        panic!("Test failed: sample.parquet not found. Please run 'python create_sample.py'");
    }
}

#[test]
fn test_repl_startup_and_meta_commands() -> Result<(), Box<dyn std::error::Error>> {
    setup_data();

    // 1. Start the CLI application
    let bin_path = assert_cmd::cargo::cargo_bin!("query-fuse");
    let mut cmd = Command::new(bin_path);


    // Define the user input sequence: list tables and then exit 
    let input = ".tables\n.exit\n";

    cmd.arg("--input")
        .arg("sample.parquet")
        .write_stdin(input) // Inject our virtual keystrokes
        .assert()
        .success() 
        .stdout(predicate::str::contains("Data loaded successfully")) 
        .stdout(predicate::str::contains("Registered Tables:")) 
        .stdout(predicate::str::contains("sample")); 

    Ok(())
}

#[test]
fn test_sql_execution() -> Result<(), Box<dyn std::error::Error>> {
    setup_data();

    // Start the CLI application
    let bin_path = assert_cmd::cargo::cargo_bin!("query-fuse");
    let mut cmd = Command::new(bin_path);

    // User Input: Run a SQL query for Lagos and then exit
    let input = "SELECT city FROM sample WHERE city = 'Lagos';\n.exit\n";

    cmd.arg("-i")
        .arg("sample.parquet")
        .write_stdin(input)
        .assert()
        .success()
        .stdout(predicate::str::contains("Lagos"))
        .stdout(predicate::str::contains("Kano").not());

    Ok(())
}