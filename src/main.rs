// --- IMPORTS ---
use clap::Parser;

// --- CLI DEFINITION ---
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The path to the input data file (e.g., /path/to/data.parquet)
    #[arg(long)]
    input: String,

    /// The SQL query to execute against the data file
    #[arg(long)]
    query: String,
}

// --- ENTRY POINT ---
#[tokio::main]
async fn main() {
    let args = Cli::parse();

    // print the arguments back to the user to prove it works
    println!("Input file path: {}", args.input);
    println!("SQL Query to execute: {}", args.query);
}