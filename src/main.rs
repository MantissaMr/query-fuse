// --- IMPORTS ---
use clap::Parser;
use datafusion::prelude::{ParquetReadOptions, SessionContext};

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

   // We now `await` our `run` function because it is asynchronous.
    if let Err(e) = run(args).await {
        eprintln!("Application error: {}", e);
    }
}

// CORE LOGIC 

async fn run (args: Cli) -> Result<(), Box<dyn std::error::Error>> {
    // Create a new Datafusion SessionContext.
    let ctx = SessionContext::new();

    // Register the Parquet file as table 
    ctx.register_parquet("data", &args.input, ParquetReadOptions::default()).await?;

    // Execute the SQL query
    let df = ctx.sql(&args.query).await?;
    
    // Collect and print results 
    df.show().await?;

    
    Ok(())

}