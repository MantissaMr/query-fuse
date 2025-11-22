// --- IMPORTS ---
use clap::Parser;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::error::Error;
use std::fs::File;
use std::sync::Arc; 



// --- CLI DEFINITION ---
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The path to the input data file (.parquet, .arrow, .feather).
     #[arg(long, short)]
    input: String,
}

// --- ENTRY POINT ---
#[tokio::main]
async fn main() {
    let args = Cli::parse();
    if let Err(e) = run(args).await {
        eprintln!("Application error: {}", e);
    }
}

// --- CORE LOGIC ---
async fn run(args: Cli) -> Result<(), Box<dyn Error>> {
    // Initialize the Datafusion Context
    let ctx = SessionContext::new();

    // Register the data source 
    println!("Loading data from {}...", args.input);
    
    let file_extension = args.input.split('.').last().unwrap_or("");

    match file_extension {
        "parquet" => {
            ctx.register_parquet("data", &args.input, ParquetReadOptions::default()).await?;
        }
        "arrow" | "feather" => {
            let file = File::open(&args.input)?;
            let reader = FileReader::try_new(file, None)?;

            // Get the schema from the reader before consuming it and collect all batches into a vector
            let schema = reader.schema();
            let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;

            // Use MemTable to register in-memory Arrow data
            let table = MemTable::try_new(schema, vec![batches])?;
            ctx.register_table("data", Arc::new(table))?;
        }
        _ => {
            return Err(format!(
                "Unsupported file type '{}'. Only .parquet, .arrow, and .feather are supported.",
                file_extension
            ).into());
        }
    }

    println!("Data loaded successfully.");
    println!("Table registered as 'data'.");
    println!("Type 'exit' or 'quit' to leave.");

    // Initialize the Interactive Shell 
    let mut rl = DefaultEditor::new()?;

    // REPL Loop
    loop {
        // READ: Print prompt and wait for user input
        let readline = rl.readline("query-fuse > ");

        match readline {
            Ok(line) => {
                // Clean up the input
                let sql = line.trim();
                
                // Handle empty lines (user just hit enter)
                if sql.is_empty() {
                    continue;
                }

                // Check for exit command
                if sql.eq_ignore_ascii_case("exit") || sql.eq_ignore_ascii_case("quit") {
                    break;
                }

                // Add to history (so you can press Up Arrow)
                let _ = rl.add_history_entry(sql);

                // EXECUTE: Run the query against the context
                // We create a separate async task so a bad query doesn't crash the shell
                let df_result = ctx.sql(sql).await;

                match df_result {
                    Ok(df) => {
                        // PRINT: Show the results table
                        if let Err(e) = df.show().await {
                            println!("Error displaying results: {}", e);
                        }
                    }
                    Err(e) => {
                        // If the SQL is bad, print the error but KEEP LOOPING
                        println!("SQL Error: {}", e);
                    }
                }
            },
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            },
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            },
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    Ok(())

}