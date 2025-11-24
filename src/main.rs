// --- IMPORTS ---
use clap::Parser;
use std::error::Error;
use std::fs::File;
use std::sync::Arc;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use colored::*; 



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

    // Register the data source and table
    println!("Loading data from {}...", args.input);
    
    let file_extension = args.input.split('.').last().unwrap_or("");

    let raw_name = std::path::Path::new(&args.input)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("data");
    // Sanitize table name: replace non-alphanumeric characters with underscores
    let table_name = raw_name.replace(|c: char| !c.is_alphanumeric(), "_");

    match file_extension {
        "parquet" => {
            ctx.register_parquet(&table_name, &args.input, ParquetReadOptions::default()).await?;
        }
        "arrow" | "feather" => {
            let file = File::open(&args.input)?;
            let reader = FileReader::try_new(file, None)?;

            // Get the schema from the reader before consuming it and collect all batches into a vector
            let schema = reader.schema();
            let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;

            // Use MemTable to register in-memory Arrow data
            let table = MemTable::try_new(schema, vec![batches])?;
            ctx.register_table(&table_name, Arc::new(table))?;
        }
        _ => {
            return Err(format!(
                "Unsupported file type '{}'. Only .parquet, .arrow, and .feather are supported.",
                file_extension
            ).into());
        }
    }
    // Welcome Banner
    println!("{}", "Data loaded successfully.".green());
    println!("Table registered as '{}'.", table_name.cyan().bold());
    println!("Type {} for available commands.", ".help".yellow());
    println!("{}", "----------------------------------------".dimmed());

    let prompt = format!("{} ", "query-fuse >".cyan().bold());

    // Initialize the Interactive Shell 
    let mut rl = DefaultEditor::new()?;

    // REPL Loop
    loop {
        // READ: Print prompt and wait for user input
        let readline = rl.readline(&prompt);

        match readline {
            Ok(line) => {
                // Clean up the input
                let input = line.trim();
                
                // Handle empty lines (user just hit enter)
                if input.is_empty() {
                    continue;
                }
        //EVAL: Check for special commands or run SQL
                if input.starts_with('.') {
                    match input {
                        ".exit" | ".quit" => break,
                        ".help" => {
                            println!("{}", "Available commands:".bold());
                            println!("  {}       List all registered tables", ".tables".yellow());
                            println!("  {}  Exit the shell", ".exit, .quit".yellow());
                            println!("  {}         Show this message", ".help".yellow());
                            println!("  {}         Run any SQL query", "<SQL>".green());
                        }
                        ".tables" => {
                            let catalog = ctx.catalog("datafusion").unwrap();
                            let schema = catalog.schema("public").unwrap();
                            let table_names = schema.table_names();
                            
                            println!("{}", "Registered Tables:".bold());
                            for name in table_names {
                                println!("  - {}", name.cyan());
                            }
                        }
                        _ => {
                            println!("{} '{}'. Type {} for instructions.", 
                                "Unknown command:".red(), 
                                input,
                                ".help".yellow()
                            );
                        }
                    } continue;
                }
                if input.eq_ignore_ascii_case("exit") || input.eq_ignore_ascii_case("quit") {
                    break;
                }

                // 
                let _ = rl.add_history_entry(input);

                // Run the query against the context
                let df_result = ctx.sql(input).await; 

        // PRINT: Show results or errors
                match df_result {
                    Ok(df) => {
                        // PRINT: Show the results table
                        if let Err(e) = df.show().await {
                            println!("{} {}", "Error displaying results:".red(), e);
                        }
                    }
                    Err(e) => {
                        // If the SQL is bad, print the error but KEEP LOOPING
                        println!("{} {}", "SQL Error:".red().bold(), e);
                    }
                }
            },
        // LOOP CONTROL: Handle interruptions and EOF
            Err(ReadlineError::Interrupted) => {
                println!("{}", "CTRL-C".red());
                break;
            },
            Err(ReadlineError::Eof) => {
                println!("{}", "CTRL-D".red());
                break;
            },
            Err(err) => {
                println!("{} {:?}", "Error:".red().bold(), err);
                break;
            }
        }
    }

    Ok(())

}