use std::path::PathBuf;

use clap::{Parser, Subcommand};
use notadb::engine::lsm::LsmEngine;

#[derive(Parser)]
#[command(name = "notadb", about = "A simple LSM-tree key-value store")]
struct Cli {
    /// Directory to store data (default: ~/.notadb)
    #[arg(short, long)]
    dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Set a key to a value
    Put { key: String, value: String },
    /// Get the value for a key
    Get { key: String },
    /// Delete a key
    Delete { key: String },
    /// Compact SSTables
    Compact,
}

fn main() {
    let cli = Cli::parse();

    let dir = cli.dir.unwrap_or_else(|| {
        dirs::home_dir()
            .expect("could not find home directory")
            .join(".notadb")
    });

    let mut engine = LsmEngine::open(&dir).expect("failed to open engine");

    match cli.command {
        Command::Put { key, value } => {
            engine.put(key.as_bytes(), value.as_bytes()).expect("put failed");
            println!("ok");
        }
        Command::Get { key } => match engine.get(key.as_bytes()).expect("get failed") {
            Some(value) => println!("{}", String::from_utf8_lossy(&value)),
            None => println!("(nil)"),
        },
        Command::Delete { key } => {
            engine.delete(key.as_bytes()).expect("delete failed");
            println!("ok");
        }
        Command::Compact => {
            engine.compact().expect("compact failed");
            println!("ok");
        }
    }
}
