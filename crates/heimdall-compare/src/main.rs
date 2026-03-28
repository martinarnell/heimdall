/// heimdall-compare — Benchmark Heimdall geocoder accuracy against Nominatim.
///
/// Subcommands:
///   generate-queries   Sample queries from loaded indices → JSONL
///   run                Execute benchmark from JSONL → SQLite
///   report             Generate report from results database
///   conflicts          Browse conflict cases
///   continuous         Long-running comparison (legacy --run mode)

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "heimdall-compare", about = "Benchmark Heimdall vs Nominatim")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate a benchmark query set from loaded indices
    GenerateQueries {
        /// Index directories to sample from
        #[arg(short, long, num_args = 1..)]
        index: Vec<PathBuf>,

        /// Total queries to generate (excluding variants)
        #[arg(short, long, default_value = "100000")]
        count: usize,

        /// RNG seed for reproducibility
        #[arg(long, default_value = "42")]
        seed: u64,

        /// Output JSONL file path
        #[arg(short, long, default_value = "queries.jsonl")]
        output: PathBuf,
    },

    /// Run benchmark: send queries to both geocoders, store results
    Run {
        /// Path to queries JSONL file
        #[arg(short = 'q', long)]
        queries: PathBuf,

        /// Heimdall server URL
        #[arg(long, default_value = "http://localhost:2399")]
        heimdall_url: String,

        /// Nominatim server URL
        #[arg(long, default_value = "https://nominatim.openstreetmap.org")]
        nominatim_url: String,

        /// Requests per second to Nominatim
        #[arg(long, default_value = "1")]
        rps: f64,

        /// Output SQLite database path
        #[arg(short, long, default_value = "results.sqlite")]
        output: PathBuf,
    },

    /// Generate report from results database
    Report {
        /// Path to results SQLite database
        #[arg(long, default_value = "results.sqlite")]
        db: PathBuf,

        /// Output markdown file (omit for stdout)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// Browse and export conflict cases
    Conflicts {
        /// Path to results SQLite database
        #[arg(long, default_value = "results.sqlite")]
        db: PathBuf,

        /// Filter by country code
        #[arg(long)]
        country: Option<String>,

        /// Minimum distance in meters (default: 2000 = 2km)
        #[arg(long, default_value = "2000")]
        min_distance: f64,

        /// Export to CSV file
        #[arg(long)]
        export_csv: Option<PathBuf>,
    },

    /// Long-running continuous comparison (loads indices, samples on-the-fly)
    Continuous {
        /// Index directories
        #[arg(short, long, num_args = 1..)]
        index: Vec<PathBuf>,

        /// Heimdall server URL
        #[arg(long, default_value = "http://localhost:2399")]
        heimdall_url: String,

        /// Nominatim server URL
        #[arg(long, default_value = "https://nominatim.openstreetmap.org")]
        nominatim_url: String,

        /// Requests per second to Nominatim
        #[arg(long, default_value = "1")]
        rps: f64,

        /// Total queries to sample
        #[arg(long, default_value = "100000")]
        pool_size: usize,

        /// Output SQLite database path
        #[arg(long, default_value = "compare.db")]
        db: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::GenerateQueries {
            index,
            count,
            seed,
            output,
        } => {
            heimdall_compare::generate::generate_queries(&index, count, seed, &output)?;
        }

        Commands::Run {
            queries,
            heimdall_url,
            nominatim_url,
            rps,
            output,
        } => {
            heimdall_compare::runner::run_benchmark(
                &queries,
                &heimdall_url,
                &nominatim_url,
                rps,
                &output,
            )
            .await?;
        }

        Commands::Report { db, output } => {
            heimdall_compare::report::generate_report(&db, output.as_deref())?;
        }

        Commands::Conflicts {
            db,
            country,
            min_distance,
            export_csv,
        } => {
            heimdall_compare::conflicts::show_conflicts(
                &db,
                country.as_deref(),
                min_distance,
                export_csv.as_deref(),
            )?;
        }

        Commands::Continuous {
            index,
            heimdall_url,
            nominatim_url,
            rps,
            pool_size,
            db,
        } => {
            heimdall_compare::continuous::run_continuous(
                &index,
                &heimdall_url,
                &nominatim_url,
                rps,
                pool_size,
                &db,
            )
            .await?;
        }
    }

    Ok(())
}
