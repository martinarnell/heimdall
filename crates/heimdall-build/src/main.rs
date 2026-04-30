#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// heimdall-build — OSM PBF → Heimdall index
///
/// Pipeline:
///   1. extract  — OSM PBF → raw Parquet (place records + admin hierarchy)
///   2. enrich   — resolve admin hierarchy, compute importance scores
///   3. normalize— generate all name variants per place
///   4. sort     — geohash-sort for delta coordinate encoding
///   5. pack     — write record store (binary) + FSTs
///
/// Usage:
///   heimdall-build build --input sweden-latest.osm.pbf --output ./heimdall-sweden
///   heimdall-build stats --index ./heimdall-sweden

use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use anyhow::{bail, Result};
use tracing::info;

mod extract;
mod enrich;
mod pack;
mod pack_addr;
mod audit;
mod bench;
mod addr_audit;
mod lantmateriet;
mod geonorge;
mod dawa;
mod dvv;
mod photon;
mod lucene;
mod rebuild;
mod ssr;
mod tiger;
mod oa;
mod gnaf;
mod nar;
mod linz;
mod bag;
mod best;
mod ban;
mod lt;
mod ads;
mod vzd;
mod swisstopo;
mod bev;
mod ruian;
mod prg;
mod abr;
mod juso;
mod cnefe;
mod geocache;
mod osc;
mod package;
mod verify;

#[derive(Parser)]
#[command(name = "heimdall-build", about = "Build a Heimdall geocoder index from OSM data")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Full build: OSM PBF → index directory (always uses mmap node cache, ~500MB RAM)
    Build {
        #[arg(short, long)]
        input: PathBuf,

        #[arg(short, long)]
        output: PathBuf,

        /// Skip extraction if Parquet already exists
        #[arg(long)]
        skip_extract: bool,

        /// Only index places with population >= N (0 = all)
        #[arg(long, default_value = "0")]
        min_population: u32,
    },

    /// Build all countries sequentially
    BuildAll {
        /// PBF input and index output pairs: --country input.pbf:output_dir
        #[arg(long, num_args = 1..)]
        country: Vec<String>,

        /// Only index places with population >= N (0 = all)
        #[arg(long, default_value = "0")]
        min_population: u32,
    },

    /// Print statistics about a built index
    Stats {
        #[arg(short, long)]
        index: PathBuf,
    },

    /// Download Lantmäteriet address data and merge with OSM
    Lantmateriet {
        /// Path to index directory (must contain addresses.parquet from prior build)
        #[arg(short, long)]
        index: PathBuf,

        /// Lantmäteriet username
        #[arg(long, env = "LM_USER")]
        lm_user: String,

        /// Lantmäteriet password
        #[arg(long, env = "LM_PASS")]
        lm_pass: String,
    },

    /// Merge Kartverket/Geonorge addresses with existing OSM addresses
    MergeAddresses {
        /// Path to index directory (must contain addresses.parquet)
        #[arg(short, long)]
        index: PathBuf,

        /// Path to Kartverket address CSV
        #[arg(long)]
        kartverket_csv: PathBuf,
    },

    /// Merge DAWA (Danish) addresses with existing OSM addresses
    MergeDawa {
        /// Path to index directory (must contain addresses.parquet)
        #[arg(short, long)]
        index: PathBuf,

        /// Path to DAWA address CSV (or .csv.gz)
        #[arg(long)]
        dawa_csv: PathBuf,
    },

    /// Download DVV (Finnish) addresses and merge with existing OSM addresses
    MergeDvv {
        /// Path to index directory (must contain addresses.parquet)
        #[arg(short, long)]
        index: PathBuf,
    },

    /// Merge SSR (Kartverket place names) GML into existing OSM places
    MergeSsr {
        /// Path to index directory (must contain places.parquet)
        #[arg(short, long)]
        index: PathBuf,

        /// Path to SSR GML file
        #[arg(long)]
        gml: PathBuf,
    },

    /// Merge Photon JSONL dump into an existing index (places + addresses)
    MergePhoton {
        /// Path to index directory (must contain places.parquet / addresses.parquet)
        #[arg(long)]
        index: PathBuf,

        /// Path to Photon .jsonl.zst dump file
        #[arg(long)]
        input: PathBuf,
    },

    /// Import a Photon JSONL dump (e.g. UK from Graphhopper)
    PhotonImport {
        /// Path to Photon .jsonl.zst dump file
        #[arg(short, long)]
        input: PathBuf,

        /// Output index directory
        #[arg(short, long)]
        output: PathBuf,

        /// Filter by ISO country code (e.g. GB, DE). Only records matching this code are imported.
        #[arg(long)]
        country: Option<String>,
    },

    /// Audit address data in an OSM PBF file
    AddrAudit {
        /// Path to OSM PBF file
        #[arg(short, long)]
        input: PathBuf,
    },

    /// Generate benchmark queries from the Parquet data
    GenQueries {
        /// Path to index directory (must contain places.parquet)
        #[arg(short, long)]
        index: PathBuf,

        /// Output file for queries (one per line)
        #[arg(short, long)]
        output: PathBuf,

        /// Number of queries to generate
        #[arg(short, long, default_value = "250")]
        count: usize,
    },

    /// Benchmark Heimdall vs Nominatim accuracy
    Bench {
        /// File with one query per line
        #[arg(short, long)]
        queries: PathBuf,

        /// Heimdall server URL
        #[arg(long, default_value = "http://127.0.0.1:2399")]
        heimdall_url: String,

        /// Output CSV path for detailed results
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// Import TIGER/Line 2025 shapefiles: admin boundaries + ZIP codes for US
    TigerImport {
        /// Output directory for TIGER data (e.g. data/index-us)
        #[arg(short, long)]
        output: PathBuf,
    },

    /// Import OpenAddresses US address data
    OaImport {
        /// Output index directory (e.g. data/index-us)
        #[arg(short, long)]
        output: PathBuf,

        /// Local directory with .geojson/.csv files (skip download)
        #[arg(long)]
        local: Option<PathBuf>,
    },

    /// Import G-NAF (Australian) addresses from ZIP and merge with existing addresses
    GnafImport {
        /// Path to index directory (must contain addresses.parquet)
        #[arg(short, long)]
        index: PathBuf,

        /// Path to G-NAF ZIP file
        #[arg(long)]
        gnaf_zip: PathBuf,
    },

    /// Import NAR (Canadian) addresses from ZIP and merge with existing addresses
    NarImport {
        /// Path to index directory (must contain addresses.parquet)
        #[arg(short, long)]
        index: PathBuf,

        /// Path to NAR ZIP file
        #[arg(long)]
        nar_zip: PathBuf,
    },

    /// Import LINZ (New Zealand) addresses from GeoPackage and merge with existing addresses.
    /// Either provide --linz-gpkg for a pre-downloaded file, or --token to download from LINZ API.
    LinzImport {
        /// Path to index directory (must contain addresses.parquet)
        #[arg(short, long)]
        index: PathBuf,

        /// Path to pre-downloaded LINZ GeoPackage (.gpkg) file
        #[arg(long, required_unless_present = "token")]
        linz_gpkg: Option<PathBuf>,

        /// LINZ Data Service API token (downloads layer 105689 as GeoPackage)
        #[arg(long, env = "LINZ_API_KEY")]
        token: Option<String>,
    },

    /// Import BAG (Dutch) addresses from NLExtract CSV and merge with existing addresses
    BagImport {
        #[arg(short, long)]
        index: PathBuf,
        /// Path to NLExtract BAG CSV file (.csv or .csv.gz)
        #[arg(long)]
        bag_csv: PathBuf,
    },

    /// Import BeST (Belgian) addresses from regional ZIP files and merge with existing addresses
    BestImport {
        #[arg(short, long)]
        index: PathBuf,
        /// Path(s) to BeST regional ZIP files (Flanders, Brussels, Wallonia)
        #[arg(long, num_args = 1..)]
        best_zip: Vec<PathBuf>,
    },

    /// Import BAN (French) addresses from département CSVs and merge with existing addresses
    BanImport {
        #[arg(short, long)]
        index: PathBuf,
        /// Directory containing downloaded BAN .csv.gz files (or downloads them)
        #[arg(long)]
        ban_dir: Option<PathBuf>,
    },

    /// Import Lithuanian addresses from govlt SQLite and merge with existing addresses
    LtImport {
        #[arg(short, long)]
        index: PathBuf,
        /// Path to boundaries.sqlite
        #[arg(long)]
        sqlite: PathBuf,
    },

    /// Import Estonian ADS addresses from CSV and merge with existing addresses
    AdsImport {
        #[arg(short, long)]
        index: PathBuf,
        /// Path to ADS CSV export
        #[arg(long)]
        ads_csv: PathBuf,
    },

    /// Import Latvian VZD addresses from CSV and merge with existing addresses
    VzdImport {
        #[arg(short, long)]
        index: PathBuf,
        /// Path to VZD address CSV
        #[arg(long)]
        vzd_csv: PathBuf,
    },

    /// Import swisstopo (Swiss) addresses from CSVs and merge with existing addresses
    SwisstopoImport {
        #[arg(short, long)]
        index: PathBuf,
        /// Path to ADRESSE.csv
        #[arg(long)]
        addresses: PathBuf,
        /// Path to STRASSE.csv
        #[arg(long)]
        streets: PathBuf,
        /// Path to ORTSCHAFT.csv
        #[arg(long)]
        localities: PathBuf,
    },

    /// Import BEV (Austrian) addresses from extracted CSV directory and merge with existing addresses
    BevImport {
        #[arg(short, long)]
        index: PathBuf,
        /// Path to extracted BEV directory containing ADRESSE.csv, STRASSE.csv, GEMEINDE.csv
        #[arg(long)]
        bev_dir: PathBuf,
    },

    /// Import RÚIAN (Czech) addresses from CSV and merge with existing addresses
    RuianImport {
        #[arg(short, long)]
        index: PathBuf,
        /// Path to RÚIAN address CSV
        #[arg(long)]
        ruian_csv: PathBuf,
    },

    /// Import PRG (Polish) addresses from GML/CSV and merge with existing addresses
    PrgImport {
        #[arg(short, long)]
        index: PathBuf,
        /// Path to PRG address data (CSV or GML/ZIP)
        #[arg(long)]
        prg_input: PathBuf,
    },

    /// Import ABR (Japanese) addresses from abr-geocoder SQLite and merge with existing addresses
    AbrImport {
        #[arg(short, long)]
        index: PathBuf,
        /// Path to abr-geocoder SQLite database
        #[arg(long)]
        sqlite: PathBuf,
    },

    /// Import juso.go.kr (Korean) road-name addresses and merge with existing addresses
    JusoImport {
        #[arg(short, long)]
        index: PathBuf,
        /// Directory containing juso.go.kr data files (TXT or ZIP)
        #[arg(long)]
        juso_dir: PathBuf,
    },

    /// Import CNEFE (Brazilian) addresses from state ZIPs/CSVs and merge with existing addresses
    CnefeImport {
        #[arg(short, long)]
        index: PathBuf,
        /// Directory containing CNEFE state ZIP files or extracted CSVs
        #[arg(long)]
        cnefe_dir: PathBuf,
    },

    /// Package built indices into distributable .tar.zst tarballs with manifest.json
    Package {
        /// Index directories to package (can be repeated)
        #[arg(short, long, num_args = 1..)]
        index: Vec<PathBuf>,

        /// Output directory for tarballs and manifest
        #[arg(short, long, default_value = "dist")]
        output: PathBuf,

        /// Version string (default: date-based YYYY.MM.DD)
        #[arg(long)]
        version: Option<String>,

        /// Create bundle tarballs (nordic, europe, world) in addition to per-country
        #[arg(long)]
        bundles: bool,

        /// Base URL where tarballs will be hosted (for manifest.json)
        #[arg(long, default_value = "https://github.com/martinarnell/heimdall/releases/latest/download")]
        base_url: String,
    },

    /// Automated rebuild: detect changes, download, and rebuild affected country indices.
    /// Uses mmap node cache (~1-2 GB RAM). Downloads are kept for reuse by default.
    Rebuild {
        /// Config file with country/source definitions
        #[arg(long, default_value = "data/sources.toml")]
        config: PathBuf,

        /// State tracking file (persisted between runs)
        #[arg(long, default_value = "data/rebuild-state.json")]
        state_file: PathBuf,

        /// Comma-separated country codes to rebuild (default: all)
        #[arg(long)]
        country: Option<String>,

        /// Force redownload everything, ignoring change detection
        #[arg(long)]
        redownload: bool,

        /// Show what would change without doing anything
        #[arg(long)]
        dry_run: bool,

        /// Skip downloading — reuse existing files, fail if missing
        #[arg(long)]
        skip_download: bool,

        /// Delete downloaded sources after building (saves disk, but forces re-download next time)
        #[arg(long)]
        cleanup: bool,

        /// Only index places with population >= N (0 = all)
        #[arg(long, default_value = "0")]
        min_population: u32,

        /// Number of parallel country builds (default: 1 = sequential)
        #[arg(long, default_value = "1")]
        jobs: usize,

        /// Maximum RAM budget in GB for parallel builds (0 = unlimited)
        #[arg(long, default_value = "0")]
        ram_budget: u64,
    },

    /// Download all sources without building (pre-fetch for later rebuild)
    Download {
        /// Config file with country/source definitions
        #[arg(long, default_value = "data/sources.toml")]
        config: PathBuf,

        /// State tracking file (persisted between runs)
        #[arg(long, default_value = "data/rebuild-state.json")]
        state_file: PathBuf,

        /// Comma-separated country codes to download (default: all)
        #[arg(long)]
        country: Option<String>,

        /// Force redownload everything, ignoring change detection
        #[arg(long)]
        redownload: bool,

        /// Show what would be downloaded without doing anything
        #[arg(long)]
        dry_run: bool,
    },

    /// Build a geocoding cache from existing Parquet files (for incremental diff updates)
    BuildGeocache {
        /// Path to index directory (must contain places.parquet or addresses*.parquet)
        #[arg(short, long)]
        index: PathBuf,

        /// Output file path (default: {index}/geocache.bin)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// OSM replication sequence number to store in cache header
        #[arg(long, default_value = "0")]
        sequence: u64,
    },

    /// Apply OSM .osc.gz diffs to existing parquet files (incremental update)
    ApplyDiffs {
        /// Path to index directory (must contain places.parquet)
        #[arg(short, long)]
        index: PathBuf,

        /// Path(s) to local .osc.gz diff file(s) to apply (in order)
        #[arg(long, num_args = 1..)]
        diff: Vec<PathBuf>,
    },

    /// Dump complete index contents to deterministic text files (for verification)
    DumpIndex {
        /// Path to the index directory to dump
        #[arg(short, long)]
        index: PathBuf,

        /// Output directory for dump files
        #[arg(short, long)]
        output: PathBuf,
    },

    /// Verify two indices are equivalent (no data loss)
    Verify {
        /// Path to the baseline (reference) index directory
        #[arg(long)]
        baseline: PathBuf,

        /// Path to the experimental index directory
        #[arg(long)]
        experiment: PathBuf,

        /// Optional directory to store dump files (for inspection). Uses temp dir if omitted.
        #[arg(long)]
        dump_dir: Option<PathBuf>,

        /// Run functional query verification (slower, but catches query-path regressions)
        #[arg(long)]
        functional: bool,

        /// Number of queries for functional verification (default 10000)
        #[arg(long, default_value = "10000")]
        query_count: usize,

        /// File with queries (one per line) for functional verification
        #[arg(long)]
        queries: Option<PathBuf>,
    },

    /// Repack existing indices: convert V2 records.bin → V3 (LZ4 block-compressed)
    /// and V4 addr_streets.bin → V5 (LZ4 block-compressed). Avoids a full rebuild.
    Repack {
        /// Index directories to repack
        #[arg(short, long, num_args = 1..)]
        index: Vec<PathBuf>,

        /// Repack all indices in data directory
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Country codes (comma-separated)
        #[arg(long, value_delimiter = ',')]
        country: Vec<String>,

        /// Keep backup files (.bak)
        #[arg(long, default_value = "true")]
        keep_backup: bool,
    },

    /// Build a global name FST from all per-country indices (single FST for all countries)
    BuildGlobalFst {
        /// Data directory containing index-* subdirectories
        #[arg(long, default_value = "data")]
        data_dir: PathBuf,

        /// Output directory for global FST files
        #[arg(long)]
        output: Option<PathBuf>,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Build {
            input,
            output,
            skip_extract,
            min_population,
        } => {
            build_country(&input, &output, skip_extract, min_population)?;
        }

        Commands::BuildAll {
            country,
            min_population,
        } => {
            let pairs: Vec<(PathBuf, PathBuf)> = country.iter()
                .map(|spec| {
                    let parts: Vec<&str> = spec.splitn(2, ':').collect();
                    if parts.len() != 2 {
                        anyhow::bail!("Invalid --country format: '{}' (expected input.pbf:output_dir)", spec);
                    }
                    Ok((PathBuf::from(parts[0]), PathBuf::from(parts[1])))
                })
                .collect::<Result<Vec<_>>>()?;

            info!("Building {} countries in parallel...", pairs.len());
            let start = std::time::Instant::now();

            let handles: Vec<_> = pairs.into_iter()
                .map(|(input, output)| {
                    std::thread::spawn(move || {
                        build_country(&input, &output, false, min_population)
                    })
                })
                .collect();

            let mut errors = Vec::new();
            for handle in handles {
                match handle.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => errors.push(e),
                    Err(_) => errors.push(anyhow::anyhow!("Thread panicked")),
                }
            }

            let elapsed = start.elapsed();
            if errors.is_empty() {
                info!("All countries built in {:.1}s", elapsed.as_secs_f64());
            } else {
                for e in &errors {
                    tracing::error!("Build failed: {}", e);
                }
                anyhow::bail!("{} build(s) failed", errors.len());
            }
        }

        Commands::Lantmateriet { index, lm_user, lm_pass } => {
            info!("Downloading Lantmäteriet address data...");

            let lm_dir = index.join("lantmateriet");
            let rt = tokio::runtime::Runtime::new()?;
            let gpkg_files = rt.block_on(
                lantmateriet::download_all(&lm_dir, &lm_user, &lm_pass)
            )?;

            info!("Extracting addresses from {} GeoPackage files...", gpkg_files.len());
            let mut lm_addresses: Vec<extract::RawAddress> = Vec::new();
            for (i, gpkg) in gpkg_files.iter().enumerate() {
                if (i + 1) % 50 == 0 || i == 0 {
                    info!("[{}/{}] Reading {}...", i + 1, gpkg_files.len(), gpkg.display());
                }
                match lantmateriet::read_geopackage(gpkg) {
                    Ok(addrs) => lm_addresses.extend(addrs),
                    Err(e) => tracing::warn!("Failed to read {}: {}", gpkg.display(), e),
                }
            }
            info!("Extracted {} Lantmäteriet addresses", lm_addresses.len());

            // Read existing OSM addresses from Parquet
            let osm_parquet = index.join("addresses.parquet");
            let osm_addresses = if osm_parquet.exists() {
                info!("Reading existing OSM addresses...");
                read_osm_addresses(&osm_parquet)?
            } else {
                info!("No existing addresses.parquet found");
                vec![]
            };

            // Merge
            let merged = lantmateriet::merge_addresses(&osm_addresses, &lm_addresses);

            // Write merged Parquet (overwrite)
            info!("Writing merged addresses.parquet...");
            write_merged_addresses(&merged, &osm_parquet)?;
            info!(
                "Done! {} total addresses ({} OSM + {} Lantmäteriet new)",
                merged.len(),
                osm_addresses.len(),
                merged.len() - osm_addresses.len(),
            );
            info!("Run 'build --skip-extract' to rebuild the index with merged addresses.");
        }

        Commands::MergeAddresses { index, kartverket_csv } => {
            info!("Reading Kartverket addresses...");
            let kv_addresses = geonorge::read_kartverket_addresses(&kartverket_csv)?;

            let osm_parquet = index.join("addresses.parquet");
            let osm_addresses = if osm_parquet.exists() {
                info!("Reading existing OSM addresses...");
                read_osm_addresses(&osm_parquet)?
            } else {
                vec![]
            };

            let merged = lantmateriet::merge_addresses(&osm_addresses, &kv_addresses);

            info!("Writing merged addresses.parquet...");
            write_merged_addresses(&merged, &osm_parquet)?;
            info!(
                "Done! {} total ({} OSM + {} Kartverket new). Run 'build --skip-extract' to rebuild.",
                merged.len(), osm_addresses.len(), merged.len() - osm_addresses.len(),
            );
        }

        Commands::MergeDawa { index, dawa_csv } => {
            info!("Reading DAWA addresses...");
            let dawa_addresses = dawa::read_dawa_addresses(&dawa_csv)?;

            let osm_parquet = index.join("addresses.parquet");
            let osm_addresses = if osm_parquet.exists() {
                info!("Reading existing OSM addresses...");
                read_osm_addresses(&osm_parquet)?
            } else {
                vec![]
            };

            let merged = lantmateriet::merge_addresses(&osm_addresses, &dawa_addresses);

            info!("Writing merged addresses.parquet...");
            write_merged_addresses(&merged, &osm_parquet)?;
            info!(
                "Done! {} total ({} OSM + {} DAWA new). Run 'build --skip-extract' to rebuild.",
                merged.len(), osm_addresses.len(), merged.len() - osm_addresses.len(),
            );
        }

        Commands::MergeDvv { index } => {
            info!("Downloading DVV addresses from OGC API...");
            let dvv_addresses = dvv::download_dvv_addresses()?;

            let osm_parquet = index.join("addresses.parquet");
            let osm_addresses = if osm_parquet.exists() {
                info!("Reading existing OSM addresses...");
                read_osm_addresses(&osm_parquet)?
            } else {
                vec![]
            };

            let merged = lantmateriet::merge_addresses(&osm_addresses, &dvv_addresses);

            info!("Writing merged addresses.parquet...");
            write_merged_addresses(&merged, &osm_parquet)?;
            info!(
                "Done! {} total ({} OSM + {} DVV new). Run 'build --skip-extract' to rebuild.",
                merged.len(), osm_addresses.len(), merged.len() - osm_addresses.len(),
            );
        }

        Commands::MergeSsr { index, gml } => {
            let ssr_places = ssr::read_ssr_places(&gml)?;

            let places_parquet = index.join("places.parquet");
            let osm_places = if places_parquet.exists() {
                info!("Reading existing places...");
                read_osm_places(&places_parquet)?
            } else {
                vec![]
            };

            let merged = ssr::merge_ssr_places(&osm_places, &ssr_places);

            info!("Writing merged places.parquet...");
            photon::write_places_parquet(&merged, &places_parquet)?;
            info!(
                "Done! {} total ({} existing + {} SSR new). Run 'build --skip-extract' to rebuild.",
                merged.len(), osm_places.len(), merged.len() - osm_places.len(),
            );
        }

        Commands::MergePhoton { index, input } => {
            // Determine input type: .tar.bz2 (ES dump) or .jsonl.zst (Nominatim export)
            let input_str = input.to_string_lossy();
            let photon_data = if input_str.ends_with(".tar.bz2") || input_str.ends_with(".tar") {
                // Elasticsearch/Lucene dump from Graphhopper
                info!("Extracting Photon Elasticsearch dump...");
                let extract_dir = index.join("photon_extract");
                std::fs::create_dir_all(&extract_dir)?;

                let status = std::process::Command::new("tar")
                    .arg("xjf")
                    .arg(&input)
                    .arg("-C")
                    .arg(&extract_dir)
                    .status()?;
                if !status.success() {
                    bail!("tar extraction failed");
                }

                // Find the Lucene index directory inside the extracted data
                let lucene_dir = find_lucene_index_dir(&extract_dir)?;
                info!("Reading Lucene index from {}", lucene_dir.display());

                let json_docs = lucene::read_all_json(&lucene_dir)?;
                info!("Read {} documents from Lucene index", json_docs.len());

                let result = photon::parse_es_documents(&json_docs);

                // Clean up extracted files
                std::fs::remove_dir_all(&extract_dir).ok();

                result
            } else {
                // JSONL format (Nominatim export)
                info!("Parsing Photon JSONL dump...");
                photon::parse(&input)?
            };

            info!(
                "Photon: {} places, {} addresses",
                photon_data.places.len(),
                photon_data.addresses.len(),
            );

            // Merge addresses
            let addr_parquet = index.join("addresses.parquet");
            let osm_addresses = if addr_parquet.exists() {
                info!("Reading existing addresses...");
                read_osm_addresses(&addr_parquet)?
            } else {
                vec![]
            };

            let merged_addresses =
                lantmateriet::merge_addresses(&osm_addresses, &photon_data.addresses);

            info!("Writing merged addresses.parquet...");
            write_merged_addresses(&merged_addresses, &addr_parquet)?;

            // Merge places
            let places_parquet = index.join("places.parquet");
            let osm_places = if places_parquet.exists() {
                info!("Reading existing places...");
                read_osm_places(&places_parquet)?
            } else {
                vec![]
            };

            let merged_places = merge_places(&osm_places, &photon_data.places);

            info!("Writing merged places.parquet...");
            photon::write_places_parquet(&merged_places, &places_parquet)?;

            info!(
                "Done! {} places (+{}), {} addresses (+{}). Run 'build --skip-extract' to rebuild.",
                merged_places.len(),
                merged_places.len() - osm_places.len(),
                merged_addresses.len(),
                merged_addresses.len() - osm_addresses.len(),
            );
        }

        Commands::PhotonImport { input, output, .. } => {
            info!("Importing Photon dump...");
            let result = photon::import(&input, &output)?;
            info!(
                "Imported {} places + {} addresses ({} admin regions)",
                result.place_count, result.address_count, result.admin_count,
            );

            // Copy normalizer TOML
            let dest_toml = output.join("sv.toml");
            if !dest_toml.exists() {
                let source = find_normalizer_toml(&output);
                if source.exists() && source != dest_toml {
                    std::fs::copy(&source, &dest_toml)?;
                    info!("Copied normalizer {} → sv.toml", source.display());
                }
            }

            // Pack places (FSTs + record store)
            info!("Packing places...");
            let enriched = enrich::EnrichResult {
                admin_count: result.admin_count,
            };
            let places_parquet = output.join("places.parquet");
            let stats = pack::pack(&places_parquet, &output, &enriched)?;

            // Pack addresses
            info!("Packing addresses...");
            let normalizer = {
                let toml_path = find_normalizer_toml(&output);
                if toml_path.exists() {
                    heimdall_normalize::Normalizer::from_config(&toml_path)
                } else {
                    heimdall_normalize::Normalizer::swedish()
                }
            };
            let addr_parquet = output.join("addresses.parquet");
            let admin_map_path = output.join("admin_map.bin");
            let addr_stats = pack_addr::pack_addresses(
                &[addr_parquet.as_path()], &output, &admin_map_path, &normalizer,
            )?;

            // Write meta.json
            let meta = serde_json::json!({
                "version": 2,
                "source": input.display().to_string(),
                "source_type": "photon",
                "record_count": stats.record_count,
                "address_count": addr_stats.address_count,
                "fst_exact_bytes": stats.fst_exact_bytes,
                "fst_phonetic_bytes": stats.fst_phonetic_bytes,
                "fst_addr_bytes": addr_stats.fst_bytes,
                "record_store_bytes": stats.record_store_bytes,
                "addr_record_bytes": addr_stats.record_bytes,
                "built_at": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            });
            std::fs::write(
                output.join("meta.json"),
                serde_json::to_string_pretty(&meta)?,
            )?;

            // Clean up build-only files not needed for serving
            for name in &["admin_polygons.bin", "admin_map.bin"] {
                let p = output.join(name);
                if p.exists() {
                    let sz = std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
                    if let Ok(()) = std::fs::remove_file(&p) {
                        info!("Deleted {} ({:.1} MB freed)", name, sz as f64 / 1e6);
                    }
                }
            }

            let place_total = stats.fst_exact_bytes
                + stats.fst_phonetic_bytes
                + stats.fst_ngram_bytes
                + stats.record_store_bytes;
            let addr_total = addr_stats.fst_bytes + addr_stats.record_bytes;

            info!("Build complete!");
            info!("  Places:    {}", stats.record_count);
            info!("  Addresses: {} ({} streets)", addr_stats.address_count, addr_stats.street_count);
            info!("  TOTAL:     {:.1} MB", (place_total + addr_total) as f64 / 1e6);
        }

        Commands::AddrAudit { input } => {
            addr_audit::audit_addresses(&input)?;
        }

        Commands::Stats { index } => {
            let parquet_path = index.join("places.parquet");
            if parquet_path.exists() {
                audit::audit_parquet(&parquet_path)?;
            } else {
                info!("No places.parquet found at {}", index.display());
            }
        }

        Commands::GenQueries { index, output, count } => {
            let parquet_path = index.join("places.parquet");
            bench::generate_queries(&parquet_path, &output, count)?;
        }

        Commands::Bench { queries, heimdall_url, output } => {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(bench::run_benchmark(
                &queries,
                &heimdall_url,
                output.as_deref(),
            ))?;
        }

        Commands::TigerImport { output } => {
            info!("Starting TIGER/Line 2025 import...");
            let result = tiger::run_tiger_import(&output)?;
            info!("TIGER import complete!");
            info!("  States:  {}", result.state_count);
            info!("  Counties: {}", result.county_count);
            info!("  Places:  {}", result.place_count);
            info!("  ZIPs:    {}", result.zip_count);
        }

        Commands::OaImport { output, local } => {
            if let Some(local_dir) = local {
                info!("Importing OpenAddresses from local directory: {}", local_dir.display());
                let result = oa::run_oa_import_local(&local_dir, &output)?;
                info!("OA import complete! {} addresses", result.address_count);
            } else {
                info!("Downloading and importing OpenAddresses US data...");
                let result = oa::run_oa_import(&output)?;
                info!("OA import complete! {} addresses", result.address_count);
            }
            info!("Run 'build --skip-extract' or pack_addr to build the address index.");
        }

        Commands::GnafImport { index, gnaf_zip } => {
            info!("Reading G-NAF addresses from {}...", gnaf_zip.display());
            let gnaf_addresses = gnaf::read_gnaf_addresses(&gnaf_zip)?;

            let osm_parquet = index.join("addresses.parquet");
            let osm_addresses = if osm_parquet.exists() {
                info!("Reading existing addresses...");
                read_osm_addresses(&osm_parquet)?
            } else {
                vec![]
            };

            let merged = lantmateriet::merge_addresses(&osm_addresses, &gnaf_addresses);

            info!("Writing merged addresses.parquet...");
            write_merged_addresses(&merged, &osm_parquet)?;
            info!(
                "Done! {} total ({} existing + {} G-NAF new). Run 'build --skip-extract' to rebuild.",
                merged.len(), osm_addresses.len(), merged.len() - osm_addresses.len(),
            );
            info!("Run 'build --skip-extract' or pack_addr to build the address index.");
        }

        Commands::NarImport { index, nar_zip } => {
            info!("Reading NAR addresses from {}...", nar_zip.display());
            let nar_addresses = nar::read_nar_addresses(&nar_zip)?;

            let osm_parquet = index.join("addresses.parquet");
            let osm_addresses = if osm_parquet.exists() {
                info!("Reading existing addresses...");
                read_osm_addresses(&osm_parquet)?
            } else {
                vec![]
            };

            let merged = lantmateriet::merge_addresses(&osm_addresses, &nar_addresses);

            info!("Writing merged addresses.parquet...");
            write_merged_addresses(&merged, &osm_parquet)?;
            info!(
                "Done! {} total ({} existing + {} NAR new). Run 'build --skip-extract' to rebuild.",
                merged.len(), osm_addresses.len(), merged.len() - osm_addresses.len(),
            );
            info!("Run 'build --skip-extract' or pack_addr to build the address index.");
        }

        Commands::LinzImport { index, linz_gpkg, token } => {
            // Resolve the GeoPackage: either pre-downloaded or download via LINZ API
            let gpkg_path = match (linz_gpkg, token) {
                (Some(path), _) => {
                    info!("Using pre-downloaded LINZ GeoPackage: {}", path.display());
                    path
                }
                (None, Some(api_key)) => {
                    info!("Downloading LINZ NZ Addresses via Data Service API...");
                    let download_dir = index.parent().unwrap_or(Path::new(".")).join("downloads");
                    linz::download_linz_gpkg(&api_key, &download_dir)?
                }
                (None, None) => {
                    bail!("Provide either --linz-gpkg <path> or --token <LINZ_API_KEY>");
                }
            };

            let linz_addresses = linz::read_linz_addresses(&gpkg_path)?;

            let osm_parquet = index.join("addresses.parquet");
            let osm_addresses = if osm_parquet.exists() {
                info!("Reading existing addresses...");
                read_osm_addresses(&osm_parquet)?
            } else {
                vec![]
            };

            let merged = lantmateriet::merge_addresses(&osm_addresses, &linz_addresses);

            info!("Writing merged addresses.parquet...");
            write_merged_addresses(&merged, &osm_parquet)?;
            info!(
                "Done! {} total ({} existing + {} LINZ new). Run 'build --skip-extract' to rebuild.",
                merged.len(), osm_addresses.len(), merged.len() - osm_addresses.len(),
            );
            info!("Run 'build --skip-extract' or pack_addr to build the address index.");
        }

        Commands::BagImport { index, bag_csv } => {
            info!("Reading BAG addresses from {}...", bag_csv.display());
            let new_addresses = bag::read_bag_addresses(&bag_csv)?;
            merge_and_write_addresses("BAG", &index, &new_addresses)?;
        }

        Commands::BestImport { index, best_zip } => {
            let paths: Vec<&Path> = best_zip.iter().map(|p| p.as_path()).collect();
            info!("Reading BeST addresses from {} ZIP files...", paths.len());
            let new_addresses = best::read_best_addresses(&paths)?;
            merge_and_write_addresses("BeST", &index, &new_addresses)?;
        }

        Commands::BanImport { index, ban_dir } => {
            let dir = ban_dir.unwrap_or_else(|| index.join("ban_downloads"));
            info!("Reading BAN addresses from {}...", dir.display());
            let new_addresses = if dir.exists() {
                ban::read_ban_addresses(&dir)?
            } else {
                std::fs::create_dir_all(&dir)?;
                ban::download_ban_addresses(&dir)?
            };
            merge_and_write_addresses("BAN", &index, &new_addresses)?;
        }

        Commands::LtImport { index, sqlite } => {
            info!("Reading Lithuanian addresses from {}...", sqlite.display());
            let new_addresses = lt::read_lt_addresses(&sqlite)?;
            merge_and_write_addresses("LT", &index, &new_addresses)?;
        }

        Commands::AdsImport { index, ads_csv } => {
            info!("Reading Estonian ADS addresses from {}...", ads_csv.display());
            let new_addresses = ads::read_ads_addresses(&ads_csv)?;
            merge_and_write_addresses("ADS", &index, &new_addresses)?;
        }

        Commands::VzdImport { index, vzd_csv } => {
            info!("Reading Latvian VZD addresses from {}...", vzd_csv.display());
            let new_addresses = vzd::read_vzd_addresses(&vzd_csv)?;
            merge_and_write_addresses("VZD", &index, &new_addresses)?;
        }

        Commands::SwisstopoImport { index, addresses, streets, localities } => {
            info!("Reading swisstopo addresses...");
            let new_addresses = swisstopo::read_swisstopo_addresses(&addresses, &streets, &localities)?;
            merge_and_write_addresses("swisstopo", &index, &new_addresses)?;
        }

        Commands::BevImport { index, bev_dir } => {
            info!("Reading BEV addresses from {}...", bev_dir.display());
            let new_addresses = bev::read_bev_addresses(&bev_dir)?;
            merge_and_write_addresses("BEV", &index, &new_addresses)?;
        }

        Commands::RuianImport { index, ruian_csv } => {
            info!("Reading RÚIAN addresses from {}...", ruian_csv.display());
            let new_addresses = ruian::read_ruian_addresses(&ruian_csv)?;
            merge_and_write_addresses("RÚIAN", &index, &new_addresses)?;
        }

        Commands::PrgImport { index, prg_input } => {
            info!("Reading PRG addresses from {}...", prg_input.display());
            let new_addresses = prg::read_prg_addresses(&prg_input)?;
            merge_and_write_addresses("PRG", &index, &new_addresses)?;
        }

        Commands::AbrImport { index, sqlite } => {
            info!("Reading ABR addresses from {}...", sqlite.display());
            let new_addresses = abr::read_abr_addresses(&sqlite)?;
            merge_and_write_addresses("ABR", &index, &new_addresses)?;
        }

        Commands::JusoImport { index, juso_dir } => {
            info!("Reading juso.go.kr addresses from {}...", juso_dir.display());
            let new_addresses = juso::read_juso_addresses(&juso_dir)?;
            merge_and_write_addresses("juso", &index, &new_addresses)?;
        }

        Commands::CnefeImport { index, cnefe_dir } => {
            info!("Reading CNEFE addresses from {}...", cnefe_dir.display());
            let new_addresses = cnefe::read_cnefe_addresses(&cnefe_dir)?;
            merge_and_write_addresses("CNEFE", &index, &new_addresses)?;
        }

        Commands::Package {
            index,
            output,
            version,
            bundles,
            base_url,
        } => {
            let ver = version.unwrap_or_else(|| {
                // Date-based version without chrono dependency
                let secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                // Good enough date calculation
                let days = secs / 86400;
                let y = (days * 400 / 146097) + 1970; // approximate
                let doy = days - ((y - 1970) * 365 + (y - 1969) / 4 - (y - 1901) / 100 + (y - 1601) / 400);
                let m = (doy * 12 + 6) / 367 + 1;
                let d = doy - (367 * m - 362) / 12 + 1;
                format!("{}.{:02}.{:02}", y, m.min(12), d.clamp(1, 31))
            });
            // If no --index flags, auto-discover data/index-*
            let index_dirs = if index.is_empty() {
                let mut dirs: Vec<PathBuf> = std::fs::read_dir("data")
                    .ok()
                    .into_iter()
                    .flatten()
                    .filter_map(|e| e.ok())
                    .filter(|e| {
                        e.file_name().to_str().map(|n| n.starts_with("index-")).unwrap_or(false)
                            && e.path().is_dir()
                    })
                    .map(|e| e.path())
                    .collect();
                dirs.sort();
                if dirs.is_empty() {
                    anyhow::bail!("No index directories found in data/. Use --index to specify.");
                }
                info!("Auto-discovered {} index directories", dirs.len());
                dirs
            } else {
                index
            };
            package::package(&index_dirs, &output, &ver, bundles, &base_url)?;
        }

        Commands::Rebuild {
            config,
            state_file,
            country,
            redownload,
            dry_run,
            skip_download,
            cleanup,
            min_population,
            jobs,
            ram_budget,
        } => {
            rebuild::run_rebuild(
                &config,
                &state_file,
                country.as_deref(),
                redownload,
                dry_run,
                skip_download,
                cleanup,
                min_population,
                jobs,
                ram_budget,
            )?;
        }

        Commands::Download {
            config,
            state_file,
            country,
            redownload,
            dry_run,
        } => {
            rebuild::run_download(
                &config,
                &state_file,
                country.as_deref(),
                redownload,
                dry_run,
            )?;
        }

        Commands::BuildGeocache { index, output, sequence } => {
            let places_parquet = index.join("places.parquet");
            let addr_parquet = index.join("addresses.parquet");
            let addr_national = index.join("addresses_national.parquet");
            let addr_photon = index.join("addresses_photon.parquet");

            let mut addr_paths: Vec<&Path> = Vec::new();
            if addr_parquet.exists() { addr_paths.push(&addr_parquet); }
            if addr_national.exists() { addr_paths.push(&addr_national); }
            if addr_photon.exists() { addr_paths.push(&addr_photon); }

            let output_path = output.unwrap_or_else(|| index.join("geocache.bin"));

            if !places_parquet.exists() && addr_paths.is_empty() {
                let records_path = index.join("records.bin");
                if records_path.exists() {
                    let rs = heimdall_core::record_store::RecordStore::open(&records_path)
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    let place_count = rs.len();
                    let addr_streets = index.join("addr_streets.bin");
                    let addr_count = if addr_streets.exists() {
                        let meta = std::fs::metadata(&addr_streets)?;
                        (meta.len() / 4) as usize
                    } else {
                        0
                    };
                    let (raw, compressed) = geocache::estimate_cache_size(place_count, addr_count);
                    info!("No parquet files found. Estimated cache size from record counts:");
                    info!("  {} places, ~{} addresses", place_count, addr_count);
                    info!("  Raw: {:.1} MB, Compressed (ZSTD): {:.1} MB",
                        raw as f64 / 1e6, compressed as f64 / 1e6);
                    bail!("Cannot build geocache without parquet files. Re-run build without --skip-extract, or use 'rebuild' which now retains parquet.");
                } else {
                    bail!("No parquet files or records.bin found in {}", index.display());
                }
            }

            info!("Building geocache from parquet files in {}", index.display());
            let stats = geocache::build_from_parquet(
                &places_parquet,
                &addr_paths,
                &output_path,
                sequence,
            )?;

            info!("Geocache built: {}", output_path.display());
            info!("  Places:      {}", stats.place_count);
            info!("  Addresses:   {}", stats.address_count);
            info!("  Fixed bytes: {:.1} MB (places) + {:.1} MB (addresses)",
                stats.place_fixed_bytes as f64 / 1e6,
                stats.address_fixed_bytes as f64 / 1e6);
            info!("  String pool: {:.1} MB", stats.string_pool_bytes as f64 / 1e6);
            info!("  Total:       {:.1} MB", stats.total_bytes as f64 / 1e6);
            info!("  Avg/place:   {} bytes", stats.avg_place_bytes);
            info!("  Avg/addr:    {} bytes", stats.avg_addr_bytes);
        }

        Commands::ApplyDiffs { index, diff } => {
            let places_parquet = index.join("places.parquet");
            let addr_parquet = index.join("addresses.parquet");

            if diff.is_empty() {
                bail!("No diff files specified. Use --diff <path.osc.gz>");
            }

            let mut all_changes = Vec::new();
            let mut total_stats = osc::OscStats::default();

            for diff_path in &diff {
                info!("Parsing {}...", diff_path.display());
                let (changes, stats) = osc::parse_osc_gz(diff_path)?;
                info!("  {} elements, {} geocoding-relevant ({} places, {} addresses)",
                    stats.total_elements, stats.geocoding_relevant,
                    stats.place_creates + stats.place_modifies + stats.place_deletes,
                    stats.addr_creates + stats.addr_modifies + stats.addr_deletes);
                if stats.ways_skipped_no_coords > 0 {
                    info!("  {} ways skipped (no coordinates in diffs)", stats.ways_skipped_no_coords);
                }
                total_stats.total_elements += stats.total_elements;
                total_stats.geocoding_relevant += stats.geocoding_relevant;
                total_stats.place_creates += stats.place_creates;
                total_stats.place_modifies += stats.place_modifies;
                total_stats.place_deletes += stats.place_deletes;
                total_stats.addr_creates += stats.addr_creates;
                total_stats.addr_modifies += stats.addr_modifies;
                total_stats.addr_deletes += stats.addr_deletes;
                total_stats.ways_skipped_no_coords += stats.ways_skipped_no_coords;
                all_changes.extend(changes);
            }

            if all_changes.is_empty() {
                info!("No geocoding-relevant changes found in {} diff file(s)", diff.len());
                return Ok(());
            }

            info!("Applying {} changes to parquet files...", all_changes.len());
            let apply_stats = osc::apply_diffs_to_parquet(
                &places_parquet, &addr_parquet, &all_changes,
            )?;

            info!("Done.");
            info!("  Places:    {} → {} (+{} modified:{} deleted:{})",
                apply_stats.places_before, apply_stats.places_after,
                apply_stats.places_added, apply_stats.places_modified, apply_stats.places_deleted);
            info!("  Addresses: {} → {} (+{} modified:{} deleted:{})",
                apply_stats.addresses_before, apply_stats.addresses_after,
                apply_stats.addresses_added, apply_stats.addresses_modified, apply_stats.addresses_deleted);
        }

        Commands::DumpIndex { index, output } => {
            info!("Dumping index {} → {}", index.display(), output.display());
            let result = verify::dump_index(&index, &output)?;
            println!();
            println!("=== Dump Summary ===");
            println!("  FST exact keys:    {}", result.fst_exact_keys);
            println!("  FST phonetic keys: {}", result.fst_phonetic_keys);
            println!("  FST addr keys:     {}", result.fst_addr_keys);
            println!("  Records:           {}", result.record_count);
            println!("  Admin entries:     {}", result.admin_count);
            println!("  Streets:           {}", result.street_count);
            println!("  House entries:     {}", result.house_count);
            println!("  Geohash entries:   {}", result.geohash_count);
            println!();
            println!("  SHA-256 digest:    {}", result.digest);
            println!("  Output directory:  {}", result.output_dir.display());
        }

        Commands::Verify {
            baseline,
            experiment,
            dump_dir,
            functional,
            query_count,
            queries,
        } => {
            info!(
                "Verifying index: baseline={} experiment={}",
                baseline.display(),
                experiment.display()
            );

            // Step 1: Structural comparison (dump + diff)
            let vr = verify::verify_indices(
                &baseline,
                &experiment,
                dump_dir.as_deref(),
            )?;

            println!();
            println!("=== Structural Verification ===");
            for line in &vr.summary {
                println!("{}", line);
            }
            println!();
            if vr.identical {
                println!("RESULT: PASS — indices are structurally identical");
            } else {
                println!(
                    "RESULT: FAIL — {} structural differences detected",
                    vr.total_differences,
                );
            }

            // Step 2: Optional functional verification
            if functional {
                println!();
                println!("=== Functional Verification ===");
                let fr = verify::functional_verify(
                    &baseline,
                    &experiment,
                    queries.as_deref(),
                    query_count,
                )?;
                fr.print_summary();

                // Final verdict
                println!();
                if vr.identical && fr.only_a == 0 && fr.different == 0 {
                    println!("FINAL VERDICT: PASS — no data loss detected");
                } else {
                    println!("FINAL VERDICT: FAIL — differences detected");
                    std::process::exit(1);
                }
            } else if !vr.identical {
                std::process::exit(1);
            }
        }

        Commands::Repack {
            index,
            data_dir,
            country,
            keep_backup,
        } => {
            // Collect index directories from all sources
            let mut dirs: Vec<PathBuf> = index;

            // Add directories from --data-dir
            if let Some(ref dd) = data_dir {
                let mut discovered = discover_index_dirs(dd)?;
                dirs.append(&mut discovered);
            }

            // Add directories from --country codes
            for cc in &country {
                let dir = PathBuf::from(format!("data/index-{}", cc.to_lowercase()));
                if dir.is_dir() {
                    dirs.push(dir);
                } else {
                    tracing::warn!("Index directory not found: {}", dir.display());
                }
            }

            // If nothing specified, auto-discover from data/
            if dirs.is_empty() {
                dirs = discover_index_dirs(Path::new("data"))?;
            }

            if dirs.is_empty() {
                bail!("No index directories found. Use --index, --data-dir, or --country.");
            }

            dirs.sort();
            dirs.dedup();

            println!("Repacking {} index directories...", dirs.len());
            let mut repacked = 0;
            let mut skipped = 0;
            let mut errors = 0;

            for dir in &dirs {
                let name = dir.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("?");
                println!("\n[{}]", name);
                match repack_index(dir, keep_backup) {
                    Ok(did_work) => {
                        if did_work { repacked += 1; } else { skipped += 1; }
                    }
                    Err(e) => {
                        tracing::error!("  Failed: {}", e);
                        errors += 1;
                    }
                }
            }

            println!("\nRepack complete: {} repacked, {} already current, {} errors",
                repacked, skipped, errors);
            if errors > 0 {
                bail!("{} repack(s) failed", errors);
            }
        }

        Commands::BuildGlobalFst { data_dir, output } => {
            let output_dir = output.unwrap_or_else(|| data_dir.join("global"));
            build_global_fst(&data_dir, &output_dir)?;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Core build pipeline
// ---------------------------------------------------------------------------

fn build_country(input: &Path, output: &Path, skip_extract: bool, min_population: u32) -> Result<()> {
    let dir_name = output.file_name().and_then(|n| n.to_str()).unwrap_or("index");
    info!("[{}] Starting build from {}", dir_name, input.display());

    std::fs::create_dir_all(output)?;

    // Auto-copy the correct normalizer TOML into the index directory as sv.toml.
    // This ensures pack, pack_addr, and the API all find the right config.
    let dest_toml = output.join("sv.toml");
    if !dest_toml.exists() {
        let source = find_normalizer_toml(output);
        if source.exists() && source != dest_toml {
            std::fs::copy(&source, &dest_toml)?;
            info!("[{}] Copied normalizer {} → sv.toml", dir_name, source.display());
        }
    }

    let parquet_path = output.join("places.parquet");

    // Step 1: Extract (with auto-caching)
    let should_extract = if skip_extract {
        false
    } else if !parquet_path.exists() {
        true
    } else {
        pbf_changed(input, output)
    };

    if should_extract {
        info!("[{}] Step 1/5: Extracting places from OSM PBF...", dir_name);
        let result = extract::extract_places(input, &parquet_path, min_population, true)?;
        info!("[{}] Extracted {} places + {} addresses", dir_name, result.place_count, result.address_count);
    } else {
        info!("[{}] Step 1/5: Skipping extraction (PBF unchanged, Parquet cached)", dir_name);
    }

    // Step 2: Enrich
    info!("[{}] Step 2/5: Enriching admin hierarchy...", dir_name);
    let enriched = enrich::enrich(&parquet_path, output)?;
    info!("[{}] Admin index: {} entries", dir_name, enriched.admin_count);

    // Step 3+4: Pack places
    info!("[{}] Step 3/5: Packing FSTs and record store...", dir_name);
    let stats = pack::pack(&parquet_path, output, &enriched)?;

    // Step 5: Pack addresses
    info!("[{}] Step 4/5: Packing address index...", dir_name);
    let normalizer = {
        let toml_path = find_normalizer_toml(output);
        if toml_path.exists() {
            heimdall_normalize::Normalizer::from_config(&toml_path)
        } else {
            heimdall_normalize::Normalizer::swedish()
        }
    };
    let addr_parquet = output.join("addresses.parquet");
    let admin_map_path = output.join("admin_map.bin");
    let addr_stats = pack_addr::pack_addresses(
        &[addr_parquet.as_path()], output, &admin_map_path, &normalizer,
    )?;

    // Step 5: Write meta.json (with PBF metadata for caching)
    let pbf_meta = std::fs::metadata(input).ok();
    let meta = serde_json::json!({
        "version": 2,
        "source": input.display().to_string(),
        "record_count": stats.record_count,
        "address_count": addr_stats.address_count,
        "fst_exact_bytes": stats.fst_exact_bytes,
        "fst_phonetic_bytes": stats.fst_phonetic_bytes,
        "fst_ngram_bytes": stats.fst_ngram_bytes,
        "fst_addr_bytes": addr_stats.fst_bytes,
        "record_store_bytes": stats.record_store_bytes,
        "addr_record_bytes": addr_stats.record_bytes,
        "built_at": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        "pbf_size": pbf_meta.as_ref().map(|m| m.len()),
        "pbf_modified": pbf_meta.as_ref().and_then(|m|
            m.modified().ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
        ),
    });
    std::fs::write(
        output.join("meta.json"),
        serde_json::to_string_pretty(&meta)?,
    )?;

    // Clean up build-only files not needed for serving
    for name in &["admin_polygons.bin", "admin_map.bin"] {
        let p = output.join(name);
        if p.exists() {
            let sz = std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
            if let Ok(()) = std::fs::remove_file(&p) {
                info!("[{}] Deleted {} ({:.1} MB freed)", dir_name, name, sz as f64 / 1e6);
            }
        }
    }

    let place_total = stats.fst_exact_bytes
        + stats.fst_phonetic_bytes
        + stats.fst_ngram_bytes
        + stats.record_store_bytes;
    let addr_total = addr_stats.fst_bytes + addr_stats.record_bytes;

    info!("[{}] Build complete!", dir_name);
    info!("[{}]   Places:      {}", dir_name, stats.record_count);
    info!("[{}]   Addresses:   {} ({} streets)", dir_name, addr_stats.address_count, addr_stats.street_count);
    info!("[{}]   TOTAL INDEX: {:.1} MB", dir_name, (place_total + addr_total) as f64 / 1e6);

    Ok(())
}

// ---------------------------------------------------------------------------
// PBF change detection for extraction caching
// ---------------------------------------------------------------------------

/// Check if the PBF file has changed since the last build.
/// Compares file size and modification time against stored metadata.
fn pbf_changed(pbf_path: &Path, output_dir: &Path) -> bool {
    let meta_path = output_dir.join("meta.json");
    let meta_str = match std::fs::read_to_string(&meta_path) {
        Ok(s) => s,
        Err(_) => return true, // No meta.json → must extract
    };
    let meta: serde_json::Value = match serde_json::from_str(&meta_str) {
        Ok(v) => v,
        Err(_) => return true,
    };

    let pbf_meta = match std::fs::metadata(pbf_path) {
        Ok(m) => m,
        Err(_) => return true,
    };

    let current_size = pbf_meta.len();
    let current_mtime = pbf_meta.modified().ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let stored_size = meta["pbf_size"].as_u64().unwrap_or(0);
    let stored_mtime = meta["pbf_modified"].as_u64().unwrap_or(0);

    if stored_size == 0 || stored_mtime == 0 {
        return true; // Old meta.json without PBF metadata
    }

    current_size != stored_size || current_mtime != stored_mtime
}

// ---------------------------------------------------------------------------
// Normalizer TOML resolution
// ---------------------------------------------------------------------------

/// Find the best normalizer TOML for a given index directory.
/// Checks for sv.toml in the index dir first, then detects country from dir name
/// and looks in data/normalizers/.
pub(crate) fn find_normalizer_toml(index_dir: &Path) -> PathBuf {
    // 1. Check for sv.toml already in the index dir (user copied it)
    let local = index_dir.join("sv.toml");
    if local.exists() {
        return local;
    }

    // 2. Detect country from directory name and pick the right source
    let dir_name = index_dir.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");

    if dir_name.contains("germany") || dir_name.contains("-de") {
        let de = PathBuf::from("data/normalizers/de.toml");
        if de.exists() { return de; }
    } else if dir_name.contains("denmark") || dir_name.contains("-dk") {
        let da = PathBuf::from("data/normalizers/da.toml");
        if da.exists() { return da; }
    } else if dir_name.contains("finland") || dir_name.contains("-fi") {
        let fi = PathBuf::from("data/normalizers/fi.toml");
        if fi.exists() { return fi; }
    } else if dir_name.contains("norway") || dir_name.contains("-no") {
        let no = PathBuf::from("data/normalizers/no.toml");
        if no.exists() { return no; }
    } else if dir_name.contains("-gb") || dir_name.contains("-uk") || dir_name.contains("britain") {
        let gb = PathBuf::from("data/normalizers/en-gb.toml");
        if gb.exists() { return gb; }
    } else if dir_name.contains("-us") || dir_name.contains("united-states") || dir_name.contains("america") {
        let us = PathBuf::from("data/normalizers/en-us.toml");
        if us.exists() { return us; }
    } else if dir_name.contains("-au") || dir_name.contains("australia") {
        let au = PathBuf::from("data/normalizers/en-au.toml");
        if au.exists() { return au; }
    } else if dir_name.contains("-ca") || dir_name.contains("canada") {
        let ca = PathBuf::from("data/normalizers/en-ca.toml");
        if ca.exists() { return ca; }
    }

    // 3. Fallback to Swedish
    let sv = PathBuf::from("data/normalizers/sv.toml");
    if sv.exists() { return sv; }

    // 4. Return the local path (will trigger hardcoded defaults)
    local
}

// ---------------------------------------------------------------------------
// Shared merge helper for country importers
// ---------------------------------------------------------------------------

fn merge_and_write_addresses(label: &str, index: &Path, new_addresses: &[extract::RawAddress]) -> Result<()> {
    let osm_parquet = index.join("addresses.parquet");
    let osm_addresses = if osm_parquet.exists() {
        info!("Reading existing addresses...");
        read_osm_addresses(&osm_parquet)?
    } else {
        vec![]
    };

    let merged = lantmateriet::merge_addresses(&osm_addresses, new_addresses);

    info!("Writing merged addresses.parquet...");
    write_merged_addresses(&merged, &osm_parquet)?;
    info!(
        "Done! {} total ({} existing + {} {} new). Run 'build --skip-extract' to rebuild.",
        merged.len(), osm_addresses.len(), merged.len() - osm_addresses.len(), label,
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers for Lantmäteriet merge
// ---------------------------------------------------------------------------

pub(crate) fn read_osm_addresses(parquet_path: &Path) -> Result<Vec<extract::RawAddress>> {
    use arrow::array::*;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = std::fs::File::open(parquet_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut addresses = Vec::new();
    for batch_result in reader {
        let batch = batch_result?;
        let osm_ids = batch.column_by_name("osm_id").unwrap()
            .as_any().downcast_ref::<Int64Array>().unwrap();
        let streets = batch.column_by_name("street").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        let housenumbers = batch.column_by_name("housenumber").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        let postcodes = batch.column_by_name("postcode").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        let cities = batch.column_by_name("city").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        let lats = batch.column_by_name("lat").unwrap()
            .as_any().downcast_ref::<Float64Array>().unwrap();
        let lons = batch.column_by_name("lon").unwrap()
            .as_any().downcast_ref::<Float64Array>().unwrap();

        for i in 0..batch.num_rows() {
            addresses.push(extract::RawAddress {
                osm_id: osm_ids.value(i),
                street: streets.value(i).to_owned(),
                housenumber: housenumbers.value(i).to_owned(),
                postcode: if postcodes.is_null(i) { None } else { Some(postcodes.value(i).to_owned()) },
                city: if cities.is_null(i) { None } else { Some(cities.value(i).to_owned()) },
                lat: lats.value(i),
                lon: lons.value(i),
            });
        }
    }
    Ok(addresses)
}

pub(crate) fn write_merged_addresses(addresses: &[extract::RawAddress], path: &Path) -> Result<()> {
    use arrow::array::*;
    use arrow::datatypes::*;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("osm_id", DataType::Int64, false),
        Field::new("street", DataType::Utf8, false),
        Field::new("housenumber", DataType::Utf8, false),
        Field::new("postcode", DataType::Utf8, true),
        Field::new("city", DataType::Utf8, true),
        Field::new("lat", DataType::Float64, false),
        Field::new("lon", DataType::Float64, false),
    ]));

    let chunk_size = 500_000;
    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None)?;

    for chunk_start in (0..addresses.len()).step_by(chunk_size) {
        let chunk = &addresses[chunk_start..(chunk_start + chunk_size).min(addresses.len())];
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(chunk.iter().map(|a| a.osm_id).collect::<Vec<_>>())),
                Arc::new(StringArray::from(chunk.iter().map(|a| a.street.as_str()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(chunk.iter().map(|a| a.housenumber.as_str()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(chunk.iter().map(|a| a.postcode.as_deref()).collect::<Vec<Option<&str>>>())),
                Arc::new(StringArray::from(chunk.iter().map(|a| a.city.as_deref()).collect::<Vec<Option<&str>>>())),
                Arc::new(Float64Array::from(chunk.iter().map(|a| a.lat).collect::<Vec<_>>())),
                Arc::new(Float64Array::from(chunk.iter().map(|a| a.lon).collect::<Vec<_>>())),
            ],
        )?;
        writer.write(&batch)?;
    }

    writer.close()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers for Photon merge
// ---------------------------------------------------------------------------

pub(crate) fn read_osm_places(parquet_path: &Path) -> Result<Vec<heimdall_core::types::RawPlace>> {
    use arrow::array::*;
    use heimdall_core::types::*;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = std::fs::File::open(parquet_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut places = Vec::new();
    for batch_result in reader {
        let batch = batch_result?;
        let osm_ids = batch
            .column_by_name("osm_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let lats = batch
            .column_by_name("lat")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let lons = batch
            .column_by_name("lon")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let place_types = batch
            .column_by_name("place_type")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        let admin_levels = batch
            .column_by_name("admin_level")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        let populations = batch
            .column_by_name("population")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let wikidatas = batch
            .column_by_name("wikidata")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let alt_names_col = batch
            .column_by_name("alt_names")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let old_names_col = batch
            .column_by_name("old_names")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let name_intl_col = batch
            .column_by_name("name_intl")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for i in 0..batch.num_rows() {
            let place_type = place_type_from_u8(place_types.value(i));

            let alt_names = if alt_names_col.is_null(i) {
                vec![]
            } else {
                alt_names_col
                    .value(i)
                    .split(';')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_owned())
                    .collect()
            };

            let old_names = if old_names_col.is_null(i) {
                vec![]
            } else {
                old_names_col
                    .value(i)
                    .split(';')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_owned())
                    .collect()
            };

            let name_intl = if name_intl_col.is_null(i) {
                vec![]
            } else {
                name_intl_col
                    .value(i)
                    .split(';')
                    .filter_map(|pair| {
                        let parts: Vec<&str> = pair.splitn(2, '=').collect();
                        if parts.len() == 2 {
                            Some((parts[0].to_owned(), parts[1].to_owned()))
                        } else {
                            None
                        }
                    })
                    .collect()
            };

            places.push(RawPlace {
                osm_id: osm_ids.value(i),
                osm_type: OsmType::Node,
                name: names.value(i).to_owned(),
                name_intl,
                alt_names,
                old_names,
                coord: Coord::new(lats.value(i), lons.value(i)),
                place_type,
                admin_level: if admin_levels.is_null(i) {
                    None
                } else {
                    Some(admin_levels.value(i))
                },
                country_code: None,
                admin1: None,
                admin2: None,
                population: if populations.is_null(i) {
                    None
                } else {
                    Some(populations.value(i))
                },
                wikidata: if wikidatas.is_null(i) {
                    None
                } else {
                    Some(wikidatas.value(i).to_owned())
                },
            });
        }
    }
    Ok(places)
}

fn place_type_from_u8(v: u8) -> heimdall_core::types::PlaceType {
    use heimdall_core::types::PlaceType;
    match v {
        0 => PlaceType::Country,
        1 => PlaceType::State,
        2 => PlaceType::County,
        3 => PlaceType::City,
        4 => PlaceType::Town,
        5 => PlaceType::Village,
        6 => PlaceType::Hamlet,
        7 => PlaceType::Farm,
        8 => PlaceType::Locality,
        10 => PlaceType::Suburb,
        11 => PlaceType::Quarter,
        12 => PlaceType::Neighbourhood,
        13 => PlaceType::Island,
        14 => PlaceType::Islet,
        20 => PlaceType::Lake,
        21 => PlaceType::River,
        22 => PlaceType::Mountain,
        23 => PlaceType::Forest,
        24 => PlaceType::Bay,
        25 => PlaceType::Cape,
        30 => PlaceType::Airport,
        31 => PlaceType::Station,
        _ => PlaceType::Unknown,
    }
}

/// Merge places by osm_id — keeps existing OSM version on collision.
pub(crate) fn merge_places(
    existing: &[heimdall_core::types::RawPlace],
    new: &[heimdall_core::types::RawPlace],
) -> Vec<heimdall_core::types::RawPlace> {
    use std::collections::HashSet;

    let mut seen: HashSet<i64> = existing.iter().map(|p| p.osm_id).collect();
    let mut merged = existing.to_vec();

    let mut added = 0usize;
    let mut deduped = 0usize;
    for place in new {
        if place.osm_id != 0 && seen.contains(&place.osm_id) {
            deduped += 1;
            continue;
        }
        seen.insert(place.osm_id);
        merged.push(place.clone());
        added += 1;
    }

    info!(
        "Place merge: {} total ({} existing + {} new, {} deduped)",
        merged.len(),
        existing.len(),
        added,
        deduped,
    );
    merged
}

/// Walk a directory tree to find the Lucene index dir (contains segments_N).
pub(crate) fn find_lucene_index_dir(root: &Path) -> Result<PathBuf> {
    // Typical Photon structure:
    //   photon_data/elasticsearch/data/nodes/0/indices/{uuid}/0/index/
    for entry in walkdir(root)? {
        let name = entry
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
        if name.starts_with("segments_") && name != "segments.gen" {
            if let Some(parent) = entry.parent() {
                return Ok(parent.to_owned());
            }
        }
    }
    bail!(
        "No Lucene segments file found in {}. Expected Photon dump structure.",
        root.display()
    );
}

/// Extract a .tar.bz2 archive using the fastest available bzip2 decompressor.
/// Tries lbzip2 (parallel) → pbzip2 (parallel) → tar xjf (single-threaded fallback).
pub(crate) fn extract_tar_bz2(archive: &Path, output_dir: &Path) -> Result<()> {
    use std::process::{Command, Stdio};

    for decompressor in &["lbzip2", "pbzip2"] {
        if Command::new(decompressor)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok()
        {
            info!("Extracting {} using {} (parallel bzip2)", archive.display(), decompressor);
            let decomp = Command::new(decompressor)
                .arg("-dc")
                .arg(archive)
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn();

            if let Ok(mut decomp_child) = decomp {
                let decomp_stdout = decomp_child.stdout.take().unwrap();
                let tar_status = Command::new("tar")
                    .arg("xf").arg("-").arg("-C").arg(output_dir)
                    .stdin(decomp_stdout)
                    .status();
                let decomp_status = decomp_child.wait();
                if let (Ok(ts), Ok(ds)) = (&tar_status, &decomp_status) {
                    if ts.success() && ds.success() {
                        return Ok(());
                    }
                }
                info!("{} extraction failed, trying next decompressor", decompressor);
            }
        }
    }

    // Fallback: standard single-threaded tar xjf
    info!("Extracting {} using tar xjf (single-threaded fallback)", archive.display());
    let status = Command::new("tar")
        .arg("xjf").arg(archive).arg("-C").arg(output_dir)
        .status()?;
    if !status.success() {
        bail!("tar extraction failed for {}", archive.display());
    }
    Ok(())
}

/// Simple recursive directory walker (avoids adding walkdir crate).
pub(crate) fn walkdir(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut results = Vec::new();
    if dir.is_dir() {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                results.extend(walkdir(&path)?);
            } else {
                results.push(path);
            }
        }
    }
    Ok(results)
}

// ---------------------------------------------------------------------------
// Discover index directories
// ---------------------------------------------------------------------------

fn discover_index_dirs(data_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(data_dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name().to_str().map(|n| n.starts_with("index-")).unwrap_or(false)
                && e.path().is_dir()
        })
        .map(|e| e.path())
        .collect();
    dirs.sort();
    Ok(dirs)
}

// ---------------------------------------------------------------------------
// Repack: V2 records.bin → V3, V4 addr_streets.bin → V5
// ---------------------------------------------------------------------------

/// Repack an index directory. Returns true if any file was repacked, false if all skipped.
fn repack_index(dir: &Path, keep_backup: bool) -> Result<bool> {
    use heimdall_core::record_store::{RecordStore, RecordStoreBuilder};
    use heimdall_core::addr_store::{AddrStore, AddrStoreBuilder};

    let records_path = dir.join("records.bin");
    let addr_path = dir.join("addr_streets.bin");
    let mut did_work = false;

    // Check records.bin version
    if records_path.exists() {
        let data = std::fs::read(&records_path)?;
        if data.len() >= 8 {
            let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
            if version == 2 {
                println!("  Repacking records.bin V2 -> V3...");
                let store = RecordStore::open(&records_path)
                    .map_err(|e| anyhow::anyhow!("{}", e))?;

                let mut builder = RecordStoreBuilder::new();
                for id in 0..store.len() as u32 {
                    let record = store.get(id).map_err(|e| anyhow::anyhow!("{}", e))?;
                    let name = store.primary_name(&record);
                    let all = store.all_names(&record);
                    let alts: Vec<&str> = all.iter().skip(1).map(|s| s.as_str()).collect();
                    builder.add(record, &name, &alts);
                }

                let tmp_path = records_path.with_extension("bin.tmp");
                builder.write(&tmp_path).map_err(|e| anyhow::anyhow!("{}", e))?;

                let old_size = data.len();
                if keep_backup {
                    std::fs::rename(&records_path, records_path.with_extension("bin.v2.bak"))?;
                }
                std::fs::rename(&tmp_path, &records_path)?;

                let new_size = std::fs::metadata(&records_path)?.len();
                println!("    {} -> {} ({:.1}% of original)",
                    format_size(old_size), format_size(new_size as usize),
                    new_size as f64 / old_size as f64 * 100.0);
                did_work = true;
            } else if version == 3 {
                println!("  records.bin already V3, skipping");
            } else {
                println!("  records.bin V{}, skipping", version);
            }
        }
    }

    // Check addr_streets.bin version
    if addr_path.exists() {
        let data = std::fs::read(&addr_path)?;
        if data.len() >= 8 {
            let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
            if version == 4 {
                println!("  Repacking addr_streets.bin V4 -> V5...");
                let store = AddrStore::open(&addr_path)
                    .map_err(|e| anyhow::anyhow!("{}", e))?
                    .ok_or_else(|| anyhow::anyhow!("Failed to open addr store"))?;

                let mut builder = AddrStoreBuilder::new();
                for sid in 0..store.street_count() as u32 {
                    let header = store.get_street(sid)
                        .ok_or_else(|| anyhow::anyhow!("Missing street {}", sid))?;
                    let name = store.street_name(&header).to_owned();
                    let houses = store.street_houses(&header);

                    // Convert HouseEntry delta coords back to absolute coords for the builder
                    let house_tuples: Vec<(u16, u8, i32, i32)> = houses.iter()
                        .map(|h| (
                            h.number,
                            h.suffix,
                            header.base_lat + h.delta_lat as i32,
                            header.base_lon + h.delta_lon as i32,
                        ))
                        .collect();

                    builder.add_street(
                        &name,
                        header.base_lat,
                        header.base_lon,
                        header.postcode,
                        &house_tuples,
                    );
                }

                let tmp_path = addr_path.with_extension("bin.tmp");
                builder.write_v5(&tmp_path).map_err(|e| anyhow::anyhow!("{}", e))?;

                let old_size = data.len();
                if keep_backup {
                    std::fs::rename(&addr_path, addr_path.with_extension("bin.v4.bak"))?;
                }
                std::fs::rename(&tmp_path, &addr_path)?;

                let new_size = std::fs::metadata(&addr_path)?.len();
                println!("    {} -> {} ({:.1}% of original)",
                    format_size(old_size), format_size(new_size as usize),
                    new_size as f64 / old_size as f64 * 100.0);
                did_work = true;
            } else if version == 5 {
                println!("  addr_streets.bin already V5, skipping");
            } else {
                println!("  addr_streets.bin V{}, skipping", version);
            }
        }
    }

    Ok(did_work)
}

// ---------------------------------------------------------------------------
// Build global FST from per-country indices
// ---------------------------------------------------------------------------

fn build_global_fst(data_dir: &Path, output_dir: &Path) -> Result<()> {
    use heimdall_core::global_index::GlobalIndexBuilder;
    use heimdall_core::record_store::RecordStore;
    use heimdall_core::compressed_io;
    use fst::{IntoStreamer, Map, Streamer};

    let mut builder = GlobalIndexBuilder::new();

    // Discover all index directories
    let index_dirs = discover_index_dirs(data_dir)?;

    if index_dirs.is_empty() {
        bail!("No index-* directories found in {}", data_dir.display());
    }

    println!("Building global FST from {} country indices...", index_dirs.len());

    // Decode a posting list at `value` from a sidecar blob.
    // Sidecar layout: at every offset, [u16 count][u32 rec_id]*count.
    // Returns rec_ids in importance-desc order. When `sidecar` is `None`
    // (legacy v2 per-country index), `value` is itself a record_id.
    fn decode_postings(sidecar: Option<&[u8]>, value: u64) -> Vec<u32> {
        let bytes = match sidecar {
            Some(b) => b,
            None => return vec![value as u32],
        };
        let off = value as usize;
        if off + 2 > bytes.len() { return vec![]; }
        // Clamp to MAX_POSTINGS_PER_KEY (8) so a corrupted sidecar can't
        // request unbounded allocation. Builders never write more.
        let count = (u16::from_le_bytes([bytes[off], bytes[off + 1]]) as usize).min(8);
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let p = off + 2 + i * 4;
            if p + 4 > bytes.len() { break; }
            out.push(u32::from_le_bytes([bytes[p], bytes[p + 1], bytes[p + 2], bytes[p + 3]]));
        }
        out
    }

    for (country_id, dir) in index_dirs.iter().enumerate() {
        let cc = dir.file_name().unwrap().to_str().unwrap()
            .strip_prefix("index-").unwrap_or("??");

        let records_path = dir.join("records.bin");
        let fst_exact_path = dir.join("fst_exact.fst");
        let fst_phonetic_path = dir.join("fst_phonetic.fst");
        let record_lists_exact_path = dir.join("record_lists.bin");
        let record_lists_phonetic_path = dir.join("record_lists_phonetic.bin");

        if !records_path.exists() || !fst_exact_path.exists() {
            println!("  Skipping {} (missing files)", cc.to_uppercase());
            continue;
        }

        let records = RecordStore::open(&records_path)
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        // Read exact FST and (optionally) the posting-list sidecar so we
        // pick up same-name alternates that share an FST key.
        let fst_data = compressed_io::read_maybe_compressed(&fst_exact_path)?;
        let fst_exact = Map::new(fst_data)
            .map_err(|e| anyhow::anyhow!("FST: {}", e))?;
        let exact_sidecar: Option<Vec<u8>> = if record_lists_exact_path.exists()
            && std::fs::metadata(&record_lists_exact_path).map(|m| m.len()).unwrap_or(0) > 0
        {
            Some(compressed_io::read_maybe_compressed(&record_lists_exact_path)?)
        } else {
            None
        };

        let mut exact_count = 0u64;
        let mut stream = fst_exact.into_stream();
        while let Some((key, value)) = stream.next() {
            let name_bytes = key.to_owned();
            let postings = decode_postings(exact_sidecar.as_deref(), value);
            for record_id in postings {
                if let Ok(record) = records.get(record_id) {
                    let name = std::str::from_utf8(&name_bytes).unwrap_or("").to_owned();
                    builder.add_exact(name, country_id as u16, record_id, record.importance);
                    exact_count += 1;
                }
            }
        }

        // Read phonetic FST + sidecar
        if fst_phonetic_path.exists() {
            let fst_data = compressed_io::read_maybe_compressed(&fst_phonetic_path)?;
            let fst_phonetic = Map::new(fst_data)
                .map_err(|e| anyhow::anyhow!("FST: {}", e))?;
            let phonetic_sidecar: Option<Vec<u8>> = if record_lists_phonetic_path.exists()
                && std::fs::metadata(&record_lists_phonetic_path).map(|m| m.len()).unwrap_or(0) > 0
            {
                Some(compressed_io::read_maybe_compressed(&record_lists_phonetic_path)?)
            } else {
                None
            };

            let mut stream = fst_phonetic.into_stream();
            while let Some((key, value)) = stream.next() {
                let name_bytes = key.to_owned();
                let postings = decode_postings(phonetic_sidecar.as_deref(), value);
                for record_id in postings {
                    if let Ok(record) = records.get(record_id) {
                        let name = std::str::from_utf8(&name_bytes).unwrap_or("").to_owned();
                        builder.add_phonetic(name, country_id as u16, record_id, record.importance);
                    }
                }
            }
        }

        println!("  {} {:>10} exact entries", cc.to_uppercase(), exact_count);
    }

    println!("\nWriting global FST to {}...", output_dir.display());
    builder.write(output_dir).map_err(|e| anyhow::anyhow!("{}", e))?;

    // Print file sizes
    for name in &["fst_exact.fst", "fst_phonetic.fst", "postings.bin", "postings_phonetic.bin"] {
        let p = output_dir.join(name);
        if p.exists() {
            let size = std::fs::metadata(&p)?.len();
            println!("  {}: {}", name, format_size(size as usize));
        }
    }

    // Write a country_order.json that records the country_id -> country_code mapping.
    // This is critical for the API to know which country_id maps to which CountryIndex.
    let country_order: Vec<String> = index_dirs.iter()
        .map(|d| d.file_name().unwrap().to_str().unwrap()
            .strip_prefix("index-").unwrap_or("??").to_uppercase())
        .collect();
    let order_json = serde_json::to_string_pretty(&country_order)?;
    std::fs::write(output_dir.join("country_order.json"), order_json)?;
    println!("  country_order.json: {} countries", country_order.len());

    Ok(())
}

// ---------------------------------------------------------------------------
// Format size helper
// ---------------------------------------------------------------------------

fn format_size(bytes: usize) -> String {
    if bytes > 1_000_000 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes > 1_000 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}
