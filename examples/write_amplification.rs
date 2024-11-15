use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use rand::SeedableRng;
use rand::{distributions::Alphanumeric, Rng};
use std::error::Error;
use std::path::PathBuf;

use veifka::DataStore;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Size of keys in bytes
    #[arg(short, long)]
    key_size: Option<usize>,

    /// Size of values in bytes
    #[arg(short, long)]
    value_size: Option<usize>,

    /// Number of key-value pairs to generate
    #[arg(short, long)]
    count: Option<usize>,

    /// Path to the database directory
    #[arg(short, long, default_value = "./data")]
    db_path: PathBuf,

    /// Run all test combinations
    #[arg(short, long)]
    run_all: bool,
}

struct TestResult {
    key_size: usize,
    value_size: usize,
    count: usize,
    total_written: usize,
    disk_usage: u64,
    write_amp: f64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    if args.run_all {
        run_all_combinations(&args.db_path)?;
    } else {
        // Ensure all required parameters are provided for single test
        let key_size = args
            .key_size
            .ok_or("key_size is required for single test")?;
        let value_size = args
            .value_size
            .ok_or("value_size is required for single test")?;
        let count = args.count.ok_or("count is required for single test")?;

        run_single_test(&args.db_path, key_size, value_size, count)?;
    }

    Ok(())
}

fn run_single_test(
    db_path: &PathBuf,
    key_size: usize,
    value_size: usize,
    count: usize,
) -> Result<(), Box<dyn Error>> {
    let data_store = DataStore::new(db_path.to_str().unwrap(), "test_partition")?;

    let total_written = generate_and_write_kv_pairs(&data_store, key_size, value_size, count)?;

    println!("Total data written: {} bytes", total_written);
    println!(
        "Data written: {:.2} MB",
        total_written as f64 / (1024.0 * 1024.0)
    );
    println!(
        "Disk space usage from keyspace: {}",
        data_store.keyspace().disk_space()
    );

    Ok(())
}

fn run_all_combinations(base_db_path: &PathBuf) -> Result<(), Box<dyn Error>> {
    let key_sizes = vec![16, 32, 64, 128];
    let value_sizes = vec![16, 32, 64, 128, 256];
    let counts = vec![10000, 100000, 1000000];

    let mut results = Vec::new();

    for key_size in key_sizes.iter() {
        for value_size in value_sizes.iter() {
            for count in counts.iter() {
                let db_path =
                    base_db_path.join(format!("data_k{}_v{}_{}", key_size, value_size, count));

                println!(
                    "\nRunning test: key_size={}, value_size={}, count={}",
                    key_size, value_size, count
                );

                let data_store = DataStore::new(db_path.to_str().unwrap(), "test_partition")?;
                let total_written =
                    generate_and_write_kv_pairs(&data_store, *key_size, *value_size, *count)?;

                // Flush active journal
                data_store.keyspace().persist(fjall::PersistMode::SyncAll)?;

                let disk_usage = data_store.keyspace().disk_space();
                let write_amp = disk_usage as f64 / total_written as f64;

                results.push(TestResult {
                    key_size: *key_size,
                    value_size: *value_size,
                    count: *count,
                    total_written,
                    disk_usage,
                    write_amp,
                });

                println!(
                    "Key Size: {}, Value Size: {}, Count: {}",
                    key_size, value_size, count
                );
                println!("Total Written: {} bytes", total_written);
                println!("Disk Usage: {} bytes", disk_usage);
                println!("Write Amplification: {:.2}", write_amp);
                println!("----------------------------------------");

                std::fs::remove_dir_all(&db_path)?;
            }
        }
    }

    println!("\nSummary of all tests:");
    println!("Key Size | Value Size | Count | Total Written | Disk Usage | Write Amp");
    println!("---------+------------+-------+---------------+------------+-----------");
    for result in results {
        println!(
            "{:8} | {:10} | {:6} | {:13} | {:10} | {:.2}",
            result.key_size,
            result.value_size,
            result.count,
            result.total_written,
            result.disk_usage,
            result.write_amp
        );
    }

    Ok(())
}

fn generate_and_write_kv_pairs(
    data_store: &DataStore,
    key_size: usize,
    value_size: usize,
    count: usize,
) -> Result<usize, Box<dyn Error>> {
    let mut total_bytes = 0;
    let mut rng = rand::rngs::SmallRng::from_entropy();
    let pb = ProgressBar::new(count as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
            )?
            .progress_chars("#>-"),
    );

    for _ in 0..count {
        let key = generate_random_bytes(&mut rng, key_size);
        let value = generate_random_bytes(&mut rng, value_size);
        data_store.set(&key, &value)?;
        total_bytes += key.len() + value.len();
        pb.inc(1);
    }

    pb.finish_with_message("Finished writing key-value pairs");
    Ok(total_bytes)
}

fn generate_random_bytes<R: Rng + ?Sized>(rng: &mut R, size: usize) -> Vec<u8> {
    rng.sample_iter(&Alphanumeric).take(size).collect()
}
