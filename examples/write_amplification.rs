use clap::Parser;
use fjall::PartitionCreateOptions;
use indicatif::{ProgressBar, ProgressStyle};
use rand::SeedableRng;
use rand::{distributions::Alphanumeric, Rng};
use std::error::Error;
use std::path::{Path, PathBuf};

use veifka::{DataStore, DataStorePartition};

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

    let data_store = DataStore::new(args.db_path.to_str().unwrap()).unwrap();
    // // Flush active journal
    // data_store.keyspace().persist(fjall::PersistMode::SyncAll)?;

    if args.run_all {
        // run_all_combinations(&data_store)?;
        run_all_combinations()?;
    } else {
        // Ensure all required parameters are provided for single test
        let key_size = args
            .key_size
            .ok_or("key_size is required for single test")?;
        let value_size = args
            .value_size
            .ok_or("value_size is required for single test")?;
        let count = args.count.ok_or("count is required for single test")?;

        run_single_test(&data_store, key_size, value_size, count)?;
    }

    Ok(())
}

fn run_single_test(
    data_store: &DataStore,
    key_size: usize,
    value_size: usize,
    count: usize,
) -> Result<(), Box<dyn Error>> {
    let partition_name = format!("test_partition_k{}_v{}_c{}", key_size, value_size, count);
    let partition_handle = data_store.create_partition(&partition_name)?;
    let partition = DataStorePartition::new(partition_handle);

    let total_written = generate_and_write_kv_pairs(&partition, key_size, value_size, count)?;

    println!("Total data written: {} bytes", total_written);
    println!(
        "Total data written in MB: {:.3} MB",
        total_written as f64 / (1024.0 * 1024.0)
    );
    println!(
        "Disk space usage from keyspace: {}",
        data_store.keyspace().disk_space()
    );
    // Get disk usage for this partition
    let partition_handle = data_store
        .keyspace()
        .open_partition(&partition_name, PartitionCreateOptions::default())?;
    let disk_usage = partition_handle.disk_space();
    println!("Disk space usage from partition: {}", disk_usage);

    Ok(())
}

// fn run_all_combinations(data_store: &DataStore) -> Result<(), Box<dyn Error>> {
fn run_all_combinations() -> Result<(), Box<dyn Error>> {
    let key_sizes = [16, 32, 64, 128];
    let value_sizes = [16, 32, 64, 128, 256];
    let counts = [100000, 1000000];

    let mut results = Vec::new();

    for key_size in key_sizes.iter() {
        for value_size in value_sizes.iter() {
            for count in counts.iter() {
                println!(
                    "\nRunning test: key_size={}, value_size={}, count={}",
                    key_size, value_size, count
                );

                let data_store_name = format!("datastore_k{}_v{}_c{}", key_size, value_size, count);
                // Create a new datastore each time
                let data_store = DataStore::new(&data_store_name)?;

                // Create a unique partition for each test
                let partition_name =
                    format!("test_partition_k{}_v{}_c{}", key_size, value_size, count);
                let partition_handle = data_store.create_partition(&partition_name)?;
                let partition_data_store = DataStorePartition::new(partition_handle);

                let total_written = generate_and_write_kv_pairs(
                    &partition_data_store,
                    *key_size,
                    *value_size,
                    *count,
                )?;
                // Flush active journal
                // data_store.keyspace().persist(fjall::PersistMode::SyncAll)?;

                // Add a small delay to ensure all disk operations are complete
                // std::thread::sleep(std::time::Duration::from_millis(100));

                // Get disk usage for this partition
                // let partition_handle = data_store
                //     .keyspace()
                //     .open_partition(&partition_name, PartitionCreateOptions::default())?;
                // let disk_usage = partition_handle.disk_space();
                let disk_usage_keyspace = data_store.keyspace().disk_space();
                let write_amp = disk_usage_keyspace as f64 / total_written as f64;

                results.push(TestResult {
                    key_size: *key_size,
                    value_size: *value_size,
                    count: *count,
                    total_written,
                    disk_usage: disk_usage_keyspace,
                    write_amp,
                });
                println!(
                    "Key Size: {}, Value Size: {}, Count: {}",
                    key_size, value_size, count
                );
                println!("Total data: {} bytes", total_written);
                println!("Keyspace disk usage: {}", disk_usage_keyspace);

                // println!(
                //     "Total data written (in MB): {:.3} MB",
                //     total_written as f64 / (1024.0 * 1024.0)
                // );
                // println!("Partition disk Usage: {} bytes", disk_usage);
                println!("Write Amplification: {:.2}", write_amp);
                println!("----------------------------------------");

                std::fs::remove_dir_all(&data_store_name)?;
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
    partition: &DataStorePartition,
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
        partition.set(&key, &value)?;
        total_bytes += key.len() + value.len();
        pb.inc(1);
    }

    pb.finish_with_message("Finished writing key-value pairs");
    Ok(total_bytes)
}

fn generate_random_bytes<R: Rng + ?Sized>(rng: &mut R, size: usize) -> Vec<u8> {
    rng.sample_iter(&Alphanumeric).take(size).collect()
}
