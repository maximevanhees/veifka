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
    key_size: usize,

    /// Size of values in bytes
    #[arg(short, long)]
    value_size: usize,

    /// Number of key-value pairs to generate
    #[arg(short, long)]
    count: usize,

    /// Path to the database directory
    #[arg(short, long, default_value = "./data")]
    db_path: PathBuf,
}

fn main() -> Result<(), Box<dyn Error>> {
    // Parse command-line arguments
    let args = Args::parse();

    // Initialize data store using your library
    let data_store = DataStore::new(args.db_path.to_str().unwrap(), "test_partition")?;

    // Generate and write key-value pairs
    let total_data_written =
        generate_and_write_kv_pairs(&data_store, args.key_size, args.value_size, args.count)?;

    println!("Total data written: {} bytes", total_data_written);

    println!(
        "Data written: {:.2} MB",
        total_data_written as f64 / (1024.0 * 1024.0)
    );
    println!(
        "Disk space usage from keyspace: {:?}",
        data_store.keyspace().disk_space()
    );
    // println!(
    //     "You can now check the disk usage of the database directory: {:?}",
    //     args.db_path
    // );
    // println!(
    //     "Use a command like `du -sh {:?}` to see disk usage.",
    //     args.db_path
    // );

    Ok(())
}

fn generate_and_write_kv_pairs(
    data_store: &DataStore,
    key_size: usize,
    value_size: usize,
    count: usize,
) -> Result<usize, Box<dyn Error>> {
    let mut total_bytes = 0;

    // Initialize the RNG here
    let mut rng = rand::rngs::SmallRng::from_entropy();

    // Create a progress bar
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

        // let mut key = [0; 64];
        // let mut value = [0; 128];
        // key[..8].copy_from_slice(&i.to_be_bytes());
        // value[..8].copy_from_slice(&i.to_le_bytes());

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
