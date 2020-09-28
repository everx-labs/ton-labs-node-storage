use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use ton_node_storage::archives::{package::Package, package_entry::PackageEntry};
use ton_types::{fail, Result};

fn print_separator() {
    println!("+{}+{}+", "-".repeat(170 + 2), "-".repeat(6 + 2));
}

fn print_row(values: &[impl AsRef<str>]) {
    println!("| {0: <170} | {1: >6} |", values[0].as_ref(), values[1].as_ref());
}

async fn print_entry(entry: PackageEntry, count: Arc<AtomicU32>) -> Result<bool> {
    print_row(&[entry.filename(), &entry.data().len().to_string()]);
    count.fetch_add(1, Ordering::SeqCst);
    Ok(true)
}

async fn run(filename: PathBuf) -> Result<()> {
    println!("Filename: {:?}", &filename);

    let package = Package::open(Arc::new(filename), true, false).await?;

    print_separator();
    print_row(&["File Name".to_uppercase(), "Size".to_uppercase()]);
    print_separator();

    let count = Arc::new(AtomicU32::new(0));
    package.for_each(print_entry, Arc::clone(&count)).await?;

    print_separator();
    print_row(&[&"Entries count".to_uppercase(), &count.load(Ordering::SeqCst).to_string()]);
    print_separator();

    Ok(())
}

fn main() -> Result<()> {
    let mut args = Vec::new();
    for arg in std::env::args() {
        args.push(arg);
    }

    if args.len() < 2 {
        println!("Usage: {} <filename>", args[0]);
        fail!("Filename is not specified")
    }

    let filename = PathBuf::from(&args[1]);

    tokio::runtime::Builder::new()
        .build()
        .expect("Can't create tokio runtime")
        .block_on(run(filename))
}