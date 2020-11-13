use std::path::PathBuf;

use ton_types::{fail, Result};

use ton_node_storage::archives::package::read_package_from_file;

fn print_separator() {
    println!("+{}+{}+", "-".repeat(170 + 2), "-".repeat(6 + 2));
}

fn print_row(values: &[impl AsRef<str>]) {
    println!("| {0: <170} | {1: >6} |", values[0].as_ref(), values[1].as_ref());
}

async fn run(filename: PathBuf) -> Result<()> {
    println!("Filename: {:?}", &filename);

    print_separator();
    print_row(&["File Name".to_uppercase(), "Size".to_uppercase()]);
    print_separator();

    let mut count = 0;
    let mut reader = read_package_from_file(filename).await?;
    while let Some(entry) = reader.next().await? {
        print_row(&[entry.filename(), &entry.data().len().to_string()]);
        count += 1;
    }

    print_separator();
    print_row(&[&"Entries count".to_uppercase(), &count.to_string()]);
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
