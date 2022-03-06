use arrow::ipc::writer::FileWriter;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::reader::SerializedFileReader;
use std::fs::File;
use std::sync::Arc;
use clap::Parser;
use anyhow::{Context, Result, anyhow};

/// Tool to convert a Parquet file to an Apache Arrow file.
#[derive(Parser, Debug)]
#[clap(version, about, long_about = None)]
struct Args {
    /// Path of Parquet file to read and convert
    #[clap(short, long)]
    input: String,

    /// Path of Arrow file to write
    #[clap(short, long)]
    output: String,

    /// Display additional details e.g. converted Arrow schema
    #[clap(short, long, parse(from_flag))]
    verbose: bool
    // /// Limit to records
    // #[clap(short, long, default_value_t = 1)]
    // limit: u8,
}

fn main() -> Result<()> {
    let args = Args::parse();
    
    let file = File::open(args.input).context("Failed to open input file")?;


    std::fs::create_dir_all(std::path::Path::new(&args.output).parent().ok_or(anyhow!("Failed to get parent directory"))?)?;
    let mut out_file = File::create(args.output).context("Failed to create output file")?;

    let file_reader = SerializedFileReader::new(file).context("Failed to create file reader")?;
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));
    let parquet_schema = arrow_reader.get_schema().context("Failed to get Arrow schema from Parquet")?;
    if args.verbose {
        println!("Converted arrow schema is: {:#?}", parquet_schema);
    }
    let record_batch_reader = arrow_reader.get_record_reader(8192).context("Failed to get Arrow record reader")?;

    let mut fw = FileWriter::try_new(&mut out_file, &parquet_schema).context("Failed to create Arrow file writer")?;
    if args.verbose {
        println!("Wrote schema");
    }

    let mut total = 0;
    let mut batch_idx = 0;
    for maybe_record_batch in record_batch_reader {
        let record_batch = maybe_record_batch.context(format!("Failed to read next batch after {}", batch_idx))?;
        total += record_batch.num_rows();
        fw.write(&record_batch).context(format!("Failed to write batch {}", batch_idx))?;
        batch_idx += 1;
    }
    fw.finish().expect("Failed to finalize file.");

    println!("Done, wrote {} rows", total);
    Ok(())
}
