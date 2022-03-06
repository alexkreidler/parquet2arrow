use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatchReader;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::reader::SerializedFileReader;
use std::fs::File;
use std::io::BufWriter;
use std::sync::Arc;
use clap::Parser;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[clap(version, about, long_about = None)]
struct Args {
    /// path of Parquet file to read and convert
    #[clap(short, long)]
    input: String,

    /// path of Arrow file to write
    #[clap(short, long)]
    output: String,

    /// whether to print additional details
    #[clap(short, long, parse(from_flag))]
    verbose: bool
    // /// Limit to records
    // #[clap(short, long, default_value_t = 1)]
    // limit: u8,
}

fn main() {
    let args = Args::parse();
    
    let file = File::open(args.input).unwrap();

    let mut out_file = File::create(args.output).unwrap();

    let file_reader = SerializedFileReader::new(file).unwrap();
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));
    let parquet_schema = arrow_reader.get_schema().unwrap();
    if args.verbose {
        println!("Converted arrow schema is: {:#?}", parquet_schema);
    }
    let record_batch_reader = arrow_reader.get_record_reader(4095).unwrap();

    let mut fw = FileWriter::try_new(&mut out_file, &parquet_schema).unwrap();
    if args.verbose {
        println!("Wrote schema");
    }

    let mut total = 0;

    for maybe_record_batch in record_batch_reader {
        let record_batch = maybe_record_batch.unwrap();
        total += record_batch.num_rows();
        fw.write(&record_batch).expect("write batch error");
    }
    fw.finish().expect("finish write error");

    println!("Done, wrote {} rows", total);
}
