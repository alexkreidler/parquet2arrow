# parquet2arrow-rs

This is a command line tool to convert `.parquet` files to `.arrow` files.

Why?: It's simple, and fast.

On my computer, it converts a 500MB Parquet file with 14.6 million rows to an arrow file in around 2 seconds.

```
time ./target/release/parquet2arrow -i ~/Downloads/data.parquet -o ./out/example.arrow
Done, wrote 14656519 rows

________________________________________________________
Executed in    2.17 secs   fish           external 
   usr time  1189.07 millis    0.00 micros  1189.07 millis 
   sys time  645.92 millis  421.00 micros  645.50 millis 
```

## Installation

```
cargo install parquet2arrow
```

## Usage

```
parquet2arrow 0.1.0
Tool to convert a Parquet file to an Apache Arrow file

USAGE:
    parquet2arrow [OPTIONS] --input <INPUT> --output <OUTPUT>

OPTIONS:
    -h, --help               Print help information
    -i, --input <INPUT>      Path of Parquet file to read and convert
    -o, --output <OUTPUT>    Path of Arrow file to write
    -v, --verbose            Display additional details e.g. converted Arrow schema
    -V, --version            Print version information
```

