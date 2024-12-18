use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_writer::ArrowWriter;
use clap::Parser;
use std::fs::{self, File};

/// CLI Arguments
#[derive(Parser)]
struct Args {
    /// Input Parquet file path
    #[arg(short, long)]
    parquet_file: String,

    /// Output directory for chunked files
    #[arg(short, long, default_value = "output")]
    output_dir: String,

    /// Output file prefix
    #[arg(short = 'p', long, default_value = "output_part")]
    output_part: String,

    /// Number of records per chunk
    #[arg(short, long, default_value = "50000")]
    chunk_size: usize,
}

/// Write a RecordBatch to a Parquet file
fn write_parquet_file(output_path: &str, batch: &RecordBatch) -> parquet::errors::Result<()> {
    let file = File::create(output_path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn main() -> parquet::errors::Result<()> {
    let args = Args::parse();

    // Ensure the output directory exists
    fs::create_dir_all(&args.output_dir)?;

    // Open the input Parquet file
    let file = File::open(&args.parquet_file)?;
    let mut record_reader = ParquetRecordBatchReader::try_new(file, args.chunk_size)?;

    let mut chunk_index = 0;

    // Process each batch and write to a new file
    while let Some(batch) = record_reader.next() {
        let batch = batch?;
        let output_path = format!("{}/{}_{}.parquet", args.output_dir, args.output_part, chunk_index);

        // Write the batch to a new Parquet file
        write_parquet_file(&output_path, &batch)?;
        println!("Wrote chunk {} to {}", chunk_index, output_path);

        chunk_index += 1;
    }

    println!("Finished splitting the Parquet file.");
    Ok(())
}
