use std::fs::File;
use std::path::Path;
use std::error::Error;
use fluvio::{Fluvio, TopicProducerPool, TopicProducerConfigBuilder, Compression};
use fluvio::metadata::topic::TopicSpec;
use parquet::record::{Field, List, Map, Row};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::data_type::Decimal;
use clap::Parser;
use serde_json::Value;
use chrono::{NaiveDate, DateTime};

// Constants
const PROGRESS_PRINT_COUNTER: usize = 5000;
const FACTOR: usize = 1024 * 1024;       // 500kb
const PRODUCE_BATCH_SIZE_BYTES: usize = 1 * FACTOR;
const PRODUCE_MAX_REQUEST_SIZE_BYTES: usize = 10 * FACTOR;

/// Command-line arguments
#[derive(Parser, Debug)]
#[command(
    name = "Parquet Processor", version = "1.0", 
    about = "Process Parquet files incrementally and send to Fluvio"
)]
struct Args {
    /// Path to the Parquet file
    #[arg(short, long)]
    file: String,

    /// Name of the topic for Fluvio producer
    #[arg(short, long, default_value = "parquet")]
    topic: String,
}

// Parquet to JSON conversion
fn row_to_json(row: &Row) -> Value {
    let mut json_object = serde_json::Map::new();

    for (key, field) in row.get_column_iter() {
        let json_value = field_to_json(&field);
        json_object.insert(key.to_string(), json_value);
    }

    Value::Object(json_object)
}

/// Convert Decimal to JSON
fn decimal_to_json(decimal: &Decimal) -> Value {
    match decimal {
        Decimal::Int32 { value, .. } => {
            let num = i32::from_be_bytes(*value); // Convert 4-byte array to i32
            Value::String(num.to_string())
        }
        Decimal::Int64 { value, .. } => {
            let num = i64::from_be_bytes(*value); // Convert 8-byte array to i64
            Value::String(num.to_string())
        }
        Decimal::Bytes { value, .. } => {
            // Convert byte array to a readable hex string
            Value::String(format!("0x{}", hex::encode(value)))
        }
    }
}

/// Convert Date (days since epoch) to JSON
fn date_to_json(days: i32) -> Value {
    match NaiveDate::from_num_days_from_ce_opt(days + 719_163) {
        Some(naive_date) => Value::String(naive_date.to_string()),
        None => Value::Null,
    }
}

/// Convert Timestamp to JSON
fn timestamp_to_json(timestamp: i64, divisor: i64) -> Value {
    let naive_datetime = DateTime::from_timestamp(
        timestamp / divisor,
        ((timestamp % divisor) * 1_000_000) as u32, // Nanoseconds as i32
    );
    match naive_datetime {
        Some(datetime) => Value::String(datetime.to_string()),
        None => Value::Null,
    }
}

/// Convert MapInternal to JSON
fn map_to_json(map: &Map) -> Value {
    let mut json_object = serde_json::Map::new();
    for (key, value) in map.entries() {
        json_object.insert(key.to_string(), field_to_json(value));
    }
    Value::Object(json_object)
}

/// Convert Group to JSON
fn group_to_json(group: &Row) -> Value {
    row_to_json(group) // Recursively process nested rows
}

/// Convert ListInternal to JSON
fn list_to_json(list: &List) -> Value {
    let json_array: Vec<Value> = list.elements().iter().map(field_to_json).collect();
    Value::Array(json_array)
}

/// Convert individual Field to JSON
fn field_to_json(field: &Field) -> Value {
    match field {
        Field::Null => Value::Null,
        Field::Bool(v) => Value::Bool(*v),
        Field::Byte(v) => Value::Number((*v as i64).into()),
        Field::Short(v) => Value::Number((*v as i64).into()),
        Field::Int(v) => Value::Number((*v as i64).into()),
        Field::Long(v) => Value::Number((*v).into()),
        Field::UByte(v) => Value::Number((*v as u64).into()),
        Field::UShort(v) => Value::Number((*v as u64).into()),
        Field::UInt(v) => Value::Number((*v as u64).into()),
        Field::ULong(v) => Value::Number((*v).into()),
        Field::Float(v) => serde_json::Number::from_f64(*v as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Field::Float16(v) => serde_json::Number::from_f64(v.to_f64())
            .map(Value::Number)            
            .unwrap_or(Value::Null),
        Field::Double(v) => serde_json::Number::from_f64(*v)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Field::Str(v) => Value::String(v.clone()),
        Field::Bytes(v) => Value::String(hex::encode(v)),
        Field::Decimal(decimal) => decimal_to_json(decimal),
        Field::Date(days) => date_to_json(*days),
        Field::TimestampMillis(ts) => timestamp_to_json(*ts, 1_000),
        Field::TimestampMicros(ts) => timestamp_to_json(*ts, 1_000_000),
        Field::MapInternal(map) => map_to_json(map),
        Field::Group(group) => group_to_json(group),
        Field::ListInternal(list) => list_to_json(list),
    }
}

pub async fn create_fluvio_topic(topic_name: &str) -> Result<(), Box<dyn Error>> {
    // Connect to the Fluvio cluster
    let fluvio = Fluvio::connect().await?;

    // Create a topic
    let admin = fluvio.admin().await;
    let topic_spec = TopicSpec::new_computed(1, 1, None);
    let _ = admin.create(topic_name.to_string(), false, topic_spec).await;

    Ok(())
}   

async fn get_fluvio_producer(topic_name: &str) -> Result<TopicProducerPool, Box<dyn Error>> {
    let fluvio = Fluvio::connect().await?;

    let pconfig = TopicProducerConfigBuilder::default()
        .batch_size(PRODUCE_BATCH_SIZE_BYTES)
        .compression(Compression::Gzip)
        .max_request_size(PRODUCE_MAX_REQUEST_SIZE_BYTES)
        .build()?;
    fluvio.topic_producer_with_config(topic_name, pconfig).await.map_err(|e| e.into())
}

async fn process_parquet_file(
    file_path: &str,
    topic_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let print_counter = PROGRESS_PRINT_COUNTER;
    let mut count = 0;

    // Open the file
    let file = File::open(file_path)?;

    // Create a Parquet reader
    let reader = SerializedFileReader::new(file)?;

    // Create Fluvio topic & producer
    create_fluvio_topic(&topic_name).await?;
    let producer = get_fluvio_producer(&topic_name).await?;

    // Iterate over parquet rows
    for row_result in reader.get_row_iter(None)? {
        let row = row_result?;
        let result = row_to_json(&row);

        let res = serde_json::to_string(&result)?;
        producer.send("key", res).await?;
        count += 1;

        // Print progress
        if count % print_counter == 0 {
            println!("{} records", count);
        }
    }

    // flush remaining records
    producer.flush().await?;
    println!("Processed {} records", count);
    Ok(())
}

// Main function
#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Extract file and topic
    let file_path = args.file;
    let topic_name = args.topic;

    println!("Processing file: {}", file_path);
    println!("Sending data to topic: {}", topic_name);

    // Check if file exists
    if !Path::new(&file_path).exists() {
        println!("File not found: {}", file_path);
        return Ok(());
    }

    // Process the file
    process_parquet_file(&file_path, &topic_name).await?;

    Ok(())
}
