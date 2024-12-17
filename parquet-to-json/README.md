## Parquet to JSON Converter

This project reads parquet files, converts each row to JSON record, and writes the record to a fluvio topic.
Fluvio uses patch processing and compression to optimize performance and reduce network traffic.

For this example, we use the NYC Taxi and Limousine Commission (TLC) trip record data for Jan, 2023. 
It's large file with 18.4 million records, and it will take a few minutes so grab a cup of coffee.

## Download Dataset

```bash
(mkdir -p test-data && cd test-data && curl -o fhvhv_tripdata_2023-01.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2023-01.parquet)
```

## Build & Run

```bash
cargo run --release --bin parquet-to-json -- --topic taxi-data --file test-data/fhvhv_tripdata_2023-01.parquet
```

## Check Progress

```bash
fluvio consume taxi-data
```

## Resources

* Data Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
* Data Schema: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_hvfhs.pdf
