## Parquet Cunck File

Some parquet data files are quite large, and we only need part of it. This project reads large parquet files and breaks them up in multiple smaller files. 

## Download Dataset

```bash
(mkdir -p test-data && cd test-data && curl -o fhvhv_tripdata_2023-01.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2023-01.parquet)
```

## Build & Run

```bash
cargo run --release --bin parquet-chunk-file -- \
    --parquet-file test-data/fhvhv_tripdata_2023-01.parquet \
    --output-dir result \
    --output-part trip_data \
    --chunk-size 100000
```

The following parameters are optional:
* output-dir - default: output
* output-part - default: output_part
* chunk-size - default: 50,000
