use anyhow::Result;
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use futures_lite::StreamExt;
use serde_json::from_str;
use fluvio::{
    consumer::{ConsumerConfigExtBuilder, OffsetManagementStrategy},
    Fluvio, Offset
};

#[derive(Debug, Serialize, Deserialize, Row)]
struct Event {
    id: String,
    timestamp: String,
    payload: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize ClickHouse client
    let clickhouse_client = Client::default()
        .with_url("http://localhost:8123")
        .with_database("default")
        .with_user("default")
        .with_password(""); 

    // Stream Fluvio messages
    let fluvio = Fluvio::connect().await?;
    let mut stream = fluvio
        .consumer_with_config(
            ConsumerConfigExtBuilder::default()
                .topic("clickhouse".to_string())
                .offset_consumer("ch-consumer".to_string())
                .offset_start(Offset::end())
                .offset_strategy(OffsetManagementStrategy::Auto)
                .build()?,
        )
        .await?;

    while let Some(Ok(record)) = stream.next().await {
        // parse fluvio event into Event struct
        let value_str = String::from_utf8_lossy(record.get_value()).to_string();
        let event: Event = from_str(&value_str)?;

        // Insert the event into ClickHouse
        let query = format!(
            "INSERT INTO events (id, timestamp, payload) VALUES ('{}', '{}', '{}')",
            event.id, event.timestamp, event.payload
        );
        clickhouse_client.query(&query).execute().await?;

        println!("Inserted event: {:?}", event);
    }

    Ok(())
}

