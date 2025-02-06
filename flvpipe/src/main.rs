
use anyhow::Result;
use clap::Parser;
use fluvio::Fluvio;
use fluvio::dataplane::link::ErrorCode;
use fluvio::{spu::SpuSocketPool, TopicProducer};
use fluvio::{
    Offset,
    RecordKey
};
use fluvio::consumer::{
    ConsumerStream,
    Record,
};
use futures_util::stream::StreamExt;

mod otel;

const DEFAULT_RUST_LOG: &str = "debug,fluvio=off,hyper=off";

#[derive(Parser, Debug)]
#[clap(name = "getsample", about = "A sample CLI application")]
struct CliOpts {
    /// topic to consume samples from
    in_topic: String,

    #[clap(long = "in-profile")]
    in_profile: Option<String>,

    /// destination to produce to, a topic name e.g. "cat-facts", or "otelm:"
    ///
    /// ""otelm:<addr>" send to otel metrics collector endpoint
    ///    "otelm:", blank default http://localhost:4318/v1/metrics
    ///    "otelm:http://another.host:4318/v1/metrics" for another host
    out_dest: String,

    /// fluvio profile to use, current if not provided
    #[clap(long = "out-profile")]
    out_profile: Option<String>,

    #[clap(short, long)]
    num_records: Option<u64>,

    #[clap(short, long, conflicts_with = "end")]
    start: Option<i64>,

    #[clap(short, long, conflicts_with = "start")]
    end: Option<u32>,
}


#[tokio::main]
async fn main() {
    let opts = CliOpts::parse();

    setup_logging();

    let mut consume = consume_stream(&opts).await.expect("failed to create consumer");
    let mut n = opts.num_records.unwrap_or(u64::MAX);

    const OTEL_METRICS_PREFIX: &str = "otelm:";
    if opts.out_dest.starts_with(OTEL_METRICS_PREFIX) {

        // split out addr from the reset of the parameter
        let addr = opts.out_dest.trim_start_matches(OTEL_METRICS_PREFIX);
        let mut otel = otel::OtelMetrics::new(addr.to_string());
        otel.connect().await.expect("failed to connect to otel");

        while n > 0 {
            let Some(Ok(record)) = consume.next().await else {
                break;
            };
            let raw_metrics_bytes = record.value().to_vec();
            otel.send_metrics_packet(raw_metrics_bytes).await
                .inspect_err(|err| {
                    tracing::debug!("error sending metrics: {}", err);
                }).expect("failed to send metrics");
            n -= 1;
        }
    } else {
        let produce = produce_stream(&opts).await.expect("failed to create producer");
        while n > 0 {
            let Some(Ok(record)) = consume.next().await else {
                break;
            };
            let fut = match record.key() {
                Some(key) => produce.send(key, record.value()).await,
                None => produce.send(RecordKey::NULL, record.value()).await,
            };
            fut.inspect_err(|err| {
                tracing::debug!("error sending metrics: {}", err);
            }).expect("failed to send metrics");
            n -= 1;
        }
    }
}

/// connect to fluvio with consume side options
async fn consume_stream(opts: &CliOpts) -> Result<impl ConsumerStream<Item = std::result::Result<Record, ErrorCode>>> {
    let fluvio = match &opts.in_profile {
        None => Fluvio::connect().await,
        Some(profile) if profile == "-" => Fluvio::connect().await,
        Some(profile) => Fluvio::connect_with_profile(profile).await,
    }.expect("failed to connect to fluvio");

    let off_start = match (opts.start, opts.end) {
        (None, None) => Offset::beginning(),
        (Some(start), None) => Offset::absolute(start).expect("invalid start offset, start must be positive"),
        (None, Some(end)) => {
            Offset::from_end(end)
        },
        _ => panic!("cannot specify both start and end"),
    };

    let consumer_config = fluvio::consumer::ConsumerConfigExtBuilder::default()
        .topic(&opts.in_topic)
        .offset_start(off_start)
        .build()
        .expect("failed to setup consumer config");

    fluvio.consumer_with_config(consumer_config).await
}

/// connect to fluvio with consume side options
async fn produce_stream(opts: &CliOpts) -> Result<TopicProducer<SpuSocketPool>> {
    let fluvio = match &opts.out_profile {
        Some(profile) => Fluvio::connect_with_profile(profile)
            .await,
        None => Fluvio::connect()
            .await,
    }.expect("failed to connect to fluvio");

    let config = fluvio::TopicProducerConfigBuilder::default()
        .build()
        .expect("failed to setup producer config");

    fluvio.topic_producer_with_config(&opts.out_dest, config).await
}

/// setup to use RUST_LOG
fn setup_logging() {
    use tracing_subscriber::EnvFilter;

    tracing_subscriber::fmt()
    // Use the RUST_LOG environment variable for filter directives
    .with_env_filter(
        EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(DEFAULT_RUST_LOG))
    )
    // Show line numbers
    .with_file(true)
    .with_line_number(true)
    // Show target module path
    // .with_target(true)
    // // Show span events (enter/exit)
    // .with_span_events(tracing_subscriber::FmtSpan::FULL)
    // Initialize the subscriber
    .init();
}