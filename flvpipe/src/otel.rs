use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest, metrics::v1::ResourceMetrics,
};
use prost::Message;
use reqwest::{header, Client};
use std::error::Error;

pub(crate) struct OtelMetrics {
    endpoint: String,
    // channel: Option<Channel>,
    client: reqwest::Client,
}

impl OtelMetrics {
    pub fn new(in_endpoint: String) -> Self {
        let endpoint = if in_endpoint.is_empty() {
            "http://localhost:4318/v1/metrics".to_string()
        } else {
            in_endpoint
        };
        Self {
            endpoint,
            // channel: None,
            client: Client::new(),
        }
    }

    pub async fn connect(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    pub async fn send_metrics_packet(
        &self,
        raw_metrics_bytes: Vec<u8>,
    ) -> Result<(), Box<dyn Error>> {
        let rm = ResourceMetrics::decode(&raw_metrics_bytes[..])?;
        let export_request = ExportMetricsServiceRequest {
            resource_metrics: vec![rm],
        };
        let bytes = export_request.encode_to_vec();

        // Set up the headers required by the OTLP collector
        let response = self
            .client
            .post(&self.endpoint)
            .header(header::CONTENT_TYPE, "application/x-protobuf")
            .header(
                "x-protobuf-message",
                "opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest",
            )
            .body(bytes)
            .send()
            .await?;

        match response.status() {
            status if status.is_success() => {
                tracing::trace!("Successfully sent metrics to collector");
                Ok(())
            }
            status => {
                let error_body = response.text().await?;
                Err(format!(
                    "Failed to send metrics. Status: {}, Body: {}",
                    status, error_body
                )
                .into())
            }
        }
    }
}

// grpc fail
// use anyhow::anyhow;
// use tonic::transport::Channel;
// use opentelemetry_proto::tonic::{
//     collector::metrics::v1::{
//         metrics_service_client::MetricsServiceClient,
//         ExportMetricsServiceRequest,
//     },
//     metrics::v1::ResourceMetrics,
// };
// impl OtelMetrics {
//     pub fn new(endpoint: String) -> Self {
//         Self {
//             endpoint,
//             channel: None,
//         }
//     }

//     pub async fn connect(&mut self) -> Result<(), Box<dyn Error>> {
//         let channel = Channel::from_shared(self.endpoint.clone())?
//             .connect()
//             .await
//             .map_err(|e| {
//                 tracing::debug!("Connection Error to {}: {}", self.endpoint, e);
//                 anyhow!("Connection Error")
//             })?;
//         self.channel = Some(channel);
//         Ok(())
//     }

//     pub async fn send_metrics_packet(&self, raw_metrics_bytes: Vec<u8>) -> Result<(), Box<dyn Error>> {
//         let channel = self.channel.as_ref().ok_or("channel not connected")?;
//         let mut client = MetricsServiceClient::new(channel.clone());
//         let rm = ResourceMetrics::decode(&raw_metrics_bytes[..])?;
//         let exp = ExportMetricsServiceRequest {
//             resource_metrics: vec![rm],
//         };
//         let _response = client.export(exp).await?;
//         Ok(())
//     }
// }
