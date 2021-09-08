use tonic::{transport::Server, Request, Response, Status};

use kamu_adapter::adapter_server::{Adapter, AdapterServer};
use kamu_adapter::{HelloReply, HelloRequest};

pub mod kamu_adapter {
    tonic::include_proto!("kamu_adapter");
}

#[derive(Debug, Default)]
pub struct AdapterImpl {}

#[tonic::async_trait]
impl Adapter for AdapterImpl {
    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        println!("Got a request: {:?}", request);

        let reply = HelloReply {
            message: format!("Hello {}!", request.into_inner().name).into(),
        };

        Ok(Response::new(reply))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

const BINARY_NAME: &str = env!("CARGO_PKG_NAME");
const VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_LOGGING_CONFIG: &str = "info";

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();

    tracing::info!("kamu-engine-spark ODF adapter (v{})", VERSION);

    let addr = "0.0.0.0:2884".parse()?;
    let adapter = AdapterImpl::default();

    tracing::info!("gRPC listening on: http://{}", addr);

    Server::builder()
        .add_service(AdapterServer::new(adapter))
        .serve(addr)
        .await?;

    Ok(())
}

/////////////////////////////////////////////////////////////////////////////////////////

fn init_logging() {
    use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
    use tracing_log::LogTracer;
    use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Registry};

    // Redirect all standard logging to tracing events
    LogTracer::init().expect("Failed to set LogTracer");

    // Use configuration from RUST_LOG env var if provided
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or(EnvFilter::new(DEFAULT_LOGGING_CONFIG.to_owned()));

    // TODO: Use non-blocking writer?
    // Configure Bunyan JSON formatter
    let formatting_layer = BunyanFormattingLayer::new(BINARY_NAME.to_owned(), std::io::stdout);
    let subscriber = Registry::default()
        .with(env_filter)
        .with(JsonStorageLayer)
        .with(formatting_layer);

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");
}
