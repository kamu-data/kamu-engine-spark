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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:2884".parse()?;
    let adapter = AdapterImpl::default();

    println!("RUUUUUUUN");

    Server::builder()
        .add_service(AdapterServer::new(adapter))
        .serve(addr)
        .await?;

    Ok(())
}
