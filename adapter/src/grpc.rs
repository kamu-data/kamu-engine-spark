use std::sync::Arc;

use opendatafabric::serde::{EngineProtocolDeserializer, EngineProtocolSerializer};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use opendatafabric::engine::generated::engine_server::Engine as EngineGRPC;
use opendatafabric::engine::generated::{
    ExecuteQueryRequest as ExecuteQueryRequestGRPC,
    ExecuteQueryResponse as ExecuteQueryResponseGRPC,
};
use opendatafabric::serde::flatbuffers::FlatbuffersEngineProtocol;
use tracing::info;

use crate::adapter::SparkODFAdapter;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct EngineGRPCImpl {
    adapter: Arc<SparkODFAdapter>,
}

impl EngineGRPCImpl {
    pub fn new(adapter: Arc<SparkODFAdapter>) -> Self {
        Self { adapter }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tonic::async_trait]
impl EngineGRPC for EngineGRPCImpl {
    type ExecuteQueryStream = ReceiverStream<Result<ExecuteQueryResponseGRPC, Status>>;

    async fn execute_query(
        &self,
        request_grpc: Request<ExecuteQueryRequestGRPC>,
    ) -> Result<Response<Self::ExecuteQueryStream>, Status> {
        let span = tracing::span!(tracing::Level::INFO, "execute_query");
        let _enter = span.enter();

        let request = FlatbuffersEngineProtocol
            .read_execute_query_request(&request_grpc.get_ref().flatbuffer)
            .unwrap();

        info!(message = "Got request", request = ?request);

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let adapter = self.adapter.clone();

        tokio::spawn(async move {
            let response = adapter.execute_query_impl(request).await.unwrap();

            let response_fb = FlatbuffersEngineProtocol
                .write_execute_query_response(&response)
                .unwrap();

            let response_grpc = ExecuteQueryResponseGRPC {
                flatbuffer: response_fb.collapse_vec(),
            };

            tx.send(Ok(response_grpc)).await.unwrap();

            // let responses = [
            //     ExecuteQueryResponse::Progress,
            //     ExecuteQueryResponse::Progress,
            //     ExecuteQueryResponse::Error,
            // ];

            // for response in responses {
            //     tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            //     let response_fb = FlatbuffersEngineProtocol
            //         .write_execute_query_response(&response)
            //         .unwrap();

            //     let response_grpc = ExecuteQueryResponseGRPC {
            //         flatbuffer: response_fb.collapse_vec(),
            //     };

            //     tx.send(Ok(response_grpc)).await.unwrap();
            // }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}