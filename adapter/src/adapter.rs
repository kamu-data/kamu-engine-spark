use tonic::{Request, Response, Status};

use opendatafabric::engine::generated::engine_server::Engine as EngineGRPC;
use opendatafabric::engine::generated::{ExecuteQueryRequest, ExecuteQueryResponse};

#[derive(Debug, Default)]
pub struct SparkODFAdapter {}

#[tonic::async_trait]
impl EngineGRPC for SparkODFAdapter {
    async fn execute_query(
        &self,
        _request: Request<ExecuteQueryRequest>,
    ) -> Result<Response<ExecuteQueryResponse>, Status> {
        let span = tracing::span!(tracing::Level::INFO, "execute_query");
        let _enter = span.enter();

        let response = ExecuteQueryResponse {
            flatbuffer: Vec::new(),
        };

        Ok(Response::new(response))
    }
}
