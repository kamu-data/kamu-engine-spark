use tonic::{Request, Response, Status};

use crate::generated::adapter_server::Adapter;
use crate::generated::{ExecuteQueryRequest, ExecuteQueryResponse};

#[derive(Debug, Default)]
pub struct AdapterImpl {}

#[tonic::async_trait]
impl Adapter for AdapterImpl {
    async fn execute_query(&self, _request: Request<ExecuteQueryRequest>)->Result<Response<ExecuteQueryResponse>, Status> {
        let span = tracing::span!(tracing::Level::INFO, "execute_query");
        let _enter = span.enter();

        let response = ExecuteQueryResponse { flatbuffer: Vec::new() };

        Ok(Response::new(response))
    }
}