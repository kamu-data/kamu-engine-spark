use std::path::PathBuf;

use opendatafabric::{
    serde::{yaml::YamlEngineProtocol, EngineProtocolDeserializer, EngineProtocolSerializer},
    ExecuteQueryRequest, ExecuteQueryResponse, ExecuteQueryResponseInternalError,
};
use tracing::{error, info};

use crate::spark::forward_to_logging;

// use crate::spark::{SparkMaster, SparkWorker};

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct SparkODFAdapter {
    // spark_master: SparkMaster,
// spark_worker: SparkWorker,
}

impl SparkODFAdapter {
    pub async fn new() -> Self {
        /*
        let spark_master = SparkMaster::start().await.unwrap();
        let spark_worker = SparkWorker::start().await.unwrap();
        Self {
            spark_master,
            spark_worker,
        }
        */
        Self {}
    }

    pub async fn execute_query_impl(
        &self,
        request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponse, Box<dyn std::error::Error>> {
        let in_out_dir = PathBuf::from("/opt/engine/in-out");
        let _ = std::fs::remove_dir_all(&in_out_dir);
        let _ = std::fs::create_dir_all(&in_out_dir);

        {
            let request_path = PathBuf::from("/opt/engine/in-out/request.yaml");
            let data = YamlEngineProtocol.write_execute_query_request(&request)?;
            std::fs::write(request_path, data)?;
        }

        let mut cmd = tokio::process::Command::new("/opt/bitnami/spark/bin/spark-submit");
        cmd.current_dir("/opt/bitnami/spark")
            .args(&[
                // "--master",
                // "spark://127.0.0.1:7077",
                "--master=local[4]",
                "--driver-memory=2g",
                "--conf",
                "spark.ui.enabled=false",
                "--class",
                "dev.kamu.engine.spark.transform.TransformApp",
                "/opt/engine/bin/engine.spark.jar",
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);

        info!(message = "Submitting Spark job", cmd = ?cmd);

        let mut process = cmd.spawn()?;

        let stdout_task = forward_to_logging(process.stdout.take().unwrap(), "submit", "stdout");
        let stderr_task = forward_to_logging(process.stderr.take().unwrap(), "submit", "stderr");

        let exit_status = process.wait().await?;
        stdout_task.await?;
        stderr_task.await?;

        let response_path = PathBuf::from("/opt/engine/in-out/response.yaml");

        if response_path.exists() {
            let data = std::fs::read_to_string(&response_path)?;
            Ok(YamlEngineProtocol.read_execute_query_response(data.as_bytes())?)
        } else if !exit_status.success() {
            error!(
                message = "Job exited with non-zero code",
                code = exit_status.code().unwrap(),
            );

            Ok(ExecuteQueryResponse::InternalError(
                ExecuteQueryResponseInternalError {
                    message: format!(
                        "Engine exited with non-zero status: {}",
                        exit_status.code().unwrap()
                    ),
                    backtrace: None,
                },
            ))
        } else {
            Ok(ExecuteQueryResponse::InternalError(
                ExecuteQueryResponseInternalError {
                    message: format!("Engine did not write the response file"),
                    backtrace: None,
                },
            ))
        }
    }
}
