use std::path::{Path, PathBuf};

use opendatafabric::{
    serde::{yaml::YamlEngineProtocol, EngineProtocolDeserializer, EngineProtocolSerializer},
    *,
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

    // TODO: Generalize with `execute_transform`
    pub async fn execute_raw_query_impl(
        &self,
        request: RawQueryRequest,
    ) -> Result<RawQueryResponse, Box<dyn std::error::Error>> {
        let in_out_dir = PathBuf::from("/opt/engine/in-out");
        let _ = std::fs::remove_dir_all(&in_out_dir);
        let _ = std::fs::create_dir_all(&in_out_dir);

        let request_path = in_out_dir.join("request.yaml");
        let response_path = in_out_dir.join("response.yaml");

        // Write request
        {
            let data = YamlEngineProtocol.write_raw_query_request(&request)?;
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
                "dev.kamu.engine.spark.RawQueryApp",
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

        if response_path.exists() {
            let data = std::fs::read_to_string(&response_path)?;
            Ok(YamlEngineProtocol.read_raw_query_response(data.as_bytes())?)
        } else if !exit_status.success() {
            error!(
                message = "Job exited with non-zero code",
                code = exit_status.code().unwrap(),
            );

            Ok(RawQueryResponseInternalError {
                message: format!(
                    "Engine exited with non-zero status: {}",
                    exit_status.code().unwrap()
                ),
                backtrace: None,
            }
            .into())
        } else {
            Ok(RawQueryResponseInternalError {
                message: format!("Engine did not write the response file"),
                backtrace: None,
            }
            .into())
        }
    }

    pub async fn execute_transform_impl(
        &self,
        mut request: TransformRequest,
    ) -> Result<TransformResponse, Box<dyn std::error::Error>> {
        let in_out_dir = PathBuf::from("/opt/engine/in-out");
        let _ = std::fs::remove_dir_all(&in_out_dir);
        let _ = std::fs::create_dir_all(&in_out_dir);

        let request_path = in_out_dir.join("request.yaml");
        let response_path = in_out_dir.join("response.yaml");

        // Prepare checkpoint
        let orig_new_checkpoint_path = request.new_checkpoint_path;
        request.new_checkpoint_path = in_out_dir.join("new-checkpoint");
        if let Some(tar_path) = &request.prev_checkpoint_path {
            let restored_checkpoint_path = in_out_dir.join("prev-checkpoint");
            Self::unpack_checkpoint(tar_path, &restored_checkpoint_path)?;
            request.prev_checkpoint_path = Some(restored_checkpoint_path);
        }

        // Write request
        {
            let data = YamlEngineProtocol.write_transform_request(&request)?;
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
                "dev.kamu.engine.spark.TransformApp",
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

        if response_path.exists() {
            // Pack checkpoint
            if request.new_checkpoint_path.exists()
                && request.new_checkpoint_path.read_dir()?.next().is_some()
            {
                std::fs::create_dir_all(orig_new_checkpoint_path.parent().unwrap())?;
                Self::pack_checkpoint(&request.new_checkpoint_path, &orig_new_checkpoint_path)?;
            }

            let data = std::fs::read_to_string(&response_path)?;
            Ok(YamlEngineProtocol.read_transform_response(data.as_bytes())?)
        } else if !exit_status.success() {
            error!(
                message = "Job exited with non-zero code",
                code = exit_status.code().unwrap(),
            );

            Ok(TransformResponse::InternalError(
                TransformResponseInternalError {
                    message: format!(
                        "Engine exited with non-zero status: {}",
                        exit_status.code().unwrap()
                    ),
                    backtrace: None,
                },
            ))
        } else {
            Ok(TransformResponse::InternalError(
                TransformResponseInternalError {
                    message: format!("Engine did not write the response file"),
                    backtrace: None,
                },
            ))
        }
    }

    fn unpack_checkpoint(
        prev_checkpoint_path: &Path,
        target_dir: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!(
            ?prev_checkpoint_path,
            ?target_dir,
            "Unpacking previous checkpoint"
        );
        std::fs::create_dir(target_dir)?;
        let mut archive = tar::Archive::new(std::fs::File::open(prev_checkpoint_path)?);
        archive.unpack(target_dir)?;
        Ok(())
    }

    fn pack_checkpoint(
        source_dir: &Path,
        new_checkpoint_path: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!(?source_dir, ?new_checkpoint_path, "Packing new checkpoint");
        let mut ar = tar::Builder::new(std::fs::File::create(new_checkpoint_path)?);
        ar.follow_symlinks(false);
        ar.append_dir_all(".", source_dir)?;
        ar.finish()?;
        Ok(())
    }
}
