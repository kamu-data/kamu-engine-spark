use std::error::Error;
use tokio::{
    io::AsyncBufReadExt,
    process::{Child, Command},
    task::JoinHandle,
};

use tracing::info;

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct SparkMaster {
    process: Child,
}

impl SparkMaster {
    pub async fn start() -> Result<Self, Box<dyn Error>> {
        info!("Starting Spark master");

        let mut process = Command::new("sbin/start-master.sh")
            .current_dir("/opt/bitnami/spark")
            .env("SPARK_NO_DAEMONIZE", "1")
            .args(&["--host", "127.0.0.1", "--port", "7077"])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;

        forward_to_logging(process.stdout.take().unwrap(), "master", "stdout");
        forward_to_logging(process.stderr.take().unwrap(), "master", "stderr");

        Ok(Self { process })
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct SparkWorker {
    process: Child,
}

impl SparkWorker {
    pub async fn start() -> Result<Self, Box<dyn Error>> {
        info!("Starting Spark worker");

        let mut process = Command::new("sbin/start-worker.sh")
            .current_dir("/opt/bitnami/spark")
            .env("SPARK_NO_DAEMONIZE", "1")
            .args(&["spark://127.0.0.1:7077"])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;

        forward_to_logging(process.stdout.take().unwrap(), "worker", "stdout");
        forward_to_logging(process.stderr.take().unwrap(), "worker", "stderr");

        Ok(Self { process })
    }
}

///////////////////////////////////////////////////////////////////////////////

pub fn forward_to_logging<R>(
    reader: R,
    process: &'static str,
    stream: &'static str,
) -> JoinHandle<()>
where
    R: tokio::io::AsyncRead + Send + 'static + Unpin,
{
    let stderr = tokio::io::BufReader::new(reader);
    tokio::spawn(async move {
        let mut line_reader = stderr.lines();
        while let Some(line) = line_reader.next_line().await.unwrap() {
            info!(message = line.as_str(), process = process, stream = stream);
        }
    })
}
