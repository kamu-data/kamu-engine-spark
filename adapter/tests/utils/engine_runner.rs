use std::{
    process::{Child, Stdio},
    time::Duration,
};
use thiserror::Error;

use super::container_runtime::{ContainerRuntime, RunArgs, TimeoutError};

pub struct Engine {
    runtime: ContainerRuntime,
    container_name: String,
    adapter_host_port: u16,
    process: Child,
}

impl Engine {
    pub const ENGINE_IMAGE: &'static str = "kamudata/engine-spark:0.12.0-spark_3.1.2";
    pub const ADAPTER_PORT: u16 = 2474;

    pub fn new(runtime: ContainerRuntime) -> Result<Self, EngineStartError> {
        use rand::Rng;

        let mut container_name = "kamu-engine-spark-".to_owned();
        container_name.extend(
            rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(10)
                .map(char::from),
        );

        let process = runtime
            .run_cmd(RunArgs {
                image: Self::ENGINE_IMAGE.to_owned(),
                container_name: Some(container_name.clone()),
                ..RunArgs::default()
            })
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| EngineStartError::from(e))?;

        eprintln!("SPPPPPPPPPPPAAAAAAAAAAAAAAAAWn");

        let adapter_host_port = runtime
            .wait_for_host_port(&container_name, Self::ADAPTER_PORT, Duration::from_secs(10))
            .map_err(|e| EngineStartError::from(e))?;

        eprintln!("PORTTTTTTTTTTT");

        Ok(Self {
            runtime,
            container_name,
            adapter_host_port,
            process,
        })
    }

    pub fn wait_to_start(&self, timeout: Duration) -> Result<(), TimeoutError> {
        self.runtime
            .wait_for_socket(self.adapter_host_port, timeout)
    }

    pub fn has_exited(&mut self) -> Result<bool, std::io::Error> {
        Ok(self.process.try_wait()?.map(|_| true).unwrap_or(false))
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        unsafe {
            libc::kill(self.process.id() as i32, libc::SIGTERM);
        }

        let start = chrono::Utc::now();

        while (chrono::Utc::now() - start).num_seconds() < 3 {
            if self.has_exited().unwrap_or(true) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        let _ = self.process.kill();
    }
}

#[derive(Debug, Error)]
#[error("Engine failed to start")]
pub struct EngineStartError {
    source: Box<dyn std::error::Error>,
}

impl EngineStartError {
    fn from<E: std::error::Error + 'static>(e: E) -> Self {
        Self {
            source: Box::new(e),
        }
    }
}
