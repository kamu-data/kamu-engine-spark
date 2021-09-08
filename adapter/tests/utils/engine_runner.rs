use std::{ops::{Deref, DerefMut}, process::{Child, Stdio}, time::Duration};
use thiserror::Error;

use super::container_runtime::{ContainerRuntime, RunArgs};

use super::generated::adapter_client::AdapterClient;
use super::generated::ExecuteQueryRequest;

pub struct Engine {
    client: AdapterClient<tonic::transport::Channel>,
    _process: OwnedProcess,
}

impl Engine {
    pub const ENGINE_IMAGE: &'static str = "kamudata/engine-spark:0.12.0-spark_3.1.2";
    pub const ADAPTER_PORT: u16 = 2884;

    pub async fn new(runtime: ContainerRuntime, timeout: Duration) -> Result<Self, EngineStartError> {
        use rand::Rng;

        let mut container_name = "kamu-engine-spark-".to_owned();
        container_name.extend(
            rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(10)
                .map(char::from),
        );

        let process = OwnedProcess(runtime
            .run_cmd(RunArgs {
                image: Self::ENGINE_IMAGE.to_owned(),
                container_name: Some(container_name.clone()),
                expose_ports: vec![Self::ADAPTER_PORT],
                ..RunArgs::default()
            })
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| EngineStartError::from(e))?);

        let adapter_host_port = runtime
            .wait_for_host_port(&container_name, Self::ADAPTER_PORT, Duration::from_secs(10))
            .map_err(|e| EngineStartError::from(e))?;

        runtime
            .wait_for_socket(adapter_host_port, timeout).map_err(|e| EngineStartError::from(e))?;

        let client = AdapterClient::connect(
            format!("http://{}:{}", 
                runtime.get_runtime_host_addr(), 
                adapter_host_port)
        ).await.map_err(|e| EngineStartError::from(e))?;

        Ok(Self {
            client,
            _process: process,
        })
    }

    pub async fn say_hello(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let request = tonic::Request::new(ExecuteQueryRequest {
            flatbuffer: Vec::new(),
        });

        let response = self.client.execute_query(request).await?;

        println!("RESPONSE={:?}", response);

        Ok(())
    }
}

struct OwnedProcess(Child);

impl OwnedProcess {
    pub fn has_exited(&mut self) -> Result<bool, std::io::Error> {
        Ok(self.0.try_wait()?.map(|_| true).unwrap_or(false))
    }
}

impl Deref for OwnedProcess {
    type Target = Child;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for OwnedProcess {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for OwnedProcess {
    fn drop(&mut self) {
        unsafe {
            libc::kill(self.0.id() as i32, libc::SIGTERM);
        }

        let start = chrono::Utc::now();

        while (chrono::Utc::now() - start).num_seconds() < 3 {
            if self.has_exited().unwrap_or(true) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        let _ = self.0.kill();
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
