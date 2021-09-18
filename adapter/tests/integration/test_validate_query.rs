use std::time::Duration;

use container_runtime::{ContainerRuntime, ContainerRuntimeConfig, ContainerRuntimeType};

use crate::utils::engine_container::EngineContainer;

#[tokio::test]
async fn test_validate_query_simple() {
    let runtime = ContainerRuntime::new(ContainerRuntimeConfig {
        runtime: ContainerRuntimeType::Docker,
        ..ContainerRuntimeConfig::default()
    });

    let engine_container = EngineContainer::new(runtime, Duration::from_secs(15)).unwrap();
    let mut engine_client = engine_container.get_client().await.unwrap();

    //engine_client.execute_query().await.unwrap();
}
