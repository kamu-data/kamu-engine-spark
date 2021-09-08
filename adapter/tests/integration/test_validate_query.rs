use std::time::Duration;

use crate::utils::{
    container_runtime::{ContainerRuntime, ContainerRuntimeConfig, ContainerRuntimeType},
    engine_runner::Engine,
};

#[tokio::test]
async fn test_validate_query_simple() {
    let runtime =  ContainerRuntime::new(ContainerRuntimeConfig {
        runtime: ContainerRuntimeType::Docker,
        ..ContainerRuntimeConfig::default()
    });

    let mut engine = Engine::new(runtime, Duration::from_secs(15)).await.unwrap();

    engine.say_hello().await.unwrap();
}
