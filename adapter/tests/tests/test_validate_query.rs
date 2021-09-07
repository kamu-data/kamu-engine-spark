use std::time::Duration;

use crate::utils::{
    container_runtime::{ContainerRuntime, ContainerRuntimeConfig, ContainerRuntimeType},
    engine_runner::Engine,
};

#[test]
fn test_validate_query_simple() {
    let engine = Engine::new(ContainerRuntime::new(ContainerRuntimeConfig {
        runtime: ContainerRuntimeType::Docker,
        ..ContainerRuntimeConfig::default()
    }))
    .unwrap();

    eprintln!("WAITIIIIIIIIIIIIING");
    engine.wait_to_start(Duration::from_secs(15)).unwrap();
    eprintln!("WAITIIIIIEEEEEEEEEEEEEEEED");
}
