use cordis_core::CoreError;
use cordis_host::CordisHost;

#[tokio::test]
async fn host_owns_runtime_without_assuming_a_loader_plugin() {
    let _: fn() -> Result<CordisHost, CoreError> = CordisHost::new;
    let host = CordisHost::new().unwrap();
    assert!(host.root().effect().is_ok());
    assert!(!host.runtime().scheduler_stopped());
    host.shutdown().await.unwrap();
}
