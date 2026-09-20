use cordis_host::CordisHost;

#[tokio::test]
async fn host_owns_runtime_without_assuming_a_loader_plugin() {
    let host = CordisHost::new().unwrap();
    assert!(!host.runtime().scheduler_stopped());
    host.shutdown().await.unwrap();
}
