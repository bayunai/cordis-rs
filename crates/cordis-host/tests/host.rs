use cordis_host::CordisHost;
use cordis_loader::{ExtensionCatalog, LOADER};

#[tokio::test]
async fn host_starts_loader_and_exposes_its_root_service() {
    let host = CordisHost::new(ExtensionCatalog::new()).unwrap();

    let direct = host.loader();
    let from_root = host.root().get(LOADER).unwrap();
    assert!(direct.entries().unwrap().extensions.is_empty());
    assert!(from_root.entries().unwrap().extensions.is_empty());

    host.shutdown().await.unwrap();
}
