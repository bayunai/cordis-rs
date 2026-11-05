use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin, Runtime};
use cordis_loader::{ExtensionCatalog, ExtensionFactory, LoaderError, PreparedExtension};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// 此模块刻意不导入 PluginDefinition / LazyLock，验证宏路径独立于调用环境。
mod definitions {
    cordis_core::plugin_definition!(
        /// 可由调用方复用的共享定义。
        pub static PUBLIC = "macro.same-name";
    );
    cordis_core::plugin_definition!(pub(super) static RESTRICTED = "macro.same-name");
    cordis_core::plugin_definition!(static PRIVATE = "macro.private";);
    cordis_core::plugin_definition!(
        #[allow(non_upper_case_globals)]
        pub(super) static attributed = "macro.attributes";
    );

    pub(super) fn private_id() -> cordis_core::PluginDefinitionId {
        PRIVATE.id()
    }
}

#[test]
fn declarations_preserve_visibility_attributes_and_independent_identity() {
    assert_eq!(definitions::PUBLIC.name(), Some("macro.same-name"));
    assert_eq!(definitions::RESTRICTED.name(), Some("macro.same-name"));
    assert_ne!(definitions::PUBLIC.id(), definitions::RESTRICTED.id());
    assert_eq!(definitions::attributed.name(), Some("macro.attributes"));
    assert_ne!(definitions::private_id(), definitions::PUBLIC.id());
    let clone = definitions::PUBLIC.clone();
    assert_eq!(clone.id(), definitions::PUBLIC.id());
}

#[test]
fn function_local_definition_is_shared_across_calls_and_threads() {
    fn definition() -> &'static cordis_core::PluginDefinition {
        cordis_core::plugin_definition!(static LOCAL = "macro.local");
        &LOCAL
    }
    let ids: Vec<_> = (0..8)
        .map(|_| std::thread::spawn(|| definition().id()))
        .collect();
    let expected = definition().id();
    for thread in ids {
        assert_eq!(thread.join().unwrap(), expected);
    }
}

struct Example;
#[async_trait]
impl Plugin for Example {
    async fn apply(&self, _: &Context) -> Result<(), CoreError> {
        Ok(())
    }
}
#[derive(Deserialize, Serialize, JsonSchema)]
struct Config {}
struct Factory;
impl ExtensionFactory for Factory {
    type Config = Config;
    fn id(&self) -> &'static str {
        "macro.example"
    }
    fn prepare(&self, _: Config) -> Result<PreparedExtension, LoaderError> {
        Ok(PreparedExtension::ordinary(|| Ok(Arc::new(Example))))
    }
}

#[tokio::test]
async fn definitions_work_with_existing_mount_catalog_and_group_unmount_interfaces() {
    let mut catalog = ExtensionCatalog::new();
    catalog.register(&definitions::PUBLIC, Factory).unwrap();
    assert_eq!(
        catalog.definition("macro.example").unwrap().id(),
        definitions::PUBLIC.id()
    );
    let runtime = Runtime::new().unwrap();
    let a = runtime
        .root()
        .plugin(&definitions::PUBLIC, Arc::new(Example))
        .await
        .unwrap();
    let b = runtime
        .root()
        .plugin(&definitions::PUBLIC, Arc::new(Example))
        .await
        .unwrap();
    let unrelated = runtime
        .root()
        .plugin(&definitions::RESTRICTED, Arc::new(Example))
        .await
        .unwrap();
    assert_eq!(a.definition().id(), b.definition().id());
    runtime.unmount(definitions::PUBLIC.id()).await.unwrap();
    assert!(a.is_disposed());
    assert!(b.is_disposed());
    assert!(!unrelated.is_disposed());
    runtime.shutdown().await.unwrap();
}
