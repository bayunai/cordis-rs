//! Fiber 状态事件既驱动快照失效，也驱动生命周期日志；丢失历史必须明确诊断。
use crate::plugin::LoaderInner;
use cordis_core::FiberStateChange;
use std::sync::{Arc, atomic::Ordering};
use tokio::sync::broadcast;

pub(crate) async fn observe_fiber_events(
    inner: Arc<LoaderInner>,
    mut receiver: broadcast::Receiver<FiberStateChange>,
) {
    while inner.alive.load(Ordering::Acquire) {
        match receiver.recv().await {
            Ok(change) => inner.observe_fiber_state(change),
            Err(broadcast::error::RecvError::Lagged(missed)) => {
                // 先丢弃积压再拍摄实际状态；不能将旧事件当作新基线之后的生命周期。
                receiver = receiver.resubscribe();
                recover_fiber_event_gap(&inner, missed);
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

fn recover_fiber_event_gap(inner: &LoaderInner, missed: u64) {
    // 警告不经过来源 enable_logs；缓存 Logger 在 Context 关闭期间仍可记录诊断。
    inner.root.lifecycle_logs.warn_event_gap(missed);
    for tree in std::iter::once(inner.root.clone()).chain(inner.runtime_trees()) {
        tree.resync_lifecycle();
    }
    inner.prune_disposed_bindings();
    inner.publish_snapshot_changed();
    inner.notify_state_progress();
}

#[cfg(test)]
mod tests {
    use super::observe_fiber_events;
    use crate::{
        EntryOptions, EntryTreeSource, ExtensionCatalog, ExtensionFactory, ExtensionsConfig,
        LOADER, Loader, LoaderError, LoaderPlugin, PreparedExtension, SourceDocument, SourceError,
        SourceRevision,
    };
    use async_trait::async_trait;
    use cordis_core::{
        Context, CoreError, Fiber, FiberState, FiberStateChange, LogLevel, Plugin, PluginKey,
        Runtime,
    };
    use schemars::JsonSchema;
    use serde::Deserialize;
    use std::sync::Arc;
    use tokio::sync::broadcast;

    struct TestSource(bool);
    #[async_trait]
    impl EntryTreeSource for TestSource {
        fn identity(&self) -> String {
            "event-gap-memory-source".into()
        }
        fn enable_logs(&self) -> bool {
            self.0
        }
        fn context(&self, owner: &Context) -> Result<Context, SourceError> {
            Ok(owner.clone())
        }
        async fn read(&self) -> Result<SourceDocument, SourceError> {
            Ok(SourceDocument {
                config: ExtensionsConfig {
                    version: 3,
                    extensions: vec![EntryOptions::new("leaf", "leaf")],
                },
                revision: SourceRevision::new(vec![0]),
            })
        }
        async fn check_revision(&self, _: &SourceRevision) -> Result<(), SourceError> {
            Ok(())
        }
        async fn validate_write(&self, _: &ExtensionsConfig) -> Result<(), SourceError> {
            Ok(())
        }
        async fn write(
            &self,
            _: &ExtensionsConfig,
            _: &SourceRevision,
        ) -> Result<SourceRevision, SourceError> {
            Ok(SourceRevision::new(vec![0]))
        }
    }

    #[derive(Deserialize, serde::Serialize, JsonSchema)]
    struct Empty {}
    struct Carrier(Arc<TestSource>);
    impl ExtensionFactory for Carrier {
        type Config = Empty;
        fn id(&self) -> &'static str {
            "carrier"
        }
        fn prepare(&self, _: Empty) -> Result<PreparedExtension, LoaderError> {
            let source = self.0.clone();
            Ok(PreparedExtension::tree(
                PluginKey::new("event-gap.carrier"),
                (),
                (),
                move |_, _, _| {
                    let source = source.clone();
                    async move { Ok(source as Arc<dyn EntryTreeSource>) }
                },
            ))
        }
    }
    struct LeafFactory;
    struct Leaf;
    impl ExtensionFactory for LeafFactory {
        type Config = Empty;
        fn id(&self) -> &'static str {
            "leaf"
        }
        fn prepare(&self, _: Empty) -> Result<PreparedExtension, LoaderError> {
            Ok(PreparedExtension::ordinary(|| Ok(Arc::new(Leaf))))
        }
    }
    #[async_trait]
    impl Plugin for Leaf {
        fn key(&self) -> PluginKey {
            PluginKey::new("event-gap.leaf")
        }
        async fn apply(&self, _: &Context) -> Result<(), CoreError> {
            Ok(())
        }
    }

    async fn fixture(logs: bool) -> (Runtime, Fiber, Loader) {
        let mut catalog = ExtensionCatalog::new();
        catalog
            .register(Carrier(Arc::new(TestSource(logs))))
            .unwrap();
        catalog.register(LeafFactory).unwrap();
        let runtime = Runtime::new().unwrap();
        let fiber = runtime
            .root()
            .plugin(Arc::new(LoaderPlugin::new(catalog)))
            .await
            .unwrap();
        let loader = (*runtime.root().get(LOADER).unwrap()).clone();
        loader
            .create(EntryOptions::new("tree", "carrier"), None, None)
            .await
            .unwrap();
        loader.await_idle().await.unwrap();
        tokio::task::yield_now().await;
        (runtime, fiber, loader)
    }
    fn info_count(runtime: &Runtime) -> usize {
        runtime
            .logger_service()
            .buffer_snapshot()
            .iter()
            .filter(|log| log.target == "loader" && log.level == LogLevel::Info)
            .count()
    }
    async fn wait_until(predicate: impl Fn() -> bool) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !predicate() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("event observer should converge");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn small_event_buffer_reports_gap_without_replaying_history_and_rebuilds_dedup_baseline()
    {
        for logs in [false, true] {
            let (runtime, _fiber, loader) = fixture(logs).await;
            let subtree = loader.subtree("tree").unwrap();
            let id = subtree
                .snapshot()
                .unwrap()
                .entry("tree:leaf")
                .unwrap()
                .fiber_id
                .unwrap();
            let tree = subtree.tree.clone();
            // 模拟已丢失的 Active 事件造成的旧基线，而实际 Fiber 仍是 Active。
            tree.record_lifecycle_state(id, FiberState::Pending);
            let before = info_count(&runtime);
            let change = |current| FiberStateChange {
                fiber_id: id,
                plugin_key: PluginKey::new("event-gap.leaf"),
                previous: Some(FiberState::Loading),
                current,
                unavailable_dependencies: vec![],
            };
            let (sender, receiver) = broadcast::channel(1);
            sender.send(change(FiberState::Pending)).unwrap();
            sender.send(change(FiberState::Loading)).unwrap();
            sender.send(change(FiberState::Active)).unwrap();
            let mut snapshots = loader.subscribe().unwrap();
            let observer = tokio::spawn(observe_fiber_events(loader.inner.clone(), receiver));
            snapshots.recv().await.unwrap();
            let warnings: Vec<_> = runtime
                .logger_service()
                .buffer_snapshot()
                .into_iter()
                .filter(|log| log.level == LogLevel::Warn)
                .collect();
            assert_eq!(warnings.len(), 1);
            assert!(warnings[0].message.contains("丢失 2 条"));
            assert!(warnings[0].message.contains("日志存在缺口"));
            assert_eq!(
                info_count(&runtime),
                before,
                "recovery must not invent historical mounts"
            );

            // 新基线已是 Active，重复 Active 不应重发挂载。
            sender.send(change(FiberState::Active)).unwrap();
            snapshots.recv().await.unwrap();
            assert_eq!(info_count(&runtime), before);
            // 此后的完整周期恢复正常去重，每步等消费，避免再次制造滞后。
            sender.send(change(FiberState::Unloading)).unwrap();
            snapshots.recv().await.unwrap();
            sender.send(change(FiberState::Active)).unwrap();
            snapshots.recv().await.unwrap();
            assert_eq!(info_count(&runtime), before + if logs { 2 } else { 0 });
            drop(sender);
            observer.await.unwrap();
            runtime.shutdown().await.unwrap();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn recovery_tracks_actual_fibers_and_removes_disposed_records_without_info_logs() {
        let (runtime, _fiber, loader) = fixture(true).await;
        let subtree = loader.subtree("tree").unwrap();
        let id = subtree
            .snapshot()
            .unwrap()
            .entry("tree:leaf")
            .unwrap()
            .fiber_id
            .unwrap();
        let tree = subtree.tree.clone();
        let before = info_count(&runtime);
        super::recover_fiber_event_gap(&loader.inner, 7);
        assert_eq!(info_count(&runtime), before);
        assert!(tree.record_lifecycle_state(id, FiberState::Active));
        assert_eq!(info_count(&runtime), before);

        // 真实 Core 卸载经过已有订阅输出一次，不受受控测试通道替代。
        runtime
            .unmount(PluginKey::new("event-gap.leaf"))
            .await
            .unwrap();
        wait_until(|| info_count(&runtime) == before + 1).await;
        let after = info_count(&runtime);
        // 注入一个代表“丢失 Disposed 事件”的旧缓存，恢复必须按真实状态移除它。
        tree.track_lifecycle(id, &"tree:leaf".into(), "leaf", FiberState::Pending);
        super::recover_fiber_event_gap(&loader.inner, 3);
        assert_eq!(info_count(&runtime), after);
        assert!(!tree.record_lifecycle_state(id, FiberState::Active));
        subtree.reload().await.unwrap();
        wait_until(|| info_count(&runtime) == after + 1).await;
        runtime.shutdown().await.unwrap();
    }
}
