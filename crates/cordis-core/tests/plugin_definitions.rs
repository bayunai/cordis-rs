//! 定义身份、统一匿名插件、控制句柄与显式失败恢复。
use async_trait::async_trait;
use cordis_core::{
    Context, CoreError, FiberHandle, FiberState, Plugin, PluginDefinition, Runtime, ServiceId,
    ServiceKey,
};
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

static DATA: ServiceKey<u64> = ServiceKey::new("test.definitions.data");

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("lifecycle must converge")
}

struct Noop;
#[async_trait]
impl Plugin for Noop {
    async fn apply(&self, _: &Context) -> Result<(), CoreError> {
        Ok(())
    }
}
struct Other;
#[async_trait]
impl Plugin for Other {
    async fn apply(&self, _: &Context) -> Result<(), CoreError> {
        Ok(())
    }
}

#[test]
fn definitions_have_identity_independent_of_names_and_instances() {
    let a = PluginDefinition::named("same-name");
    let alias = a.clone();
    let b = PluginDefinition::named("same-name");
    assert_eq!(a.id(), alias.id());
    assert_eq!(a.name(), b.name());
    assert_ne!(a.id(), b.id());
    let anonymous = PluginDefinition::anonymous();
    assert_eq!(anonymous.name(), None);
    assert_ne!(anonymous.id(), PluginDefinition::anonymous().id());
}

#[tokio::test]
async fn definitions_group_different_executors_and_replacement_preserves_identity() {
    let runtime = Runtime::new().unwrap();
    let root = runtime.root();
    let definition = PluginDefinition::named("same-name");
    let separate = PluginDefinition::named("same-name");
    let mut a = root.plugin(&definition, Arc::new(Noop)).await.unwrap();
    let b = root
        .plugin(&definition.clone(), Arc::new(Other))
        .await
        .unwrap();
    let c = root.plugin(&separate, Arc::new(Noop)).await.unwrap();
    let id = a.id();
    a.replace(Arc::new(Other)).await.unwrap();
    assert_eq!(a.id(), id);
    assert_eq!(a.definition().id(), definition.id());
    let snapshot = runtime.diagnostics();
    assert_eq!(
        snapshot
            .definitions
            .iter()
            .find(|d| d.definition_id == definition.id())
            .unwrap()
            .fiber_ids
            .len(),
        2
    );
    assert_eq!(runtime.unmount(definition.id()).await.unwrap(), 2);
    assert_eq!(a.state(), FiberState::Disposed);
    assert_eq!(b.state(), FiberState::Disposed);
    assert_eq!(c.state(), FiberState::Active);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn one_definition_can_be_reused_by_independent_runtimes() {
    let a = Runtime::new().unwrap();
    let b = Runtime::new().unwrap();
    let definition = PluginDefinition::named("shared-definition");
    let left = a.root().plugin(&definition, Arc::new(Noop)).await.unwrap();
    let right = b.root().plugin(&definition, Arc::new(Noop)).await.unwrap();
    assert_eq!(a.unmount(definition.id()).await.unwrap(), 1);
    assert_eq!(left.state(), FiberState::Disposed);
    assert_eq!(right.state(), FiberState::Active);
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn owning_drop_disposes_but_control_handle_drop_does_not() {
    let runtime = Runtime::new().unwrap();
    let definition = PluginDefinition::named("owned");
    let owner = runtime
        .root()
        .plugin(&definition, Arc::new(Noop))
        .await
        .unwrap();
    let handle = owner.handle();
    drop(handle.clone());
    assert_eq!(handle.state(), FiberState::Active);
    drop(owner);
    handle.dispose_wait().await.unwrap();
    assert_eq!(handle.state(), FiberState::Disposed);
    assert_eq!(handle.definition().id(), definition.id());

    let source = runtime.root().effect().unwrap();
    source.provide(DATA, 1).unwrap();
    let parent = runtime.root().effect().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    drop(
        parent
            .inject([DATA.id()], move |_, _| {
                seen.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            })
            .unwrap(),
    );
    runtime.settle().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        runtime
            .diagnostics()
            .fibers
            .iter()
            .any(|fiber| fiber.name.is_none() && fiber.state == FiberState::Active)
    );
    parent.dispose_wait().await.unwrap();
    runtime.settle().await;
    assert!(runtime.diagnostics().fibers.is_empty());
    runtime.shutdown().await.unwrap();
}

struct Parent {
    child: Arc<Mutex<Option<FiberHandle>>>,
}
#[async_trait]
impl Plugin for Parent {
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let child = ctx.inject([DATA.id()], |_, effect| async move {
            effect.logger()?.info("anonymous-plugin");
            Ok(())
        })?;
        *self.child.lock().unwrap() = Some(child);
        Ok(())
    }
}

#[tokio::test]
async fn anonymous_plugins_share_state_events_parent_tree_and_log_identity() {
    let runtime = Runtime::new().unwrap();
    let root = runtime.root();
    let source = root.effect().unwrap();
    source.provide(DATA, 1).unwrap();
    let definition = PluginDefinition::named("named-parent");
    let child = Arc::new(Mutex::new(None));
    let mut events = runtime.subscribe_fiber_states();
    let mut parent = root
        .plugin(
            &definition,
            Arc::new(Parent {
                child: child.clone(),
            }),
        )
        .await
        .unwrap();
    runtime.settle().await;
    let child = child.lock().unwrap().clone().unwrap();
    let snapshot = child.snapshot();
    assert_eq!(snapshot.parent_fiber_id, Some(parent.id()));
    assert_eq!(snapshot.name, None);
    assert_eq!(snapshot.state, FiberState::Active);
    let mut states = Vec::new();
    while let Ok(event) = events.try_recv() {
        if event.fiber_id == child.id() {
            assert_eq!(event.definition_id, child.definition().id());
            assert_eq!(event.parent_fiber_id, Some(parent.id()));
            states.push(event.current);
        }
    }
    assert_eq!(
        states,
        [FiberState::Pending, FiberState::Loading, FiberState::Active]
    );
    let records = runtime.logger_service().buffer_snapshot();
    let log = records
        .iter()
        .find(|record| record.message == "anonymous-plugin")
        .unwrap();
    assert_eq!(log.fiber_id, Some(child.id()));
    assert_eq!(log.definition_id, Some(child.definition().id()));
    assert_eq!(log.plugin_name, None);
    assert_eq!(log.target, "named-parent");
    parent.dispose_wait().await.unwrap();
    child.dispose_wait().await.unwrap();
    assert_eq!(child.state(), FiberState::Disposed);
    runtime.shutdown().await.unwrap();
}

struct Failable {
    calls: Arc<AtomicUsize>,
    fail: Arc<AtomicBool>,
}
#[async_trait]
impl Plugin for Failable {
    fn inject(&self) -> Vec<ServiceId> {
        vec![DATA.id()]
    }
    async fn apply(&self, _: &Context) -> Result<(), CoreError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            Err(CoreError::PluginApply("intentional failure".into()))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn failed_named_plugin_requires_explicit_recovery_after_dependency_changes() {
    let runtime = Runtime::new().unwrap();
    let root = runtime.root();
    let old = root.effect().unwrap();
    old.provide(DATA, 1).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let fail = Arc::new(AtomicBool::new(true));
    let mut fiber = root
        .plugin(
            &PluginDefinition::named("failable"),
            Arc::new(Failable {
                calls: calls.clone(),
                fail: fail.clone(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(fiber.state(), FiberState::Failed);
    fail.store(false, Ordering::SeqCst);
    old.dispose_wait().await.unwrap();
    let replacement = root.effect().unwrap();
    replacement.provide(DATA, 2).unwrap();
    runtime.settle().await;
    assert_eq!(fiber.state(), FiberState::Failed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    fiber.restart().await.unwrap();
    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn anonymous_control_handles_recover_and_dispose_without_owning_lifecycle() {
    let runtime = Runtime::new().unwrap();
    let root = runtime.root();
    let old = root.effect().unwrap();
    old.provide(DATA, 1).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let fail = Arc::new(AtomicBool::new(true));
    let count = calls.clone();
    let failing = fail.clone();
    let handle = root
        .inject([DATA.id()], move |_, _| {
            count.fetch_add(1, Ordering::SeqCst);
            let failing = failing.clone();
            async move {
                if failing.load(Ordering::SeqCst) {
                    Err(CoreError::PluginApply("anonymous failure".into()))
                } else {
                    Ok(())
                }
            }
        })
        .unwrap();
    runtime.settle().await;
    assert_eq!(handle.state(), FiberState::Failed);
    assert!(handle.last_error().unwrap().contains("anonymous failure"));
    fail.store(false, Ordering::SeqCst);
    old.dispose_wait().await.unwrap();
    let replacement = root.effect().unwrap();
    replacement.provide(DATA, 2).unwrap();
    runtime.settle().await;
    assert_eq!(handle.state(), FiberState::Failed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    handle.clone().restart().await.unwrap();
    assert_eq!(handle.state(), FiberState::Active);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(runtime.unmount(handle.definition().id()).await.unwrap(), 1);
    assert_eq!(handle.state(), FiberState::Disposed);
    assert!(matches!(
        handle.restart().await,
        Err(CoreError::FiberDisposed)
    ));
    runtime.shutdown().await.unwrap();
}

struct CleanupGate {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    fail: bool,
}
#[async_trait]
impl Plugin for CleanupGate {
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let entered = self.entered.clone();
        let release = self.release.clone();
        ctx.effect()?.on_dispose_async(move || async move {
            entered.add_permits(1);
            release.acquire().await.unwrap().forget();
            Ok(())
        })?;
        if self.fail {
            Err(CoreError::PluginApply("startup failure".into()))
        } else {
            Ok(())
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_initial_mount_during_failed_cleanup_leaves_no_registry_orphan() {
    bounded(async {
        let runtime = Runtime::new().unwrap();
        let root = runtime.root();
        let definition = PluginDefinition::named("cancel-during-cleanup");
        let id = definition.id();
        let entered = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let plugin = Arc::new(CleanupGate {
            entered: entered.clone(),
            release: release.clone(),
            fail: true,
        });
        let mounting = tokio::spawn(async move { root.plugin(&definition, plugin).await });
        entered.acquire().await.unwrap().forget();
        mounting.abort();
        assert!(matches!(mounting.await, Err(error) if error.is_cancelled()));
        release.add_permits(1);
        while runtime
            .diagnostics()
            .fibers
            .iter()
            .any(|fiber| fiber.definition_id == id)
        {
            tokio::task::yield_now().await;
        }
        assert!(
            runtime
                .diagnostics()
                .definitions
                .iter()
                .all(|definition| definition.definition_id != id)
        );
        runtime.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn definition_unmount_rejects_new_mounts_until_all_instances_finish() {
    bounded(async {
        let runtime = Runtime::new().unwrap();
        let definition = PluginDefinition::named("unmounting");
        let entered = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let owner = runtime
            .root()
            .plugin(
                &definition,
                Arc::new(CleanupGate {
                    entered: entered.clone(),
                    release: release.clone(),
                    fail: false,
                }),
            )
            .await
            .unwrap();
        let other = runtime
            .root()
            .plugin(&definition, Arc::new(Noop))
            .await
            .unwrap();
        let id = definition.id();
        let stopping = tokio::spawn({
            let runtime = runtime.clone();
            async move { runtime.unmount(id).await }
        });
        entered.acquire().await.unwrap().forget();
        let rejected = runtime.root().plugin(&definition, Arc::new(Noop)).await;
        assert!(matches!(rejected, Err(CoreError::PluginUnmounting { plugin }) if plugin == id));
        release.add_permits(1);
        assert_eq!(stopping.await.unwrap().unwrap(), 2);
        assert_eq!(owner.state(), FiberState::Disposed);
        assert_eq!(other.state(), FiberState::Disposed);
        runtime.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registration_racing_parent_disposal_publishes_pending_before_terminal_state() {
    bounded(async {
        let runtime = Runtime::new().unwrap();
        let source = runtime.root().effect().unwrap();
        source.provide(DATA, 1).unwrap();
        let mut events = runtime.subscribe_fiber_states();
        for _ in 0..32 {
            let parent = runtime.root().effect().unwrap();
            let gate = Arc::new(tokio::sync::Barrier::new(2));
            let mounting = tokio::spawn({
                let gate = gate.clone();
                let parent = parent.clone();
                async move {
                    gate.wait().await;
                    parent.inject([DATA.id()], |_, _| async { Ok(()) })
                }
            });
            gate.wait().await;
            parent.dispose_wait().await.unwrap();
            if let Ok(handle) = mounting.await.unwrap() {
                handle.dispose_wait().await.unwrap();
            }
        }
        runtime.settle().await;
        let mut seen = std::collections::HashMap::new();
        while let Ok(event) = events.try_recv() {
            let previous = seen.insert(event.fiber_id, event.current);
            if previous.is_none() {
                assert_eq!(event.current, FiberState::Pending);
                assert_eq!(event.previous, None);
            } else {
                assert_ne!(previous, Some(FiberState::Disposed));
            }
        }
        assert!(runtime.diagnostics().fibers.is_empty());
        runtime.shutdown().await.unwrap();
    })
    .await;
}

struct BrokenStartup;
#[async_trait]
impl Plugin for BrokenStartup {
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        ctx.effect()?
            .on_dispose_async(|| async { Err(CoreError::PluginApply("cleanup-marker".into())) })?;
        Err(CoreError::PluginApply("startup-marker".into()))
    }
}

#[tokio::test]
async fn failed_initial_cleanup_delivers_a_recoverable_failed_instance() {
    let runtime = Runtime::new().unwrap();
    let definition = PluginDefinition::named("broken-startup");
    let mut fiber = bounded(runtime.root().plugin(&definition, Arc::new(BrokenStartup)))
        .await
        .unwrap();
    assert_eq!(fiber.state(), FiberState::Failed);
    let error = fiber.last_error().unwrap();
    assert!(error.contains("startup-marker"));
    assert!(error.contains("cleanup-marker"));
    fiber.replace(Arc::new(Noop)).await.unwrap();
    assert_eq!(fiber.definition().id(), definition.id());
    assert_eq!(fiber.state(), FiberState::Active);
    runtime.shutdown().await.unwrap();
}

struct PeerCleanup {
    peer: Arc<Semaphore>,
    waits: bool,
}
#[async_trait]
impl Plugin for PeerCleanup {
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let peer = self.peer.clone();
        let waits = self.waits;
        ctx.effect()?.on_dispose_async(move || async move {
            if waits {
                peer.acquire().await.unwrap().forget();
            } else {
                peer.add_permits(1);
            }
            Ok(())
        })?;
        Ok(())
    }
}

#[tokio::test]
async fn definition_unmount_starts_all_instances_before_waiting_for_cleanup() {
    bounded(async {
        let runtime = Runtime::new().unwrap();
        let definition = PluginDefinition::named("peer-cleanup");
        let peer = Arc::new(Semaphore::new(0));
        let first = runtime
            .root()
            .plugin(
                &definition,
                Arc::new(PeerCleanup {
                    peer: peer.clone(),
                    waits: true,
                }),
            )
            .await
            .unwrap();
        let second = runtime
            .root()
            .plugin(&definition, Arc::new(PeerCleanup { peer, waits: false }))
            .await
            .unwrap();
        assert_eq!(runtime.unmount(definition.id()).await.unwrap(), 2);
        assert_eq!(first.state(), FiberState::Disposed);
        assert_eq!(second.state(), FiberState::Disposed);
        runtime.shutdown().await.unwrap();
    })
    .await;
}
