//! 验证 Plugin 挂载合同：provide、dispose 清资源、同 Key 归组与统一卸载。
//!
//! 配置/Schema/发现/热更新属 Host，本文件不测；夹具见 `common/helpers`。

#[path = "common/helpers.rs"]
mod common;
use common::*;

#[tokio::test]
async fn plugin_mount_provides_and_dispose_clears_resources() {
    let runtime = runtime();
    let root = runtime.root();
    let plugin = TestPlugin::new()
        .provide_clone(NUMBER, Number(9))
        .setup(|ctx| {
            ctx.on(PING, |_| Ok(()))?;
            Ok(())
        });
    let dispose_count = plugin.on_dispose_counter();
    let mut handle = root.plugin(plugin.into_arc()).await.unwrap();
    assert_eq!(root.get(NUMBER).unwrap().0, 9);

    handle.dispose();
    handle.dispose();
    assert!(handle.is_disposed());
    assert_eq!(dispose_count.load(Ordering::SeqCst), 1);
    assert_service_unavailable(&root, NUMBER);
}

struct FailingPlugin;

#[async_trait]
impl Plugin for FailingPlugin {
    fn key(&self) -> cordis_core::PluginKey {
        cordis_core::PluginKey::new("test.failingplugin")
    }
    async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
        Err(CoreError::PluginApply("boom".into()))
    }
}

#[tokio::test]
async fn plugin_apply_error_surfaces() {
    let runtime = runtime();
    let fiber = runtime
        .root()
        .plugin(Arc::new(FailingPlugin))
        .await
        .unwrap();
    assert_eq!(fiber.state(), cordis_core::FiberState::Failed);
    assert!(fiber.last_error().is_some());
}

struct SettleInApplyPlugin {
    runtime: Runtime,
}

#[async_trait]
impl Plugin for SettleInApplyPlugin {
    fn key(&self) -> cordis_core::PluginKey {
        cordis_core::PluginKey::new("test.settle-in-apply")
    }
    fn inject(&self) -> Vec<cordis_core::ServiceId> {
        vec![NUMBER.id()]
    }
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        // 若调度器在 await apply 时仍持有 recompute_lock / 或 settle 等待本轮 recompute，会死锁。
        self.runtime.settle().await;
        assert_eq!(ctx.get(NUMBER)?.0, 1);
        ctx.provide(DERIVED, Number(2))?;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plugin_apply_calling_settle_does_not_deadlock() {
    let runtime = runtime();
    let root = runtime.root();
    let mut fiber = root
        .plugin(Arc::new(SettleInApplyPlugin {
            runtime: runtime.clone(),
        }))
        .await
        .unwrap();
    assert_eq!(fiber.state(), FiberState::Pending);

    // 必须通过“Pending → Provider 出现 → scheduler 自动 apply”的真实路径，
    // 而不是挂载调用者直接 try_activate() 的路径。
    let provider = root.effect().unwrap();
    provider.provide(NUMBER, Number(1)).unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        wait_until(|| fiber.state() == FiberState::Active),
    )
    .await
    .expect("scheduler apply timed out — likely settle deadlock");
    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(root.get(DERIVED).unwrap().0, 2);
    fiber.dispose_wait().await.expect("dispose_wait");
    runtime.shutdown().await.expect("shutdown");
}

struct PanicApplyPlugin;

#[async_trait]
impl Plugin for PanicApplyPlugin {
    fn key(&self) -> cordis_core::PluginKey {
        cordis_core::PluginKey::new("test.panic-apply")
    }

    fn inject(&self) -> Vec<cordis_core::ServiceId> {
        vec![NUMBER.id()]
    }

    async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
        panic!("plugin apply boom");
    }
}

struct HealthyAfterPanicPlugin {
    runs: Arc<AtomicUsize>,
}

#[async_trait]
impl Plugin for HealthyAfterPanicPlugin {
    fn key(&self) -> cordis_core::PluginKey {
        cordis_core::PluginKey::new("test.healthy-after-panic")
    }

    fn inject(&self) -> Vec<cordis_core::ServiceId> {
        vec![NUMBER.id()]
    }

    async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn plugin_apply_panic_fails_only_its_fiber_and_scheduler_continues() {
    let runtime = runtime();
    let root = runtime.root();
    let panic_fiber = root.plugin(Arc::new(PanicApplyPlugin)).await.unwrap();
    let healthy_runs = Arc::new(AtomicUsize::new(0));
    let healthy_fiber = root
        .plugin(Arc::new(HealthyAfterPanicPlugin {
            runs: healthy_runs.clone(),
        }))
        .await
        .unwrap();
    assert_eq!(panic_fiber.state(), FiberState::Pending);
    assert_eq!(healthy_fiber.state(), FiberState::Pending);

    root.effect().unwrap().provide(NUMBER, Number(1)).unwrap();
    wait_until(|| panic_fiber.state() == FiberState::Failed).await;
    wait_until(|| healthy_fiber.state() == FiberState::Active).await;
    assert_eq!(healthy_runs.load(Ordering::SeqCst), 1);
    assert!(
        panic_fiber
            .last_error()
            .is_some_and(|error| error.contains("plugin apply") && error.contains("boom"))
    );
    tokio::time::timeout(std::time::Duration::from_secs(2), runtime.settle())
        .await
        .expect("scheduler must remain available after plugin panic");
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn plugin_remount_does_not_accumulate_child_scopes() {
    let runtime = runtime();
    let root = runtime.root();
    let baseline = root.child_scope_count();
    let finished = Arc::new(AtomicUsize::new(0));

    for _ in 0..8 {
        let finished = finished.clone();
        struct TaskPlugin {
            finished: Arc<AtomicUsize>,
        }
        #[async_trait]
        impl Plugin for TaskPlugin {
            fn key(&self) -> cordis_core::PluginKey {
                cordis_core::PluginKey::new("test.taskplugin")
            }
            async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
                let effect = ctx.effect()?;
                let finished = self.finished.clone();
                effect.spawn(move |cancel| async move {
                    cancel.cancelled().await;
                    finished.fetch_add(1, Ordering::SeqCst);
                })?;
                Ok(())
            }
        }
        let mut handle = root
            .plugin(Arc::new(TaskPlugin {
                finished: finished.clone(),
            }))
            .await
            .unwrap();
        handle.dispose();
        assert!(
            root.child_scope_count() <= baseline + 1,
            "child scopes should not accumulate across remounts"
        );
    }
    assert_eq!(root.child_scope_count(), baseline);
    runtime.shutdown().await.expect("shutdown");
    assert_eq!(finished.load(Ordering::SeqCst), 8);
}

#[tokio::test]
async fn plugin_dispose_wait_awaits_owned_tasks() {
    let runtime = runtime();
    let root = runtime.root();
    let finished = Arc::new(AtomicBool::new(false));
    let observed = finished.clone();
    let (cancelled_tx, cancelled_rx) = oneshot::channel();

    struct WaitPlugin {
        finished: Arc<AtomicBool>,
        cancelled_tx: Mutex<Option<oneshot::Sender<()>>>,
    }

    #[async_trait]
    impl Plugin for WaitPlugin {
        fn key(&self) -> cordis_core::PluginKey {
            cordis_core::PluginKey::new("test.waitplugin")
        }
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            let effect = ctx.effect()?;
            let finished = self.finished.clone();
            let cancelled_tx = self
                .cancelled_tx
                .lock()
                .expect("tx")
                .take()
                .expect("tx once");
            effect.spawn(move |cancel| async move {
                cancel.cancelled().await;
                let _ = cancelled_tx.send(());
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                finished.store(true, Ordering::SeqCst);
            })?;
            Ok(())
        }
    }

    let mut handle = root
        .plugin(Arc::new(WaitPlugin {
            finished: observed,
            cancelled_tx: Mutex::new(Some(cancelled_tx)),
        }))
        .await
        .unwrap();
    handle.dispose_wait().await.expect("dispose_wait");
    cancelled_rx.await.expect("task observed cancel");
    assert!(
        finished.load(Ordering::SeqCst),
        "dispose_wait must return only after plugin tasks finish"
    );
    handle.dispose_wait().await.expect("dispose_wait");
    assert!(handle.is_disposed());
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn plugin_hot_replace_after_dispose_wait() {
    let runtime = runtime();
    let root = runtime.root();
    let a_finished = Arc::new(AtomicBool::new(false));
    let a_done = a_finished.clone();

    struct PluginA {
        finished: Arc<AtomicBool>,
    }

    #[async_trait]
    impl Plugin for PluginA {
        fn key(&self) -> cordis_core::PluginKey {
            cordis_core::PluginKey::new("test.plugina")
        }
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            ctx.provide(NUMBER, Number(1))?;
            let effect = ctx.effect()?;
            let finished = self.finished.clone();
            effect.spawn(move |cancel| async move {
                cancel.cancelled().await;
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                finished.store(true, Ordering::SeqCst);
            })?;
            Ok(())
        }
    }

    struct PluginB;

    #[async_trait]
    impl Plugin for PluginB {
        fn key(&self) -> cordis_core::PluginKey {
            cordis_core::PluginKey::new("test.pluginb")
        }
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            ctx.provide(NUMBER, Number(2))?;
            Ok(())
        }
    }

    let mut old = root
        .plugin(Arc::new(PluginA { finished: a_done }))
        .await
        .unwrap();
    assert_eq!(root.get(NUMBER).unwrap().0, 1);

    old.dispose_wait().await.expect("dispose_wait");
    assert!(
        a_finished.load(Ordering::SeqCst),
        "old plugin task must finish before remount"
    );

    let mut neu = root.plugin(Arc::new(PluginB)).await.unwrap();
    assert_eq!(root.get(NUMBER).unwrap().0, 2);
    neu.dispose_wait().await.expect("dispose_wait");
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn plugin_apply_races_parent_dispose_returns_no_handle() {
    let runtime = runtime();
    let root = runtime.root();
    let (entered_tx, entered_rx) = oneshot::channel::<()>();
    let (gate_tx, gate_rx) = oneshot::channel::<()>();
    let gate_rx = Arc::new(Mutex::new(Some(gate_rx)));
    let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));

    struct SlowPlugin {
        entered: Arc<Mutex<Option<oneshot::Sender<()>>>>,
        gate: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    }

    #[async_trait]
    impl Plugin for SlowPlugin {
        fn key(&self) -> cordis_core::PluginKey {
            cordis_core::PluginKey::new("test.slowplugin")
        }
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            ctx.provide(NUMBER, Number(1))?;
            ctx.on(PING, |_| Ok(()))?;
            if let Some(tx) = self.entered.lock().expect("entered").take() {
                let _ = tx.send(());
            }
            let rx = self.gate.lock().expect("gate").take().expect("gate once");
            let _ = rx.await;
            Ok(())
        }
    }

    let parent_owner = root.effect().unwrap();
    let parent = parent_owner.extend().unwrap();
    let parent_for_mount = parent.clone();
    let mount = tokio::spawn(async move {
        parent_for_mount
            .plugin(Arc::new(SlowPlugin {
                entered: entered_tx,
                gate: gate_rx,
            }))
            .await
    });
    entered_rx.await.expect("plugin entered apply");
    parent_owner.dispose();
    let _ = gate_tx.send(());
    let result = mount.await.expect("join").expect("fiber handle");
    assert_eq!(result.state(), cordis_core::FiberState::Disposed);
    assert_service_unavailable(&root, NUMBER);
}

#[tokio::test]
async fn isolation_provider_change_reactivates_plugin_fiber() {
    let runtime = runtime();
    let root = runtime.root();
    let (isolated, _label) = root.isolate(NUMBER).unwrap();
    let provider = isolated.effect_named("provider").unwrap();
    provider.provide(NUMBER, Number(1)).unwrap();

    struct DepPlugin;
    #[async_trait]
    impl Plugin for DepPlugin {
        fn key(&self) -> cordis_core::PluginKey {
            cordis_core::PluginKey::new("test.depplugin")
        }
        fn inject(&self) -> Vec<cordis_core::ServiceId> {
            vec![NUMBER.id()]
        }
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            let _ = ctx.get(NUMBER)?;
            Ok(())
        }
    }

    let mut fiber = isolated.plugin(Arc::new(DepPlugin)).await.unwrap();
    assert_eq!(fiber.state(), FiberState::Active);

    provider.dispose();
    runtime.settle().await;
    wait_until(|| fiber.state() == FiberState::Pending).await;
    assert_ne!(fiber.state(), FiberState::Loading);

    let again = isolated.effect_named("provider2").unwrap();
    again.provide(NUMBER, Number(2)).unwrap();
    runtime.settle().await;
    wait_until(|| fiber.state() == FiberState::Active).await;
    assert_ne!(fiber.state(), FiberState::Loading);
    fiber.dispose();
}

#[tokio::test]
async fn plugin_registry_groups_and_unmount_waits() {
    use cordis_core::PluginKey;

    static KEY: PluginKey = PluginKey::new("test.registry.group");
    let runtime = runtime();
    let root = runtime.root();
    let finished = Arc::new(AtomicUsize::new(0));

    struct SlowPlugin {
        finished: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl Plugin for SlowPlugin {
        fn key(&self) -> PluginKey {
            KEY
        }
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            let finished = self.finished.clone();
            let effect = ctx.effect()?;
            effect.spawn(move |cancel| async move {
                cancel.cancelled().await;
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                finished.fetch_add(1, Ordering::SeqCst);
            })?;
            Ok(())
        }
    }

    let _a = root
        .plugin(Arc::new(SlowPlugin {
            finished: finished.clone(),
        }))
        .await
        .unwrap();
    let _b = root
        .plugin(Arc::new(SlowPlugin {
            finished: finished.clone(),
        }))
        .await
        .unwrap();
    let snap = runtime.diagnostics();
    let group = snap
        .plugin_registry
        .iter()
        .find(|item| item.plugin_key == KEY.as_str())
        .expect("group");
    assert_eq!(group.fibers.len(), 2);

    let count = runtime.unmount(KEY).await.unwrap();
    assert_eq!(count, 2);
    assert_eq!(finished.load(Ordering::SeqCst), 2);
    assert!(
        runtime
            .diagnostics()
            .plugin_registry
            .iter()
            .all(|item| item.plugin_key != KEY.as_str())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unmount_rejects_concurrent_mount_and_replace_requires_same_key() {
    use cordis_core::PluginKey;

    static KEY_A: PluginKey = PluginKey::new("test.registry.a");
    static KEY_B: PluginKey = PluginKey::new("test.registry.b");

    let (gate_tx, gate_rx) = oneshot::channel::<()>();
    let gate_rx = Arc::new(Mutex::new(Some(gate_rx)));

    struct PluginA {
        gate: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    }
    struct PluginB;
    #[async_trait]
    impl Plugin for PluginA {
        fn key(&self) -> PluginKey {
            KEY_A
        }
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            let gate = self.gate.clone();
            let effect = ctx.effect()?;
            effect.spawn(move |cancel| async move {
                cancel.cancelled().await;
                let rx = {
                    let mut guard = gate.lock().expect("gate");
                    guard.take()
                };
                if let Some(rx) = rx {
                    let _ = rx.await;
                }
            })?;
            Ok(())
        }
    }
    #[async_trait]
    impl Plugin for PluginB {
        fn key(&self) -> PluginKey {
            KEY_B
        }
        async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
            Ok(())
        }
    }

    let runtime = runtime();
    let root = runtime.root();
    let mut fiber = root
        .plugin(Arc::new(PluginA { gate: gate_rx }))
        .await
        .unwrap();
    assert!(matches!(
        fiber.replace(Arc::new(PluginB)).await,
        Err(CoreError::PluginKeyMismatch { .. })
    ));

    let unmount = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.unmount(KEY_A).await })
    };
    wait_until(|| {
        runtime
            .diagnostics()
            .plugin_registry
            .iter()
            .any(|item| item.plugin_key == KEY_A.as_str() && item.unmounting)
    })
    .await;
    let rejected = root
        .plugin(Arc::new(PluginA {
            gate: Arc::new(Mutex::new(None)),
        }))
        .await;
    assert!(matches!(rejected, Err(CoreError::PluginUnmounting { .. })));
    let _ = gate_tx.send(());
    assert_eq!(unmount.await.unwrap().unwrap(), 1);
    assert!(fiber.is_disposed());
}

#[tokio::test]
async fn plugin_dispose_clears_registry_index() {
    use cordis_core::PluginKey;

    static KEY: PluginKey = PluginKey::new("test.registry.solo");
    struct Solo;
    #[async_trait]
    impl Plugin for Solo {
        fn key(&self) -> PluginKey {
            KEY
        }
        async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
            Ok(())
        }
    }

    let runtime = runtime();
    let root = runtime.root();
    let mut fiber = root.plugin(Arc::new(Solo)).await.unwrap();
    assert!(
        runtime
            .diagnostics()
            .plugin_registry
            .iter()
            .any(|item| item.plugin_key == KEY.as_str() && item.fibers.len() == 1)
    );
    fiber.dispose();
    assert!(
        runtime
            .diagnostics()
            .plugin_registry
            .iter()
            .all(|item| item.plugin_key != KEY.as_str())
    );
}

#[tokio::test]
async fn unmount_and_shutdown_return_dispose_errors_after_cleanup() {
    use cordis_core::PluginKey;

    static KEY: PluginKey = PluginKey::new("test.async.dispose.unmount");
    let runtime = runtime();
    let root = runtime.root();

    struct BoomPlugin;
    #[async_trait]
    impl Plugin for BoomPlugin {
        fn key(&self) -> PluginKey {
            KEY
        }
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            let effect = ctx.effect()?;
            effect.on_dispose_async(|| async move {
                Err(CoreError::EventListener("unmount boom".into()))
            })?;
            Ok(())
        }
    }

    let _fiber = root.plugin(Arc::new(BoomPlugin)).await.unwrap();
    let error = runtime.unmount(KEY).await.expect_err("unmount");
    assert!(matches!(error, CoreError::DisposeFailed { .. }));
    assert!(
        runtime
            .diagnostics()
            .plugin_registry
            .iter()
            .all(|group| group.plugin_key != KEY.as_str())
    );

    let effect = runtime.root().effect().unwrap();
    effect
        .on_dispose_async(|| async move { Err(CoreError::EventListener("shutdown boom".into())) })
        .unwrap();
    let shutdown_error = runtime.shutdown().await.expect_err("shutdown");
    assert!(matches!(shutdown_error, CoreError::DisposeFailed { .. }));
    assert!(runtime.scheduler_stopped());
}
