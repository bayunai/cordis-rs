//! 验证 Fiber 生命周期公开合同：状态转换、settle、失败/卸载与并发唤醒。
//!
//! 覆盖 Pending/Loading/Active/Failed/Unloading/Disposed 边界；夹具见 `common/helpers`。

#[path = "common/helpers.rs"]
mod common;
use common::*;

#[tokio::test]
async fn settle_does_not_hang_under_concurrent_wake() {
    let runtime = runtime();
    let root = runtime.root();
    let handle = root
        .inject([NUMBER.id()], |_services, _effect| async { Ok(()) })
        .unwrap();

    let mut jobs = Vec::new();
    for i in 0..32 {
        let runtime = runtime.clone();
        let root = root.clone();
        jobs.push(tokio::spawn(async move {
            let effect = root.effect().unwrap();
            let _ = effect.provide(NUMBER, Number(i));
            runtime.settle().await;
            effect.dispose();
            runtime.settle().await;
        }));
    }
    for job in jobs {
        tokio::time::timeout(std::time::Duration::from_secs(2), job)
            .await
            .expect("settle/job timed out")
            .expect("job join");
    }
    wait_injection(&handle, InjectionState::Pending).await;
}

#[tokio::test]
async fn fiber_restart_and_replace_wait_for_tasks() {
    let runtime = runtime();
    let root = runtime.root();
    let finished = Arc::new(AtomicBool::new(false));
    let flag = finished.clone();

    struct Taskful {
        finished: Arc<AtomicBool>,
        value: usize,
    }
    #[async_trait]
    impl Plugin for Taskful {
        fn key(&self) -> cordis_core::PluginKey {
            cordis_core::PluginKey::new("test.taskful")
        }
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            ctx.provide(NUMBER, Number(self.value))?;
            let finished = self.finished.clone();
            let effect = ctx.effect()?;
            effect.spawn(move |cancel| async move {
                cancel.cancelled().await;
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                finished.store(true, Ordering::SeqCst);
            })?;
            Ok(())
        }
    }

    let mut fiber = root
        .plugin(Arc::new(Taskful {
            finished: flag,
            value: 1,
        }))
        .await
        .unwrap();
    assert_eq!(root.get(NUMBER).unwrap().0, 1);
    wait_until(|| fiber.state() == FiberState::Active).await;

    fiber.restart().await.unwrap();
    assert!(finished.load(Ordering::SeqCst));
    assert_eq!(fiber.state(), FiberState::Active);
    assert_ne!(fiber.state(), FiberState::Loading);

    let finished2 = Arc::new(AtomicBool::new(false));
    fiber
        .replace(Arc::new(Taskful {
            finished: finished2.clone(),
            value: 2,
        }))
        .await
        .unwrap();
    assert_eq!(root.get(NUMBER).unwrap().0, 2);
    assert_eq!(fiber.state(), FiberState::Active);
    assert_ne!(fiber.state(), FiberState::Loading);
    fiber.dispose_wait().await.expect("dispose_wait");
    assert!(finished2.load(Ordering::SeqCst));
    assert!(fiber.is_disposed());
}

#[tokio::test]
async fn fiber_state_subscription_reports_ordered_lifecycle_transitions() {
    struct NoopPlugin;

    #[async_trait]
    impl Plugin for NoopPlugin {
        fn key(&self) -> cordis_core::PluginKey {
            cordis_core::PluginKey::new("test.lifecycle-events")
        }

        async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
            Ok(())
        }
    }

    let runtime = runtime();
    let root = runtime.root();
    let mut states = runtime.subscribe_fiber_states();
    let mut fiber = root.plugin(Arc::new(NoopPlugin)).await.unwrap();

    let expected = [
        (None, FiberState::Pending),
        (Some(FiberState::Pending), FiberState::Loading),
        (Some(FiberState::Loading), FiberState::Active),
    ];
    for (previous, current) in expected {
        let change = states.recv().await.unwrap();
        assert_eq!(change.fiber_id, fiber.id());
        assert_eq!(change.plugin_key.as_str(), "test.lifecycle-events");
        assert_eq!(change.previous, previous);
        assert_eq!(change.current, current);
    }

    fiber.restart().await.unwrap();
    let expected = [
        (Some(FiberState::Active), FiberState::Unloading),
        (Some(FiberState::Unloading), FiberState::Pending),
        (Some(FiberState::Pending), FiberState::Loading),
        (Some(FiberState::Loading), FiberState::Active),
    ];
    for (previous, current) in expected {
        let change = states.recv().await.unwrap();
        assert_eq!(change.fiber_id, fiber.id());
        assert_eq!(change.previous, previous);
        assert_eq!(change.current, current);
    }

    fiber.dispose();
    assert_eq!(states.recv().await.unwrap().current, FiberState::Unloading);
    assert_eq!(states.recv().await.unwrap().current, FiberState::Disposed);
}

#[tokio::test]
async fn state_subscriber_reports_lag_and_diagnostics_remain_available() {
    struct NoopPlugin;

    #[async_trait]
    impl Plugin for NoopPlugin {
        fn key(&self) -> cordis_core::PluginKey {
            cordis_core::PluginKey::new("test.lifecycle-lag")
        }

        async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
            Ok(())
        }
    }

    let runtime = runtime();
    let root = runtime.root();
    let mut states = runtime.subscribe_fiber_states();
    let mut fiber = root.plugin(Arc::new(NoopPlugin)).await.unwrap();
    for _ in 0..300 {
        fiber.restart().await.unwrap();
    }

    assert!(matches!(states.try_recv(), Err(TryRecvError::Lagged(_))));
    assert!(
        runtime
            .diagnostics()
            .plugin_fibers
            .iter()
            .any(|item| item.id == fiber.id()
                && item.state == cordis_core::FiberStateSnapshot::Active)
    );
    fiber.dispose();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provider_rebind_waits_for_old_plugin_effect_before_reactivation() {
    struct GatedDependentPlugin {
        apply_count: Arc<AtomicUsize>,
        cancelled: Arc<Mutex<Option<oneshot::Sender<()>>>>,
        release: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    }

    #[async_trait]
    impl Plugin for GatedDependentPlugin {
        fn key(&self) -> cordis_core::PluginKey {
            cordis_core::PluginKey::new("test.rebind-waits-for-unload")
        }

        fn inject(&self) -> Vec<cordis_core::ServiceId> {
            vec![NUMBER.id()]
        }

        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            self.apply_count.fetch_add(1, Ordering::SeqCst);
            let cancelled = self.cancelled.clone();
            let release = self.release.clone();
            let effect = ctx.effect()?;
            effect.spawn(move |cancel| async move {
                cancel.cancelled().await;
                if let Some(tx) = cancelled.lock().expect("cancelled").take() {
                    let _ = tx.send(());
                }
                let receiver = release.lock().expect("release").take();
                if let Some(rx) = receiver {
                    let _ = rx.await;
                }
            })?;
            Ok(())
        }
    }

    let runtime = runtime();
    let root = runtime.root();
    let provider = root.effect_named("provider").unwrap();
    provider.provide(NUMBER, Number(1)).unwrap();
    let (cancelled_tx, cancelled_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let apply_count = Arc::new(AtomicUsize::new(0));
    let mut fiber = root
        .plugin(Arc::new(GatedDependentPlugin {
            apply_count: apply_count.clone(),
            cancelled: Arc::new(Mutex::new(Some(cancelled_tx))),
            release: Arc::new(Mutex::new(Some(release_rx))),
        }))
        .await
        .unwrap();
    assert_eq!(apply_count.load(Ordering::SeqCst), 1);

    provider.dispose();
    cancelled_rx.await.expect("old effect cancellation");
    wait_until(|| fiber.state() == FiberState::Unloading).await;

    let replacement = root.effect_named("replacement").unwrap();
    replacement.provide(NUMBER, Number(2)).unwrap();
    assert_eq!(apply_count.load(Ordering::SeqCst), 1);

    let _ = release_tx.send(());
    runtime.settle().await;
    wait_until(|| fiber.state() == FiberState::Active).await;
    assert_eq!(apply_count.load(Ordering::SeqCst), 2);
    fiber.dispose_wait().await.expect("dispose_wait");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_mount_and_scheduler_apply_once() {
    let runtime = runtime();
    let root = runtime.root();
    let apply_count = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = oneshot::channel::<()>();
    let (gate_tx, gate_rx) = oneshot::channel::<()>();
    let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
    let gate_rx = Arc::new(Mutex::new(Some(gate_rx)));
    let count = apply_count.clone();

    struct GatedPlugin {
        apply_count: Arc<AtomicUsize>,
        entered: Arc<Mutex<Option<oneshot::Sender<()>>>>,
        gate: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    }

    #[async_trait]
    impl Plugin for GatedPlugin {
        fn key(&self) -> cordis_core::PluginKey {
            cordis_core::PluginKey::new("test.gatedplugin")
        }
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            self.apply_count.fetch_add(1, Ordering::SeqCst);
            if let Some(tx) = self.entered.lock().expect("entered").take() {
                let _ = tx.send(());
            }
            let rx = self.gate.lock().expect("gate").take().expect("gate once");
            let _ = rx.await;
            ctx.provide(NUMBER, Number(7))?;
            let effect = ctx.effect()?;
            effect.spawn(|_| async {})?;
            Ok(())
        }
    }

    let mount_root = root.clone();
    let mount = tokio::spawn(async move {
        mount_root
            .plugin(Arc::new(GatedPlugin {
                apply_count: count,
                entered: entered_tx,
                gate: gate_rx,
            }))
            .await
    });
    entered_rx.await.expect("entered apply");
    // 门控期间勿 settle（recompute 持锁等 apply 会死锁）；yield 让调度器有机会再试 try_activate。
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        apply_count.load(Ordering::SeqCst),
        1,
        "scheduler must not re-enter apply while Loading"
    );
    let _ = gate_tx.send(());
    let mut fiber = tokio::time::timeout(std::time::Duration::from_secs(2), mount)
        .await
        .expect("mount timed out")
        .expect("join")
        .expect("fiber");
    wait_until(|| fiber.state() == FiberState::Active).await;
    runtime.settle().await;
    assert_eq!(apply_count.load(Ordering::SeqCst), 1);
    assert_eq!(root.get(NUMBER).unwrap().0, 7);
    let snap = runtime.diagnostics();
    let plugin = snap
        .plugin_fibers
        .iter()
        .find(|item| item.id == fiber.id())
        .expect("plugin fiber");
    assert_eq!(plugin.state, cordis_core::FiberStateSnapshot::Active);
    assert_eq!(
        snap.effects
            .iter()
            .filter(|effect| effect.fiber_id == Some(fiber.id()))
            .count(),
        1
    );
    assert_eq!(
        snap.providers
            .iter()
            .filter(|provider| provider.service == NUMBER.id().as_str())
            .count(),
        1
    );
    fiber.dispose_wait().await.expect("dispose_wait");
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provider_revoked_during_loading_does_not_stick_active() {
    let runtime = runtime();
    let root = runtime.root();
    let provider = root.effect_named("provider").unwrap();
    provider.provide(NUMBER, Number(1)).unwrap();

    let (entered_tx, entered_rx) = oneshot::channel::<()>();
    let (gate_tx, gate_rx) = oneshot::channel::<()>();
    let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
    let gate_rx = Arc::new(Mutex::new(Some(gate_rx)));

    struct GatedDepPlugin {
        entered: Arc<Mutex<Option<oneshot::Sender<()>>>>,
        gate: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    }

    #[async_trait]
    impl Plugin for GatedDepPlugin {
        fn key(&self) -> cordis_core::PluginKey {
            cordis_core::PluginKey::new("test.gateddepplugin")
        }
        fn inject(&self) -> Vec<cordis_core::ServiceId> {
            vec![NUMBER.id()]
        }
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            let _ = ctx.get(NUMBER)?;
            if let Some(tx) = self.entered.lock().expect("entered").take() {
                let _ = tx.send(());
            }
            let rx = self.gate.lock().expect("gate").take().expect("gate once");
            let _ = rx.await;
            Ok(())
        }
    }

    let mount_root = root.clone();
    let mount = tokio::spawn(async move {
        mount_root
            .plugin(Arc::new(GatedDepPlugin {
                entered: entered_tx,
                gate: gate_rx,
            }))
            .await
    });
    entered_rx.await.expect("entered apply");
    provider.dispose();
    let _ = gate_tx.send(());
    let mut fiber = tokio::time::timeout(std::time::Duration::from_secs(2), mount)
        .await
        .expect("mount timed out")
        .expect("join")
        .expect("fiber");
    // 不得带着旧 Provider ID 长期 Active；依赖已撤应回到 Pending。
    wait_until(|| fiber.state() == FiberState::Pending).await;
    assert!(fiber.missing_dependencies().contains(&NUMBER.id()));
    fiber.dispose();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provider_replaced_during_loading_reactivates_with_fresh() {
    let runtime = runtime();
    let root = runtime.root();
    let provider = root.effect_named("provider").unwrap();
    provider.provide(NUMBER, Number(1)).unwrap();

    let (entered_tx, entered_rx) = oneshot::channel::<()>();
    let (gate_tx, gate_rx) = oneshot::channel::<()>();
    let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
    let gate_rx = Arc::new(Mutex::new(Some(gate_rx)));
    let seen = Arc::new(AtomicUsize::new(0));
    let seen_apply = seen.clone();

    struct GatedDepPlugin {
        entered: Arc<Mutex<Option<oneshot::Sender<()>>>>,
        gate: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
        seen: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Plugin for GatedDepPlugin {
        fn key(&self) -> cordis_core::PluginKey {
            cordis_core::PluginKey::new("test.gateddepplugin")
        }
        fn inject(&self) -> Vec<cordis_core::ServiceId> {
            vec![NUMBER.id()]
        }
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            let value = ctx.get(NUMBER)?.0;
            self.seen.store(value, Ordering::SeqCst);
            if let Some(tx) = self.entered.lock().expect("entered").take() {
                let _ = tx.send(());
            }
            let gate = self.gate.lock().expect("gate").take();
            if let Some(rx) = gate {
                let _ = rx.await;
            }
            Ok(())
        }
    }

    let mount_root = root.clone();
    let mount = tokio::spawn(async move {
        mount_root
            .plugin(Arc::new(GatedDepPlugin {
                entered: entered_tx,
                gate: gate_rx,
                seen: seen_apply,
            }))
            .await
    });
    entered_rx.await.expect("entered apply");
    provider.dispose();
    let neu = root.effect_named("provider2").unwrap();
    neu.provide(NUMBER, Number(99)).unwrap();
    let _ = gate_tx.send(());
    let mut fiber = tokio::time::timeout(std::time::Duration::from_secs(2), mount)
        .await
        .expect("mount timed out")
        .expect("join")
        .expect("fiber");
    // 首次 apply 发现漂移回 Pending 后，调度器应再激活到新 Provider。
    runtime.settle().await;
    wait_until(|| fiber.state() == FiberState::Active).await;
    assert_eq!(root.get(NUMBER).unwrap().0, 99);
    assert_eq!(seen.load(Ordering::SeqCst), 99);
    fiber.dispose();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_during_parent_dispose_keeps_disposed() {
    let runtime = runtime();
    let root = runtime.root();
    let parent_owner = root.effect().unwrap();
    let parent = parent_owner.extend().unwrap();
    let finished = Arc::new(AtomicBool::new(false));
    let flag = finished.clone();

    struct SlowDisposePlugin {
        finished: Arc<AtomicBool>,
    }

    #[async_trait]
    impl Plugin for SlowDisposePlugin {
        fn key(&self) -> cordis_core::PluginKey {
            cordis_core::PluginKey::new("test.slowdisposeplugin")
        }
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            let finished = self.finished.clone();
            let effect = ctx.effect()?;
            effect.spawn(move |cancel| async move {
                cancel.cancelled().await;
                tokio::time::sleep(std::time::Duration::from_millis(40)).await;
                finished.store(true, Ordering::SeqCst);
            })?;
            Ok(())
        }
    }

    let fiber = parent
        .plugin(Arc::new(SlowDisposePlugin { finished: flag }))
        .await
        .unwrap();
    assert_eq!(fiber.state(), FiberState::Active);

    let fiber_for_restart = fiber;
    let restart = tokio::spawn(async move {
        let mut fiber = fiber_for_restart;
        let result = fiber.restart().await;
        (fiber, result)
    });
    // 给 restart 进入 dispose_wait 的窗口。
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    parent_owner.dispose();
    let (fiber, result) = restart.await.expect("join");
    assert!(matches!(result, Err(CoreError::FiberDisposed)));
    assert_eq!(fiber.state(), FiberState::Disposed);
    assert!(fiber.is_disposed());
    let snap = runtime.diagnostics();
    assert!(
        snap.plugin_fibers
            .iter()
            .all(|item| item.id != fiber.id()
                || item.state == cordis_core::FiberStateSnapshot::Disposed)
            || snap.plugin_fibers.iter().all(|item| item.id != fiber.id())
    );
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replace_during_parent_dispose_keeps_disposed_and_skips_new_plugin() {
    let runtime = runtime();
    let root = runtime.root();
    let parent_owner = root.effect().unwrap();
    let parent = parent_owner.extend().unwrap();
    let (cancelled_tx, cancelled_rx) = oneshot::channel::<()>();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let cancelled_tx = Arc::new(Mutex::new(Some(cancelled_tx)));
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    let replacement_applied = Arc::new(AtomicUsize::new(0));

    struct SlowDisposePlugin {
        cancelled: Arc<Mutex<Option<oneshot::Sender<()>>>>,
        release: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    }

    #[async_trait]
    impl Plugin for SlowDisposePlugin {
        fn key(&self) -> cordis_core::PluginKey {
            cordis_core::PluginKey::new("test.slowdisposeplugin")
        }
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            let cancelled = self.cancelled.clone();
            let release = self.release.clone();
            let effect = ctx.effect()?;
            effect.spawn(move |cancel| async move {
                cancel.cancelled().await;
                if let Some(tx) = cancelled.lock().expect("cancelled").take() {
                    let _ = tx.send(());
                }
                let receiver = release.lock().expect("release").take();
                if let Some(rx) = receiver {
                    let _ = rx.await;
                }
            })?;
            Ok(())
        }
    }

    struct ReplacementPlugin {
        applied: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Plugin for ReplacementPlugin {
        fn key(&self) -> cordis_core::PluginKey {
            // 同 Key 才能进入 dispose_wait；跨 Key 由专门用例覆盖。
            cordis_core::PluginKey::new("test.slowdisposeplugin")
        }
        async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
            self.applied.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let fiber = parent
        .plugin(Arc::new(SlowDisposePlugin {
            cancelled: cancelled_tx,
            release: release_rx,
        }))
        .await
        .unwrap();
    wait_until(|| fiber.state() == FiberState::Active).await;

    let applied = replacement_applied.clone();
    let replace = tokio::spawn(async move {
        let mut fiber = fiber;
        let result = fiber.replace(Arc::new(ReplacementPlugin { applied })).await;
        (fiber, result)
    });
    cancelled_rx.await.expect("old effect was cancelled");
    parent_owner.dispose();
    let _ = release_tx.send(());

    let (fiber, result) = replace.await.expect("join");
    assert!(matches!(result, Err(CoreError::FiberDisposed)));
    assert_eq!(fiber.state(), FiberState::Disposed);
    assert!(fiber.is_disposed());
    assert_eq!(replacement_applied.load(Ordering::SeqCst), 0);
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn fiber_restart_stops_on_async_dispose_failure() {
    use cordis_core::PluginKey;

    static KEY: PluginKey = PluginKey::new("test.async.dispose.restart");
    let runtime = runtime();
    let root = runtime.root();
    let apply_count = Arc::new(AtomicUsize::new(0));

    struct FlakyDisposePlugin {
        apply_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Plugin for FlakyDisposePlugin {
        fn key(&self) -> PluginKey {
            KEY
        }
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            self.apply_count.fetch_add(1, Ordering::SeqCst);
            let effect = ctx.effect()?;
            effect.on_dispose_async(|| async move {
                Err(CoreError::EventListener("dispose boom".into()))
            })?;
            Ok(())
        }
    }

    let mut fiber = root
        .plugin(Arc::new(FlakyDisposePlugin {
            apply_count: apply_count.clone(),
        }))
        .await
        .unwrap();
    assert_eq!(apply_count.load(Ordering::SeqCst), 1);
    let error = fiber.restart().await.expect_err("restart failed");
    assert!(matches!(error, CoreError::DisposeFailed { .. }));
    assert_eq!(fiber.state(), FiberState::Failed);
    assert_eq!(apply_count.load(Ordering::SeqCst), 1);
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn fiber_replace_stops_on_async_dispose_failure() {
    use cordis_core::PluginKey;

    static KEY: PluginKey = PluginKey::new("test.async.dispose.replace");
    let runtime = runtime();
    let root = runtime.root();
    let apply_count = Arc::new(AtomicUsize::new(0));

    struct FlakyDisposePlugin {
        apply_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Plugin for FlakyDisposePlugin {
        fn key(&self) -> PluginKey {
            KEY
        }
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            self.apply_count.fetch_add(1, Ordering::SeqCst);
            let effect = ctx.effect()?;
            effect.on_dispose_async(|| async move {
                Err(CoreError::EventListener("replace dispose boom".into()))
            })?;
            Ok(())
        }
    }

    struct ReplacementPlugin {
        apply_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Plugin for ReplacementPlugin {
        fn key(&self) -> PluginKey {
            KEY
        }
        async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
            self.apply_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let mut fiber = root
        .plugin(Arc::new(FlakyDisposePlugin {
            apply_count: apply_count.clone(),
        }))
        .await
        .unwrap();
    let error = fiber
        .replace(Arc::new(ReplacementPlugin {
            apply_count: apply_count.clone(),
        }))
        .await
        .expect_err("replace failed");
    assert!(matches!(error, CoreError::DisposeFailed { .. }));
    assert_eq!(fiber.state(), FiberState::Failed);
    assert_eq!(apply_count.load(Ordering::SeqCst), 1);
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn provider_recompute_marks_failed_when_async_dispose_errors() {
    use cordis_core::PluginKey;

    static KEY: PluginKey = PluginKey::new("test.async.dispose.provider");
    let runtime = runtime();
    let root = runtime.root();
    let apply_count = Arc::new(AtomicUsize::new(0));
    let provider = root.effect_named("provider").unwrap();
    provider.provide(NUMBER, Number(1)).unwrap();

    struct DependentPlugin {
        apply_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Plugin for DependentPlugin {
        fn key(&self) -> PluginKey {
            KEY
        }
        fn inject(&self) -> Vec<cordis_core::ServiceId> {
            vec![NUMBER.id()]
        }
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            self.apply_count.fetch_add(1, Ordering::SeqCst);
            let effect = ctx.effect()?;
            effect.on_dispose_async(|| async move {
                Err(CoreError::EventListener("provider dispose boom".into()))
            })?;
            Ok(())
        }
    }

    let fiber = root
        .plugin(Arc::new(DependentPlugin {
            apply_count: apply_count.clone(),
        }))
        .await
        .unwrap();
    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(apply_count.load(Ordering::SeqCst), 1);

    provider.dispose();
    let replacement = root.effect_named("replacement").unwrap();
    replacement.provide(NUMBER, Number(2)).unwrap();
    wait_until(|| fiber.state() == FiberState::Failed).await;
    assert_eq!(apply_count.load(Ordering::SeqCst), 1);
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn cancelled_restart_releases_busy_and_allows_later_lifecycle_work() {
    use cordis_core::PluginKey;

    static KEY: PluginKey = PluginKey::new("test.cancelled-restart");
    let runtime = runtime();
    let root = runtime.root();
    let (cancelled_tx, mut cancelled_rx) = oneshot::channel::<()>();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let cancelled_tx = Arc::new(Mutex::new(Some(cancelled_tx)));
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));

    struct GatedDisposePlugin {
        cancelled: Arc<Mutex<Option<oneshot::Sender<()>>>>,
        release: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    }

    #[async_trait]
    impl Plugin for GatedDisposePlugin {
        fn key(&self) -> PluginKey {
            KEY
        }

        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            let cancelled = self.cancelled.clone();
            let release = self.release.clone();
            let effect = ctx.effect()?;
            effect.spawn(move |cancel| async move {
                cancel.cancelled().await;
                if let Some(tx) = cancelled.lock().expect("cancelled").take() {
                    let _ = tx.send(());
                }
                let receiver = release.lock().expect("release").take();
                if let Some(rx) = receiver {
                    let _ = rx.await;
                }
            })?;
            Ok(())
        }
    }

    let mut fiber = root
        .plugin(Arc::new(GatedDisposePlugin {
            cancelled: cancelled_tx,
            release: release_rx,
        }))
        .await
        .unwrap();
    let mut restart = Box::pin(fiber.restart());
    tokio::select! {
        result = &mut restart => panic!("restart completed unexpectedly: {result:?}"),
        _ = &mut cancelled_rx => {}
    }
    drop(restart);
    let _ = release_tx.send(());

    tokio::time::timeout(std::time::Duration::from_secs(2), fiber.restart())
        .await
        .expect("later restart must not remain FiberBusy")
        .expect("later restart");
    assert_eq!(fiber.state(), FiberState::Active);
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn fiber_dispose_then_dispose_wait_preserves_async_errors() {
    use cordis_core::PluginKey;

    static KEY: PluginKey = PluginKey::new("test.async.dispose.then.wait");
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
                Err(CoreError::EventListener("fiber dispose boom".into()))
            })?;
            Ok(())
        }
    }

    let mut fiber = root.plugin(Arc::new(BoomPlugin)).await.unwrap();
    fiber.dispose();
    let error = fiber.dispose_wait().await.expect_err("wait after dispose");
    assert!(matches!(error, CoreError::DisposeFailed { .. }));
    let _ = runtime.shutdown().await;
}
