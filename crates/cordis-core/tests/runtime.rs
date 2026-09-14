use async_trait::async_trait;
use cordis_core::{
    Context, CoreError, EventKey, FiberState, InjectionState, ParallelKey, Plugin, Runtime,
    SerialKey, ServiceKey, WaterfallKey,
};
use cordis_testkit::{
    EventRecorder, TestPlugin, assert_service_unavailable, wait_injection, wait_until,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::oneshot;

#[derive(Debug, Clone)]
struct Number(usize);

#[derive(Debug)]
struct Other;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Ping(u32);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Pong(u32);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Decision(String);

static NUMBER: ServiceKey<Number> = ServiceKey::new("test.number@1");
static OTHER_NUMBER: ServiceKey<Other> = ServiceKey::new("test.number@1");
static DERIVED: ServiceKey<Number> = ServiceKey::new("test.derived@1");
static PING: EventKey<Ping> = EventKey::new("test.ping@1");
static PONG_AS_PING: EventKey<Pong> = EventKey::new("test.ping@1");
static TRANSFORM: WaterfallKey<Ping> = WaterfallKey::new("test.transform@1");
static TRANSFORM_AS_OBSERVE: EventKey<Ping> = EventKey::new("test.transform@1");
static DECIDE: SerialKey<Ping, Decision> = SerialKey::new("test.decide@1");
static DECIDE_WRONG_ANSWER: SerialKey<Ping, Pong> = SerialKey::new("test.decide@1");
static FANOUT: ParallelKey<Ping> = ParallelKey::new("test.fanout@1");

fn runtime() -> Runtime {
    Runtime::new().expect("tokio runtime required")
}

#[tokio::test]
async fn late_provider_activates_consumer_without_manual_settle() {
    let runtime = runtime();
    let root = runtime.root();
    let activated = Arc::new(AtomicUsize::new(0));
    let observed = activated.clone();
    let handle = root
        .inject([NUMBER.id()], move |services, _effect| {
            let observed = observed.clone();
            async move {
                assert_eq!(services.get(NUMBER)?.0, 7);
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();
    wait_injection(&handle, InjectionState::Pending).await;

    root.provide(NUMBER, Number(7)).unwrap();
    wait_injection(&handle, InjectionState::Active).await;
    assert_eq!(activated.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn effect_owned_extended_context_overrides_then_disposal_falls_back_to_parent() {
    let runtime = runtime();
    let root = runtime.root();
    root.provide(NUMBER, Number(1)).unwrap();
    let owner = root.effect().unwrap();
    let child = owner.extend().unwrap();
    child.provide(NUMBER, Number(2)).unwrap();
    assert_eq!(child.get(NUMBER).unwrap().0, 2);
    owner.dispose();
    assert_eq!(root.get(NUMBER).unwrap().0, 1);
}

#[tokio::test]
async fn provider_removal_disposes_consumer_and_reactivation_rebuilds_it() {
    let runtime = runtime();
    let root = runtime.root();
    let disposed = Arc::new(AtomicUsize::new(0));
    let counter = disposed.clone();
    let handle = root
        .inject([NUMBER.id()], move |_services, effect| {
            let counter = counter.clone();
            async move {
                effect.on_dispose(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                });
                Ok(())
            }
        })
        .unwrap();
    let provider = root.effect().unwrap();
    provider.provide(NUMBER, Number(1)).unwrap();
    wait_injection(&handle, InjectionState::Active).await;

    provider.dispose();
    wait_injection(&handle, InjectionState::Pending).await;
    assert_eq!(disposed.load(Ordering::SeqCst), 1);

    let replacement = root.effect().unwrap();
    replacement.provide(NUMBER, Number(2)).unwrap();
    wait_injection(&handle, InjectionState::Active).await;
}

#[tokio::test]
async fn rejects_conflicts_and_type_mismatch() {
    let runtime = runtime();
    let root = runtime.root();
    root.provide(NUMBER, Number(1)).unwrap();
    assert!(matches!(
        root.provide(NUMBER, Number(2)),
        Err(CoreError::ServiceConflict { .. })
    ));
    assert!(matches!(
        root.get(OTHER_NUMBER),
        Err(CoreError::ServiceTypeMismatch { .. })
    ));
}

#[tokio::test]
async fn nested_inject_and_failed_callback_follow_dependency_changes() {
    let runtime = runtime();
    let root = runtime.root();
    let leaf_runs = Arc::new(AtomicUsize::new(0));
    let leaf_counter = leaf_runs.clone();

    root.inject([NUMBER.id()], move |services, effect| {
        let leaf_counter = leaf_counter.clone();
        async move {
            let source = services.get(NUMBER)?;
            effect.provide(DERIVED, Number(source.0 + 1))?;
            effect.inject([DERIVED.id()], move |derived, _| {
                let leaf_counter = leaf_counter.clone();
                async move {
                    assert_eq!(derived.get(DERIVED)?.0, 2);
                    leaf_counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })?;
            Ok(())
        }
    })
    .unwrap();

    let provider = root.effect().unwrap();
    provider.provide(NUMBER, Number(1)).unwrap();
    wait_until(|| leaf_runs.load(Ordering::SeqCst) == 1).await;

    let failed = root
        .inject([DERIVED.id()], |_services, _| async {
            Err(CoreError::ServiceUnavailable {
                service: DERIVED.id(),
            })
        })
        .unwrap();
    wait_injection(&failed, InjectionState::Failed).await;
}

#[tokio::test]
async fn shutdown_cancels_and_waits_for_controlled_tasks() {
    let runtime = runtime();
    let root = runtime.root();
    let effect = root.effect().unwrap();
    let stopped = Arc::new(AtomicBool::new(false));
    let observed = stopped.clone();
    effect
        .spawn(move |cancel| async move {
            cancel.cancelled().await;
            observed.store(true, Ordering::SeqCst);
        })
        .unwrap();
    runtime.shutdown().await;
    assert!(stopped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn disposed_context_fail_fast() {
    let runtime = runtime();
    let owner = runtime.root().effect().unwrap();
    let child = owner.extend().unwrap();
    owner.dispose();
    assert!(matches!(
        child.provide(NUMBER, Number(1)),
        Err(CoreError::ContextDisposed)
    ));
    assert!(matches!(child.get(NUMBER), Err(CoreError::ContextDisposed)));
}

#[tokio::test]
async fn cross_context_isolation() {
    let runtime = runtime();
    let left = runtime.root().extend().unwrap();
    let right = runtime.root().extend().unwrap();
    left.provide(NUMBER, Number(1)).unwrap();
    assert_service_unavailable(&right, NUMBER);
    assert_eq!(left.get(NUMBER).unwrap().0, 1);
}

#[tokio::test]
async fn cyclic_injections_stay_pending_with_missing_deps_only() {
    let runtime = runtime();
    let root = runtime.root();
    let a = root
        .inject([DERIVED.id()], |services, effect| async move {
            let value = services.get(DERIVED)?;
            effect.provide(NUMBER, Number(value.0))?;
            Ok(())
        })
        .unwrap();
    let b = root
        .inject([NUMBER.id()], |services, effect| async move {
            let value = services.get(NUMBER)?;
            effect.provide(DERIVED, Number(value.0))?;
            Ok(())
        })
        .unwrap();
    runtime.settle().await;
    assert_eq!(a.state(), InjectionState::Pending);
    assert_eq!(b.state(), InjectionState::Pending);

    let snapshot = runtime.diagnostics();
    assert!(
        snapshot
            .inject_fibers
            .iter()
            .any(|fiber| !fiber.missing_dependencies.is_empty())
    );
    let debug = format!("{snapshot:?}");
    assert!(!debug.contains("cycles"));
}

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

#[tokio::test]
async fn events_preserve_order_and_propagate_errors() {
    let runtime = runtime();
    let root = runtime.root();
    let recorder = EventRecorder::<Ping>::new();
    let _sub = recorder.subscribe(&root, PING).unwrap();
    root.on(PING, |ping| {
        if ping.0 == 2 {
            return Err(CoreError::EventListener("stop".into()));
        }
        Ok(())
    })
    .unwrap();

    root.emit(PING, &Ping(1)).unwrap();
    assert_eq!(recorder.snapshot(), vec![Ping(1)]);
    let err = root.emit(PING, &Ping(2)).unwrap_err();
    assert!(matches!(err, CoreError::EventListener(_)));
}

#[tokio::test]
async fn event_unsubscribes_when_scope_disposes() {
    let runtime = runtime();
    let root = runtime.root();
    let effect = root.effect().unwrap();
    let recorder = EventRecorder::<Ping>::new();
    let _sub = recorder.subscribe(effect.as_context(), PING).unwrap();
    effect.dispose();
    root.emit(PING, &Ping(1)).unwrap();
    assert!(recorder.is_empty());
}

#[tokio::test]
async fn waterfall_rewrites_and_short_circuits() {
    let runtime = runtime();
    let root = runtime.root();
    let inner_ran = Arc::new(AtomicBool::new(false));
    let observed = inner_ran.clone();
    root.on_waterfall(TRANSFORM, |value, next| async move {
        let mut value = next.call(value).await?;
        value.0 *= 10;
        Ok(value)
    })
    .unwrap();
    root.on_waterfall(TRANSFORM, move |value, next| {
        let observed = observed.clone();
        async move {
            observed.store(true, Ordering::SeqCst);
            next.call(Ping(value.0 + 1)).await
        }
    })
    .unwrap();
    assert_eq!(root.waterfall(TRANSFORM, Ping(3)).await.unwrap(), Ping(40));
    assert!(inner_ran.load(Ordering::SeqCst));

    let skipped = Arc::new(AtomicBool::new(false));
    let skipped_flag = skipped.clone();
    let short = WaterfallKey::<Ping>::new("test.short@1");
    root.on_waterfall(short, |_value, _next| async move { Ok(Ping(99)) })
        .unwrap();
    root.on_waterfall(short, move |value, next| {
        let skipped_flag = skipped_flag.clone();
        async move {
            skipped_flag.store(true, Ordering::SeqCst);
            next.call(value).await
        }
    })
    .unwrap();
    assert_eq!(root.waterfall(short, Ping(1)).await.unwrap(), Ping(99));
    assert!(!skipped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn serial_returns_first_some() {
    let runtime = runtime();
    let root = runtime.root();
    root.on_serial(DECIDE, |_| async { Ok(None) }).unwrap();
    root.on_serial(DECIDE, |ping| {
        let n = ping.0;
        async move { Ok(Some(Decision(format!("hit-{n}")))) }
    })
    .unwrap();
    root.on_serial(DECIDE, |_| async {
        Ok(Some(Decision("should-not-run".into())))
    })
    .unwrap();
    assert_eq!(
        root.serial(DECIDE, &Ping(7)).await.unwrap(),
        Some(Decision("hit-7".into()))
    );
}

#[tokio::test]
async fn parallel_runs_all_and_aggregates_errors() {
    let runtime = runtime();
    let root = runtime.root();
    let hits = Arc::new(AtomicUsize::new(0));
    let a = hits.clone();
    let b = hits.clone();
    root.on_parallel(FANOUT, move |_| {
        let a = a.clone();
        async move {
            a.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    })
    .unwrap();
    root.on_parallel(FANOUT, move |_| {
        let b = b.clone();
        async move {
            b.fetch_add(1, Ordering::SeqCst);
            Err(CoreError::EventListener("boom".into()))
        }
    })
    .unwrap();
    let err = root.parallel(FANOUT, &Ping(1)).await.unwrap_err();
    assert!(matches!(
        err,
        CoreError::ParallelDispatchFailed { errors, .. } if errors.len() == 1
    ));
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn event_mode_and_answer_conflicts() {
    let runtime = runtime();
    let root = runtime.root();
    root.on_waterfall(
        TRANSFORM,
        |value, next| async move { next.call(value).await },
    )
    .unwrap();
    assert!(matches!(
        root.on(TRANSFORM_AS_OBSERVE, |_| Ok(())),
        Err(CoreError::EventModeMismatch { .. })
    ));
    root.on_serial(DECIDE, |_| async { Ok(None) }).unwrap();
    assert!(matches!(
        root.on_serial(DECIDE_WRONG_ANSWER, |_| async { Ok(None) }),
        Err(CoreError::EventAnswerTypeConflict { .. })
    ));
}

#[tokio::test]
async fn async_event_unsubscribes_when_scope_disposes() {
    let runtime = runtime();
    let root = runtime.root();
    let effect = root.effect().unwrap();
    let ran = Arc::new(AtomicBool::new(false));
    let flag = ran.clone();
    effect
        .on_parallel(FANOUT, move |_| {
            let flag = flag.clone();
            async move {
                flag.store(true, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();
    effect.dispose();
    root.parallel(FANOUT, &Ping(1)).await.unwrap();
    assert!(!ran.load(Ordering::SeqCst));
}

#[tokio::test]
async fn diagnostics_do_not_leak_service_payloads() {
    let runtime = runtime();
    let root = runtime.root();
    root.provide(NUMBER, Number(42)).unwrap();
    let snapshot = runtime.diagnostics();
    let debug = format!("{snapshot:?}");
    assert!(!debug.contains("42"));
    assert!(
        snapshot
            .providers
            .iter()
            .any(|provider| provider.service == NUMBER.id().as_str())
    );
}

#[tokio::test]
async fn concurrent_provide_and_dispose_converge() {
    let runtime = runtime();
    let root = runtime.root();
    let runs = Arc::new(AtomicUsize::new(0));
    let counter = runs.clone();
    let handle = root
        .inject([NUMBER.id()], move |_services, _effect| {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();

    let mut current = None::<cordis_core::EffectContext>;
    for value in 0..8 {
        if let Some(previous) = current.take() {
            previous.dispose();
        }
        let effect = root.effect().unwrap();
        effect.provide(NUMBER, Number(value)).unwrap();
        current = Some(effect);
        runtime.settle().await;
    }
    wait_injection(&handle, InjectionState::Active).await;
    assert!(runs.load(Ordering::SeqCst) >= 1);

    if let Some(previous) = current.take() {
        previous.dispose();
    }
    wait_injection(&handle, InjectionState::Pending).await;
}

#[tokio::test]
async fn scope_dispose_interleaved_with_child_cleanup_and_spawn() {
    let runtime = runtime();
    let root = runtime.root();
    let cleanups = Arc::new(AtomicUsize::new(0));
    let effect = Arc::new(root.effect().unwrap());
    let barrier = Arc::new(tokio::sync::Barrier::new(17));
    let mut workers = Vec::new();

    for _ in 0..16 {
        let effect = effect.clone();
        let cleanups = cleanups.clone();
        let barrier = barrier.clone();
        workers.push(tokio::spawn(async move {
            barrier.wait().await;
            let counter = cleanups.clone();
            effect.on_dispose(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            });
            let _child = effect.extend();
            let _ = effect.spawn(|cancel| async move {
                cancel.cancelled().await;
            });
        }));
    }

    let disposer = {
        let effect = effect.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            effect.dispose();
        })
    };

    for worker in workers {
        worker.await.expect("worker");
    }
    disposer.await.expect("disposer");
    assert_eq!(cleanups.load(Ordering::SeqCst), 16);
    assert_eq!(effect.child_scope_count(), 0);
    runtime.shutdown().await;
}

#[tokio::test]
async fn shutdown_stops_scheduler_and_rejects_root_ops() {
    let runtime = runtime();
    let root = runtime.root();
    root.provide(NUMBER, Number(1)).unwrap();
    runtime.shutdown().await;
    assert!(runtime.scheduler_stopped());
    tokio::time::timeout(std::time::Duration::from_millis(200), runtime.settle())
        .await
        .expect("settle after shutdown must not hang");
    assert!(matches!(
        root.provide(NUMBER, Number(2)),
        Err(CoreError::ContextDisposed)
    ));
    assert!(matches!(
        root.inject([NUMBER.id()], |_s, _e| async { Ok(()) }),
        Err(CoreError::ContextDisposed)
    ));
}

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
    runtime.shutdown().await;
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
    handle.dispose_wait().await;
    cancelled_rx.await.expect("task observed cancel");
    assert!(
        finished.load(Ordering::SeqCst),
        "dispose_wait must return only after plugin tasks finish"
    );
    handle.dispose_wait().await;
    assert!(handle.is_disposed());
    runtime.shutdown().await;
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

    old.dispose_wait().await;
    assert!(
        a_finished.load(Ordering::SeqCst),
        "old plugin task must finish before remount"
    );

    let mut neu = root.plugin(Arc::new(PluginB)).await.unwrap();
    assert_eq!(root.get(NUMBER).unwrap().0, 2);
    neu.dispose_wait().await;
    runtime.shutdown().await;
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
async fn service_and_event_key_types_are_globally_locked() {
    let runtime = runtime();
    let root = runtime.root();
    root.provide(NUMBER, Number(1)).unwrap();

    let child = root.extend().unwrap();
    assert!(matches!(
        child.provide(OTHER_NUMBER, Other),
        Err(CoreError::ServiceKeyTypeConflict { .. })
    ));

    // Provider 撤销后类型合同仍保留。
    let provider = root.effect().unwrap();
    // root 已占用同 Key；用子 Context 提供后再撤销验证合同。
    let scoped_owner = root.effect().unwrap();
    let scoped = scoped_owner.extend().unwrap();
    scoped.provide(NUMBER, Number(2)).unwrap();
    scoped_owner.dispose();
    assert!(matches!(
        root.extend().unwrap().provide(OTHER_NUMBER, Other),
        Err(CoreError::ServiceKeyTypeConflict { .. })
    ));
    let _ = provider;

    root.on(PING, |_| Ok(())).unwrap();
    assert!(matches!(
        root.on(PONG_AS_PING, |_| Ok(())),
        Err(CoreError::EventKeyTypeConflict { .. })
    ));
    assert!(matches!(
        root.emit(PONG_AS_PING, &Pong(1)),
        Err(CoreError::EventKeyTypeConflict { .. })
    ));
}

#[tokio::test]
async fn effect_dispose_removes_extended_nodes_from_diagnostics() {
    let runtime = runtime();
    let root = runtime.root();
    let before = runtime.diagnostics().contexts.len();
    let owner = root.effect().unwrap();
    let child = owner.extend().unwrap();
    child.provide(NUMBER, Number(1)).unwrap();
    child.on(PING, |_| Ok(())).unwrap();
    assert_eq!(runtime.diagnostics().contexts.len(), before + 1);
    assert!(!runtime.diagnostics().providers.is_empty());
    owner.dispose();
    assert_eq!(runtime.diagnostics().contexts.len(), before);
    assert!(runtime.diagnostics().providers.is_empty());
    root.emit(PING, &Ping(1)).unwrap();
}

#[tokio::test]
async fn dispose_cancels_without_abort_shutdown_waits() {
    let runtime = runtime();
    let root = runtime.root();
    let effect = root.effect().unwrap();
    let finished = Arc::new(AtomicBool::new(false));
    let observed = finished.clone();
    let (cancelled_tx, cancelled_rx) = oneshot::channel();
    effect
        .spawn(move |cancel| async move {
            cancel.cancelled().await;
            let _ = cancelled_tx.send(());
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            observed.store(true, Ordering::SeqCst);
        })
        .unwrap();
    effect.dispose();
    cancelled_rx.await.expect("task should observe cancel");
    assert!(
        !finished.load(Ordering::SeqCst),
        "task should still be running after dispose"
    );
    runtime.shutdown().await;
    assert!(finished.load(Ordering::SeqCst));
}

#[tokio::test]
async fn provider_revoke_from_non_tokio_thread_still_converges() {
    let runtime = runtime();
    let root = runtime.root();
    let handle = root
        .inject([NUMBER.id()], |_services, _effect| async { Ok(()) })
        .unwrap();
    let provider = root.effect().unwrap();
    provider.provide(NUMBER, Number(1)).unwrap();
    wait_injection(&handle, InjectionState::Active).await;

    let provider = Arc::new(provider);
    let to_dispose = provider.clone();
    std::thread::spawn(move || {
        to_dispose.dispose();
    })
    .join()
    .expect("thread join");

    wait_injection(&handle, InjectionState::Pending).await;
}

#[tokio::test]
async fn key_type_conflict_across_plugins() {
    let runtime = runtime();
    let root = runtime.root();
    let first = TestPlugin::new().provide_clone(NUMBER, Number(1));
    let mut a = root.plugin(first.into_arc()).await.unwrap();
    a.dispose();

    struct OtherPlugin;
    #[async_trait]
    impl Plugin for OtherPlugin {
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            ctx.provide(OTHER_NUMBER, Other)?;
            Ok(())
        }
    }
    let fiber = root.plugin(Arc::new(OtherPlugin)).await.unwrap();
    assert_eq!(fiber.state(), cordis_core::FiberState::Failed);
    assert!(
        fiber
            .last_error()
            .is_some_and(|message| message.contains("类型")
                || message.contains("PluginApply")
                || message.contains("绑定"))
    );
}

#[tokio::test]
async fn drop_runtime_without_shutdown_allows_new_runtime() {
    let first = runtime();
    assert!(!first.scheduler_stopped());
    let clone = first.clone();
    drop(clone);
    assert!(!first.scheduler_stopped());
    drop(first);
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    let again = runtime();
    assert!(!again.scheduler_stopped());
    again.shutdown().await;
    assert!(again.scheduler_stopped());
}

#[tokio::test]
async fn effect_dispose_then_drop_parent_shutdown_waits_task() {
    let runtime = runtime();
    let root = runtime.root();
    let finished = Arc::new(AtomicBool::new(false));
    let observed = finished.clone();
    let child = root.effect().unwrap();
    child
        .spawn(move |cancel| async move {
            cancel.cancelled().await;
            tokio::task::yield_now().await;
            observed.store(true, Ordering::SeqCst);
        })
        .unwrap();
    child.dispose();
    drop(child);
    runtime.shutdown().await;
    assert!(
        finished.load(Ordering::SeqCst),
        "task should be hoisted to parent and awaited on shutdown"
    );
}

#[tokio::test]
async fn isolate_derives_view_without_mutating_parent_or_siblings() {
    let runtime = runtime();
    let root = runtime.root();
    root.provide(NUMBER, Number(1)).unwrap();
    let sibling = root.extend().unwrap();

    let (isolated, label) = root.isolate(NUMBER).unwrap();
    assert_eq!(root.get(NUMBER).unwrap().0, 1);
    assert_eq!(sibling.get(NUMBER).unwrap().0, 1);
    assert_service_unavailable(&isolated, NUMBER);

    let owner = isolated.effect().unwrap();
    owner.provide(NUMBER, Number(2)).unwrap();
    let nested = isolated.extend().unwrap();
    let shared = root.isolate_with(NUMBER, label).unwrap();
    assert_eq!(isolated.get(NUMBER).unwrap().0, 2);
    assert_eq!(nested.get(NUMBER).unwrap().0, 2);
    assert_eq!(shared.get(NUMBER).unwrap().0, 2);
    assert_eq!(root.get(NUMBER).unwrap().0, 1);
    assert_eq!(sibling.get(NUMBER).unwrap().0, 1);

    owner.dispose();
    assert_service_unavailable(&isolated, NUMBER);
    assert_eq!(root.get(NUMBER).unwrap().0, 1);
}

#[tokio::test]
async fn isolation_label_shares_service_across_sibling_contexts() {
    let rt = runtime();
    let root = rt.root();
    let room_base = root.extend().unwrap();
    let (room, label) = room_base.isolate(NUMBER).unwrap();
    room.provide(NUMBER, Number(11)).unwrap();

    let a = root.isolate_with(NUMBER, label.clone()).unwrap();
    assert_eq!(a.get(NUMBER).unwrap().0, 11);

    let b = root.isolate_with(NUMBER, label).unwrap();
    assert_eq!(b.get(NUMBER).unwrap().0, 11);

    let plain = root.extend().unwrap();
    assert_service_unavailable(&plain, NUMBER);

    let other_rt = runtime();
    let (_, foreign) = other_rt.root().isolate(NUMBER).unwrap();
    assert!(matches!(
        room.isolate_with(NUMBER, foreign),
        Err(CoreError::IsolationRuntimeMismatch)
    ));
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
    assert_eq!(fiber.state(), FiberState::Active);

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
    fiber.dispose_wait().await;
    assert!(finished2.load(Ordering::SeqCst));
    assert!(fiber.is_disposed());
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
    fiber.dispose_wait().await;
    runtime.shutdown().await;
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
    runtime.shutdown().await;
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
    runtime.shutdown().await;
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
    runtime.shutdown().await;
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
    assert_eq!(fiber.state(), FiberState::Active);

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
    runtime.shutdown().await;
}

#[tokio::test]
async fn effect_tree_appears_in_diagnostics_and_clears() {
    let runtime = runtime();
    let root = runtime.root();
    let before = runtime.diagnostics().effects.len();
    let effect = root.effect_named("named-fx").unwrap();
    let handle = effect.handle();
    assert_eq!(handle.name(), "named-fx");
    assert!(
        runtime
            .diagnostics()
            .effects
            .iter()
            .any(|item| item.name == "named-fx" && item.id == handle.id())
    );
    effect.provide(NUMBER, Number(1)).unwrap();
    assert!(
        runtime
            .diagnostics()
            .providers
            .iter()
            .any(|provider| provider.effect_id == Some(handle.id()))
    );
    effect.dispose();
    assert_eq!(runtime.diagnostics().effects.len(), before);
    assert!(runtime.diagnostics().providers.is_empty());
}
