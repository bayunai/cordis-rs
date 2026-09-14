use async_trait::async_trait;
use cordis_core::{
    Context, CoreError, EventKey, InjectionState, ParallelKey, Plugin, Runtime, SerialKey,
    ServiceKey, WaterfallKey,
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
async fn child_context_overrides_then_disposal_falls_back_to_parent() {
    let runtime = runtime();
    let root = runtime.root();
    root.provide(NUMBER, Number(1)).unwrap();
    let child = root.child().unwrap();
    child.provide(NUMBER, Number(2)).unwrap();
    assert_eq!(child.get(NUMBER).unwrap().0, 2);
    child.dispose();
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
    let child = runtime.root().child().unwrap();
    child.dispose();
    assert!(matches!(
        child.provide(NUMBER, Number(1)),
        Err(CoreError::ContextDisposed)
    ));
    assert!(matches!(child.get(NUMBER), Err(CoreError::ContextDisposed)));
}

#[tokio::test]
async fn cross_context_isolation() {
    let runtime = runtime();
    let left = runtime.root().child().unwrap();
    let right = runtime.root().child().unwrap();
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
            .fibers
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
    assert!(!runtime.diagnostics().plugins.is_empty());

    handle.dispose();
    handle.dispose();
    assert!(handle.is_disposed());
    assert_eq!(dispose_count.load(Ordering::SeqCst), 1);
    assert_service_unavailable(&root, NUMBER);
    assert!(runtime.diagnostics().plugins.is_empty());
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
    let result = runtime.root().plugin(Arc::new(FailingPlugin)).await;
    assert!(matches!(result, Err(CoreError::PluginApply(_))));
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
            let _child = effect.child();
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

    let parent = root.child().unwrap();
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
    parent.dispose();
    let _ = gate_tx.send(());
    let result = mount.await.expect("join");
    assert!(matches!(result, Err(CoreError::PluginApply(_))));
    assert_service_unavailable(&root, NUMBER);
    assert!(runtime.diagnostics().plugins.is_empty());
}

#[tokio::test]
async fn service_and_event_key_types_are_globally_locked() {
    let runtime = runtime();
    let root = runtime.root();
    root.provide(NUMBER, Number(1)).unwrap();

    let child = root.child().unwrap();
    assert!(matches!(
        child.provide(OTHER_NUMBER, Other),
        Err(CoreError::ServiceKeyTypeConflict { .. })
    ));

    // Provider 撤销后类型合同仍保留。
    let provider = root.effect().unwrap();
    // root 已占用同 Key；用子 Context 提供后再撤销验证合同。
    let scoped = root.child().unwrap();
    scoped.provide(NUMBER, Number(2)).unwrap();
    scoped.dispose();
    assert!(matches!(
        root.child().unwrap().provide(OTHER_NUMBER, Other),
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
async fn context_dispose_removes_node_from_diagnostics() {
    let runtime = runtime();
    let root = runtime.root();
    let before = runtime.diagnostics().contexts.len();
    let child = root.child().unwrap();
    child.provide(NUMBER, Number(1)).unwrap();
    child.on(PING, |_| Ok(())).unwrap();
    assert_eq!(runtime.diagnostics().contexts.len(), before + 1);
    assert!(!runtime.diagnostics().providers.is_empty());
    child.dispose();
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
    let result = root.plugin(Arc::new(OtherPlugin)).await;
    assert!(matches!(result, Err(CoreError::PluginApply(_))));
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
async fn child_dispose_then_drop_context_parent_shutdown_waits_task() {
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
