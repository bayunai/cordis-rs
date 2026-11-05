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
    let mut handle = root
        .plugin(&cordis_testkit::PLUGIN_DEFINITION, plugin.into_arc())
        .await
        .unwrap();
    assert_eq!(root.get(NUMBER).unwrap().0, 9);

    handle.dispose();
    handle.dispose();
    assert!(handle.is_disposed());
    handle.dispose_wait().await.unwrap();
    assert_eq!(dispose_count.load(Ordering::SeqCst), 1);
    assert_service_unavailable(&root, NUMBER);
}

struct FailingPlugin;

cordis_core::plugin_definition!(static FAILINGPLUGIN_DEFINITION = "test.failingplugin";);

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
        .plugin(&FAILINGPLUGIN_DEFINITION, Arc::new(FailingPlugin))
        .await
        .unwrap();
    assert_eq!(fiber.state(), cordis_core::FiberState::Failed);
    assert!(fiber.last_error().is_some());
}

struct SettleInApplyPlugin {
    runtime: Runtime,
}

cordis_core::plugin_definition!(static SETTLEINAPPLYPLUGIN_DEFINITION = "test.settle-in-apply";);

#[async_trait]
impl Plugin for SettleInApplyPlugin {
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
        .plugin(
            &SETTLEINAPPLYPLUGIN_DEFINITION,
            Arc::new(SettleInApplyPlugin {
                runtime: runtime.clone(),
            }),
        )
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

cordis_core::plugin_definition!(static PANICAPPLYPLUGIN_DEFINITION = "test.panic-apply";);

#[async_trait]
impl Plugin for PanicApplyPlugin {
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

cordis_core::plugin_definition!(static HEALTHYAFTERPANICPLUGIN_DEFINITION = "test.healthy-after-panic";);

#[async_trait]
impl Plugin for HealthyAfterPanicPlugin {
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
    let panic_fiber = root
        .plugin(&PANICAPPLYPLUGIN_DEFINITION, Arc::new(PanicApplyPlugin))
        .await
        .unwrap();
    let healthy_runs = Arc::new(AtomicUsize::new(0));
    let healthy_fiber = root
        .plugin(
            &HEALTHYAFTERPANICPLUGIN_DEFINITION,
            Arc::new(HealthyAfterPanicPlugin {
                runs: healthy_runs.clone(),
            }),
        )
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

struct ShutdownInApplyPlugin {
    runtime: Runtime,
}

cordis_core::plugin_definition!(static SHUTDOWNINAPPLYPLUGIN_DEFINITION = "test.shutdown-in-apply";);

#[async_trait]
impl Plugin for ShutdownInApplyPlugin {
    fn inject(&self) -> Vec<cordis_core::ServiceId> {
        vec![NUMBER.id()]
    }

    async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
        self.runtime.shutdown().await
    }
}

#[tokio::test]
async fn plugin_apply_shutdown_fails_fast_and_scheduler_continues() {
    let runtime = runtime();
    let root = runtime.root();
    let rejected = root
        .plugin(
            &SHUTDOWNINAPPLYPLUGIN_DEFINITION,
            Arc::new(ShutdownInApplyPlugin {
                runtime: runtime.clone(),
            }),
        )
        .await
        .unwrap();
    let healthy_runs = Arc::new(AtomicUsize::new(0));
    let healthy = root
        .plugin(
            &HEALTHYAFTERPANICPLUGIN_DEFINITION,
            Arc::new(HealthyAfterPanicPlugin {
                runs: healthy_runs.clone(),
            }),
        )
        .await
        .unwrap();
    root.effect().unwrap().provide(NUMBER, Number(1)).unwrap();

    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        wait_until(|| {
            rejected.state() == FiberState::Failed && healthy.state() == FiberState::Active
        }),
    )
    .await
    .expect("scheduler must not deadlock on plugin shutdown");
    assert_eq!(healthy_runs.load(Ordering::SeqCst), 1);
    assert!(
        rejected
            .last_error()
            .is_some_and(|error| error.contains("shutdown 只能由宿主"))
    );
    runtime.shutdown().await.expect("host shutdown");
}

cordis_core::plugin_definition!(static METADATA_KEY = "test.metadata";);

struct ValuePlugin(usize);

#[async_trait]
impl Plugin for ValuePlugin {
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        ctx.provide(NUMBER, Number(self.0))
    }
}

struct InjectPanicPlugin;

#[async_trait]
impl Plugin for InjectPanicPlugin {
    fn inject(&self) -> Vec<cordis_core::ServiceId> {
        panic!("inject boom");
    }

    async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
        Ok(())
    }
}

#[tokio::test]
async fn initial_plugin_metadata_panics_leave_no_runtime_residue() {
    let runtime = runtime();
    let root = runtime.root();
    let error = match root
        .plugin(&METADATA_KEY, Arc::new(InjectPanicPlugin))
        .await
    {
        Ok(_) => panic!("metadata panic must reject mount"),
        Err(error) => error,
    };
    assert!(matches!(error, CoreError::PluginApply(_)));
    let snapshot = runtime.diagnostics();
    assert!(snapshot.fibers.is_empty());
    assert!(snapshot.definitions.is_empty());
    assert!(snapshot.effects.is_empty());
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn replace_metadata_panics_keep_old_active_plugin_and_release_busy() {
    let runtime = runtime();
    let root = runtime.root();
    let mut fiber = root
        .plugin(&METADATA_KEY, Arc::new(ValuePlugin(1)))
        .await
        .unwrap();
    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(root.get(NUMBER).unwrap().0, 1);

    let error = fiber
        .replace(Arc::new(InjectPanicPlugin))
        .await
        .expect_err("metadata panic");
    assert!(matches!(error, CoreError::PluginApply(_)));
    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(root.get(NUMBER).unwrap().0, 1);

    fiber.replace(Arc::new(ValuePlugin(2))).await.unwrap();
    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(root.get(NUMBER).unwrap().0, 2);
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
        cordis_core::plugin_definition!(static TASKPLUGIN_DEFINITION = "test.taskplugin";);

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
            .plugin(
                &TASKPLUGIN_DEFINITION,
                Arc::new(TaskPlugin {
                    finished: finished.clone(),
                }),
            )
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

    cordis_core::plugin_definition!(static WAITPLUGIN_DEFINITION = "test.waitplugin";);

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
        .plugin(
            &WAITPLUGIN_DEFINITION,
            Arc::new(WaitPlugin {
                finished: observed,
                cancelled_tx: Mutex::new(Some(cancelled_tx)),
            }),
        )
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

    cordis_core::plugin_definition!(static PLUGINA_DEFINITION = "test.plugina";);

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

    cordis_core::plugin_definition!(static PLUGINB_DEFINITION = "test.pluginb";);

    #[async_trait]
    impl Plugin for PluginB {
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            ctx.provide(NUMBER, Number(2))?;
            Ok(())
        }
    }

    let mut old = root
        .plugin(&PLUGINA_DEFINITION, Arc::new(PluginA { finished: a_done }))
        .await
        .unwrap();
    assert_eq!(root.get(NUMBER).unwrap().0, 1);

    old.dispose_wait().await.expect("dispose_wait");
    assert!(
        a_finished.load(Ordering::SeqCst),
        "old plugin task must finish before remount"
    );

    let mut neu = root
        .plugin(&PLUGINB_DEFINITION, Arc::new(PluginB))
        .await
        .unwrap();
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

    cordis_core::plugin_definition!(static SLOWPLUGIN_DEFINITION = "test.slowplugin";);

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
            .plugin(
                &SLOWPLUGIN_DEFINITION,
                Arc::new(SlowPlugin {
                    entered: entered_tx,
                    gate: gate_rx,
                }),
            )
            .await
    });
    entered_rx.await.expect("plugin entered apply");
    parent_owner.dispose();
    let _ = gate_tx.send(());
    let result = mount.await.expect("join");
    assert!(matches!(result, Err(CoreError::FiberDisposed)));
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
    cordis_core::plugin_definition!(static DEPPLUGIN_DEFINITION = "test.depplugin";);

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

    let mut fiber = isolated
        .plugin(&DEPPLUGIN_DEFINITION, Arc::new(DepPlugin))
        .await
        .unwrap();
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
async fn definition_groups_and_unmount_waits() {
    cordis_core::plugin_definition!(static KEY = "test.registry.group";);
    let runtime = runtime();
    let root = runtime.root();
    let finished = Arc::new(AtomicUsize::new(0));

    struct SlowPlugin {
        finished: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl Plugin for SlowPlugin {
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
        .plugin(
            &KEY,
            Arc::new(SlowPlugin {
                finished: finished.clone(),
            }),
        )
        .await
        .unwrap();
    let _b = root
        .plugin(
            &KEY,
            Arc::new(SlowPlugin {
                finished: finished.clone(),
            }),
        )
        .await
        .unwrap();
    let snap = runtime.diagnostics();
    let group = snap
        .definitions
        .iter()
        .find(|item| item.definition_id == KEY.id())
        .expect("group");
    assert_eq!(group.fiber_ids.len(), 2);

    let count = runtime.unmount(KEY.id()).await.unwrap();
    assert_eq!(count, 2);
    assert_eq!(finished.load(Ordering::SeqCst), 2);
    assert!(
        runtime
            .diagnostics()
            .definitions
            .iter()
            .all(|item| item.definition_id != KEY.id())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unmount_rejects_concurrent_mount_for_shared_definition() {
    cordis_core::plugin_definition!(static KEY_A = "test.registry.a";);

    let (gate_tx, gate_rx) = oneshot::channel::<()>();
    let gate_rx = Arc::new(Mutex::new(Some(gate_rx)));

    struct PluginA {
        gate: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
        applies: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl Plugin for PluginA {
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            self.applies.fetch_add(1, Ordering::SeqCst);
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

    let runtime = runtime();
    let root = runtime.root();
    let initial_applies = Arc::new(AtomicUsize::new(0));
    let fiber = root
        .plugin(
            &KEY_A,
            Arc::new(PluginA {
                gate: gate_rx,
                applies: initial_applies,
            }),
        )
        .await
        .unwrap();

    let unmount = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.unmount(KEY_A.id()).await })
    };
    wait_until(|| {
        runtime
            .diagnostics()
            .definitions
            .iter()
            .any(|item| item.definition_id == KEY_A.id() && item.unmounting)
    })
    .await;
    let rejected_applies = Arc::new(AtomicUsize::new(0));
    let rejected = root
        .plugin(
            &KEY_A,
            Arc::new(PluginA {
                gate: Arc::new(Mutex::new(None)),
                applies: rejected_applies.clone(),
            }),
        )
        .await;
    assert!(matches!(rejected, Err(CoreError::PluginUnmounting { .. })));
    assert_eq!(
        rejected_applies.load(Ordering::SeqCst),
        0,
        "被卸载分组拒绝的候选插件不得执行 apply"
    );
    assert!(
        runtime
            .diagnostics()
            .fibers
            .iter()
            .all(|item| item.definition_id != KEY_A.id() || item.id == fiber.id()),
        "被拒绝候选不得残留 Fiber"
    );
    let _ = gate_tx.send(());
    assert_eq!(unmount.await.unwrap().unwrap(), 1);
    assert!(fiber.is_disposed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_unmount_waiter_does_not_leave_plugin_group_locked() {
    cordis_core::plugin_definition!(static KEY = "test.unmount-cancel-safe";);

    struct GatePlugin {
        entered: Arc<Mutex<Option<oneshot::Sender<()>>>>,
        release: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    }

    #[async_trait]
    impl Plugin for GatePlugin {
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            let entered = self.entered.clone();
            let release = self.release.clone();
            ctx.effect()?.on_dispose_async(move || async move {
                if let Some(sender) = entered.lock().expect("entered").take() {
                    let _ = sender.send(());
                }
                let receiver = { release.lock().expect("release").take() };
                if let Some(receiver) = receiver {
                    let _ = receiver.await;
                }
                Ok(())
            })?;
            Ok(())
        }
    }

    struct ReplacementPlugin;
    #[async_trait]
    impl Plugin for ReplacementPlugin {
        async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
            Ok(())
        }
    }

    let runtime = runtime();
    let root = runtime.root();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let _original = root
        .plugin(
            &KEY,
            Arc::new(GatePlugin {
                entered: Arc::new(Mutex::new(Some(entered_tx))),
                release: Arc::new(Mutex::new(Some(release_rx))),
            }),
        )
        .await
        .expect("mount");

    let waiter = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.unmount(KEY.id()).await })
    };
    entered_rx.await.expect("unmount disposer entered");
    waiter.abort();
    let _ = waiter.await;
    let _ = release_tx.send(());

    wait_until(|| {
        runtime
            .diagnostics()
            .definitions
            .iter()
            .all(|group| group.definition_id != KEY.id())
            && runtime
                .diagnostics()
                .fibers
                .iter()
                .all(|fiber| fiber.definition_id != KEY.id())
    })
    .await;

    let mut replacement = root
        .plugin(&KEY, Arc::new(ReplacementPlugin))
        .await
        .expect("unmount coordinator must release the key after waiter cancellation");
    replacement
        .dispose_wait()
        .await
        .expect("dispose replacement");
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn plugin_dispose_clears_registry_index() {
    cordis_core::plugin_definition!(static KEY = "test.registry.solo";);
    struct Solo;
    #[async_trait]
    impl Plugin for Solo {
        async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
            Ok(())
        }
    }

    let runtime = runtime();
    let root = runtime.root();
    let mut fiber = root.plugin(&KEY, Arc::new(Solo)).await.unwrap();
    assert!(
        runtime
            .diagnostics()
            .definitions
            .iter()
            .any(|item| item.definition_id == KEY.id() && item.fiber_ids.len() == 1)
    );
    fiber.dispose();
    // dispose_now 在 Effect DisposeCompletion 完成后再 unregister。
    fiber.dispose_wait().await.expect("dispose_wait");
    assert!(
        runtime
            .diagnostics()
            .definitions
            .iter()
            .all(|item| item.definition_id != KEY.id())
    );
}

#[tokio::test]
async fn unmount_and_shutdown_return_dispose_errors_after_cleanup() {
    cordis_core::plugin_definition!(static KEY = "test.async.dispose.unmount";);
    let runtime = runtime();
    let root = runtime.root();

    struct BoomPlugin;
    #[async_trait]
    impl Plugin for BoomPlugin {
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            let effect = ctx.effect()?;
            effect.on_dispose_async(|| async move {
                Err(CoreError::EventListener("unmount boom".into()))
            })?;
            Ok(())
        }
    }

    let _fiber = root.plugin(&KEY, Arc::new(BoomPlugin)).await.unwrap();
    let error = runtime.unmount(KEY.id()).await.expect_err("unmount");
    assert!(matches!(error, CoreError::DisposeFailed { .. }));
    assert!(
        runtime
            .diagnostics()
            .definitions
            .iter()
            .all(|group| group.definition_id != KEY.id())
    );

    let effect = runtime.root().effect().unwrap();
    effect
        .on_dispose_async(|| async move { Err(CoreError::EventListener("shutdown boom".into())) })
        .unwrap();
    let shutdown_error = runtime.shutdown().await.expect_err("shutdown");
    assert!(matches!(shutdown_error, CoreError::DisposeFailed { .. }));
    assert!(runtime.scheduler_stopped());
}

#[tokio::test]
async fn unmount_self_from_apply_returns_unmount_reentrant() {
    cordis_core::plugin_definition!(static KEY = "test.unmount-self-apply";);
    let runtime = runtime();
    let root = runtime.root();
    let runtime_for_plugin = runtime.clone();

    struct SelfUnmountPlugin {
        runtime: Runtime,
    }
    #[async_trait]
    impl Plugin for SelfUnmountPlugin {
        async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
            match self.runtime.unmount(KEY.id()).await {
                Err(CoreError::UnmountReentrant) => Ok(()),
                other => Err(CoreError::PluginApply(format!(
                    "expected UnmountReentrant, got {other:?}"
                ))),
            }
        }
    }

    let fiber = root
        .plugin(
            &KEY,
            Arc::new(SelfUnmountPlugin {
                runtime: runtime_for_plugin,
            }),
        )
        .await
        .unwrap();
    assert_eq!(fiber.state(), FiberState::Active);
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn unmount_unrelated_key_from_apply_succeeds() {
    cordis_core::plugin_definition!(static SELF_KEY = "test.unmount-unrelated-self";);
    cordis_core::plugin_definition!(static OTHER_KEY = "test.unmount-unrelated-other";);
    let runtime = runtime();
    let root = runtime.root();

    struct OtherPlugin;
    #[async_trait]
    impl Plugin for OtherPlugin {
        async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
            Ok(())
        }
    }
    let _other = root
        .plugin(&OTHER_KEY, Arc::new(OtherPlugin))
        .await
        .unwrap();

    let runtime_for_plugin = runtime.clone();
    struct UnmountOtherPlugin {
        runtime: Runtime,
    }
    #[async_trait]
    impl Plugin for UnmountOtherPlugin {
        async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
            self.runtime.unmount(OTHER_KEY.id()).await?;
            Ok(())
        }
    }

    let fiber = root
        .plugin(
            &SELF_KEY,
            Arc::new(UnmountOtherPlugin {
                runtime: runtime_for_plugin,
            }),
        )
        .await
        .unwrap();
    assert_eq!(fiber.state(), FiberState::Active);
    assert!(
        runtime
            .diagnostics()
            .definitions
            .iter()
            .all(|group| group.definition_id != OTHER_KEY.id())
    );
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn unmount_self_from_inject_spawn_and_async_disposer_returns_reentrant() {
    cordis_core::plugin_definition!(static KEY = "test.unmount-self-lifecycle";);
    let runtime = runtime();
    let root = runtime.root();
    root.provide(NUMBER, Number(1)).unwrap();
    let runtime_clone = runtime.clone();
    let seen = Arc::new(AtomicUsize::new(0));

    struct LifecycleUnmountPlugin {
        runtime: Runtime,
        seen: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl Plugin for LifecycleUnmountPlugin {
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            let runtime = self.runtime.clone();
            let seen = self.seen.clone();
            ctx.inject([NUMBER.id()], move |_services, effect| {
                let runtime = runtime.clone();
                let seen = seen.clone();
                async move {
                    match runtime.unmount(KEY.id()).await {
                        Err(CoreError::UnmountReentrant) => {
                            seen.fetch_add(1, Ordering::SeqCst);
                        }
                        other => {
                            return Err(CoreError::PluginApply(format!(
                                "inject expected UnmountReentrant: {other:?}"
                            )));
                        }
                    }
                    let runtime_for_spawn = runtime.clone();
                    let seen_for_spawn = seen.clone();
                    effect.spawn(move |_| {
                        let runtime = runtime_for_spawn.clone();
                        let seen = seen_for_spawn.clone();
                        async move {
                            if matches!(
                                runtime.unmount(KEY.id()).await,
                                Err(CoreError::UnmountReentrant)
                            ) {
                                seen.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    })?;
                    let runtime_for_dispose = runtime.clone();
                    let seen_for_dispose = seen.clone();
                    effect.on_dispose_async(move || {
                        let runtime = runtime_for_dispose.clone();
                        let seen = seen_for_dispose.clone();
                        async move {
                            match runtime.unmount(KEY.id()).await {
                                Err(CoreError::UnmountReentrant) => {
                                    seen.fetch_add(1, Ordering::SeqCst);
                                    Ok(())
                                }
                                other => Err(CoreError::EventListener(format!(
                                    "disposer expected UnmountReentrant: {other:?}"
                                ))),
                            }
                        }
                    })?;
                    Ok(())
                }
            })?;
            Ok(())
        }
    }

    let mut fiber = root
        .plugin(
            &KEY,
            Arc::new(LifecycleUnmountPlugin {
                runtime: runtime_clone,
                seen: seen.clone(),
            }),
        )
        .await
        .unwrap();
    wait_until(|| seen.load(Ordering::SeqCst) >= 1).await;
    fiber.dispose_wait().await.expect("dispose_wait");
    assert!(
        seen.load(Ordering::SeqCst) >= 3,
        "inject+spawn+disposer should each observe UnmountReentrant, got {}",
        seen.load(Ordering::SeqCst)
    );
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn dispose_now_async_disposer_unmount_self_returns_reentrant() {
    cordis_core::plugin_definition!(static KEY = "test.dispose-now-unmount-self";);
    let runtime = runtime();
    let root = runtime.root();
    let runtime_clone = runtime.clone();
    let seen = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = oneshot::channel::<()>();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));

    struct DisposeNowUnmountPlugin {
        runtime: Runtime,
        seen: Arc<AtomicUsize>,
        entered: Arc<Mutex<Option<oneshot::Sender<()>>>>,
        release: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    }
    #[async_trait]
    impl Plugin for DisposeNowUnmountPlugin {
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            let runtime = self.runtime.clone();
            let seen = self.seen.clone();
            let entered = self.entered.clone();
            let release = self.release.clone();
            let effect = ctx.effect()?;
            effect.on_dispose_async(move || {
                let runtime = runtime.clone();
                let seen = seen.clone();
                async move {
                    if let Some(tx) = entered.lock().expect("entered").take() {
                        let _ = tx.send(());
                    }
                    let receiver = release.lock().expect("release").take();
                    if let Some(rx) = receiver {
                        let _ = rx.await;
                    }
                    match runtime.unmount(KEY.id()).await {
                        Err(CoreError::UnmountReentrant) => {
                            seen.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }
                        other => Err(CoreError::EventListener(format!(
                            "dispose_now disposer expected UnmountReentrant: {other:?}"
                        ))),
                    }
                }
            })?;
            Ok(())
        }
    }

    let mut fiber = root
        .plugin(
            &KEY,
            Arc::new(DisposeNowUnmountPlugin {
                runtime: runtime_clone,
                seen: seen.clone(),
                entered: entered_tx,
                release: release_rx,
            }),
        )
        .await
        .unwrap();
    fiber.dispose();
    entered_rx.await.expect("disposer entered");
    // DisposeCompletion 完成前仍保留 plugin 索引与 pending_wait。
    assert!(
        runtime
            .diagnostics()
            .definitions
            .iter()
            .any(|group| group.definition_id == KEY.id())
    );
    let _ = release_tx.send(());
    fiber
        .dispose_wait()
        .await
        .expect("dispose_wait after dispose_now");
    assert_eq!(seen.load(Ordering::SeqCst), 1);
    wait_until(|| {
        runtime
            .diagnostics()
            .definitions
            .iter()
            .all(|group| group.definition_id != KEY.id())
    })
    .await;
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn dispose_now_async_disposer_can_unmount_unrelated_definition() {
    cordis_core::plugin_definition!(static SELF_KEY = "test.dispose-now-unmount-other-self";);
    cordis_core::plugin_definition!(static OTHER_KEY = "test.dispose-now-unmount-other-target";);
    let runtime = runtime();
    let root = runtime.root();

    struct OtherPlugin;
    #[async_trait]
    impl Plugin for OtherPlugin {
        async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
            Ok(())
        }
    }
    let _other = root
        .plugin(&OTHER_KEY, Arc::new(OtherPlugin))
        .await
        .unwrap();

    let runtime_clone = runtime.clone();
    let unmounted = Arc::new(AtomicBool::new(false));
    struct UnmountOtherOnDispose {
        runtime: Runtime,
        unmounted: Arc<AtomicBool>,
    }
    #[async_trait]
    impl Plugin for UnmountOtherOnDispose {
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            let runtime = self.runtime.clone();
            let unmounted = self.unmounted.clone();
            let effect = ctx.effect()?;
            effect.on_dispose_async(move || {
                let runtime = runtime.clone();
                let unmounted = unmounted.clone();
                async move {
                    runtime.unmount(OTHER_KEY.id()).await?;
                    unmounted.store(true, Ordering::SeqCst);
                    Ok(())
                }
            })?;
            Ok(())
        }
    }

    let mut fiber = root
        .plugin(
            &SELF_KEY,
            Arc::new(UnmountOtherOnDispose {
                runtime: runtime_clone,
                unmounted: unmounted.clone(),
            }),
        )
        .await
        .unwrap();
    fiber.dispose();
    fiber.dispose_wait().await.expect("dispose_wait");
    assert!(unmounted.load(Ordering::SeqCst));
    assert!(
        runtime
            .diagnostics()
            .definitions
            .iter()
            .all(|group| group.definition_id != OTHER_KEY.id())
    );
    runtime.shutdown().await.expect("shutdown");
}
