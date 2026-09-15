//! 验证服务解析公开合同：层级/隔离解析、晚到 provider 激活消费者、不可用断言。
//!
//! 不含业务作用域语义；夹具见 `common/helpers`。

#[path = "common/helpers.rs"]
mod common;
use common::*;

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
async fn scheduler_injection_calling_settle_does_not_deadlock() {
    let runtime = runtime();
    let root = runtime.root();
    let activated = Arc::new(AtomicUsize::new(0));
    let observed = activated.clone();
    let runtime_for_callback = runtime.clone();
    let handle = root
        .inject([NUMBER.id()], move |services, _effect| {
            let observed = observed.clone();
            let runtime = runtime_for_callback.clone();
            async move {
                runtime.settle().await;
                assert_eq!(services.get(NUMBER)?.0, 7);
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();
    assert_eq!(handle.state(), InjectionState::Pending);

    root.effect().unwrap().provide(NUMBER, Number(7)).unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        wait_injection(&handle, InjectionState::Active),
    )
    .await
    .expect("scheduler injection timed out — likely settle deadlock");
    assert_eq!(activated.load(Ordering::SeqCst), 1);
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn injection_callback_panic_fails_only_that_injection_and_scheduler_continues() {
    let runtime = runtime();
    let root = runtime.root();
    let failed = root
        .inject(
            [NUMBER.id()],
            |_services, _effect| -> std::future::Ready<Result<(), CoreError>> {
                panic!("injection callback boom");
            },
        )
        .unwrap();
    let healthy_runs = Arc::new(AtomicUsize::new(0));
    let counter = healthy_runs.clone();
    let healthy = root
        .inject([NUMBER.id()], move |_services, _effect| {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();

    root.effect().unwrap().provide(NUMBER, Number(1)).unwrap();
    wait_injection(&failed, InjectionState::Failed).await;
    wait_injection(&healthy, InjectionState::Active).await;
    assert_eq!(healthy_runs.load(Ordering::SeqCst), 1);
    tokio::time::timeout(std::time::Duration::from_secs(2), runtime.settle())
        .await
        .expect("scheduler must remain available after injection panic");
    runtime.shutdown().await.expect("shutdown");
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
        fn key(&self) -> cordis_core::PluginKey {
            cordis_core::PluginKey::new("test.otherplugin")
        }
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
