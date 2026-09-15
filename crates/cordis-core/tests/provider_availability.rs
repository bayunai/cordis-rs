//! Provider “已注册但未就绪”合同：严格解析、结构化诊断与可用性重算。

#[path = "common/helpers.rs"]
mod common;
use common::*;
use cordis_core::{ProviderAvailability, ProviderSnapshot};

fn availability(ready: &AtomicBool) -> ProviderAvailability {
    if ready.load(Ordering::SeqCst) {
        ProviderAvailability::Ready
    } else {
        ProviderAvailability::Unavailable {
            reason: Arc::from("connector is not ready"),
        }
    }
}

#[tokio::test]
async fn checked_provider_is_registered_but_strictly_unavailable_until_refreshed() {
    let runtime = runtime();
    let root = runtime.root();
    let ready = Arc::new(AtomicBool::new(false));
    let check_ready = ready.clone();
    let handle = root
        .provide_checked(NUMBER, Number(7), move || availability(&check_ready))
        .unwrap();

    assert!(matches!(
        root.get(NUMBER),
        Err(CoreError::ServiceUnavailable { .. })
    ));

    let snapshot = runtime.diagnostics();
    let provider = snapshot
        .providers
        .iter()
        .find(|provider: &&ProviderSnapshot| provider.service == NUMBER.id().as_str())
        .unwrap();
    assert!(matches!(
        &provider.availability,
        ProviderAvailability::Unavailable { reason } if reason.as_ref() == "connector is not ready"
    ));

    ready.store(true, Ordering::SeqCst);
    handle.refresh().unwrap();
    assert_eq!(root.get(NUMBER).unwrap().0, 7);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn checked_provider_rebuilds_injection_across_unavailable_ready_flip() {
    let runtime = runtime();
    let root = runtime.root();
    let ready = Arc::new(AtomicBool::new(false));
    let check_ready = ready.clone();
    let provider = root
        .provide_checked(NUMBER, Number(7), move || availability(&check_ready))
        .unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    let observed = runs.clone();
    let injection = root
        .inject([NUMBER.id()], move |services, _effect| {
            let observed = observed.clone();
            async move {
                assert_eq!(services.get(NUMBER)?.0, 7);
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();

    wait_injection(&injection, InjectionState::Pending).await;
    ready.store(true, Ordering::SeqCst);
    provider.refresh().unwrap();
    wait_injection(&injection, InjectionState::Active).await;
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    ready.store(false, Ordering::SeqCst);
    provider.refresh().unwrap();
    wait_injection(&injection, InjectionState::Pending).await;

    // 不等待中间调度：revision 必须使 Ready→Unavailable→Ready 仍触发重建。
    ready.store(true, Ordering::SeqCst);
    provider.refresh().unwrap();
    wait_until(|| runs.load(Ordering::SeqCst) == 2).await;
    assert_eq!(injection.state(), InjectionState::Active);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn unavailable_local_provider_shadows_ready_parent_without_fallback() {
    let runtime = runtime();
    let root = runtime.root();
    root.provide(NUMBER, Number(1)).unwrap();
    let child = root.extend().unwrap();
    let ready = Arc::new(AtomicBool::new(false));
    let check_ready = ready.clone();
    let _provider = child
        .provide_checked(NUMBER, Number(2), move || availability(&check_ready))
        .unwrap();

    assert!(matches!(
        child.get(NUMBER),
        Err(CoreError::ServiceUnavailable { .. })
    ));
    assert_eq!(root.get(NUMBER).unwrap().0, 1);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn checked_provider_handle_fails_after_owner_scope_is_disposed() {
    let runtime = runtime();
    let root = runtime.root();
    let effect = root.effect().unwrap();
    let ready = Arc::new(AtomicBool::new(true));
    let check_ready = ready.clone();
    let provider = effect
        .provide_checked(NUMBER, Number(7), move || availability(&check_ready))
        .unwrap();
    effect.dispose();
    assert!(matches!(
        provider.refresh(),
        Err(CoreError::ProviderDisposed { .. })
    ));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn provider_from_loading_plugin_is_only_available_after_plugin_becomes_active() {
    struct SelfProviding;

    #[async_trait]
    impl Plugin for SelfProviding {
        fn key(&self) -> cordis_core::PluginKey {
            cordis_core::PluginKey::new("test.self-providing@1")
        }

        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            ctx.provide(NUMBER, Number(7))?;
            assert!(matches!(
                ctx.get(NUMBER),
                Err(CoreError::ServiceUnavailable { .. })
            ));
            Ok(())
        }
    }

    let runtime = runtime();
    let root = runtime.root();
    let _fiber = root.plugin(Arc::new(SelfProviding)).await.unwrap();
    assert_eq!(root.get(NUMBER).unwrap().0, 7);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn refresh_check_panic_marks_provider_unavailable_without_stopping_runtime() {
    let runtime = runtime();
    let root = runtime.root();
    let should_panic = Arc::new(AtomicBool::new(false));
    let check_panic = should_panic.clone();
    let provider = root
        .provide_checked(NUMBER, Number(7), move || {
            if check_panic.load(Ordering::SeqCst) {
                panic!("health state corrupted");
            }
            ProviderAvailability::Ready
        })
        .unwrap();
    assert_eq!(root.get(NUMBER).unwrap().0, 7);

    should_panic.store(true, Ordering::SeqCst);
    assert!(matches!(
        provider.refresh(),
        Err(CoreError::ProviderCheck(_))
    ));
    assert!(matches!(
        root.get(NUMBER),
        Err(CoreError::ServiceUnavailable { .. })
    ));
    assert!(matches!(
        runtime.diagnostics().providers[0].availability,
        ProviderAvailability::Unavailable { .. }
    ));
    runtime.settle().await;
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn checked_provider_drives_plugin_pending_unload_and_reactivation() {
    struct Consumer {
        runs: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Plugin for Consumer {
        fn key(&self) -> cordis_core::PluginKey {
            cordis_core::PluginKey::new("test.checked-provider-consumer@1")
        }

        fn inject(&self) -> Vec<cordis_core::ServiceId> {
            vec![NUMBER.id()]
        }

        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            assert_eq!(ctx.get(NUMBER)?.0, 7);
            self.runs.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let runtime = runtime();
    let root = runtime.root();
    let mut state_events = runtime.subscribe_fiber_states();
    let ready = Arc::new(AtomicBool::new(false));
    let check_ready = ready.clone();
    let provider = root
        .provide_checked(NUMBER, Number(7), move || availability(&check_ready))
        .unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    let fiber = root
        .plugin(Arc::new(Consumer { runs: runs.clone() }))
        .await
        .unwrap();
    assert_eq!(fiber.state(), FiberState::Pending);
    assert_eq!(fiber.snapshot().unavailable_dependencies.len(), 1);
    let pending_event = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let event = state_events.recv().await.unwrap();
            if event.fiber_id == fiber.id() && event.current == FiberState::Pending {
                break event;
            }
        }
    })
    .await
    .expect("pending state event");
    assert_eq!(pending_event.unavailable_dependencies.len(), 1);

    ready.store(true, Ordering::SeqCst);
    provider.refresh().unwrap();
    wait_until(|| fiber.state() == FiberState::Active).await;
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    ready.store(false, Ordering::SeqCst);
    provider.refresh().unwrap();
    wait_until(|| fiber.state() == FiberState::Pending).await;
    assert_eq!(fiber.snapshot().unavailable_dependencies.len(), 1);
    runtime.shutdown().await.unwrap();
}
