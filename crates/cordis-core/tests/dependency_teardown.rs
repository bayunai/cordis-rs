//! 显式依赖保护实际服务资源直到消费者执行、清理和任务全部结束。

use async_trait::async_trait;
use cordis_core::{
    Context, CoreError, EffectContext, Plugin, PluginKey, Runtime, ServiceId, ServiceKey,
};
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

static RESOURCE: ServiceKey<Resource> = ServiceKey::new("test.teardown.resource@1");
static LEFT: ServiceKey<Resource> = ServiceKey::new("test.teardown.left@1");
static RIGHT: ServiceKey<Resource> = ServiceKey::new("test.teardown.right@1");
static INDEPENDENT: ServiceKey<Resource> = ServiceKey::new("test.teardown.independent@1");
static PROVIDER_KEY: PluginKey = PluginKey::new("test.teardown.provider");

#[derive(Default)]
struct ResourceState {
    closed: AtomicBool,
    cancelled: AtomicBool,
    attached: AtomicBool,
    consumer_sync_done: AtomicBool,
    consumer_done: AtomicBool,
    consumer_task_done: AtomicBool,
}

struct Resource(Arc<ResourceState>);

impl ResourceState {
    fn assert_open(&self) {
        assert!(
            !self.closed.load(Ordering::SeqCst),
            "service closed before consumer finished"
        );
        assert!(
            !self.cancelled.load(Ordering::SeqCst),
            "provider task cancelled before consumer finished"
        );
    }

    fn close(&self) {
        if self.attached.load(Ordering::SeqCst) {
            assert!(self.consumer_sync_done.load(Ordering::SeqCst));
            assert!(self.consumer_done.load(Ordering::SeqCst));
            assert!(self.consumer_task_done.load(Ordering::SeqCst));
        }
        self.closed.store(true, Ordering::SeqCst);
    }
}

struct Gates {
    entered: Semaphore,
    release: Semaphore,
}

impl Gates {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        })
    }

    async fn entered(&self) {
        self.entered.acquire().await.unwrap().forget();
    }

    fn release(&self) {
        self.release.add_permits(64);
    }
}

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("lifecycle did not converge")
}

fn own_resource(effect: &EffectContext, state: Arc<ResourceState>) -> Result<(), CoreError> {
    let closing = state.clone();
    effect.on_dispose(move || closing.close());
    effect.spawn(move |cancel| async move {
        cancel.cancelled().await;
        if state.attached.load(Ordering::SeqCst) {
            assert!(state.consumer_done.load(Ordering::SeqCst));
            assert!(state.consumer_task_done.load(Ordering::SeqCst));
        }
        state.cancelled.store(true, Ordering::SeqCst);
    })
}

fn consume(
    effect: &EffectContext,
    state: Arc<ResourceState>,
    gates: Arc<Gates>,
) -> Result<(), CoreError> {
    state.attached.store(true, Ordering::SeqCst);
    let sync_state = state.clone();
    effect.on_dispose(move || {
        sync_state.assert_open();
        sync_state.consumer_sync_done.store(true, Ordering::SeqCst);
    });
    let task_state = state.clone();
    effect.spawn(move |cancel| async move {
        cancel.cancelled().await;
        task_state.assert_open();
        task_state.consumer_task_done.store(true, Ordering::SeqCst);
    })?;
    effect.on_dispose_async(move || async move {
        state.assert_open();
        gates.entered.add_permits(1);
        gates.release.acquire().await.unwrap().forget();
        state.assert_open();
        state.consumer_done.store(true, Ordering::SeqCst);
        Ok(())
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn effect_provider_waits_for_injected_consumer_before_cleanup_and_cancellation() {
    bounded(async {
        let runtime = Runtime::new().unwrap();
        let root = runtime.root();
        let provider = root.effect().unwrap();
        let state = Arc::new(ResourceState::default());
        provider.provide(RESOURCE, Resource(state.clone())).unwrap();
        own_resource(&provider, state.clone()).unwrap();
        let gates = Gates::new();
        let consumer_gates = gates.clone();
        root.inject([RESOURCE.id()], move |services, effect| {
            let gates = consumer_gates.clone();
            async move {
                let resource = services.get(RESOURCE)?;
                consume(&effect, resource.0.clone(), gates)
            }
        })
        .unwrap();
        runtime.settle().await;
        provider.dispose();
        gates.entered().await;
        assert!(provider.as_context().is_disposed());
        assert!(matches!(
            root.get(RESOURCE),
            Err(CoreError::ServiceUnavailable { .. })
        ));
        state.assert_open();
        assert!(state.consumer_sync_done.load(Ordering::SeqCst));
        gates.release();
        provider.dispose_wait().await.unwrap();
        assert!(state.consumer_done.load(Ordering::SeqCst));
        assert!(state.consumer_task_done.load(Ordering::SeqCst));
        assert!(state.closed.load(Ordering::SeqCst));
        assert!(state.cancelled.load(Ordering::SeqCst));
        runtime.shutdown().await.unwrap();
    })
    .await;
}

struct Provider {
    generations: Arc<Mutex<Vec<Arc<ResourceState>>>>,
}

#[async_trait]
impl Plugin for Provider {
    fn key(&self) -> PluginKey {
        PROVIDER_KEY
    }
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let state = Arc::new(ResourceState::default());
        self.generations.lock().unwrap().push(state.clone());
        ctx.provide(RESOURCE, Resource(state.clone()))?;
        // The provider's children own its worker and resource cleanup.
        own_resource(&ctx.effect()?, state)
    }
}

struct Consumer {
    gates: Arc<Gates>,
}

#[async_trait]
impl Plugin for Consumer {
    fn key(&self) -> PluginKey {
        PluginKey::new("test.teardown.consumer")
    }
    fn inject(&self) -> Vec<ServiceId> {
        vec![RESOURCE.id()]
    }
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let resource = ctx.get(RESOURCE)?;
        consume(&ctx.effect()?, resource.0.clone(), self.gates.clone())
    }
}

#[derive(Clone, Copy)]
enum ProviderExit {
    Dispose,
    Restart,
    Replace,
    Unmount,
    Shutdown,
}

async fn check_plugin_exit(exit: ProviderExit) {
    bounded(async {
        let runtime = Runtime::new().unwrap();
        let root = runtime.root();
        let generations = Arc::new(Mutex::new(Vec::new()));
        let mut provider = root
            .plugin(Arc::new(Provider {
                generations: generations.clone(),
            }))
            .await
            .unwrap();
        let gates = Gates::new();
        let consumer = root
            .plugin(Arc::new(Consumer {
                gates: gates.clone(),
            }))
            .await
            .unwrap();
        runtime.settle().await;
        let state = generations.lock().unwrap()[0].clone();
        let operation_runtime = runtime.clone();
        let operation_generations = generations.clone();
        let operation = tokio::spawn(async move {
            let result = match exit {
                ProviderExit::Dispose => provider.dispose_wait().await,
                ProviderExit::Restart => provider.restart().await,
                ProviderExit::Replace => {
                    provider
                        .replace(Arc::new(Provider {
                            generations: operation_generations,
                        }))
                        .await
                }
                ProviderExit::Unmount => operation_runtime.unmount(PROVIDER_KEY).await.map(|_| ()),
                ProviderExit::Shutdown => operation_runtime.shutdown().await,
            };
            (result, provider)
        });
        gates.entered().await;
        state.assert_open();
        assert!(root.get(RESOURCE).is_err());
        assert!(
            !operation.is_finished(),
            "provider operation finished before consumer cleanup"
        );
        gates.release();
        let (result, provider) = operation.await.unwrap();
        result.unwrap();
        assert!(state.closed.load(Ordering::SeqCst));
        assert!(state.cancelled.load(Ordering::SeqCst));
        assert!(state.consumer_done.load(Ordering::SeqCst));
        assert!(state.consumer_task_done.load(Ordering::SeqCst));
        drop(consumer);
        drop(provider);
        runtime.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn sibling_plugin_dispose_preserves_provider_resource() {
    check_plugin_exit(ProviderExit::Dispose).await;
}
#[tokio::test]
async fn sibling_plugin_restart_preserves_provider_resource() {
    check_plugin_exit(ProviderExit::Restart).await;
}
#[tokio::test]
async fn sibling_plugin_replace_preserves_provider_resource() {
    check_plugin_exit(ProviderExit::Replace).await;
}
#[tokio::test]
async fn sibling_plugin_unmount_preserves_provider_resource() {
    check_plugin_exit(ProviderExit::Unmount).await;
}
#[tokio::test]
async fn shutdown_preserves_sibling_provider_resource() {
    check_plugin_exit(ProviderExit::Shutdown).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loading_consumer_keeps_provider_open_until_callback_and_cleanup_finish() {
    bounded(async {
        let runtime = Runtime::new().unwrap();
        let root = runtime.root();
        let provider = root.effect().unwrap();
        let state = Arc::new(ResourceState::default());
        provider.provide(RESOURCE, Resource(state.clone())).unwrap();
        // This test has no managed consumer task; model its completion explicitly.
        state.consumer_task_done.store(true, Ordering::SeqCst);
        let closing = state.clone();
        provider.on_dispose(move || closing.close());
        let entered = Arc::new(Semaphore::new(0));
        let resume = Arc::new(Semaphore::new(0));
        let cleanup = Gates::new();
        let (callback_entered, callback_resume, callback_cleanup) =
            (entered.clone(), resume.clone(), cleanup.clone());
        root.inject([RESOURCE.id()], move |services, effect| {
            let (entered, resume, gates) = (
                callback_entered.clone(),
                callback_resume.clone(),
                callback_cleanup.clone(),
            );
            async move {
                let resource = services.get(RESOURCE)?;
                resource.0.attached.store(true, Ordering::SeqCst);
                let sync_state = resource.0.clone();
                effect.on_dispose(move || {
                    sync_state.assert_open();
                    sync_state.consumer_sync_done.store(true, Ordering::SeqCst);
                });
                let cleanup_state = resource.0.clone();
                effect.on_dispose_async(move || async move {
                    cleanup_state.assert_open();
                    gates.entered.add_permits(1);
                    gates.release.acquire().await.unwrap().forget();
                    cleanup_state.consumer_done.store(true, Ordering::SeqCst);
                    Ok(())
                })?;
                entered.add_permits(1);
                resume.acquire().await.unwrap().forget();
                resource.0.assert_open();
                Ok(())
            }
        })
        .unwrap();
        entered.acquire().await.unwrap().forget();
        provider.dispose();
        state.assert_open();
        assert!(!state.consumer_sync_done.load(Ordering::SeqCst));
        assert!(root.get(RESOURCE).is_err());
        resume.add_permits(1);
        cleanup.entered().await;
        state.assert_open();
        cleanup.release();
        provider.dispose_wait().await.unwrap();
        assert!(state.closed.load(Ordering::SeqCst));
        runtime.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn consumer_cleanup_cannot_wait_for_its_upstream_provider() {
    bounded(async {
        let runtime = Runtime::new().unwrap();
        let root = runtime.root();
        let provider = root.effect().unwrap();
        provider
            .provide(RESOURCE, Resource(Arc::new(ResourceState::default())))
            .unwrap();
        let upstream = provider.clone();
        let rejected = Arc::new(AtomicBool::new(false));
        let observed = rejected.clone();
        root.inject([RESOURCE.id()], move |services, effect| {
            let upstream = upstream.clone();
            let rejected = rejected.clone();
            async move {
                let resource = services.get(RESOURCE)?;
                effect.on_dispose_async(move || async move {
                    resource.0.assert_open();
                    assert!(matches!(
                        upstream.dispose_wait().await,
                        Err(CoreError::LifecycleReentrant { .. })
                    ));
                    rejected.store(true, Ordering::SeqCst);
                    Ok(())
                })
            }
        })
        .unwrap();
        runtime.settle().await;
        provider.dispose_wait().await.unwrap();
        assert!(observed.load(Ordering::SeqCst));
        runtime.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn blocked_provider_does_not_block_independent_service_graph() {
    bounded(async {
        let runtime = Runtime::new().unwrap();
        let root = runtime.root();
        let provider = root.effect().unwrap();
        let state = Arc::new(ResourceState::default());
        provider.provide(RESOURCE, Resource(state.clone())).unwrap();
        own_resource(&provider, state.clone()).unwrap();
        let gates = Gates::new();
        let consumer_gates = gates.clone();
        root.inject([RESOURCE.id()], move |services, effect| {
            let gates = consumer_gates.clone();
            async move { consume(&effect, services.get(RESOURCE)?.0.clone(), gates) }
        })
        .unwrap();
        runtime.settle().await;
        provider.dispose();
        gates.entered().await;
        let other = root.effect().unwrap();
        let other_state = Arc::new(ResourceState::default());
        other.provide(INDEPENDENT, Resource(other_state)).unwrap();
        let independent_ran = Arc::new(Semaphore::new(0));
        let ran = independent_ran.clone();
        root.inject([INDEPENDENT.id()], move |services, _effect| {
            let ran = ran.clone();
            async move {
                services.get(INDEPENDENT)?.0.assert_open();
                ran.add_permits(1);
                Ok(())
            }
        })
        .unwrap();
        independent_ran.acquire().await.unwrap().forget();
        state.assert_open();
        gates.release();
        provider.dispose_wait().await.unwrap();
        runtime.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diamond_dependency_reports_a_shared_cleanup_failure_once() {
    bounded(async {
        let runtime = Runtime::new().unwrap();
        let root = runtime.root();
        let provider = root.effect().unwrap();
        provider
            .provide(RESOURCE, Resource(Arc::new(ResourceState::default())))
            .unwrap();
        for key in [LEFT, RIGHT] {
            root.inject([RESOURCE.id()], move |services, effect| async move {
                effect.provide(key, Resource(services.get(RESOURCE)?.0.clone()))?;
                Ok(())
            })
            .unwrap();
        }
        root.inject([LEFT.id(), RIGHT.id()], |services, effect| async move {
            let left = services.get(LEFT)?;
            let right = services.get(RIGHT)?;
            effect.on_dispose_async(move || async move {
                left.0.assert_open();
                right.0.assert_open();
                Err(CoreError::EventListener(
                    "diamond-leaf-cleanup-failed".into(),
                ))
            })
        })
        .unwrap();
        runtime.settle().await;
        let error = provider
            .dispose_wait()
            .await
            .expect_err("leaf failure must reach provider waiter");
        let CoreError::DisposeFailed { errors } = error else {
            panic!("unexpected error: {error:?}");
        };
        assert_eq!(
            errors
                .iter()
                .filter(|error| error.contains("diamond-leaf-cleanup-failed"))
                .count(),
            1,
            "{errors:?}"
        );
        let error = runtime
            .shutdown()
            .await
            .expect_err("shared failure must remain observable at shutdown");
        let CoreError::DisposeFailed { errors } = error else {
            panic!("unexpected error: {error:?}");
        };
        assert_eq!(
            errors
                .iter()
                .filter(|error| error.contains("diamond-leaf-cleanup-failed"))
                .count(),
            1,
            "{errors:?}"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_consumers_finish_even_when_first_provider_waiter_is_cancelled() {
    bounded(async {
        let runtime = Runtime::new().unwrap();
        let root = runtime.root();
        let provider = root.effect().unwrap();
        let state = Arc::new(ResourceState::default());
        provider.provide(RESOURCE, Resource(state.clone())).unwrap();
        let finished = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let closing = state.clone();
        let completed = finished.clone();
        provider.on_dispose(move || {
            assert_eq!(completed.load(Ordering::SeqCst), 2);
            closing.closed.store(true, Ordering::SeqCst);
        });
        let gates = [Gates::new(), Gates::new()];
        let done = [Arc::new(Semaphore::new(0)), Arc::new(Semaphore::new(0))];
        for (gate, done) in gates.iter().zip(done.iter()) {
            let (gate, done, finished) = (gate.clone(), done.clone(), finished.clone());
            root.inject([RESOURCE.id()], move |services, effect| {
                let (gate, done, finished) = (gate.clone(), done.clone(), finished.clone());
                async move {
                    let resource = services.get(RESOURCE)?;
                    effect.on_dispose_async(move || async move {
                        resource.0.assert_open();
                        gate.entered.add_permits(1);
                        gate.release.acquire().await.unwrap().forget();
                        resource.0.assert_open();
                        finished.fetch_add(1, Ordering::SeqCst);
                        done.add_permits(1);
                        Ok(())
                    })
                }
            })
            .unwrap();
        }
        runtime.settle().await;
        let first_provider = provider.clone();
        let first_waiter = tokio::spawn(async move { first_provider.dispose_wait().await });
        for gate in &gates {
            gate.entered().await;
        }
        first_waiter.abort();
        assert!(first_waiter.await.unwrap_err().is_cancelled());
        gates[0].release();
        done[0].acquire().await.unwrap().forget();
        assert_eq!(finished.load(Ordering::SeqCst), 1);
        state.assert_open();
        gates[1].release();
        provider.dispose_wait().await.unwrap();
        assert_eq!(finished.load(Ordering::SeqCst), 2);
        assert!(state.closed.load(Ordering::SeqCst));
        runtime.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_injection_finishes_before_parent_consumer_and_upstream_resource() {
    bounded(async {
        let runtime = Runtime::new().unwrap();
        let root = runtime.root();
        let provider = root.effect().unwrap();
        let state = Arc::new(ResourceState::default());
        provider.provide(RESOURCE, Resource(state.clone())).unwrap();
        own_resource(&provider, state.clone()).unwrap();
        let gates = Gates::new();
        let nested_gates = gates.clone();
        root.inject([RESOURCE.id()], move |services, effect| {
            let gates = nested_gates.clone();
            async move {
                let resource = services.get(RESOURCE)?;
                effect.provide(LEFT, Resource(resource.0.clone()))?;
                let nested_state = resource.0.clone();
                effect.on_dispose(move || {
                    nested_state.assert_open();
                    assert!(nested_state.consumer_done.load(Ordering::SeqCst));
                    assert!(nested_state.consumer_task_done.load(Ordering::SeqCst));
                });
                effect.inject([LEFT.id()], move |services, child| {
                    let gates = gates.clone();
                    async move { consume(&child, services.get(LEFT)?.0.clone(), gates) }
                })?;
                Ok(())
            }
        })
        .unwrap();
        runtime.settle().await;
        provider.dispose();
        gates.entered().await;
        state.assert_open();
        gates.release();
        provider.dispose_wait().await.unwrap();
        assert!(state.closed.load(Ordering::SeqCst));
        runtime.shutdown().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn plain_get_and_arc_clones_do_not_extend_provider_lifecycle() {
    bounded(async {
        let runtime = Runtime::new().unwrap();
        let root = runtime.root();
        let provider = root.effect().unwrap();
        let state = Arc::new(ResourceState::default());
        provider.provide(RESOURCE, Resource(state.clone())).unwrap();
        let closing = state.clone();
        provider.on_dispose(move || closing.closed.store(true, Ordering::SeqCst));
        let retained = root.get(RESOURCE).unwrap();
        let another = retained.clone();
        provider.dispose_wait().await.unwrap();
        assert!(retained.0.closed.load(Ordering::SeqCst));
        assert!(another.0.closed.load(Ordering::SeqCst));
        assert!(matches!(
            root.get(RESOURCE),
            Err(CoreError::ServiceUnavailable { .. })
        ));
        runtime.shutdown().await.unwrap();
    })
    .await;
}
