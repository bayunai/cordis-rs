//! 依赖图与资源归属共同决定循环等待；候选绑定参与消费分支判定。
use cordis_core::{CoreError, InjectionState, Runtime, ServiceKey};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

static B: ServiceKey<()> = ServiceKey::new("test.graph.b");
static C: ServiceKey<()> = ServiceKey::new("test.graph.c");

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transitive_consumer_drains_before_its_ancestor_provider() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let runtime = Runtime::new().unwrap();
        let root = runtime.root();
        let b = root.effect().unwrap();
        b.provide(B, ()).unwrap();
        let c_done = Arc::new(AtomicBool::new(false));
        let x_done = Arc::new(AtomicBool::new(false));
        let x_finished = x_done.clone();
        let c_finished = c_done.clone();
        root.inject([B.id()], move |_, effect| {
            let x_finished = x_finished.clone();
            let c_finished = c_finished.clone();
            async move {
                effect.provide(C, ())?;
                effect.on_dispose(move || {
                    assert!(x_finished.load(Ordering::SeqCst));
                    c_finished.store(true, Ordering::SeqCst);
                });
                Ok(())
            }
        })
        .unwrap();
        runtime.settle().await;
        let nested = b
            .inject([C.id()], move |_, effect| {
                let x_done = x_done.clone();
                async move {
                    effect.on_dispose(move || {
                        x_done.store(true, Ordering::SeqCst);
                    });
                    Ok(())
                }
            })
            .unwrap();
        runtime.settle().await;
        assert_eq!(nested.state(), InjectionState::Active);
        b.on_dispose(move || {
            assert!(c_done.load(Ordering::SeqCst));
        });
        runtime.shutdown().await.unwrap();
    })
    .await
    .expect("transitive consumer branch must drain before upstream resources");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_candidate_dependencies_participate_in_consumer_branch_exemption() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let runtime = Runtime::new().unwrap();
        let root = runtime.root();
        let b = root.effect().unwrap();
        b.provide(B, ()).unwrap();
        root.inject([B.id()], |_, effect| async move { effect.provide(C, ()) })
            .unwrap();
        runtime.settle().await;
        // X also consumes B, so X may clean up before C; B must then wait for both.
        let consumer = b.inject([B.id(), C.id()], |_, _| async { Ok(()) }).unwrap();
        runtime.settle().await;
        assert_eq!(consumer.state(), InjectionState::Active);
        runtime.shutdown().await.unwrap();
    })
    .await
    .expect("acyclic candidate graph must converge");
}

#[tokio::test]
async fn controlled_task_cannot_wait_for_its_own_scope() {
    let runtime = Runtime::new().unwrap();
    let effect = runtime.root().effect().unwrap();
    let target = effect.clone();
    let (sent, received) = tokio::sync::oneshot::channel();
    effect
        .spawn(move |_| async move {
            let error = target.dispose_wait().await.unwrap_err();
            sent.send(error).unwrap();
        })
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(5), received)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(error, CoreError::LifecycleReentrant { .. }));
    assert!(!effect.handle().is_disposed());
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broad_provider_owner_cannot_introduce_a_cycle_between_resource_branches() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let runtime = Runtime::new().unwrap();
        let root = runtime.root();
        root.provide(B, ()).unwrap();
        let provider = root.effect().unwrap();
        provider.provide(C, ()).unwrap();
        root.inject([C.id()], |_, _| async { Ok(()) }).unwrap();
        runtime.settle().await;
        let ran = Arc::new(AtomicBool::new(false));
        let flag = ran.clone();
        let nested = provider
            .inject([B.id()], move |_, _| {
                flag.store(true, Ordering::SeqCst);
                async { Ok(()) }
            })
            .unwrap();
        runtime.settle().await;
        assert_eq!(nested.state(), InjectionState::Failed);
        assert!(!ran.load(Ordering::SeqCst));
        runtime.shutdown().await.unwrap();
    })
    .await
    .expect("inherited cycle away from candidate owner must fail fast");
}

#[tokio::test]
async fn failed_injection_keeps_both_startup_and_cleanup_diagnostics() {
    let runtime = Runtime::new().unwrap();
    let provider = runtime.root().effect().unwrap();
    provider.provide(B, ()).unwrap();
    let consumer = runtime
        .root()
        .inject([B.id()], |_, effect| async move {
            effect.on_dispose_async(|| async {
                Err(CoreError::EventListener("cleanup-marker".into()))
            })?;
            Err(CoreError::EventListener("startup-marker".into()))
        })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), runtime.settle())
        .await
        .unwrap();
    assert_eq!(consumer.state(), InjectionState::Failed);
    let snapshot = runtime.diagnostics();
    let failure = snapshot
        .inject_fibers
        .iter()
        .find_map(|entry| entry.last_error.as_ref())
        .unwrap();
    assert!(failure.contains("startup-marker"));
    assert!(failure.contains("cleanup-marker"));
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_factory_racing_disposal_cannot_start_unowned_work() {
    let runtime = Runtime::new().unwrap();
    let effect = runtime.root().effect().unwrap();
    let owner = effect.clone();
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let ran = Arc::new(AtomicBool::new(false));
    let flag = ran.clone();
    let spawn = tokio::task::spawn_blocking(move || {
        owner.spawn(move |_| {
            entered.send(()).unwrap();
            resume.recv().unwrap();
            async move {
                flag.store(true, Ordering::SeqCst);
            }
        })
    });
    tokio::time::timeout(Duration::from_secs(5), started)
        .await
        .unwrap()
        .unwrap();
    effect.dispose_wait().await.unwrap();
    release.send(()).unwrap();
    assert!(matches!(
        spawn.await.unwrap(),
        Err(CoreError::ContextDisposed)
    ));
    assert!(!ran.load(Ordering::SeqCst));
    runtime.shutdown().await.unwrap();
}
