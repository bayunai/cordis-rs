//! 验证 EffectScope 资源归属与清理合同：受控任务、同步/异步 dispose 与等待顺序。
//!
//! 不覆盖插件 Fiber 状态机；夹具见 `common/helpers`。

#[path = "common/helpers.rs"]
mod common;
use common::*;

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
    runtime.shutdown().await.expect("shutdown");
    assert!(stopped.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controlled_task_spawn_from_std_thread_uses_scope_runtime() {
    let runtime = runtime();
    let effect = runtime.root().effect().unwrap();
    let started = Arc::new(AtomicBool::new(false));
    let stopped = Arc::new(AtomicBool::new(false));
    let thread_effect = effect.clone();
    let thread_started = started.clone();
    let thread_stopped = stopped.clone();

    let thread = std::thread::spawn(move || {
        thread_effect.spawn(move |cancel| async move {
            thread_started.store(true, Ordering::SeqCst);
            cancel.cancelled().await;
            thread_stopped.store(true, Ordering::SeqCst);
        })
    });
    thread
        .join()
        .expect("spawn must not panic outside Tokio context")
        .expect("spawn must use the scope runtime handle");

    wait_until(|| started.load(Ordering::SeqCst)).await;
    effect.dispose_wait().await.expect("dispose_wait");
    assert!(stopped.load(Ordering::SeqCst));
    runtime.shutdown().await.expect("shutdown");
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
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn shutdown_stops_scheduler_and_rejects_root_ops() {
    let runtime = runtime();
    let root = runtime.root();
    root.provide(NUMBER, Number(1)).unwrap();
    runtime.shutdown().await.expect("shutdown");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_and_subsequent_shutdown_share_same_dispose_failed() {
    let runtime = runtime();
    let effect = runtime.root().effect().unwrap();
    let (started_tx, started_rx) = oneshot::channel::<()>();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let started_tx = Arc::new(Mutex::new(Some(started_tx)));
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    effect
        .on_dispose_async(move || {
            let started_tx = started_tx.clone();
            let release_rx = release_rx.clone();
            async move {
                if let Some(tx) = started_tx.lock().expect("started").take() {
                    let _ = tx.send(());
                }
                let receiver = release_rx.lock().expect("release").take();
                if let Some(rx) = receiver {
                    let _ = rx.await;
                }
                Err(CoreError::EventListener("shared shutdown boom".into()))
            }
        })
        .unwrap();

    let first = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.shutdown().await })
    };
    started_rx.await.expect("dispose started");
    let second = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.shutdown().await })
    };
    tokio::task::yield_now().await;
    let _ = release_tx.send(());

    let err_a = first.await.expect("join a").expect_err("a");
    let err_b = second.await.expect("join b").expect_err("b");
    for error in [&err_a, &err_b] {
        match error {
            CoreError::DisposeFailed { errors } => {
                assert!(
                    errors
                        .iter()
                        .any(|item| item.contains("shared shutdown boom")),
                    "missing boom: {errors:?}"
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
    assert_eq!(format!("{err_a:?}"), format!("{err_b:?}"));
    assert!(runtime.scheduler_stopped());

    let err_later = runtime.shutdown().await.expect_err("subsequent");
    assert_eq!(format!("{err_a:?}"), format!("{err_later:?}"));
    assert!(runtime.scheduler_stopped());
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
    runtime.shutdown().await.expect("shutdown");
    assert!(finished.load(Ordering::SeqCst));
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
    again.shutdown().await.expect("shutdown");
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
    runtime.shutdown().await.expect("shutdown");
    assert!(
        finished.load(Ordering::SeqCst),
        "task should be hoisted to parent and awaited on shutdown"
    );
}

#[tokio::test]
async fn effect_tree_appears_in_diagnostics_and_clears() {
    let runtime = runtime();
    let root = runtime.root();
    let before = runtime.diagnostics().effects.len();
    let effect = root.effect_named("named-fx").unwrap();
    let handle = effect.handle();
    assert_eq!(handle.name(), "named-fx");
    assert!(!handle.is_disposed());
    assert!(!handle.is_cancelled());
    assert_eq!(handle.child_count(), 0);
    assert_eq!(handle.task_count(), 0);
    let cleanup_baseline = handle.cleanup_count();

    let child = effect.as_context().effect_named("child-fx").unwrap();
    let child_handle = child.handle();
    assert_eq!(child_handle.parent_id(), Some(handle.id()));
    assert_eq!(handle.child_count(), 1);
    child.dispose_wait().await.unwrap();
    assert_eq!(handle.child_count(), 0);

    effect.on_dispose(|| {});
    assert_eq!(handle.cleanup_count(), cleanup_baseline + 1);
    effect
        .spawn(|cancel| async move {
            cancel.cancelled().await;
        })
        .unwrap();
    assert_eq!(handle.task_count(), 1);
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
    effect.dispose_wait().await.unwrap();
    assert!(handle.is_disposed());
    assert!(handle.is_cancelled());
    assert_eq!(runtime.diagnostics().effects.len(), before);
    assert!(runtime.diagnostics().providers.is_empty());
}

#[tokio::test]
async fn on_dispose_async_runs_once_on_first_dispose_only() {
    let runtime = runtime();
    let root = runtime.root();
    let effect = root.effect().unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    let counter = runs.clone();
    effect
        .on_dispose_async(move || {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    effect.dispose_wait().await.expect("dispose_wait");
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    effect.dispose_wait().await.expect("dispose_wait");
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn dispose_runs_sync_cleanup_immediately_and_hoists_async_to_parent_wait() {
    let runtime = runtime();
    let root = runtime.root();
    let parent = root.effect().unwrap();
    let child = parent.extend().unwrap().effect().unwrap();
    let sync_done = Arc::new(AtomicBool::new(false));
    let async_done = Arc::new(AtomicBool::new(false));
    let (started_tx, started_rx) = oneshot::channel::<()>();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let started_tx = Arc::new(Mutex::new(Some(started_tx)));
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    let sync_flag = sync_done.clone();
    child.on_dispose(move || {
        sync_flag.store(true, Ordering::SeqCst);
    });
    let async_flag = async_done.clone();
    child
        .on_dispose_async(move || {
            let started_tx = started_tx.clone();
            let release_rx = release_rx.clone();
            let async_flag = async_flag.clone();
            async move {
                if let Some(tx) = started_tx.lock().expect("started").take() {
                    let _ = tx.send(());
                }
                let receiver = release_rx.lock().expect("release").take();
                if let Some(rx) = receiver {
                    let _ = rx.await;
                }
                async_flag.store(true, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();

    child.dispose();
    assert!(sync_done.load(Ordering::SeqCst));
    assert!(!async_done.load(Ordering::SeqCst));
    started_rx.await.expect("async disposer started");
    let _ = release_tx.send(());
    parent.dispose_wait().await.expect("parent wait");
    assert!(async_done.load(Ordering::SeqCst));
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn dispose_wait_respects_lifo_for_sync_and_async_cleanups() {
    let runtime = runtime();
    let root = runtime.root();
    let effect = root.effect().unwrap();
    let order = Arc::new(Mutex::new(Vec::new()));

    for label in ["sync-a", "sync-b"] {
        let order = order.clone();
        let label = label.to_string();
        effect.on_dispose(move || {
            order.lock().expect("order").push(label);
        });
    }
    for label in ["async-a", "async-b"] {
        let order = order.clone();
        let label = label.to_string();
        effect
            .on_dispose_async(move || {
                let order = order.clone();
                let label = label.clone();
                async move {
                    order.lock().expect("order").push(label);
                    Ok(())
                }
            })
            .unwrap();
    }

    effect.dispose_wait().await.expect("dispose_wait");
    assert_eq!(
        *order.lock().expect("order"),
        vec![
            "sync-b".to_string(),
            "sync-a".to_string(),
            "async-b".to_string(),
            "async-a".to_string()
        ]
    );
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn dispose_wait_aggregates_async_disposer_errors_and_continues() {
    let runtime = runtime();
    let root = runtime.root();
    let effect = root.effect().unwrap();
    let ran = Arc::new(AtomicUsize::new(0));

    let counter = ran.clone();
    effect
        .on_dispose_async(move || {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Err(CoreError::EventListener("first".into()))
            }
        })
        .unwrap();
    let counter = ran.clone();
    effect
        .on_dispose_async(move || {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Err(CoreError::EventListener("second".into()))
            }
        })
        .unwrap();
    let counter = ran.clone();
    effect
        .on_dispose_async(move || {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();

    let error = effect.dispose_wait().await.expect_err("aggregated");
    assert_eq!(ran.load(Ordering::SeqCst), 3);
    match error {
        CoreError::DisposeFailed { errors } => {
            assert_eq!(errors.len(), 2);
            assert!(errors.iter().any(|item| item.contains("first")));
            assert!(errors.iter().any(|item| item.contains("second")));
        }
        other => panic!("unexpected error: {other:?}"),
    }
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn dispose_wait_captures_sync_cleanup_panic_and_continues() {
    let runtime = runtime();
    let root = runtime.root();
    let effect = root.effect().unwrap();
    let ran = Arc::new(AtomicUsize::new(0));

    let first = ran.clone();
    effect.on_dispose(move || {
        first.fetch_add(1, Ordering::SeqCst);
    });
    effect.on_dispose(|| panic!("sync boom"));
    let third = ran.clone();
    effect.on_dispose(move || {
        third.fetch_add(1, Ordering::SeqCst);
    });

    let error = effect.dispose_wait().await.expect_err("sync panic");
    assert_eq!(ran.load(Ordering::SeqCst), 2);
    match error {
        CoreError::DisposeFailed { errors } => {
            assert_eq!(errors.len(), 1);
            assert!(errors[0].contains("sync boom"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn dispose_wait_captures_async_disposer_panic_and_continues() {
    let runtime = runtime();
    let root = runtime.root();
    let effect = root.effect().unwrap();
    let ran = Arc::new(AtomicUsize::new(0));

    let first = ran.clone();
    effect
        .on_dispose_async(move || {
            let first = first.clone();
            async move {
                first.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();
    effect
        .on_dispose_async(|| async move { panic!("async boom") })
        .unwrap();
    let third = ran.clone();
    effect
        .on_dispose_async(move || {
            let third = third.clone();
            async move {
                third.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();

    let error = effect.dispose_wait().await.expect_err("async panic");
    assert_eq!(ran.load(Ordering::SeqCst), 2);
    match error {
        CoreError::DisposeFailed { errors } => {
            assert_eq!(errors.len(), 1);
            assert!(errors[0].contains("async boom"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn parent_dispose_wait_hoists_child_disposer_panic() {
    let runtime = runtime();
    let root = runtime.root();
    let parent = root.effect().unwrap();
    let child = parent.extend().unwrap().effect().unwrap();
    child.on_dispose(|| panic!("child sync boom"));
    child
        .on_dispose_async(|| async move { panic!("child async boom") })
        .unwrap();

    let error = parent.dispose_wait().await.expect_err("hoisted panic");
    match error {
        CoreError::DisposeFailed { errors } => {
            assert!(
                errors.iter().any(|item| item.contains("child sync boom")),
                "missing sync panic: {errors:?}"
            );
            assert!(
                errors.iter().any(|item| item.contains("child async boom")),
                "missing async panic: {errors:?}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn on_dispose_async_rejects_after_scope_disposed() {
    let runtime = runtime();
    let root = runtime.root();
    let effect = root.effect().unwrap();
    effect.dispose();
    let error = effect
        .on_dispose_async(|| async move { Ok(()) })
        .expect_err("disposed");
    assert!(matches!(error, CoreError::ContextDisposed));
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn dispose_then_dispose_wait_shares_same_completion_result() {
    let runtime = runtime();
    let root = runtime.root();
    let effect = root.effect().unwrap();
    let (started_tx, started_rx) = oneshot::channel::<()>();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let started_tx = Arc::new(Mutex::new(Some(started_tx)));
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    effect
        .on_dispose_async(move || {
            let started_tx = started_tx.clone();
            let release_rx = release_rx.clone();
            async move {
                if let Some(tx) = started_tx.lock().expect("started").take() {
                    let _ = tx.send(());
                }
                let receiver = release_rx.lock().expect("release").take();
                if let Some(rx) = receiver {
                    let _ = rx.await;
                }
                Err(CoreError::EventListener("shared boom".into()))
            }
        })
        .unwrap();

    effect.dispose();
    started_rx.await.expect("started");
    let wait_a = {
        let effect = effect.clone();
        tokio::spawn(async move { effect.dispose_wait().await })
    };
    let wait_b = {
        let effect = effect.clone();
        tokio::spawn(async move { effect.dispose_wait().await })
    };
    let _ = release_tx.send(());
    let err_a = wait_a.await.expect("join").expect_err("a");
    let err_b = wait_b.await.expect("join").expect_err("b");
    for error in [err_a, err_b] {
        match error {
            CoreError::DisposeFailed { errors } => {
                assert_eq!(errors.len(), 1);
                assert!(errors[0].contains("shared boom"));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
    runtime.shutdown().await.expect_err("hoisted dispose error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispose_from_std_thread_starts_async_disposer_without_panic() {
    let runtime = runtime();
    let root = runtime.root();
    let effect = root.effect().unwrap();
    let finished = Arc::new(AtomicBool::new(false));
    let flag = finished.clone();
    effect
        .on_dispose_async(move || {
            let flag = flag.clone();
            async move {
                flag.store(true, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();

    let effect_for_thread = effect.clone();
    std::thread::spawn(move || {
        effect_for_thread.dispose();
    })
    .join()
    .expect("thread join");

    effect.dispose_wait().await.expect("dispose_wait");
    assert!(finished.load(Ordering::SeqCst));
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_disposers_run_strict_serial_lifo() {
    let runtime = runtime();
    let root = runtime.root();
    let effect = root.effect().unwrap();
    let order = Arc::new(Mutex::new(Vec::new()));
    let (gate_tx, gate_rx) = oneshot::channel::<()>();
    let gate_rx = Arc::new(Mutex::new(Some(gate_rx)));

    // 先注册 first（应后跑），再注册 second（应先跑并阻塞）。
    let order_first = order.clone();
    effect
        .on_dispose_async(move || {
            let order_first = order_first.clone();
            async move {
                order_first
                    .lock()
                    .expect("order")
                    .push("first-start".to_string());
                Ok(())
            }
        })
        .unwrap();
    let order_second = order.clone();
    effect
        .on_dispose_async(move || {
            let order_second = order_second.clone();
            let gate_rx = gate_rx.clone();
            async move {
                order_second
                    .lock()
                    .expect("order")
                    .push("second-start".to_string());
                let receiver = gate_rx.lock().expect("gate").take();
                if let Some(rx) = receiver {
                    let _ = rx.await;
                }
                order_second
                    .lock()
                    .expect("order")
                    .push("second-end".to_string());
                Ok(())
            }
        })
        .unwrap();

    let wait = {
        let effect = effect.clone();
        tokio::spawn(async move { effect.dispose_wait().await })
    };
    wait_until(|| {
        order
            .lock()
            .expect("order")
            .iter()
            .any(|item| item == "second-start")
    })
    .await;
    assert!(
        !order
            .lock()
            .expect("order")
            .iter()
            .any(|item| item == "first-start"),
        "earlier disposer must not start before later disposer finishes"
    );
    let _ = gate_tx.send(());
    wait.await.expect("join").expect("dispose_wait");
    assert_eq!(
        *order.lock().expect("order"),
        vec![
            "second-start".to_string(),
            "second-end".to_string(),
            "first-start".to_string()
        ]
    );
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parent_async_disposer_waits_for_child_scope_completion() {
    let runtime = runtime();
    let root = runtime.root();
    let parent = root.effect().unwrap();
    let child = parent.extend().unwrap().effect().unwrap();
    let order = Arc::new(Mutex::new(Vec::new()));
    let (child_started_tx, child_started_rx) = oneshot::channel::<()>();
    let (child_release_tx, child_release_rx) = oneshot::channel::<()>();
    let child_started_tx = Arc::new(Mutex::new(Some(child_started_tx)));
    let child_release_rx = Arc::new(Mutex::new(Some(child_release_rx)));

    let child_order = order.clone();
    child
        .on_dispose_async(move || {
            let child_order = child_order.clone();
            let child_started_tx = child_started_tx.clone();
            let child_release_rx = child_release_rx.clone();
            async move {
                child_order
                    .lock()
                    .expect("order")
                    .push("child-start".to_string());
                if let Some(tx) = child_started_tx.lock().expect("started").take() {
                    let _ = tx.send(());
                }
                let receiver = child_release_rx.lock().expect("release").take();
                if let Some(rx) = receiver {
                    let _ = rx.await;
                }
                child_order
                    .lock()
                    .expect("order")
                    .push("child-end".to_string());
                Ok(())
            }
        })
        .unwrap();

    let parent_order = order.clone();
    parent
        .on_dispose_async(move || {
            let parent_order = parent_order.clone();
            async move {
                parent_order
                    .lock()
                    .expect("order")
                    .push("parent-start".to_string());
                Ok(())
            }
        })
        .unwrap();

    let wait = {
        let parent = parent.clone();
        tokio::spawn(async move { parent.dispose_wait().await })
    };
    child_started_rx.await.expect("child started");
    assert!(
        !order
            .lock()
            .expect("order")
            .iter()
            .any(|item| item == "parent-start"),
        "parent async disposer must wait for child completion"
    );
    let _ = child_release_tx.send(());
    wait.await.expect("join").expect("parent dispose wait");
    assert_eq!(
        *order.lock().expect("order"),
        vec![
            "child-start".to_string(),
            "child-end".to_string(),
            "parent-start".to_string()
        ]
    );
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn drop_runtime_without_shutdown_does_not_run_pending_async_disposers() {
    let finished = Arc::new(AtomicBool::new(false));
    {
        let runtime = runtime();
        let root = runtime.root();
        let effect = root.effect().unwrap();
        let flag = finished.clone();
        effect
            .on_dispose_async(move || {
                let flag = flag.clone();
                async move {
                    flag.store(true, Ordering::SeqCst);
                    Ok(())
                }
            })
            .unwrap();
        // 不调用 dispose/shutdown，仅 drop Runtime。
        drop(effect);
        drop(runtime);
    }
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert!(
        !finished.load(Ordering::SeqCst),
        "Drop must not start pending async disposers"
    );
}

#[tokio::test]
async fn async_disposer_factory_panic_is_collected_and_later_cleanup_runs() {
    fn panic_factory() -> std::future::Ready<Result<(), CoreError>> {
        panic!("factory boom");
    }

    let runtime = runtime();
    let effect = runtime.root().effect().unwrap();
    let completed = Arc::new(AtomicBool::new(false));
    let observed = completed.clone();
    effect
        .on_dispose_async(move || async move {
            observed.store(true, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    effect.on_dispose_async(panic_factory).unwrap();

    let error = effect.dispose_wait().await.expect_err("factory panic");
    assert!(completed.load(Ordering::SeqCst));
    match error {
        CoreError::DisposeFailed { errors } => assert!(
            errors
                .iter()
                .any(|item| item.contains("dispose callback") && item.contains("factory boom")),
            "missing factory panic: {errors:?}"
        ),
        other => panic!("unexpected: {other:?}"),
    }
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_coordinator_survives_first_waiter_abort() {
    let runtime = runtime();
    let effect = runtime.root().effect().unwrap();
    let (started_tx, started_rx) = oneshot::channel::<()>();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let started_tx = Arc::new(Mutex::new(Some(started_tx)));
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    effect
        .on_dispose_async(move || {
            let started_tx = started_tx.clone();
            let release_rx = release_rx.clone();
            async move {
                if let Some(tx) = started_tx.lock().expect("started").take() {
                    let _ = tx.send(());
                }
                let receiver = release_rx.lock().expect("release").take();
                if let Some(rx) = receiver {
                    let _ = rx.await;
                }
                Ok(())
            }
        })
        .unwrap();

    let first = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.shutdown().await })
    };
    started_rx.await.expect("shutdown disposal started");
    first.abort();
    assert!(
        first
            .await
            .expect_err("first caller must be aborted")
            .is_cancelled()
    );

    let second = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.shutdown().await })
    };
    let _ = release_tx.send(());
    tokio::time::timeout(std::time::Duration::from_secs(2), second)
        .await
        .expect("second shutdown waiter timed out")
        .expect("second shutdown join")
        .expect("shutdown result");
    assert!(runtime.scheduler_stopped());
}

#[tokio::test]
async fn controlled_task_shutdown_fails_fast_and_host_can_still_shutdown() {
    let runtime = runtime();
    let effect = runtime.root().effect().unwrap();
    let (result_tx, result_rx) = oneshot::channel();
    let task_runtime = runtime.clone();
    effect
        .spawn(move |_cancel| async move {
            let _ = result_tx.send(task_runtime.shutdown().await);
        })
        .unwrap();

    let result = tokio::time::timeout(std::time::Duration::from_secs(2), result_rx)
        .await
        .expect("controlled task shutdown must not hang")
        .expect("task result");
    assert!(matches!(result, Err(CoreError::ShutdownReentrant)));
    runtime.shutdown().await.expect("host shutdown");
}

#[tokio::test]
async fn async_disposer_shutdown_fails_fast_without_blocking_coordinator() {
    let runtime = runtime();
    let effect = runtime.root().effect().unwrap();
    let disposer_runtime = runtime.clone();
    effect
        .on_dispose_async(move || {
            let runtime = disposer_runtime.clone();
            async move { runtime.shutdown().await }
        })
        .unwrap();

    let error = tokio::time::timeout(std::time::Duration::from_secs(2), runtime.shutdown())
        .await
        .expect("shutdown coordinator must not self-wait")
        .expect_err("disposer shutdown rejection is aggregated");
    match error {
        CoreError::DisposeFailed { errors } => assert!(
            errors
                .iter()
                .any(|item| item.contains("shutdown 只能由宿主")),
            "missing shutdown rejection: {errors:?}"
        ),
        other => panic!("unexpected: {other:?}"),
    }
    assert!(runtime.scheduler_stopped());
}

// P1-4: 子 AwaitLocal（dispose_wait）进行中时，父 dispose_wait 必须等待子 async disposer。
#[tokio::test]
async fn p1_parent_dispose_wait_awaits_await_local_child_async_disposer() {
    let runtime = runtime();
    let root = runtime.root();
    let parent = root.effect().unwrap();
    let child = parent.extend().unwrap().effect().unwrap();
    let (entered_tx, entered_rx) = oneshot::channel::<()>();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    let async_done = Arc::new(AtomicBool::new(false));
    let flag = async_done.clone();

    child
        .on_dispose_async(move || {
            let entered_tx = entered_tx.clone();
            let release_rx = release_rx.clone();
            let flag = flag.clone();
            async move {
                if let Some(tx) = entered_tx.lock().expect("entered").take() {
                    let _ = tx.send(());
                }
                let receiver = release_rx.lock().expect("release").take();
                if let Some(rx) = receiver {
                    let _ = rx.await;
                }
                flag.store(true, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();

    let child_task = tokio::spawn(async move { child.dispose_wait().await });
    entered_rx.await.expect("child disposer entered");

    let parent_while_gated =
        tokio::time::timeout(std::time::Duration::from_millis(100), parent.dispose_wait()).await;
    assert!(
        parent_while_gated.is_err(),
        "parent dispose_wait must not finish while child async disposer is gated; got {parent_while_gated:?}"
    );
    assert!(
        !async_done.load(Ordering::SeqCst),
        "child disposer should still be gated"
    );

    let _ = release_tx.send(());
    tokio::time::timeout(std::time::Duration::from_secs(2), child_task)
        .await
        .expect("child dispose_wait timed out")
        .expect("join")
        .expect("child dispose_wait");
    // 父可能已在超时路径启动；再 wait 同一 completion。
    tokio::time::timeout(std::time::Duration::from_secs(2), parent.dispose_wait())
        .await
        .expect("parent dispose_wait timed out")
        .expect("parent dispose_wait");
    assert!(async_done.load(Ordering::SeqCst));
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn p1_parent_dispose_wait_aggregates_await_local_child_disposer_error() {
    let runtime = runtime();
    let root = runtime.root();
    let parent = root.effect().unwrap();
    let child = parent.extend().unwrap().effect().unwrap();
    let (entered_tx, entered_rx) = oneshot::channel::<()>();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));

    child
        .on_dispose_async(move || {
            let entered_tx = entered_tx.clone();
            let release_rx = release_rx.clone();
            async move {
                if let Some(tx) = entered_tx.lock().expect("entered").take() {
                    let _ = tx.send(());
                }
                let receiver = release_rx.lock().expect("release").take();
                if let Some(rx) = receiver {
                    let _ = rx.await;
                }
                Err(CoreError::PluginApply("await-local child boom".into()))
            }
        })
        .unwrap();

    let child_task = tokio::spawn(async move { child.dispose_wait().await });
    entered_rx.await.expect("child disposer entered");
    let parent_task = tokio::spawn(async move { parent.dispose_wait().await });
    let _ = release_tx.send(());

    let child_error = tokio::time::timeout(std::time::Duration::from_secs(2), child_task)
        .await
        .expect("child dispose_wait timed out")
        .expect("join")
        .expect_err("child disposer error");
    match child_error {
        CoreError::DisposeFailed { errors } => assert!(
            errors
                .iter()
                .any(|item| item.contains("await-local child boom")),
            "{errors:?}"
        ),
        other => panic!("unexpected child error: {other:?}"),
    }

    let parent_error = tokio::time::timeout(std::time::Duration::from_secs(2), parent_task)
        .await
        .expect("parent dispose_wait timed out")
        .expect("join")
        .expect_err("parent must aggregate child error");
    match parent_error {
        CoreError::DisposeFailed { errors } => {
            let hits = errors
                .iter()
                .filter(|item| item.contains("await-local child boom"))
                .count();
            assert_eq!(
                hits, 1,
                "child error must aggregate exactly once: {errors:?}"
            );
        }
        other => panic!("unexpected parent error: {other:?}"),
    }
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parent_dispose_races_child_error_aggregates_once() {
    let runtime = runtime();
    let root = runtime.root();
    let parent = root.effect().unwrap();
    let child = parent.extend().unwrap().effect().unwrap();
    let (entered_tx, entered_rx) = oneshot::channel::<()>();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));

    child
        .on_dispose_async(move || {
            let entered_tx = entered_tx.clone();
            let release_rx = release_rx.clone();
            async move {
                if let Some(tx) = entered_tx.lock().expect("entered").take() {
                    let _ = tx.send(());
                }
                let receiver = release_rx.lock().expect("release").take();
                if let Some(rx) = receiver {
                    let _ = rx.await;
                }
                Err(CoreError::PluginApply("race child boom".into()))
            }
        })
        .unwrap();

    // 子先进入 async disposer，再与父 dispose_wait 交错释放。
    let child_for_wait = child.clone();
    let child_task = tokio::spawn(async move { child_for_wait.dispose_wait().await });
    entered_rx.await.expect("child disposer entered");

    // 交错：父开始释放；子仍在 gate 内。释放后父必须等到子 Completion，错误恰好一次。
    let parent_task = tokio::spawn(async move { parent.dispose_wait().await });
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    let _ = release_tx.send(());

    let child_err = tokio::time::timeout(std::time::Duration::from_secs(2), child_task)
        .await
        .expect("child timed out")
        .expect("join")
        .expect_err("child error");
    match child_err {
        CoreError::DisposeFailed { errors } => {
            assert_eq!(
                errors
                    .iter()
                    .filter(|item| item.contains("race child boom"))
                    .count(),
                1,
                "{errors:?}"
            );
        }
        other => panic!("unexpected child: {other:?}"),
    }

    let parent_err = tokio::time::timeout(std::time::Duration::from_secs(2), parent_task)
        .await
        .expect("parent timed out")
        .expect("join")
        .expect_err("parent must see child error");
    match parent_err {
        CoreError::DisposeFailed { errors } => {
            assert_eq!(
                errors
                    .iter()
                    .filter(|item| item.contains("race child boom"))
                    .count(),
                1,
                "must not double-aggregate: {errors:?}"
            );
        }
        other => panic!("unexpected parent: {other:?}"),
    }
    // 子已自行完成；父不得因丢子而提前 Ok。
    runtime.shutdown().await.expect("shutdown");
}
