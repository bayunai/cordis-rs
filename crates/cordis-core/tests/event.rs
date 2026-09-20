//! 验证事件公开合同：监听注册、并行/串行/瀑布分发、错误传播与 once/prepend。
//!
//! 事件不控制 Core 生命周期；夹具见 `common/helpers`。

#[path = "common/helpers.rs"]
mod common;
use common::*;

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
async fn waterfall_short_circuit_does_not_evaluate_downstream_metadata() {
    let runtime = runtime();
    let root = runtime.root();
    let key = WaterfallKey::<Ping>::new("test.waterfall.short-metadata@1");
    let delegate = Arc::new(AtomicBool::new(false));
    let delegate_flag = delegate.clone();
    root.on_waterfall(key, move |value, next| {
        let delegate_flag = delegate_flag.clone();
        async move {
            if delegate_flag.load(Ordering::SeqCst) {
                next.call(value).await
            } else {
                Ok(Ping(99))
            }
        }
    })
    .unwrap();
    root.on_waterfall_with_options(
        key,
        ListenOptions::new().filter(|_: &Ping| Err(CoreError::EventListener("blocked".into()))),
        |_value, _next| async move { Ok(Ping(0)) },
    )
    .unwrap();

    assert_eq!(root.waterfall(key, Ping(1)).await.unwrap(), Ping(99));
    delegate.store(true, Ordering::SeqCst);
    assert!(matches!(
        root.waterfall(key, Ping(1)).await,
        Err(CoreError::EventListener(message)) if message == "blocked"
    ));
}

#[tokio::test]
async fn waterfall_once_is_claimed_only_when_listener_runs() {
    let runtime = runtime();
    let root = runtime.root();
    let key = WaterfallKey::<Ping>::new("test.waterfall.once-at-invocation@1");
    let delegate = Arc::new(AtomicBool::new(false));
    let delegate_flag = delegate.clone();
    let hits = Arc::new(AtomicUsize::new(0));
    root.on_waterfall(key, move |value, next| {
        let delegate_flag = delegate_flag.clone();
        async move {
            if delegate_flag.load(Ordering::SeqCst) {
                next.call(value).await
            } else {
                Ok(value)
            }
        }
    })
    .unwrap();
    let count = hits.clone();
    root.on_waterfall_with_options(key, ListenOptions::new().once(), move |value, next| {
        let count = count.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            next.call(value).await
        }
    })
    .unwrap();

    root.waterfall(key, Ping(1)).await.unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    delegate.store(true, Ordering::SeqCst);
    root.waterfall(key, Ping(1)).await.unwrap();
    root.waterfall(key, Ping(1)).await.unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn waterfall_filter_receives_transformed_payload() {
    let runtime = runtime();
    let root = runtime.root();
    let key = WaterfallKey::<Ping>::new("test.waterfall.transformed-filter@1");
    let hits = Arc::new(AtomicUsize::new(0));
    root.on_waterfall(key, |value, next| async move {
        next.call(Ping(value.0 + 1)).await
    })
    .unwrap();
    let count = hits.clone();
    root.on_waterfall_with_options(
        key,
        ListenOptions::new().filter(|value: &Ping| Ok(value.0 == 2)),
        move |value, next| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                next.call(value).await
            }
        },
    )
    .unwrap();

    assert_eq!(root.waterfall(key, Ping(1)).await.unwrap(), Ping(2));
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn parallel_filter_error_prevents_all_handlers_and_skipped_once_remains_available() {
    let runtime = runtime();
    let root = runtime.root();
    let rejected = ParallelKey::<Ping>::new("test.parallel.filter-error@1");
    let ran = Arc::new(AtomicUsize::new(0));
    root.on_parallel_with_options(
        rejected,
        ListenOptions::new().filter(|_: &Ping| Err(CoreError::EventListener("blocked".into()))),
        |_| async move { Ok(()) },
    )
    .unwrap();
    let ran_flag = ran.clone();
    root.on_parallel(rejected, move |_| {
        let ran_flag = ran_flag.clone();
        async move {
            ran_flag.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    })
    .unwrap();
    assert!(matches!(
        root.parallel(rejected, &Ping(1)).await,
        Err(CoreError::EventListener(message)) if message == "blocked"
    ));
    assert_eq!(ran.load(Ordering::SeqCst), 0);

    let once_key = ParallelKey::<Ping>::new("test.parallel.skipped-once@1");
    let once_hits = Arc::new(AtomicUsize::new(0));
    let count = once_hits.clone();
    root.on_parallel_with_options(
        once_key,
        ListenOptions::new()
            .once()
            .filter(|value: &Ping| Ok(value.0 == 2)),
        move |_| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        },
    )
    .unwrap();
    root.parallel(once_key, &Ping(1)).await.unwrap();
    root.parallel(once_key, &Ping(2)).await.unwrap();
    root.parallel(once_key, &Ping(2)).await.unwrap();
    assert_eq!(once_hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn event_filter_skip_and_error() {
    let runtime = runtime();
    let root = runtime.root();
    let hits = Arc::new(AtomicUsize::new(0));
    let count = hits.clone();
    root.on_with_options(
        PING,
        ListenOptions::<Ping>::new().filter(|ping: &Ping| {
            if ping.0 == 99 {
                return Err(CoreError::EventListener("bad".into()));
            }
            Ok(ping.0 % 2 == 1)
        }),
        move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    )
    .unwrap();
    root.emit(PING, &Ping(1)).unwrap();
    root.emit(PING, &Ping(2)).unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    let err = root.emit(PING, &Ping(99)).unwrap_err();
    assert!(matches!(err, CoreError::EventListener(_)));
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_once_runs_at_most_once_under_concurrent_emit() {
    let runtime = runtime();
    let root = runtime.root();
    let hits = Arc::new(AtomicUsize::new(0));
    let count = hits.clone();
    root.on_with_options(PING, ListenOptions::new().once(), move |_| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .unwrap();
    let mut joins = Vec::new();
    for _ in 0..32 {
        let root = root.clone();
        joins.push(tokio::spawn(async move {
            root.emit(PING, &Ping(1)).unwrap();
        }));
    }
    for join in joins {
        join.await.unwrap();
    }
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    root.emit(PING, &Ping(2)).unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn event_prepend_orders_newest_first_then_normal() {
    let runtime = runtime();
    let root = runtime.root();
    let order = Arc::new(Mutex::new(Vec::new()));
    let push = |label: &'static str, order: Arc<Mutex<Vec<&'static str>>>| {
        move |_: &Ping| {
            order.lock().expect("order").push(label);
            Ok(())
        }
    };
    root.on(PING, push("normal-a", order.clone())).unwrap();
    root.on_with_options(
        PING,
        ListenOptions::new().prepend(),
        push("pre-1", order.clone()),
    )
    .unwrap();
    root.on(PING, push("normal-b", order.clone())).unwrap();
    root.on_with_options(
        PING,
        ListenOptions::new().prepend(),
        push("pre-2", order.clone()),
    )
    .unwrap();
    root.emit(PING, &Ping(1)).unwrap();
    assert_eq!(
        *order.lock().expect("order"),
        vec!["pre-2", "pre-1", "normal-a", "normal-b"]
    );
}

#[tokio::test]
async fn event_global_crosses_sibling_contexts_local_does_not() {
    let runtime = runtime();
    let root = runtime.root();
    let left = root.extend().unwrap();
    let right = root.extend().unwrap();
    let local_hits = Arc::new(AtomicUsize::new(0));
    let global_hits = Arc::new(AtomicUsize::new(0));
    let local = local_hits.clone();
    let global = global_hits.clone();
    left.on(PING, move |_| {
        local.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .unwrap();
    left.on_with_options(PING, ListenOptions::new().global(), move |_| {
        global.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .unwrap();
    right.emit(PING, &Ping(1)).unwrap();
    assert_eq!(local_hits.load(Ordering::SeqCst), 0);
    assert_eq!(global_hits.load(Ordering::SeqCst), 1);
    left.emit(PING, &Ping(2)).unwrap();
    assert_eq!(local_hits.load(Ordering::SeqCst), 1);
    assert_eq!(global_hits.load(Ordering::SeqCst), 2);
}
