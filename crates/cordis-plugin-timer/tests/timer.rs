//! Timer 生命周期与虚拟时间回归。

use async_trait::async_trait;
use cordis_core::{
    Context, CoreError, EffectContext, FiberState, Plugin, PluginKey, Runtime, ServiceId,
};
use cordis_plugin_timer::{TIMER, TimerError, TimerExt, TimerPlugin};
use cordis_testkit::wait_until;
use futures_util::StreamExt;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

fn runtime() -> Runtime {
    Runtime::new().expect("tokio runtime required")
}

async fn mount_timer(root: &Context) -> cordis_core::Fiber {
    root.plugin(Arc::new(TimerPlugin))
        .await
        .expect("mount timer")
}

/// 在 `start_paused` 下让受管任务有机会被轮询。
async fn flush() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn timer_plugin_provides_and_unmount_removes_service() {
    let runtime = runtime();
    let root = runtime.root();
    assert!(matches!(
        root.get(TIMER),
        Err(CoreError::ServiceUnavailable { .. })
    ));

    let mut fiber = mount_timer(&root).await;
    assert!(root.get(TIMER).is_ok());

    fiber.dispose_wait().await.expect("dispose timer fiber");
    wait_until(|| fiber.state() == FiberState::Disposed).await;
    assert!(matches!(
        root.get(TIMER),
        Err(CoreError::ServiceUnavailable { .. })
    ));
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn timer_ext_requires_mounted_plugin() {
    let runtime = runtime();
    let root = runtime.root();
    let effect = root.effect().expect("effect");

    assert!(matches!(
        effect.timeout(|| {}, Duration::from_millis(10)),
        Err(TimerError::Core(CoreError::ServiceUnavailable { .. }))
    ));
    assert!(matches!(
        effect.sleep(Duration::from_millis(10)),
        Err(TimerError::Core(CoreError::ServiceUnavailable { .. }))
    ));
    assert!(matches!(
        effect.interval(|| {}, Duration::from_millis(10)),
        Err(TimerError::Core(CoreError::ServiceUnavailable { .. }))
    ));
    assert!(matches!(
        effect.ticks(Duration::from_millis(10)),
        Err(TimerError::Core(CoreError::ServiceUnavailable { .. }))
    ));
    assert!(matches!(
        effect.throttle(|_: ()| {}, Duration::from_millis(10), false),
        Err(TimerError::Core(CoreError::ServiceUnavailable { .. }))
    ));
    assert!(matches!(
        effect.debounce(|_: ()| {}, Duration::from_millis(10)),
        Err(TimerError::Core(CoreError::ServiceUnavailable { .. }))
    ));
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn timeout_fires_once_and_cancel_skips() {
    let runtime = runtime();
    let root = runtime.root();
    let _timer = mount_timer(&root).await;
    let effect = root.effect().expect("effect");

    let fired = Arc::new(AtomicUsize::new(0));
    let count = fired.clone();
    let _handle = effect
        .timeout(
            move || {
                count.fetch_add(1, Ordering::SeqCst);
            },
            Duration::from_millis(50),
        )
        .expect("timeout");

    tokio::time::sleep(Duration::from_millis(49)).await;
    flush().await;
    assert_eq!(fired.load(Ordering::SeqCst), 0);

    tokio::time::sleep(Duration::from_millis(1)).await;
    flush().await;
    assert_eq!(fired.load(Ordering::SeqCst), 1);

    let skipped = Arc::new(AtomicUsize::new(0));
    let count = skipped.clone();
    let handle = effect
        .timeout(
            move || {
                count.fetch_add(1, Ordering::SeqCst);
            },
            Duration::from_millis(50),
        )
        .expect("timeout");
    handle.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
    flush().await;
    assert_eq!(skipped.load(Ordering::SeqCst), 0);

    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn timeout_effect_dispose_and_shutdown_skip() {
    let runtime = runtime();
    let root = runtime.root();
    let _timer = mount_timer(&root).await;

    let fired = Arc::new(AtomicUsize::new(0));
    let effect = root.effect().expect("effect");
    let count = fired.clone();
    let _handle = effect
        .timeout(
            move || {
                count.fetch_add(1, Ordering::SeqCst);
            },
            Duration::from_millis(50),
        )
        .expect("timeout");
    effect.dispose();
    tokio::time::sleep(Duration::from_millis(100)).await;
    flush().await;
    assert_eq!(fired.load(Ordering::SeqCst), 0);

    let fired2 = Arc::new(AtomicUsize::new(0));
    let effect2 = root.effect().expect("effect");
    let count = fired2.clone();
    let _handle = effect2
        .timeout(
            move || {
                count.fetch_add(1, Ordering::SeqCst);
            },
            Duration::from_millis(50),
        )
        .expect("timeout");
    runtime.shutdown().await.expect("shutdown");
    tokio::time::sleep(Duration::from_millis(100)).await;
    flush().await;
    assert_eq!(fired2.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn sleep_ok_dispose_and_drop_cancel() {
    let runtime = runtime();
    let root = runtime.root();
    let _timer = mount_timer(&root).await;
    let effect = root.effect().expect("effect");

    let sleep = effect.sleep(Duration::from_millis(40)).expect("sleep");
    let join = tokio::spawn(sleep);
    tokio::time::sleep(Duration::from_millis(40)).await;
    flush().await;
    assert!(join.await.expect("join").is_ok());

    let effect2 = root.effect().expect("effect");
    let sleep = effect2.sleep(Duration::from_millis(40)).expect("sleep");
    let join = tokio::spawn(sleep);
    effect2.dispose();
    flush().await;
    assert!(matches!(
        join.await.expect("join"),
        Err(TimerError::Disposed)
    ));

    let effect3 = root.effect().expect("effect");
    let sleep = effect3.sleep(Duration::from_millis(40)).expect("sleep");
    drop(sleep);
    tokio::time::sleep(Duration::from_millis(100)).await;
    flush().await;

    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn interval_first_tick_after_delay_and_cancel() {
    let runtime = runtime();
    let root = runtime.root();
    let _timer = mount_timer(&root).await;
    let effect = root.effect().expect("effect");

    let ticks = Arc::new(AtomicUsize::new(0));
    let count = ticks.clone();
    let handle = effect
        .interval(
            move || {
                count.fetch_add(1, Ordering::SeqCst);
            },
            Duration::from_millis(30),
        )
        .expect("interval");

    tokio::time::sleep(Duration::from_millis(29)).await;
    flush().await;
    assert_eq!(ticks.load(Ordering::SeqCst), 0);

    tokio::time::sleep(Duration::from_millis(1)).await;
    flush().await;
    assert_eq!(ticks.load(Ordering::SeqCst), 1);

    tokio::time::sleep(Duration::from_millis(30)).await;
    flush().await;
    assert_eq!(ticks.load(Ordering::SeqCst), 2);

    handle.cancel();
    tokio::time::sleep(Duration::from_millis(90)).await;
    flush().await;
    assert_eq!(ticks.load(Ordering::SeqCst), 2);

    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn ticks_stream_coalesce_and_disposed() {
    let runtime = runtime();
    let root = runtime.root();
    let _timer = mount_timer(&root).await;
    let effect = root.effect().expect("effect");

    let mut stream = effect.ticks(Duration::from_millis(20)).expect("ticks");

    tokio::time::sleep(Duration::from_millis(20)).await;
    flush().await;
    assert!(matches!(stream.next().await, Some(Ok(()))));

    // 消费者落后：多个周期合并为一次。
    tokio::time::sleep(Duration::from_millis(60)).await;
    flush().await;
    assert!(matches!(stream.next().await, Some(Ok(()))));

    let cancel_effect = root.effect().expect("effect");
    let mut stream2 = cancel_effect
        .ticks(Duration::from_millis(20))
        .expect("ticks");
    cancel_effect.dispose();
    flush().await;
    assert!(matches!(
        stream2.next().await,
        Some(Err(TimerError::Disposed))
    ));
    assert!(stream2.next().await.is_none());

    let effect3 = root.effect().expect("effect");
    let stream3 = effect3.ticks(Duration::from_millis(20)).expect("ticks");
    drop(stream3);
    tokio::time::sleep(Duration::from_millis(100)).await;
    flush().await;

    // 丢掉未消费的 stream，避免 Drop 取消与后续断言互相干扰。
    drop(stream);
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn throttle_leading_trailing_and_no_trailing() {
    let runtime = runtime();
    let root = runtime.root();
    let _timer = mount_timer(&root).await;
    let effect = root.effect().expect("effect");

    let log = Arc::new(Mutex::new(Vec::new()));
    let sink = log.clone();
    let throttled = effect
        .throttle(
            move |v: u32| {
                sink.lock().expect("log").push(v);
            },
            Duration::from_millis(50),
            false,
        )
        .expect("throttle");

    throttled.call(1).expect("call");
    assert_eq!(*log.lock().expect("log"), vec![1]);
    throttled.call(2).expect("call");
    throttled.call(3).expect("call");
    assert_eq!(*log.lock().expect("log"), vec![1]);

    tokio::time::sleep(Duration::from_millis(50)).await;
    flush().await;
    assert_eq!(*log.lock().expect("log"), vec![1, 3]);

    let log2 = Arc::new(Mutex::new(Vec::new()));
    let sink = log2.clone();
    let throttled2 = effect
        .throttle(
            move |v: u32| {
                sink.lock().expect("log").push(v);
            },
            Duration::from_millis(50),
            true,
        )
        .expect("throttle");
    throttled2.call(10).expect("call");
    throttled2.call(11).expect("call");
    tokio::time::sleep(Duration::from_millis(50)).await;
    flush().await;
    assert_eq!(*log2.lock().expect("log"), vec![10]);

    throttled2.dispose();
    assert!(matches!(throttled2.call(12), Err(TimerError::Disposed)));

    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn debounce_resets_and_last_wins() {
    let runtime = runtime();
    let root = runtime.root();
    let _timer = mount_timer(&root).await;
    let effect = root.effect().expect("effect");

    let log = Arc::new(Mutex::new(Vec::new()));
    let sink = log.clone();
    let debounced = effect
        .debounce(
            move |v: u32| {
                sink.lock().expect("log").push(v);
            },
            Duration::from_millis(40),
        )
        .expect("debounce");

    debounced.call(1).expect("call");
    tokio::time::sleep(Duration::from_millis(20)).await;
    flush().await;
    debounced.call(2).expect("call");
    tokio::time::sleep(Duration::from_millis(20)).await;
    flush().await;
    debounced.call(3).expect("call");
    assert!(log.lock().expect("log").is_empty());

    tokio::time::sleep(Duration::from_millis(40)).await;
    flush().await;
    assert_eq!(*log.lock().expect("log"), vec![3]);

    debounced.dispose();
    assert!(matches!(debounced.call(4), Err(TimerError::Disposed)));
    tokio::time::sleep(Duration::from_millis(40)).await;
    flush().await;
    assert_eq!(*log.lock().expect("log"), vec![3]);

    runtime.shutdown().await.expect("shutdown");
}

struct TimerConsumer {
    ticks: Arc<AtomicUsize>,
}

#[async_trait]
impl Plugin for TimerConsumer {
    fn key(&self) -> PluginKey {
        PluginKey::new("test.timer.consumer")
    }

    fn inject(&self) -> Vec<ServiceId> {
        vec![TIMER.id()]
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let effect: EffectContext = ctx.effect_named("consumer-timers")?;
        let ticks = self.ticks.clone();
        let handle = effect
            .interval(
                move || {
                    ticks.fetch_add(1, Ordering::SeqCst);
                },
                Duration::from_millis(25),
            )
            .map_err(|err| match err {
                TimerError::Core(core) => core,
                other => CoreError::PluginApply(other.to_string()),
            })?;
        // Handle Drop 会取消计时器；挂到 Effect 上以跟随 Fiber 生命周期。
        effect.on_dispose(move || {
            drop(handle);
        });
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn consumer_pending_when_timer_fiber_disposed() {
    let runtime = runtime();
    let root = runtime.root();
    let mut timer_fiber = mount_timer(&root).await;
    assert_eq!(timer_fiber.state(), FiberState::Active);

    let ticks = Arc::new(AtomicUsize::new(0));
    let consumer = root
        .plugin(Arc::new(TimerConsumer {
            ticks: ticks.clone(),
        }))
        .await
        .expect("mount consumer");
    assert_eq!(
        consumer.state(),
        FiberState::Active,
        "last_error={:?}",
        consumer.last_error()
    );

    flush().await;
    tokio::time::advance(Duration::from_millis(25)).await;
    flush().await;
    assert!(
        ticks.load(Ordering::SeqCst) >= 1,
        "interval should fire inside consumer fiber, got {}",
        ticks.load(Ordering::SeqCst)
    );
    let before = ticks.load(Ordering::SeqCst);

    timer_fiber.dispose_wait().await.expect("dispose timer");
    runtime.settle().await;
    for _ in 0..64 {
        if consumer.state() == FiberState::Pending {
            break;
        }
        tokio::task::yield_now().await;
        runtime.settle().await;
    }
    assert_eq!(consumer.state(), FiberState::Pending);

    tokio::time::advance(Duration::from_millis(100)).await;
    flush().await;
    assert_eq!(ticks.load(Ordering::SeqCst), before);

    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn debounce_stale_timer_cannot_steal_newer_args() {
    let runtime = runtime();
    let root = runtime.root();
    let _timer = mount_timer(&root).await;
    let effect = root.effect().expect("effect");

    let log = Arc::new(Mutex::new(Vec::new()));
    let sink = log.clone();
    let debounced = effect
        .debounce(
            move |v: u32| {
                sink.lock().expect("log").push(v);
            },
            Duration::from_millis(50),
        )
        .expect("debounce");

    // 旧 timer 接近到期时再次 call：旧任务即使进入 sleep 分支也不能抢走新 args。
    debounced.call(1).expect("call");
    flush().await;
    tokio::time::advance(Duration::from_millis(49)).await;
    flush().await;
    debounced.call(2).expect("call");
    flush().await;
    assert!(log.lock().expect("log").is_empty());

    // 推进到旧 delay 本会到期的时刻：仍不应执行。
    tokio::time::advance(Duration::from_millis(1)).await;
    flush().await;
    assert!(log.lock().expect("log").is_empty());

    // 仅在新一轮完整 delay 后执行最新参数。
    tokio::time::advance(Duration::from_millis(49)).await;
    flush().await;
    assert_eq!(*log.lock().expect("log"), vec![2]);

    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn ticks_full_channel_dispose_wait_bounded() {
    let runtime = runtime();
    let root = runtime.root();
    let _timer = mount_timer(&root).await;
    let effect = root.effect().expect("effect");

    let _stream = effect.ticks(Duration::from_millis(5)).expect("ticks");
    // 不消费 stream，让容量 1 的 channel 填满并持续合并。
    tokio::time::sleep(Duration::from_millis(30)).await;

    tokio::time::timeout(Duration::from_millis(500), effect.dispose_wait())
        .await
        .expect("dispose_wait must finish within bound")
        .expect("dispose_wait");

    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn sleep_returns_disposed_on_runtime_shutdown() {
    let runtime = runtime();
    let root = runtime.root();
    let _timer = mount_timer(&root).await;
    let effect = root.effect().expect("effect");

    let sleep = effect.sleep(Duration::from_secs(10)).expect("sleep");
    let join = tokio::spawn(sleep);
    flush().await;
    runtime.shutdown().await.expect("shutdown");
    flush().await;
    assert!(matches!(
        join.await.expect("join"),
        Err(TimerError::Disposed)
    ));
}

#[tokio::test(start_paused = true)]
async fn ticks_returns_disposed_once_on_runtime_shutdown() {
    let runtime = runtime();
    let root = runtime.root();
    let _timer = mount_timer(&root).await;
    let effect = root.effect().expect("effect");

    let mut stream = effect.ticks(Duration::from_secs(10)).expect("ticks");
    flush().await;
    runtime.shutdown().await.expect("shutdown");
    flush().await;
    assert!(matches!(
        stream.next().await,
        Some(Err(TimerError::Disposed))
    ));
    assert!(stream.next().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn interval_stops_after_effect_dispose() {
    let runtime = runtime();
    let root = runtime.root();
    let _timer = mount_timer(&root).await;
    let effect = root.effect().expect("effect");

    let ticks = Arc::new(AtomicUsize::new(0));
    let count = ticks.clone();
    let _handle = effect
        .interval(
            move || {
                count.fetch_add(1, Ordering::SeqCst);
            },
            Duration::from_millis(20),
        )
        .expect("interval");

    flush().await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    flush().await;
    assert_eq!(ticks.load(Ordering::SeqCst), 1);

    effect.dispose();
    flush().await;
    tokio::time::sleep(Duration::from_millis(60)).await;
    flush().await;
    assert_eq!(ticks.load(Ordering::SeqCst), 1);

    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn handle_drop_cancels_timeout_and_interval() {
    let runtime = runtime();
    let root = runtime.root();
    let _timer = mount_timer(&root).await;
    let effect = root.effect().expect("effect");

    let timeout_fired = Arc::new(AtomicUsize::new(0));
    let count = timeout_fired.clone();
    let handle = effect
        .timeout(
            move || {
                count.fetch_add(1, Ordering::SeqCst);
            },
            Duration::from_millis(40),
        )
        .expect("timeout");
    drop(handle);

    let interval_fired = Arc::new(AtomicUsize::new(0));
    let count = interval_fired.clone();
    let handle = effect
        .interval(
            move || {
                count.fetch_add(1, Ordering::SeqCst);
            },
            Duration::from_millis(20),
        )
        .expect("interval");
    drop(handle);

    flush().await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    flush().await;
    assert_eq!(timeout_fired.load(Ordering::SeqCst), 0);
    assert_eq!(interval_fired.load(Ordering::SeqCst), 0);

    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn throttle_trailing_then_immediate_call_waits_next_window() {
    let runtime = runtime();
    let root = runtime.root();
    let _timer = mount_timer(&root).await;
    let effect = root.effect().expect("effect");

    let log = Arc::new(Mutex::new(Vec::new()));
    let sink = log.clone();
    let throttled = effect
        .throttle(
            move |v: u32| {
                sink.lock().expect("log").push(v);
            },
            Duration::from_millis(50),
            false,
        )
        .expect("throttle");

    // t=0: leading 1；call(2) 仅作尾随。
    throttled.call(1).expect("call");
    throttled.call(2).expect("call");
    assert_eq!(*log.lock().expect("log"), vec![1]);

    // t=50: 执行尾随 2，并开启下一窗口。
    tokio::time::sleep(Duration::from_millis(50)).await;
    flush().await;
    assert_eq!(*log.lock().expect("log"), vec![1, 2]);

    // t=50: call(3) 落在新窗口内，不得 leading，只作尾随。
    throttled.call(3).expect("call");
    assert_eq!(*log.lock().expect("log"), vec![1, 2]);

    tokio::time::sleep(Duration::from_millis(49)).await;
    flush().await;
    assert_eq!(*log.lock().expect("log"), vec![1, 2]);

    // t=100: 执行尾随 3。
    tokio::time::sleep(Duration::from_millis(1)).await;
    flush().await;
    assert_eq!(*log.lock().expect("log"), vec![1, 2, 3]);

    runtime.shutdown().await.expect("shutdown");
}
