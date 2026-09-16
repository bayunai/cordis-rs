//! 并发 Registry 操作的固定负载尾延迟基准。
//!
//! 这不是吞吐 benchmark：每一类操作都记录单次耗时，并报告 p50 / p95 / p99。
//! `refresh` 工作线程共同切换一个 Provider 的可用性；另一些线程在各自独立的
//! Context 视图中创建 Effect、注册临时 Service 并释放 Effect，避免 Service 槽位冲突。

use cordis_core::{ProviderAvailability, Runtime, ServiceKey};
use std::{
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

const SHARED_PROVIDER: ServiceKey<u64> = ServiceKey::new("bench.contention.shared-provider");
const TRANSIENT_PROVIDER: ServiceKey<u64> = ServiceKey::new("bench.contention.transient");
const REFRESH_THREADS: usize = 4;
const PROVIDE_DISPOSE_THREADS: usize = 4;
const OPERATIONS_PER_THREAD: usize = 1_000;
const ROUNDS: usize = 5;

fn percentile(samples: &mut [Duration], percentage: usize) -> Duration {
    assert!(!samples.is_empty());
    samples.sort_unstable();
    let index = (samples.len() - 1) * percentage / 100;
    samples[index]
}

fn print_distribution(name: &str, samples: &mut [Duration]) {
    println!(
        "{name}: samples={}, p50={:?}, p95={:?}, p99={:?}, max={:?}",
        samples.len(),
        percentile(samples, 50),
        percentile(samples, 95),
        percentile(samples, 99),
        samples.last().expect("non-empty sorted samples"),
    );
}

fn main() {
    let executor = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("benchmark Tokio runtime");
    let runtime = {
        let _entered = executor.enter();
        Runtime::new().expect("create Core runtime")
    };
    let root = runtime.root();
    let ready = Arc::new(AtomicBool::new(true));
    let check_ready = ready.clone();
    let provider = root
        .provide_checked(SHARED_PROVIDER, 1_u64, move || {
            if check_ready.load(Ordering::Acquire) {
                ProviderAvailability::Ready
            } else {
                ProviderAvailability::Unavailable {
                    reason: Arc::from("benchmark state toggle"),
                }
            }
        })
        .expect("provide checked service");

    let refresh_samples = Arc::new(Mutex::new(Vec::with_capacity(
        REFRESH_THREADS * OPERATIONS_PER_THREAD * ROUNDS,
    )));
    let provide_dispose_samples = Arc::new(Mutex::new(Vec::with_capacity(
        PROVIDE_DISPOSE_THREADS * OPERATIONS_PER_THREAD * ROUNDS,
    )));

    for _ in 0..ROUNDS {
        let barrier = Arc::new(Barrier::new(REFRESH_THREADS + PROVIDE_DISPOSE_THREADS));
        let worker_contexts = (0..PROVIDE_DISPOSE_THREADS)
            .map(|_| root.extend().expect("derive worker context"))
            .collect::<Vec<_>>();

        thread::scope(|scope| {
            for _ in 0..REFRESH_THREADS {
                let barrier = barrier.clone();
                let ready = ready.clone();
                let provider = provider.clone();
                let target = refresh_samples.clone();
                scope.spawn(move || {
                    let mut local = Vec::with_capacity(OPERATIONS_PER_THREAD);
                    barrier.wait();
                    for _ in 0..OPERATIONS_PER_THREAD {
                        let started = std::time::Instant::now();
                        ready.fetch_xor(true, Ordering::AcqRel);
                        provider.refresh().expect("refresh checked provider");
                        local.push(started.elapsed());
                    }
                    target.lock().expect("refresh samples").extend(local);
                });
            }

            for context in worker_contexts {
                let barrier = barrier.clone();
                let target = provide_dispose_samples.clone();
                scope.spawn(move || {
                    let mut local = Vec::with_capacity(OPERATIONS_PER_THREAD);
                    barrier.wait();
                    for index in 0..OPERATIONS_PER_THREAD {
                        let started = std::time::Instant::now();
                        let effect = context.effect().expect("create effect");
                        effect
                            .provide(TRANSIENT_PROVIDER, index as u64)
                            .expect("provide transient service");
                        effect.dispose();
                        local.push(started.elapsed());
                    }
                    target
                        .lock()
                        .expect("provide/dispose samples")
                        .extend(local);
                });
            }
        });
        executor.block_on(runtime.settle());
    }

    executor
        .block_on(runtime.shutdown())
        .expect("shutdown Core runtime");

    println!(
        "contention setup: refresh_threads={REFRESH_THREADS}, provide_dispose_threads={PROVIDE_DISPOSE_THREADS}, operations_per_thread={OPERATIONS_PER_THREAD}, rounds={ROUNDS}"
    );
    print_distribution(
        "refresh with availability toggles",
        &mut refresh_samples.lock().expect("refresh samples"),
    );
    print_distribution(
        "effect + provide + dispose",
        &mut provide_dispose_samples
            .lock()
            .expect("provide/dispose samples"),
    );
}
