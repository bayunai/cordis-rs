use async_trait::async_trait;
use cordis_core::{
    Context, CoreError, EventKey, Plugin, PluginKey, ProviderAvailability, Runtime, SerialKey,
    ServiceKey,
};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

const COUNTER: ServiceKey<u64> = ServiceKey::new("bench.counter");
const OBSERVE: EventKey<u64> = EventKey::new("bench.observe");
const DECIDE: SerialKey<u64, u64> = SerialKey::new("bench.decide");
const BATCH_PLUGIN: PluginKey = PluginKey::new("bench.lifecycle.noop@1");
const CONSUMER_DEPENDENCY: ServiceKey<u64> = ServiceKey::new("bench.lifecycle.dependency");

struct NoopPlugin;

#[async_trait]
impl Plugin for NoopPlugin {
    fn key(&self) -> PluginKey {
        BATCH_PLUGIN
    }

    async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
        Ok(())
    }
}

fn with_runtime<T>(operation: impl FnOnce(&tokio::runtime::Runtime, Runtime) -> T) -> T {
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark Tokio runtime");
    let core = {
        let _entered = executor.enter();
        Runtime::new().expect("create Core runtime")
    };
    operation(&executor, core)
}

fn bench_service_get(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("service_get");

    with_runtime(|_executor, runtime| {
        let root = runtime.root();
        root.provide(COUNTER, 42).expect("provide root counter");
        group.bench_function("root", |bencher| {
            bencher.iter(|| black_box(root.get(COUNTER).expect("root service")))
        });

        let child = root.extend().expect("derive child context");
        group.bench_function("one_derived_view", |bencher| {
            bencher.iter(|| black_box(child.get(COUNTER).expect("inherited service")))
        });

        let deep = (0..10).fold(root.clone(), |context, _| {
            context.extend().expect("derive nested context")
        });
        group.bench_function("ten_derived_views", |bencher| {
            bencher.iter(|| black_box(deep.get(COUNTER).expect("deep inherited service")))
        });
    });

    group.finish();
}

fn bench_events(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("events");

    with_runtime(|executor, runtime| {
        let root = runtime.root();
        root.on(OBSERVE, |value| {
            black_box(*value);
            Ok(())
        })
        .expect("subscribe observe listener");
        group.bench_function("emit_one_listener", |bencher| {
            bencher.iter(|| root.emit(OBSERVE, black_box(&1_u64)).expect("emit"))
        });

        let short_circuit = root.extend().expect("derive short circuit context");
        short_circuit
            .on_serial(DECIDE, |_value| async { Ok(Some(1_u64)) })
            .expect("subscribe short circuit listener");
        group.bench_function("serial_first_listener_short_circuits", |bencher| {
            bencher.to_async(executor).iter(|| async {
                black_box(
                    short_circuit
                        .serial(DECIDE, &1_u64)
                        .await
                        .expect("serial")
                        .expect("decision"),
                )
            })
        });

        let full_chain = root.extend().expect("derive full chain context");
        for _ in 0..4 {
            full_chain
                .on_serial(DECIDE, |_value| async { Ok(None) })
                .expect("subscribe continuing listener");
        }
        full_chain
            .on_serial(DECIDE, |_value| async { Ok(Some(5_u64)) })
            .expect("subscribe final listener");
        group.bench_function("serial_five_listeners_last_short_circuits", |bencher| {
            bencher.to_async(executor).iter(|| async {
                black_box(
                    full_chain
                        .serial(DECIDE, &1_u64)
                        .await
                        .expect("serial")
                        .expect("decision"),
                )
            })
        });
    });

    group.finish();
}

fn bench_lifecycle(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("lifecycle");

    with_runtime(|executor, _runtime| {
        for &count in &[16_usize, 100, 1_000] {
            group.bench_with_input(
                BenchmarkId::new("mount_noop_plugins", count),
                &count,
                |bencher, &count| {
                    bencher
                        .to_async(executor)
                        .iter_custom(|iterations| async move {
                            let mut elapsed = Duration::ZERO;
                            for _ in 0..iterations {
                                let runtime = Runtime::new().expect("create Core runtime");
                                let root = runtime.root();
                                let plugin: Arc<dyn Plugin> = Arc::new(NoopPlugin);
                                let mut fibers = Vec::with_capacity(count);

                                let started = Instant::now();
                                for _ in 0..count {
                                    fibers.push(
                                        root.plugin(plugin.clone()).await.expect("mount plugin"),
                                    );
                                }
                                elapsed += started.elapsed();
                                black_box(&fibers);

                                runtime
                                    .unmount(BATCH_PLUGIN)
                                    .await
                                    .expect("unmount plugins");
                                runtime.shutdown().await.expect("shutdown runtime");
                            }
                            elapsed
                        })
                },
            );

            group.bench_with_input(
                BenchmarkId::new("unmount_plugins_with_same_key", count),
                &count,
                |bencher, &count| {
                    bencher
                        .to_async(executor)
                        .iter_custom(|iterations| async move {
                            let mut elapsed = Duration::ZERO;
                            for _ in 0..iterations {
                                let runtime = Runtime::new().expect("create Core runtime");
                                let root = runtime.root();
                                let plugin: Arc<dyn Plugin> = Arc::new(NoopPlugin);
                                let mut fibers = Vec::with_capacity(count);
                                for _ in 0..count {
                                    fibers.push(
                                        root.plugin(plugin.clone()).await.expect("mount plugin"),
                                    );
                                }

                                let started = Instant::now();
                                let removed = runtime
                                    .unmount(BATCH_PLUGIN)
                                    .await
                                    .expect("unmount plugins");
                                elapsed += started.elapsed();
                                assert_eq!(removed, count);
                                black_box(&fibers);

                                runtime.shutdown().await.expect("shutdown runtime");
                            }
                            elapsed
                        })
                },
            );
        }

        for &consumer_count in &[100_usize, 1_000] {
            group.bench_with_input(
                BenchmarkId::new(
                    "provider_ready_unavailable_ready_with_consumers",
                    consumer_count,
                ),
                &consumer_count,
                |bencher, &consumer_count| {
                    bencher
                        .to_async(executor)
                        .iter_custom(|iterations| async move {
                            let mut elapsed = Duration::ZERO;
                            for _ in 0..iterations {
                                let runtime = Runtime::new().expect("create Core runtime");
                                let root = runtime.root();
                                let ready = Arc::new(AtomicBool::new(true));
                                let check_ready = ready.clone();
                                let provider = root
                                    .provide_checked(CONSUMER_DEPENDENCY, 7_u64, move || {
                                        if check_ready.load(Ordering::Acquire) {
                                            ProviderAvailability::Ready
                                        } else {
                                            ProviderAvailability::Unavailable {
                                                reason: Arc::from(
                                                    "benchmark connector unavailable",
                                                ),
                                            }
                                        }
                                    })
                                    .expect("provide checked dependency");
                                let consumers = (0..consumer_count)
                                    .map(|_| {
                                        root.inject(
                                            [CONSUMER_DEPENDENCY.id()],
                                            |services, _effect| async move {
                                                black_box(services.get(CONSUMER_DEPENDENCY)?);
                                                Ok(())
                                            },
                                        )
                                        .expect("register consumer")
                                    })
                                    .collect::<Vec<_>>();
                                runtime.settle().await;

                                let started = Instant::now();
                                ready.store(false, Ordering::Release);
                                provider.refresh().expect("mark unavailable");
                                runtime.settle().await;
                                ready.store(true, Ordering::Release);
                                provider.refresh().expect("mark ready");
                                runtime.settle().await;
                                elapsed += started.elapsed();
                                black_box(&consumers);

                                runtime.shutdown().await.expect("shutdown runtime");
                            }
                            elapsed
                        })
                },
            );
        }
    });

    group.finish();
}

criterion_group!(
    core_hot_paths,
    bench_service_get,
    bench_events,
    bench_lifecycle
);
criterion_main!(core_hot_paths);
