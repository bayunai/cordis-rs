//! Test helpers for [`cordis_core`]. Not intended for production hosts.

use async_trait::async_trait;
use cordis_core::{
    Context, CoreError, EventKey, InjectionHandle, InjectionState, Plugin, ServiceKey,
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::time::timeout;

/// 轮询直到注入达到期望状态或超时。
pub async fn wait_injection(handle: &InjectionHandle, expected: InjectionState) {
    timeout(Duration::from_secs(2), async {
        loop {
            if handle.state() == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reactive state should converge");
}

/// 轮询直到谓词为真或超时。
pub async fn wait_until<F>(mut predicate: F)
where
    F: FnMut() -> bool,
{
    timeout(Duration::from_secs(2), async {
        loop {
            if predicate() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition should become true");
}

/// 记录事件载荷的简单收集器。
#[derive(Clone)]
pub struct EventRecorder<T: Clone + Send + Sync + 'static> {
    events: Arc<Mutex<Vec<T>>>,
}

impl<T: Clone + Send + Sync + 'static> Default for EventRecorder<T> {
    fn default() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl<T: Clone + Send + Sync + 'static> EventRecorder<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&self, value: T) {
        self.events.lock().expect("recorder").push(value);
    }

    pub fn snapshot(&self) -> Vec<T> {
        self.events.lock().expect("recorder").clone()
    }

    pub fn len(&self) -> usize {
        self.events.lock().expect("recorder").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn subscribe(
        &self,
        ctx: &Context,
        key: EventKey<T>,
    ) -> Result<cordis_core::Unsubscribe, CoreError> {
        let recorder = self.clone();
        ctx.on(key, move |value| {
            recorder.push(value.clone());
            Ok(())
        })
    }
}

type SetupFn = Box<dyn Fn(&Context) -> Result<(), CoreError> + Send + Sync>;

/// 测试用 Plugin 构建器。
pub struct TestPlugin {
    setups: Vec<SetupFn>,
    dispose_count: Arc<AtomicUsize>,
}

impl Default for TestPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl TestPlugin {
    pub fn new() -> Self {
        Self {
            setups: Vec::new(),
            dispose_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn setup<F>(mut self, f: F) -> Self
    where
        F: Fn(&Context) -> Result<(), CoreError> + Send + Sync + 'static,
    {
        self.setups.push(Box::new(f));
        self
    }

    pub fn provide_clone<T>(self, key: ServiceKey<T>, value: T) -> Self
    where
        T: Clone + Send + Sync + 'static,
    {
        self.setup(move |ctx| {
            ctx.provide(key, value.clone())?;
            Ok(())
        })
    }

    pub fn on_dispose_counter(&self) -> Arc<AtomicUsize> {
        self.dispose_count.clone()
    }

    pub fn into_arc(self) -> Arc<dyn Plugin> {
        Arc::new(self)
    }
}

#[async_trait]
impl Plugin for TestPlugin {
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let counter = self.dispose_count.clone();
        let effect = ctx.effect()?;
        effect.on_dispose(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        });
        for setup in &self.setups {
            setup(ctx)?;
        }
        Ok(())
    }
}

/// 断言 Context 上看不到给定 Service。
pub fn assert_service_unavailable<T: Send + Sync + 'static>(ctx: &Context, key: ServiceKey<T>) {
    assert!(matches!(
        ctx.get(key),
        Err(CoreError::ServiceUnavailable { .. })
    ));
}
