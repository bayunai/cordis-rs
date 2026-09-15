//! Context：从 Runtime 派生的层级运行时视图，不拥有底层资源。
//!
//! 公开概念：[`Context`]、[`EffectContext`]、[`InjectionHandle`] / [`InjectionState`]。
//! 子模块：`effect` 提供可处置资源注册入口；`injection` 提供注入句柄与状态 DTO。

mod effect;
mod injection;

pub use effect::EffectContext;
pub use injection::{InjectionHandle, InjectionState};

use crate::{
    ConfigKey, CoreError, ServiceId, ServiceKey, Services,
    effect::EffectScope,
    event::{
        EventKey, ListenMeta, ListenOptions, Next, ParallelKey, SerialKey, Unsubscribe,
        WaterfallKey,
    },
    fiber::{Fiber, FiberInner, FiberState},
    isolation::IsolationLabel,
    plugin::{Plugin, read_metadata},
    registry::{NodeId, Registry},
    service::ErasedService,
};
use std::{
    any::Any,
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex},
};

pub(crate) struct ContextInner {
    pub(crate) id: NodeId,
    pub(crate) registry: Arc<Registry>,
    pub(crate) scope: EffectScope,
}

/// 通用的层级 Service Context。
#[derive(Clone)]
pub struct Context {
    pub(crate) inner: Arc<ContextInner>,
}

impl Context {
    /// 创建继承当前 Context 的派生视图。
    ///
    /// 视图不拥有独立 Scope；Service 与订阅的生命周期仍归属当前 Effect/Fiber。
    pub fn extend(&self) -> Result<Self, CoreError> {
        self.extend_with(HashMap::new(), HashMap::new())
    }

    fn extend_with(
        &self,
        isolations: HashMap<ServiceId, IsolationLabel>,
        configs: HashMap<crate::config::ConfigId, crate::config::ErasedConfig>,
    ) -> Result<Self, CoreError> {
        self.ensure_alive()?;
        let id = self.inner.registry.allocate_id();
        self.inner.registry.add_node(
            id,
            Some(self.inner.id),
            isolations
                .into_iter()
                .map(|(key, label)| (key, label.id()))
                .collect(),
            configs,
        )?;
        self.inner
            .registry
            .bind_node_lifecycle(id, &self.inner.scope);
        Ok(Self {
            inner: Arc::new(ContextInner {
                id,
                registry: self.inner.registry.clone(),
                scope: self.inner.scope.clone(),
            }),
        })
    }

    /// 创建仅对派生 Context 生效的新隔离标签。
    pub fn isolate<T: Send + Sync + 'static>(
        &self,
        key: ServiceKey<T>,
    ) -> Result<(Self, IsolationLabel), CoreError> {
        self.ensure_alive()?;
        let label = self.inner.registry.allocate_isolation_label();
        let view = self.extend_with(HashMap::from([(key.id(), label.clone())]), HashMap::new())?;
        Ok((view, label))
    }

    /// 创建加入既有隔离标签的派生 Context；跨 Runtime 标签报错。
    pub fn isolate_with<T: Send + Sync + 'static>(
        &self,
        key: ServiceKey<T>,
        label: IsolationLabel,
    ) -> Result<Self, CoreError> {
        self.ensure_alive()?;
        label.ensure_runtime(self.inner.registry.runtime_token())?;
        self.extend_with(HashMap::from([(key.id(), label)]), HashMap::new())
    }

    /// 创建带不可变配置覆盖的派生 Context；父与兄弟节点不变。
    pub fn intercept<T: Send + Sync + 'static>(
        &self,
        key: ConfigKey<T>,
        value: T,
    ) -> Result<Self, CoreError> {
        self.ensure_alive()?;
        self.extend_with(
            HashMap::new(),
            HashMap::from([(
                key.id(),
                crate::config::ErasedConfig {
                    type_id: key.type_id(),
                    value: Arc::new(value),
                },
            )]),
        )
    }

    /// 自当前节点向父解析最近配置覆盖。
    pub fn config<T: Send + Sync + 'static>(&self, key: ConfigKey<T>) -> Result<Arc<T>, CoreError> {
        self.ensure_alive()?;
        let erased = self
            .inner
            .registry
            .resolve_config(self.inner.id, key.id(), key.type_id())?;
        Arc::downcast::<T>(erased).map_err(|_| CoreError::ConfigTypeMismatch { config: key.id() })
    }

    /// 在当前 Context 注册类型化 Service；当前 Scope 释放时自动撤销。
    pub fn provide<T: Send + Sync + 'static>(
        &self,
        key: ServiceKey<T>,
        service: T,
    ) -> Result<(), CoreError> {
        self.ensure_alive()?;
        self.inner.registry.provide(
            self.inner.id,
            key.id(),
            ErasedService {
                type_id: key.type_id(),
                value: Arc::new(service),
            },
            self.inner.scope.clone(),
        )
    }

    /// 立即取得当前 Context 或其祖先可见的 Service。
    pub fn get<T: Send + Sync + 'static>(&self, key: ServiceKey<T>) -> Result<Arc<T>, CoreError> {
        self.ensure_alive()?;
        let Some(service) = self.inner.registry.resolve(self.inner.id, key.id()) else {
            return Err(CoreError::ServiceUnavailable { service: key.id() });
        };
        if service.type_id != key.type_id() {
            return Err(CoreError::ServiceTypeMismatch { service: key.id() });
        }
        Arc::downcast::<T>(service.value)
            .map_err(|_| CoreError::ServiceTypeMismatch { service: key.id() })
    }

    /// 在所有依赖均可见时运行回调，并在依赖变化时自动重建该回调的子 Effect。
    pub fn inject<I, F, Fut>(
        &self,
        dependencies: I,
        callback: F,
    ) -> Result<InjectionHandle, CoreError>
    where
        I: IntoIterator<Item = ServiceId>,
        F: Fn(Services, EffectContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), CoreError>> + Send + 'static,
    {
        self.ensure_alive()?;
        let dependencies = dependencies.into_iter().collect::<Vec<_>>();
        let parent = self.inner.scope.clone();
        let template = self.clone();
        let callback = Arc::new(move |services: Services, child: EffectScope| {
            template.inner.registry.register_effect(
                child.id(),
                child.name().to_string(),
                child.parent_id(),
                Some(template.inner.id),
                None,
                &child,
            );
            let context = EffectContext {
                context: Context {
                    inner: Arc::new(ContextInner {
                        id: template.inner.id,
                        registry: template.inner.registry.clone(),
                        scope: child,
                    }),
                },
            };
            Box::pin(callback(services, context))
                as std::pin::Pin<Box<dyn Future<Output = Result<(), CoreError>> + Send>>
        });
        let id = self.inner.registry.register_injection(
            self.inner.id,
            parent,
            dependencies,
            callback,
        )?;
        Ok(InjectionHandle {
            id,
            registry: Arc::downgrade(&self.inner.registry),
        })
    }

    /// 创建归属当前 Effect/Fiber Scope 的普通 Effect（默认名 `"effect"`）。
    pub fn effect(&self) -> Result<EffectContext, CoreError> {
        self.effect_named("effect")
    }

    /// 创建具名 Effect。
    pub fn effect_named(&self, name: &'static str) -> Result<EffectContext, CoreError> {
        self.ensure_alive()?;
        let scope = self.inner.scope.child_named(name);
        self.inner.registry.register_effect(
            scope.id(),
            scope.name().to_string(),
            scope.parent_id(),
            Some(self.inner.id),
            None,
            &scope,
        );
        Ok(EffectContext {
            context: Context {
                inner: Arc::new(ContextInner {
                    id: self.inner.id,
                    registry: self.inner.registry.clone(),
                    scope,
                }),
            },
        })
    }

    /// 挂载插件：返回可重启 / 可替换的 [`Fiber`]。
    pub async fn plugin(&self, plugin: Arc<dyn Plugin>) -> Result<Fiber, CoreError> {
        self.ensure_alive()?;
        let metadata = read_metadata(plugin.as_ref())?;
        let plugin_key = metadata.key;
        let id = self.inner.registry.allocate_id();
        let dependencies = metadata.dependencies;
        let inner = Arc::new(FiberInner {
            id,
            plugin_key,
            node: self.inner.id,
            registry: Arc::downgrade(&self.inner.registry),
            parent_scope: self.inner.scope.clone(),
            plugin: Mutex::new(plugin),
            dependencies: Mutex::new(dependencies),
            effect: Mutex::new(None),
            pending_wait: Mutex::new(None),
            state: Mutex::new(FiberState::Pending),
            last_error: Mutex::new(None),
            resolved_providers: Mutex::new(Vec::new()),
            disposed: std::sync::atomic::AtomicBool::new(false),
            busy: Mutex::new(false),
            mount_ctx: Mutex::new(Some(self.clone())),
        });
        self.inner.registry.register_plugin_fiber(inner.clone())?;
        inner.publish_initial_state();
        let weak = Arc::downgrade(&inner);
        self.inner.scope.on_dispose(move || {
            if let Some(fiber) = weak.upgrade() {
                fiber.dispose_now();
            }
        });
        let _ = inner.try_activate().await;
        Ok(Fiber { inner })
    }

    /// 订阅类型化事件；订阅归属当前 EffectScope。
    pub fn on<T, F>(&self, key: EventKey<T>, handler: F) -> Result<Unsubscribe, CoreError>
    where
        T: Send + Sync + 'static,
        F: Fn(&T) -> Result<(), CoreError> + Send + Sync + 'static,
    {
        self.on_with_options(key, ListenOptions::new(), handler)
    }

    /// 带 `once` / `prepend` / `global` / `filter` 选项的 Observe 订阅。
    pub fn on_with_options<T, F>(
        &self,
        key: EventKey<T>,
        options: ListenOptions<T>,
        handler: F,
    ) -> Result<Unsubscribe, CoreError>
    where
        T: Send + Sync + 'static,
        F: Fn(&T) -> Result<(), CoreError> + Send + Sync + 'static,
    {
        self.ensure_alive()?;
        let handler = Arc::new(handler);
        let type_id = key.type_id();
        let event_id = key.id();
        let meta = listen_meta_from_options(options, event_id);
        let wrapped = Arc::new(move |payload: &(dyn Any + Send + Sync)| {
            let Some(value) = payload.downcast_ref::<T>() else {
                return Err(CoreError::EventTypeMismatch { event: event_id });
            };
            handler(value)
        });
        let listener_id = self.inner.registry.subscribe_observe(
            self.inner.id,
            event_id,
            type_id,
            wrapped,
            &self.inner.scope,
            meta,
        )?;
        Ok(self.unsubscribe_handle(event_id, listener_id))
    }

    /// 向当前 Context 谱系派发观察事件；监听器错误向上返回。
    pub fn emit<T: Send + Sync + 'static>(
        &self,
        key: EventKey<T>,
        payload: &T,
    ) -> Result<(), CoreError> {
        self.ensure_alive()?;
        self.inner
            .registry
            .emit_event(self.inner.id, key.id(), key.type_id(), payload)
    }

    /// 订阅 Waterfall；须 `next.call(...)` 委托下游，否则短路。
    pub fn on_waterfall<T, F, Fut>(
        &self,
        key: WaterfallKey<T>,
        handler: F,
    ) -> Result<Unsubscribe, CoreError>
    where
        T: Send + Sync + 'static,
        F: Fn(T, Next<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, CoreError>> + Send + 'static,
    {
        self.on_waterfall_with_options(key, ListenOptions::new(), handler)
    }

    /// 带选项的 Waterfall 订阅。
    pub fn on_waterfall_with_options<T, F, Fut>(
        &self,
        key: WaterfallKey<T>,
        options: ListenOptions<T>,
        handler: F,
    ) -> Result<Unsubscribe, CoreError>
    where
        T: Send + Sync + 'static,
        F: Fn(T, Next<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, CoreError>> + Send + 'static,
    {
        self.ensure_alive()?;
        let handler = Arc::new(handler);
        let event_id = key.id();
        let type_id = key.type_id();
        let meta = listen_meta_from_options(options, event_id);
        let wrapped: crate::event::WaterfallHandler =
            Arc::new(move |boxed: Box<dyn Any + Send + Sync>, next| {
                let handler = handler.clone();
                Box::pin(async move {
                    let value = *boxed
                        .downcast::<T>()
                        .map_err(|_| CoreError::EventTypeMismatch { event: event_id })?;
                    let next_typed = Next::new(move |value: T| {
                        Box::pin(async move {
                            let boxed = next(Box::new(value)).await?;
                            boxed
                                .downcast::<T>()
                                .map(|value| *value)
                                .map_err(|_| CoreError::EventTypeMismatch { event: event_id })
                        })
                            as std::pin::Pin<Box<dyn Future<Output = Result<T, CoreError>> + Send>>
                    });
                    let result = handler(value, next_typed).await?;
                    Ok(Box::new(result) as Box<dyn Any + Send + Sync>)
                })
                    as std::pin::Pin<
                        Box<
                            dyn Future<Output = Result<Box<dyn Any + Send + Sync>, CoreError>>
                                + Send,
                        >,
                    >
            });
        let listener_id = self.inner.registry.subscribe_waterfall(
            self.inner.id,
            event_id,
            type_id,
            wrapped,
            &self.inner.scope,
            meta,
        )?;
        Ok(self.unsubscribe_handle(event_id, listener_id))
    }

    /// Waterfall 派发：按注册顺序组成洋葱链。
    pub async fn waterfall<T: Send + Sync + 'static>(
        &self,
        key: WaterfallKey<T>,
        value: T,
    ) -> Result<T, CoreError> {
        self.ensure_alive()?;
        self.inner
            .registry
            .waterfall_event(self.inner.id, key.id(), value)
            .await
    }

    /// 订阅 Serial；返回首个 `Some(R)`。
    pub fn on_serial<T, R, F, Fut>(
        &self,
        key: SerialKey<T, R>,
        handler: F,
    ) -> Result<Unsubscribe, CoreError>
    where
        T: Send + Sync + 'static,
        R: Send + Sync + 'static,
        F: Fn(&T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<R>, CoreError>> + Send + 'static,
    {
        self.on_serial_with_options(key, ListenOptions::new(), handler)
    }

    /// 带选项的 Serial 订阅。
    pub fn on_serial_with_options<T, R, F, Fut>(
        &self,
        key: SerialKey<T, R>,
        options: ListenOptions<T>,
        handler: F,
    ) -> Result<Unsubscribe, CoreError>
    where
        T: Send + Sync + 'static,
        R: Send + Sync + 'static,
        F: Fn(&T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<R>, CoreError>> + Send + 'static,
    {
        self.ensure_alive()?;
        struct SerialAdapter<T, R, F> {
            event_id: &'static str,
            handler: Arc<F>,
            _marker: std::marker::PhantomData<fn() -> (T, R)>,
        }

        #[async_trait::async_trait]
        impl<T, R, F, Fut> crate::event::SerialHandlerErased for SerialAdapter<T, R, F>
        where
            T: Send + Sync + 'static,
            R: Send + Sync + 'static,
            F: Fn(&T) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<Option<R>, CoreError>> + Send + 'static,
        {
            async fn invoke(
                &self,
                payload: &(dyn Any + Send + Sync),
            ) -> Result<Option<Box<dyn Any + Send + Sync>>, CoreError> {
                let Some(value) = payload.downcast_ref::<T>() else {
                    return Err(CoreError::EventTypeMismatch {
                        event: self.event_id,
                    });
                };
                match (self.handler)(value).await? {
                    Some(answer) => Ok(Some(Box::new(answer) as Box<dyn Any + Send + Sync>)),
                    None => Ok(None),
                }
            }
        }

        let event_id = key.id();
        let meta = listen_meta_from_options(options, event_id);
        let wrapped: Arc<dyn crate::event::SerialHandlerErased> = Arc::new(SerialAdapter {
            event_id,
            handler: Arc::new(handler),
            _marker: std::marker::PhantomData,
        });
        let listener_id = self.inner.registry.subscribe_serial(
            self.inner.id,
            event_id,
            key.payload_type_id(),
            key.answer_type_id(),
            wrapped,
            &self.inner.scope,
            meta,
        )?;
        Ok(self.unsubscribe_handle(event_id, listener_id))
    }

    /// Serial 派发：顺序 await，首个 `Some(R)` 胜出。
    pub async fn serial<T: Send + Sync + 'static, R: Send + Sync + 'static>(
        &self,
        key: SerialKey<T, R>,
        payload: &T,
    ) -> Result<Option<R>, CoreError> {
        self.ensure_alive()?;
        self.inner
            .registry
            .serial_event(self.inner.id, key.id(), payload)
            .await
    }

    /// 订阅 Parallel。
    pub fn on_parallel<T, F, Fut>(
        &self,
        key: ParallelKey<T>,
        handler: F,
    ) -> Result<Unsubscribe, CoreError>
    where
        T: Send + Sync + 'static,
        F: Fn(&T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), CoreError>> + Send + 'static,
    {
        self.on_parallel_with_options(key, ListenOptions::new(), handler)
    }

    /// 带选项的 Parallel 订阅。
    pub fn on_parallel_with_options<T, F, Fut>(
        &self,
        key: ParallelKey<T>,
        options: ListenOptions<T>,
        handler: F,
    ) -> Result<Unsubscribe, CoreError>
    where
        T: Send + Sync + 'static,
        F: Fn(&T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), CoreError>> + Send + 'static,
    {
        self.ensure_alive()?;
        struct ParallelAdapter<T, F> {
            event_id: &'static str,
            handler: Arc<F>,
            _marker: std::marker::PhantomData<fn() -> T>,
        }

        #[async_trait::async_trait]
        impl<T, F, Fut> crate::event::ParallelHandlerErased for ParallelAdapter<T, F>
        where
            T: Send + Sync + 'static,
            F: Fn(&T) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<(), CoreError>> + Send + 'static,
        {
            async fn invoke(&self, payload: &(dyn Any + Send + Sync)) -> Result<(), CoreError> {
                let Some(value) = payload.downcast_ref::<T>() else {
                    return Err(CoreError::EventTypeMismatch {
                        event: self.event_id,
                    });
                };
                (self.handler)(value).await
            }
        }

        let event_id = key.id();
        let meta = listen_meta_from_options(options, event_id);
        let wrapped: Arc<dyn crate::event::ParallelHandlerErased> = Arc::new(ParallelAdapter {
            event_id,
            handler: Arc::new(handler),
            _marker: std::marker::PhantomData,
        });
        let listener_id = self.inner.registry.subscribe_parallel(
            self.inner.id,
            event_id,
            key.type_id(),
            wrapped,
            &self.inner.scope,
            meta,
        )?;
        Ok(self.unsubscribe_handle(event_id, listener_id))
    }

    /// Parallel 派发：并发 join，错误聚合。
    pub async fn parallel<T: Send + Sync + 'static>(
        &self,
        key: ParallelKey<T>,
        payload: &T,
    ) -> Result<(), CoreError> {
        self.ensure_alive()?;
        self.inner
            .registry
            .parallel_event(self.inner.id, key.id(), key.type_id(), payload)
            .await
    }

    fn unsubscribe_handle(&self, event_id: &'static str, listener_id: u64) -> Unsubscribe {
        let registry = Arc::downgrade(&self.inner.registry);
        Unsubscribe::new(move || {
            if let Some(registry) = registry.upgrade() {
                registry.unsubscribe_event(event_id, listener_id);
            }
        })
    }

    pub fn is_disposed(&self) -> bool {
        self.inner.scope.is_disposed()
    }

    /// 当前 Context Scope 下仍登记的直接子 Scope 数量（诊断/测试用）。
    pub fn child_scope_count(&self) -> usize {
        self.inner.scope.child_scope_count()
    }

    pub(crate) fn ensure_alive(&self) -> Result<(), CoreError> {
        if self.is_disposed() {
            Err(CoreError::ContextDisposed)
        } else {
            Ok(())
        }
    }
}

fn listen_meta_from_options<T: Send + Sync + 'static>(
    options: ListenOptions<T>,
    event_id: &'static str,
) -> ListenMeta {
    let filter = options.filter.map(|filter| {
        Arc::new(move |payload: &(dyn Any + Send + Sync)| {
            let Some(value) = payload.downcast_ref::<T>() else {
                return Err(CoreError::EventTypeMismatch { event: event_id });
            };
            filter(value)
        }) as crate::event::EventFilter
    });
    ListenMeta::new(options.once, options.prepend, options.global, filter)
}
