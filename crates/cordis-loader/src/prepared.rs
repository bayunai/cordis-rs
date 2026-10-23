//! Factory 预备结果：延迟构造及可选的类型化热配置。

use crate::{ENTRY_LOCATION, EntryTreeSource, LoaderSubtree, SourceError};
use crate::{
    LoaderError,
    volatile::{ConfigRef, ConfigSnapshot, channel},
};
use cordis_core::{Context, CoreError, Plugin, PluginKey};
use std::{
    any::Any,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

pub(crate) type SourceFuture =
    Pin<Box<dyn Future<Output = Result<Arc<dyn EntryTreeSource>, SourceError>> + Send + 'static>>;
pub(crate) type TreeOpener = Arc<dyn Fn(Context) -> SourceFuture + Send + Sync>;

/// 源载体实际存活状态。重新激活使用最后成功提交的配置，而不是最初构造时的配置。
pub(crate) struct TreeState {
    pub(crate) mounted: Mutex<Option<(Context, LoaderSubtree)>>,
    pub(crate) dirty: std::sync::atomic::AtomicBool,
    opener: Mutex<TreeOpener>,
}

struct TreePlugin {
    key: PluginKey,
    state: Arc<TreeState>,
}
#[async_trait::async_trait]
impl Plugin for TreePlugin {
    fn key(&self) -> PluginKey {
        self.key
    }
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let open = self.state.opener.lock().expect("tree opener").clone();
        let source = open(ctx.clone())
            .await
            .map_err(|error| CoreError::PluginApply(error.to_string()))?;
        let loader = ctx.config(ENTRY_LOCATION)?.loader.clone();
        let tree = loader
            .attach_subtree(ctx, source)
            .await
            .map_err(|error| CoreError::PluginApply(error.to_string()))?;
        *self.state.mounted.lock().expect("tree mounted") = Some((ctx.clone(), tree.clone()));
        let state = self.state.clone();
        ctx.effect_named("source-tree")?
            .on_dispose_async(move || async move {
                state.mounted.lock().expect("tree mounted").take();
                tree.detach()
                    .await
                    .map_err(|error| CoreError::PluginApply(error.to_string()))
            })?;
        Ok(())
    }
}

/// 已校验配置的构造方式。创建预备结果不会执行构造函数。
///
/// 普通插件使用 [`Self::ordinary`]；需要热配置时使用 [`Self::volatile`]。
/// 捕获的配置和依赖必须由闭包拥有，业务校验应在 Factory 的 `prepare()` 中完成。
pub struct PreparedExtension {
    inner: Box<dyn PreparedConfig>,
}

impl PreparedExtension {
    /// 通用来源树载体。settings 改变执行完整替换；config 改变等待原子树协调完成。
    /// open 拥有 Context 并异步构造来源；Loader 不感知文件或格式。
    pub fn tree<S, C, F, Fut>(key: PluginKey, settings: S, config: C, open: F) -> Self
    where
        S: Clone + PartialEq + Send + Sync + 'static,
        C: Clone + PartialEq + Send + Sync + 'static,
        F: Fn(S, C, Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Arc<dyn EntryTreeSource>, SourceError>> + Send + 'static,
    {
        Self {
            inner: Box::new(TreePrepared {
                key,
                settings,
                config,
                open,
            }),
        }
    }
    /// 没有热配置的插件，无需创建空 Live 或配置引用。
    pub fn ordinary(
        build: impl FnOnce() -> Result<Arc<dyn Plugin>, LoaderError> + Send + 'static,
    ) -> Self {
        Self {
            inner: Box::new(Ordinary(build)),
        }
    }

    /// 将普通配置与整份热快照分开。只有需要新实例时才调用 `build`。
    pub fn volatile<S, L>(
        settings: S,
        live: L,
        build: impl FnOnce(S, ConfigRef<L>) -> Result<Arc<dyn Plugin>, LoaderError> + Send + 'static,
    ) -> Self
    where
        S: Clone + PartialEq + Send + Sync + 'static,
        L: PartialEq + Send + Sync + 'static,
    {
        Self {
            inner: Box::new(Volatile {
                settings,
                live,
                build,
            }),
        }
    }

    pub(crate) fn resolve(
        self,
        current: Option<&Arc<dyn ConfigBinding>>,
    ) -> Result<PreparedOutcome, LoaderError> {
        self.inner.resolve(current)
    }
}

trait PreparedConfig: Send {
    fn resolve(
        self: Box<Self>,
        current: Option<&Arc<dyn ConfigBinding>>,
    ) -> Result<PreparedOutcome, LoaderError>;
}

struct Ordinary<B>(B);
impl<B: FnOnce() -> Result<Arc<dyn Plugin>, LoaderError> + Send> PreparedConfig for Ordinary<B> {
    fn resolve(
        self: Box<Self>,
        _: Option<&Arc<dyn ConfigBinding>>,
    ) -> Result<PreparedOutcome, LoaderError> {
        Ok(PreparedOutcome::Plugin {
            plugin: (self.0)()?,
            binding: None,
        })
    }
}

pub(crate) trait ConfigBinding: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

struct Binding<S, L> {
    settings: S,
    sender: tokio::sync::watch::Sender<ConfigSnapshot<L>>,
}
impl<S: Send + Sync + 'static, L: Send + Sync + 'static> ConfigBinding for Binding<S, L> {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub(crate) enum PreparedOutcome {
    TreeUpdate {
        binding: Arc<dyn ConfigBinding>,
        state: Arc<TreeState>,
        open: TreeOpener,
        commit: Box<dyn FnOnce() + Send>,
    },
    Plugin {
        plugin: Arc<dyn Plugin>,
        binding: Option<Arc<dyn ConfigBinding>>,
    },
    Hot {
        binding: Arc<dyn ConfigBinding>,
        commit: Option<Box<dyn FnOnce() + Send>>,
    },
}

struct TreePrepared<S, C, F> {
    key: PluginKey,
    settings: S,
    config: C,
    open: F,
}
struct TreeBinding<S, C> {
    key: PluginKey,
    settings: S,
    config: Mutex<C>,
    state: Arc<TreeState>,
}
impl<S: Send + Sync + 'static, C: Send + Sync + 'static> ConfigBinding for TreeBinding<S, C> {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
impl<S, C, F, Fut> PreparedConfig for TreePrepared<S, C, F>
where
    S: Clone + PartialEq + Send + Sync + 'static,
    C: Clone + PartialEq + Send + Sync + 'static,
    F: Fn(S, C, Context) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Arc<dyn EntryTreeSource>, SourceError>> + Send + 'static,
{
    fn resolve(
        self: Box<Self>,
        current: Option<&Arc<dyn ConfigBinding>>,
    ) -> Result<PreparedOutcome, LoaderError> {
        let Self {
            key,
            settings,
            config,
            open,
        } = *self;
        let open_settings = settings.clone();
        let open_config = config.clone();
        let open: TreeOpener =
            Arc::new(move |ctx| Box::pin(open(open_settings.clone(), open_config.clone(), ctx)));
        if let Some(current) = current
            && let Some(binding) = current.as_any().downcast_ref::<TreeBinding<S, C>>()
            && binding.key == key
            && binding.settings == settings
        {
            if *binding.config.lock().expect("tree config") == config
                && !binding
                    .state
                    .dirty
                    .load(std::sync::atomic::Ordering::Acquire)
            {
                return Ok(PreparedOutcome::Hot {
                    binding: current.clone(),
                    commit: None,
                });
            }
            let state = binding.state.clone();
            let retained = current.clone();
            let commit_open = open.clone();
            return Ok(PreparedOutcome::TreeUpdate {
                binding: current.clone(),
                state,
                open,
                commit: Box::new(move || {
                    let binding = retained
                        .as_any()
                        .downcast_ref::<TreeBinding<S, C>>()
                        .expect("tree binding");
                    *binding.config.lock().expect("tree config") = config;
                    *binding.state.opener.lock().expect("tree opener") = commit_open;
                }),
            });
        }
        let state = Arc::new(TreeState {
            mounted: Mutex::new(None),
            dirty: std::sync::atomic::AtomicBool::new(false),
            opener: Mutex::new(open),
        });
        let plugin = Arc::new(TreePlugin {
            key,
            state: state.clone(),
        });
        Ok(PreparedOutcome::Plugin {
            plugin,
            binding: Some(Arc::new(TreeBinding {
                key,
                settings,
                config: Mutex::new(config),
                state,
            })),
        })
    }
}

struct Volatile<S, L, B> {
    settings: S,
    live: L,
    build: B,
}
impl<S, L, B> PreparedConfig for Volatile<S, L, B>
where
    S: Clone + PartialEq + Send + Sync + 'static,
    L: PartialEq + Send + Sync + 'static,
    B: FnOnce(S, ConfigRef<L>) -> Result<Arc<dyn Plugin>, LoaderError> + Send,
{
    fn resolve(
        self: Box<Self>,
        current: Option<&Arc<dyn ConfigBinding>>,
    ) -> Result<PreparedOutcome, LoaderError> {
        let Self {
            settings,
            live,
            build,
        } = *self;
        let live = Arc::new(live);
        // 能力或绑定类型改变是明确的完整替换，不复用旧写入端。
        if let Some(binding) = current
            && let Some(typed) = binding.as_any().downcast_ref::<Binding<S, L>>()
            && settings == typed.settings
        {
            let old = typed.sender.borrow().clone();
            if *old.value == *live {
                return Ok(PreparedOutcome::Hot {
                    binding: binding.clone(),
                    commit: None,
                });
            }
            let revision = old
                .revision
                .checked_add(1)
                .ok_or_else(|| LoaderError::invalid_config("hot config revision exhausted"))?;
            let binding = binding.clone();
            let retained = binding.clone();
            return Ok(PreparedOutcome::Hot {
                binding,
                commit: Some(Box::new(move || {
                    let typed = retained
                        .as_any()
                        .downcast_ref::<Binding<S, L>>()
                        .expect("prepared hot binding type");
                    typed.sender.send_replace(ConfigSnapshot {
                        revision,
                        value: live,
                    });
                })),
            });
        }
        let retained_settings = settings.clone();
        let (sender, reference) = channel(live);
        let plugin = build(settings, reference)?;
        Ok(PreparedOutcome::Plugin {
            plugin,
            binding: Some(Arc::new(Binding {
                settings: retained_settings,
                sender,
            })),
        })
    }
}
