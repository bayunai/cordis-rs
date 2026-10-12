//! Factory 预备结果：延迟构造及可选的类型化热配置。

use crate::{
    LoaderError,
    volatile::{ConfigRef, ConfigSnapshot, channel},
};
use cordis_core::Plugin;
use std::{any::Any, sync::Arc};

/// 已校验配置的构造方式。创建预备结果不会执行构造函数。
///
/// 普通插件使用 [`Self::ordinary`]；需要热配置时使用 [`Self::volatile`]。
/// 捕获的配置和依赖必须由闭包拥有，业务校验应在 Factory 的 `prepare()` 中完成。
pub struct PreparedExtension {
    inner: Box<dyn PreparedConfig>,
}

impl PreparedExtension {
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
        current: Option<&Arc<dyn VolatileBinding>>,
    ) -> Result<PreparedOutcome, LoaderError> {
        self.inner.resolve(current)
    }
}

trait PreparedConfig: Send {
    fn resolve(
        self: Box<Self>,
        current: Option<&Arc<dyn VolatileBinding>>,
    ) -> Result<PreparedOutcome, LoaderError>;
}

struct Ordinary<B>(B);
impl<B: FnOnce() -> Result<Arc<dyn Plugin>, LoaderError> + Send> PreparedConfig for Ordinary<B> {
    fn resolve(
        self: Box<Self>,
        _: Option<&Arc<dyn VolatileBinding>>,
    ) -> Result<PreparedOutcome, LoaderError> {
        Ok(PreparedOutcome::Plugin {
            plugin: (self.0)()?,
            binding: None,
        })
    }
}

pub(crate) trait VolatileBinding: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

struct Binding<S, L> {
    settings: S,
    sender: tokio::sync::watch::Sender<ConfigSnapshot<L>>,
}
impl<S: Send + Sync + 'static, L: Send + Sync + 'static> VolatileBinding for Binding<S, L> {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub(crate) enum PreparedOutcome {
    Plugin {
        plugin: Arc<dyn Plugin>,
        binding: Option<Arc<dyn VolatileBinding>>,
    },
    Hot {
        binding: Arc<dyn VolatileBinding>,
        commit: Option<Box<dyn FnOnce() + Send>>,
    },
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
        current: Option<&Arc<dyn VolatileBinding>>,
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
