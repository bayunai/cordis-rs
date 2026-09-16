//! 显式插件工厂目录。不扫描、不自动注册。

use crate::error::{HostError, format_panic_message};
use cordis_core::Plugin;
use std::{
    collections::HashMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

/// 编译期显式注册的插件工厂。
///
/// `build` 必须无副作用：禁止 I/O、spawn 或修改全局状态。副作用只属于
/// [`Plugin::apply`](cordis_core::Plugin::apply)。
pub trait ExtensionFactory: Send + Sync {
    /// 工厂身份，例如 `postgres.connector`。
    fn id(&self) -> &'static str;

    /// 按已解析的 TOML 配置构造不可变插件实例。
    fn build(&self, config: &toml::Value) -> Result<Arc<dyn Plugin>, HostError>;
}

/// 进程内工厂目录。
#[derive(Default)]
pub struct ExtensionCatalog {
    factories: HashMap<&'static str, Arc<dyn ExtensionFactory>>,
}

impl ExtensionCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// 显式注册工厂。重复 `id` 失败且不覆盖。
    ///
    /// `id()` panic 被捕获为 [`HostError::FactoryPanic`]，不会终止宿主进程。
    pub fn register(&mut self, factory: Arc<dyn ExtensionFactory>) -> Result<(), HostError> {
        let id = catch_unwind(AssertUnwindSafe(|| factory.id())).map_err(|payload| {
            HostError::FactoryPanic {
                message: format_panic_message("extension factory id", payload),
            }
        })?;
        if self.factories.contains_key(id) {
            return Err(HostError::DuplicateFactory { id: id.to_string() });
        }
        self.factories.insert(id, factory);
        Ok(())
    }

    pub fn get(&self, id: &str) -> Option<&Arc<dyn ExtensionFactory>> {
        self.factories.get(id)
    }

    pub fn contains(&self, id: &str) -> bool {
        self.factories.contains_key(id)
    }
}
