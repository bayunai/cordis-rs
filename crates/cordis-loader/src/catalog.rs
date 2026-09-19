//! 显式插件工厂目录。不扫描、不自动注册。

use crate::error::{LoaderError, format_panic_message};
use cordis_core::Plugin;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use std::{
    collections::HashMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

/// 编译期显式注册的插件工厂。
///
/// `build` 必须无副作用：禁止 I/O、spawn 或修改全局状态。副作用只属于
/// [`Plugin::apply`](cordis_core::Plugin::apply)。
pub trait ExtensionFactory: Send + Sync + 'static {
    /// 此 Factory 所需的实例配置类型。
    type Config: DeserializeOwned + JsonSchema + Send + Sync + 'static;

    /// 工厂身份，例如 `postgres.connector`。
    fn id(&self) -> &'static str;

    /// 按强类型实例配置构造不可变插件实例。
    fn build(&self, config: Self::Config) -> Result<Arc<dyn Plugin>, LoaderError>;
}

pub(crate) trait ErasedExtensionFactory: Send + Sync {
    fn build(&self, config: &toml::Value) -> Result<Arc<dyn Plugin>, LoaderError>;
    fn descriptor(&self, id: &'static str) -> Result<FactoryDescriptor, LoaderError>;
}

struct FactoryAdapter<F> {
    factory: F,
}

impl<F: ExtensionFactory> ErasedExtensionFactory for FactoryAdapter<F> {
    fn build(&self, config: &toml::Value) -> Result<Arc<dyn Plugin>, LoaderError> {
        let config = config.clone().try_into::<F::Config>().map_err(|error| {
            LoaderError::invalid_config(format!("factory `{}` config: {error}", self.factory.id()))
        })?;
        self.factory.build(config)
    }

    fn descriptor(&self, id: &'static str) -> Result<FactoryDescriptor, LoaderError> {
        serde_json::to_value(schemars::schema_for!(F::Config))
            .map(|schema| FactoryDescriptor {
                id: id.to_string(),
                schema,
            })
            .map_err(|error| LoaderError::FactorySchema {
                factory: id.to_string(),
                message: error.to_string(),
            })
    }
}

/// 可由管理界面创建的编译期 Factory 描述。
#[derive(Debug, Clone, PartialEq)]
pub struct FactoryDescriptor {
    pub id: String,
    pub schema: JsonValue,
}

struct FactoryRecord {
    factory: Arc<dyn ErasedExtensionFactory>,
    descriptor: FactoryDescriptor,
}

/// 进程内工厂目录。
#[derive(Default)]
pub struct ExtensionCatalog {
    factories: HashMap<&'static str, FactoryRecord>,
}

impl ExtensionCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// 显式注册工厂。重复 `id` 失败且不覆盖。
    ///
    /// `id()` panic 被捕获为 [`LoaderError::FactoryPanic`]，不会终止宿主进程。
    pub fn register<F: ExtensionFactory>(&mut self, factory: F) -> Result<(), LoaderError> {
        let id = catch_unwind(AssertUnwindSafe(|| factory.id())).map_err(|payload| {
            LoaderError::FactoryPanic {
                message: format_panic_message("extension factory id", payload),
            }
        })?;
        if self.factories.contains_key(id) {
            return Err(LoaderError::DuplicateFactory { id: id.to_string() });
        }
        let factory: Arc<dyn ErasedExtensionFactory> = Arc::new(FactoryAdapter { factory });
        let descriptor = factory.descriptor(id)?;
        self.factories.insert(
            id,
            FactoryRecord {
                factory,
                descriptor,
            },
        );
        Ok(())
    }

    pub(crate) fn get(&self, id: &str) -> Option<&Arc<dyn ErasedExtensionFactory>> {
        self.factories.get(id).map(|record| &record.factory)
    }

    pub fn contains(&self, id: &str) -> bool {
        self.factories.contains_key(id)
    }

    /// 返回稳定排序的可创建 Factory 与其 JSON Schema。
    pub fn factories(&self) -> Result<Vec<FactoryDescriptor>, LoaderError> {
        let mut factories = self
            .factories
            .values()
            .map(|record| record.descriptor.clone())
            .collect::<Vec<_>>();
        factories.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(factories)
    }
}
