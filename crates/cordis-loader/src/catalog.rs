//! 静态 Factory 与可配置注入目录。

use crate::error::{LoaderError, format_panic_message};
use cordis_core::{ConfigKey, Context, Plugin, ServiceId, ServiceKey};
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use std::{
    collections::HashMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

type InjectionValidator = Arc<dyn Fn(&toml::Value) -> Result<(), String> + Send + Sync>;
type InjectionApplier = Arc<dyn Fn(Context, &toml::Value) -> Result<Context, String> + Send + Sync>;

pub trait ExtensionFactory: Send + Sync + 'static {
    type Config: DeserializeOwned + JsonSchema + Send + Sync + 'static;
    fn id(&self) -> &'static str;
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
                id: id.into(),
                schema,
            })
            .map_err(|error| LoaderError::FactorySchema {
                factory: id.into(),
                message: error.to_string(),
            })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FactoryDescriptor {
    pub id: String,
    pub schema: JsonValue,
}
#[derive(Clone)]
struct FactoryRecord {
    factory: Arc<dyn ErasedExtensionFactory>,
    descriptor: FactoryDescriptor,
}

/// 编译期注册的注入项。对象形式 `inject` 的 TOML 值会被反序列化后写入关联 ConfigKey。
#[derive(Clone)]
pub struct InjectionDescriptor {
    id: &'static str,
    service: ServiceId,
    schema: Option<JsonValue>,
    validate: Option<InjectionValidator>,
    apply: Option<InjectionApplier>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct InjectionDescriptorInfo {
    pub id: String,
    pub schema: Option<JsonValue>,
}

impl InjectionDescriptor {
    pub fn required<T: Send + Sync + 'static>(id: &'static str, service: ServiceKey<T>) -> Self {
        Self {
            id,
            service: service.id(),
            schema: None,
            validate: None,
            apply: None,
        }
    }
    pub fn intercepted<S, C>(id: &'static str, service: ServiceKey<S>, config: ConfigKey<C>) -> Self
    where
        S: Send + Sync + 'static,
        C: DeserializeOwned + JsonSchema + Send + Sync + 'static,
    {
        let validate = Arc::new(|value: &toml::Value| {
            value
                .clone()
                .try_into::<C>()
                .map(|_: C| ())
                .map_err(|error| error.to_string())
        });
        let apply = Arc::new(move |context: Context, value: &toml::Value| {
            let value = value
                .clone()
                .try_into::<C>()
                .map_err(|error| error.to_string())?;
            context
                .intercept(config, value)
                .map_err(|error| error.to_string())
        });
        Self {
            id,
            service: service.id(),
            schema: Some(
                serde_json::to_value(schemars::schema_for!(C)).expect("JsonSchema serializes"),
            ),
            validate: Some(validate),
            apply: Some(apply),
        }
    }
    fn info(&self) -> InjectionDescriptorInfo {
        InjectionDescriptorInfo {
            id: self.id.into(),
            schema: self.schema.clone(),
        }
    }
}

#[derive(Clone, Default)]
pub struct ExtensionCatalog {
    factories: HashMap<&'static str, FactoryRecord>,
    injections: HashMap<&'static str, InjectionDescriptor>,
}

impl ExtensionCatalog {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn register<F: ExtensionFactory>(&mut self, factory: F) -> Result<(), LoaderError> {
        let id = catch_unwind(AssertUnwindSafe(|| factory.id())).map_err(|payload| {
            LoaderError::FactoryPanic {
                message: format_panic_message("extension factory id", payload),
            }
        })?;
        if self.factories.contains_key(id) {
            return Err(LoaderError::DuplicateFactory { id: id.into() });
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
    pub fn register_injection(
        &mut self,
        descriptor: InjectionDescriptor,
    ) -> Result<(), LoaderError> {
        if self.injections.contains_key(descriptor.id) {
            return Err(LoaderError::DuplicateInjection {
                id: descriptor.id.into(),
            });
        }
        self.injections.insert(descriptor.id, descriptor);
        Ok(())
    }
    pub(crate) fn get(&self, id: &str) -> Option<&Arc<dyn ErasedExtensionFactory>> {
        self.factories.get(id).map(|record| &record.factory)
    }
    pub fn contains(&self, id: &str) -> bool {
        self.factories.contains_key(id)
    }
    pub fn factories(&self) -> Result<Vec<FactoryDescriptor>, LoaderError> {
        let mut values = self
            .factories
            .values()
            .map(|record| record.descriptor.clone())
            .collect::<Vec<_>>();
        values.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(values)
    }
    pub fn injections(&self) -> Vec<InjectionDescriptorInfo> {
        let mut values = self
            .injections
            .values()
            .map(InjectionDescriptor::info)
            .collect::<Vec<_>>();
        values.sort_by(|left, right| left.id.cmp(&right.id));
        values
    }
    pub(crate) fn validate_injection(
        &self,
        id: &str,
        value: Option<&toml::Value>,
        entry: &str,
    ) -> Result<(), LoaderError> {
        let Some(descriptor) = self.injections.get(id) else {
            return Err(LoaderError::UnknownInjection {
                entry: entry.into(),
                injection: id.into(),
            });
        };
        if value.is_some() && descriptor.apply.is_none() {
            return Err(LoaderError::InjectionNotConfigurable {
                entry: entry.into(),
                injection: id.into(),
            });
        }
        if let (Some(value), Some(validate)) = (value, &descriptor.validate) {
            validate(value).map_err(|message| LoaderError::InvalidInjectionConfig {
                entry: entry.into(),
                injection: id.into(),
                message,
            })?;
        }
        Ok(())
    }
    pub(crate) fn resolve_injections(
        &self,
        inject: Option<&crate::config::InjectConfig>,
        entry: &str,
        context: Context,
    ) -> Result<(Context, Vec<ServiceId>), LoaderError> {
        let Some(inject) = inject else {
            return Ok((context, Vec::new()));
        };
        let mut context = context;
        let mut dependencies = Vec::new();
        let values: Vec<(&str, Option<&toml::Value>)> = match inject {
            crate::config::InjectConfig::List(ids) => {
                ids.iter().map(|id| (id.as_str(), None)).collect()
            }
            crate::config::InjectConfig::Map(map) => map
                .iter()
                .map(|(id, value)| (id.as_str(), Some(value)))
                .collect(),
        };
        for (id, value) in values {
            let descriptor =
                self.injections
                    .get(id)
                    .ok_or_else(|| LoaderError::UnknownInjection {
                        entry: entry.into(),
                        injection: id.into(),
                    })?;
            if let Some(value) = value {
                let apply = descriptor.apply.as_ref().ok_or_else(|| {
                    LoaderError::InjectionNotConfigurable {
                        entry: entry.into(),
                        injection: id.into(),
                    }
                })?;
                context = apply(context, value).map_err(|message| {
                    LoaderError::InvalidInjectionConfig {
                        entry: entry.into(),
                        injection: id.into(),
                        message,
                    }
                })?;
            }
            if !dependencies.contains(&descriptor.service) {
                dependencies.push(descriptor.service);
            }
        }
        Ok((context, dependencies))
    }
}
