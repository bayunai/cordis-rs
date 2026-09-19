use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin, PluginKey, ServiceKey};
use cordis_loader::{ExtensionFactory, LoaderError};
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::Arc;

pub static GREETING: ServiceKey<Greeting> = ServiceKey::new("demo.host.greeting@1");

#[derive(Debug)]
pub struct Greeting(pub String);

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GreetingConfig {
    message: String,
}

pub struct GreetingFactory;

impl ExtensionFactory for GreetingFactory {
    type Config = GreetingConfig;

    fn id(&self) -> &'static str {
        "demo.greeting"
    }

    fn build(&self, config: GreetingConfig) -> Result<Arc<dyn Plugin>, LoaderError> {
        Ok(Arc::new(GreetingPlugin {
            message: config.message,
        }))
    }
}

struct GreetingPlugin {
    message: String,
}

#[async_trait]
impl Plugin for GreetingPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("demo.greeting")
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        ctx.provide(GREETING, Greeting(self.message.clone()))
    }
}
