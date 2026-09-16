//! Host 集成测试夹具：可编程工厂、临时配置文件。

#![allow(dead_code)]

use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin, PluginKey, ServiceId, ServiceKey};
use cordis_host::{
    ExtensionCatalog, ExtensionEntry, ExtensionFactory, ExtensionsConfig, HostError, toml,
};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::oneshot;

pub static NUMBER: ServiceKey<u32> = ServiceKey::new("host.test.number@1");

pub fn empty_config() -> ExtensionsConfig {
    ExtensionsConfig::from_toml_str("version = 1\nextensions = []\n").expect("empty extensions")
}

pub fn parse_extensions(text: &str) -> ExtensionsConfig {
    ExtensionsConfig::from_toml_str(text).expect("extensions toml")
}

pub fn catalog_with(factory: Arc<dyn ExtensionFactory>) -> ExtensionCatalog {
    let mut catalog = ExtensionCatalog::new();
    catalog.register(factory).expect("register factory");
    catalog
}

pub fn write_pair(dir: &Path, bootstrap: &str, extensions: &str) -> PathBuf {
    let bootstrap_path = dir.join("bootstrap.toml");
    fs::write(&bootstrap_path, bootstrap).expect("write bootstrap");
    fs::write(dir.join("extensions.toml"), extensions).expect("write extensions");
    bootstrap_path
}

#[derive(Clone)]
pub struct DisposeGate {
    started: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    release: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
}

impl DisposeGate {
    pub fn new() -> (Self, oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        (
            Self {
                started: Arc::new(Mutex::new(Some(started_tx))),
                release: Arc::new(Mutex::new(Some(release_rx))),
            },
            started_rx,
            release_tx,
        )
    }
}

type SetupFn = Arc<dyn Fn(&Context, &toml::Value) -> Result<(), CoreError> + Send + Sync>;

#[derive(Clone)]
pub struct TestFactory {
    id: &'static str,
    plugin_key: &'static str,
    inject: Vec<ServiceId>,
    builds: Arc<AtomicUsize>,
    applies: Arc<AtomicUsize>,
    fail_build: Arc<Mutex<Option<String>>>,
    fail_apply: Arc<Mutex<Option<String>>>,
    setup: SetupFn,
    dispose: Option<DisposeGate>,
}

impl TestFactory {
    pub fn new(id: &'static str) -> Self {
        Self {
            id,
            plugin_key: id,
            inject: Vec::new(),
            builds: Arc::new(AtomicUsize::new(0)),
            applies: Arc::new(AtomicUsize::new(0)),
            fail_build: Arc::new(Mutex::new(None)),
            fail_apply: Arc::new(Mutex::new(None)),
            setup: Arc::new(|_, _| Ok(())),
            dispose: None,
        }
    }

    pub fn plugin_key(mut self, key: &'static str) -> Self {
        self.plugin_key = key;
        self
    }

    pub fn inject(mut self, ids: Vec<ServiceId>) -> Self {
        self.inject = ids;
        self
    }

    pub fn on_apply<F>(mut self, setup: F) -> Self
    where
        F: Fn(&Context, &toml::Value) -> Result<(), CoreError> + Send + Sync + 'static,
    {
        self.setup = Arc::new(setup);
        self
    }

    pub fn with_disposer(mut self, gate: DisposeGate) -> Self {
        self.dispose = Some(gate);
        self
    }

    pub fn builds(&self) -> Arc<AtomicUsize> {
        self.builds.clone()
    }

    pub fn applies(&self) -> Arc<AtomicUsize> {
        self.applies.clone()
    }

    pub fn fail_build(&self) -> Arc<Mutex<Option<String>>> {
        self.fail_build.clone()
    }

    pub fn fail_apply(&self) -> Arc<Mutex<Option<String>>> {
        self.fail_apply.clone()
    }

    pub fn into_arc(self) -> Arc<dyn ExtensionFactory> {
        Arc::new(self)
    }
}

impl ExtensionFactory for TestFactory {
    fn id(&self) -> &'static str {
        self.id
    }

    fn build(&self, config: &toml::Value) -> Result<Arc<dyn Plugin>, HostError> {
        self.builds.fetch_add(1, Ordering::SeqCst);
        if let Some(message) = self.fail_build.lock().expect("fail_build").clone() {
            return Err(HostError::invalid_config(message));
        }
        Ok(Arc::new(TestHostPlugin {
            key: PluginKey::new(self.plugin_key),
            inject: self.inject.clone(),
            applies: self.applies.clone(),
            fail_apply: self.fail_apply.clone(),
            setup: self.setup.clone(),
            config: config.clone(),
            dispose: self.dispose.clone(),
        }))
    }
}

struct TestHostPlugin {
    key: PluginKey,
    inject: Vec<ServiceId>,
    applies: Arc<AtomicUsize>,
    fail_apply: Arc<Mutex<Option<String>>>,
    setup: SetupFn,
    config: toml::Value,
    dispose: Option<DisposeGate>,
}

#[async_trait]
impl Plugin for TestHostPlugin {
    fn key(&self) -> PluginKey {
        self.key
    }

    fn inject(&self) -> Vec<ServiceId> {
        self.inject.clone()
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        if let Some(message) = self.fail_apply.lock().expect("fail_apply").clone() {
            return Err(CoreError::PluginApply(message));
        }
        if let Some(gate) = &self.dispose {
            let started = gate.started.clone();
            let release = gate.release.clone();
            ctx.effect()?.on_dispose_async(move || async move {
                if let Some(sender) = started.lock().expect("started").take() {
                    let _ = sender.send(());
                }
                let receiver = { release.lock().expect("release").take() };
                if let Some(receiver) = receiver {
                    let _ = receiver.await;
                }
                Ok(())
            })?;
        }
        (self.setup)(ctx, &self.config)
    }
}

pub fn enabled(instance: &str, factory: &str) -> ExtensionEntry {
    ExtensionEntry::new(instance, factory, true)
}

pub fn extensions(entries: Vec<ExtensionEntry>) -> ExtensionsConfig {
    ExtensionsConfig {
        version: 1,
        extensions: entries,
    }
}

pub fn table(pairs: &[(&str, toml::Value)]) -> toml::Value {
    let mut map = toml::Table::new();
    for (key, value) in pairs {
        map.insert((*key).to_string(), value.clone());
    }
    toml::Value::Table(map)
}
