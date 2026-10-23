use cordis_loader::{
    ConfigProjectionError, ExtensionCatalog, ExtensionFactory, LOADER, LoaderError, LoaderPlugin,
    PreparedExtension,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Deserialize, JsonSchema)]
struct Input {
    mode: u32,
}
impl Serialize for Input {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.mode {
            1 => Err(serde::ser::Error::custom("private-secret-error")),
            2 => panic!("private-secret-panic"),
            _ => serde_json::json!({"mode": self.mode}).serialize(serializer),
        }
    }
}
struct Factory(Arc<AtomicUsize>);
impl ExtensionFactory for Factory {
    type Config = Input;
    fn id(&self) -> &'static str {
        "projection.error"
    }
    fn prepare(&self, _: Input) -> Result<PreparedExtension, LoaderError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(PreparedExtension::ordinary(|| {
            panic!("projection must not build")
        }))
    }
}

#[test]
fn serialization_failures_are_redacted_and_precede_prepare_or_build() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut catalog = ExtensionCatalog::new();
    catalog.register(Factory(calls.clone())).unwrap();
    for mode in [1, 2] {
        let error = catalog
            .prepare("projection.error", &toml::toml! { mode = mode }.into())
            .err()
            .unwrap();
        assert!(!error.to_string().contains("private-secret"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    catalog
        .prepare("projection.error", &toml::toml! { mode = 0 }.into())
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn projection_distinguishes_invalid_input_from_execution_failure() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut catalog = ExtensionCatalog::new();
    catalog.register(Factory(calls.clone())).unwrap();
    assert!(matches!(
        catalog.project("projection.error", &toml::Value::Table(Default::default())),
        Err(ConfigProjectionError::InputInvalid)
    ));
    for mode in [1, 2] {
        let error = catalog
            .project("projection.error", &toml::toml! { mode = mode }.into())
            .unwrap_err();
        assert!(matches!(error, ConfigProjectionError::Loader(_)));
        assert!(!error.to_string().contains("private-secret"));
        assert!(!format!("{error:?}").contains("private-secret"));
    }
    assert!(matches!(
        catalog.project("missing", &toml::Value::Table(Default::default())),
        Err(ConfigProjectionError::Loader(
            LoaderError::UnknownFactory { .. }
        ))
    ));
    assert_eq!(
        catalog
            .project("projection.error", &toml::toml! { mode = 0 }.into())
            .unwrap(),
        serde_json::json!({"mode": 0})
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[derive(Serialize, JsonSchema)]
struct DeserializeBoundary;

impl<'de> Deserialize<'de> for DeserializeBoundary {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value.get("panic").and_then(serde_json::Value::as_bool) == Some(true) {
            panic!("private-secret-deserialize-panic");
        }
        Err(serde::de::Error::custom("private-secret-deserialize-error"))
    }
}

struct DeserializeFactory;
impl ExtensionFactory for DeserializeFactory {
    type Config = DeserializeBoundary;
    fn id(&self) -> &'static str {
        "projection.deserialize"
    }
    fn prepare(&self, _: Self::Config) -> Result<PreparedExtension, LoaderError> {
        panic!("projection must not prepare")
    }
}

#[test]
fn deserialize_error_and_panic_have_distinct_redacted_categories() {
    let mut catalog = ExtensionCatalog::new();
    catalog.register(DeserializeFactory).unwrap();
    for should_panic in [false, true] {
        let error = catalog
            .project(
                "projection.deserialize",
                &toml::toml! { panic = should_panic }.into(),
            )
            .unwrap_err();
        if should_panic {
            assert!(matches!(
                error,
                ConfigProjectionError::Loader(LoaderError::FactoryPanic { .. })
            ));
        } else {
            assert!(matches!(error, ConfigProjectionError::InputInvalid));
        }
        assert!(!error.to_string().contains("private-secret"));
        assert!(!format!("{error:?}").contains("private-secret"));
    }
}

#[tokio::test]
async fn closed_loader_is_not_an_invalid_projection_input() {
    let runtime = cordis_core::Runtime::new().unwrap();
    let _fiber = runtime
        .root()
        .plugin(Arc::new(LoaderPlugin::new(ExtensionCatalog::new())))
        .await
        .unwrap();
    let loader = (*runtime.root().get(LOADER).unwrap()).clone();
    runtime.shutdown().await.unwrap();
    assert!(matches!(
        loader.project_entry_config("missing", &toml::Value::Table(Default::default())),
        Err(ConfigProjectionError::LoaderUnavailable)
    ));
}
