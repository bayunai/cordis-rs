use cordis_loader::{ExtensionCatalog, ExtensionFactory, LoaderError, PreparedExtension};
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
