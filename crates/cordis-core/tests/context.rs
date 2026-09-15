//! 验证 Context 派生视图合同：扩展覆盖、dispose 回落、隔离与 EffectContext 门面。
//!
//! 不覆盖 Fiber / 插件挂载路径；夹具见 `common/helpers`。

#[path = "common/helpers.rs"]
mod common;
use common::*;

#[tokio::test]
async fn effect_owned_extended_context_overrides_then_disposal_falls_back_to_parent() {
    let runtime = runtime();
    let root = runtime.root();
    root.provide(NUMBER, Number(1)).unwrap();
    let owner = root.effect().unwrap();
    let child = owner.extend().unwrap();
    child.provide(NUMBER, Number(2)).unwrap();
    assert_eq!(child.get(NUMBER).unwrap().0, 2);
    owner.dispose();
    assert_eq!(root.get(NUMBER).unwrap().0, 1);
}

#[tokio::test]
async fn disposed_context_fail_fast() {
    let runtime = runtime();
    let owner = runtime.root().effect().unwrap();
    let child = owner.extend().unwrap();
    owner.dispose();
    assert!(matches!(
        child.provide(NUMBER, Number(1)),
        Err(CoreError::ContextDisposed)
    ));
    assert!(matches!(child.get(NUMBER), Err(CoreError::ContextDisposed)));
}

#[tokio::test]
async fn cross_context_isolation() {
    let runtime = runtime();
    let left = runtime.root().extend().unwrap();
    let right = runtime.root().extend().unwrap();
    left.provide(NUMBER, Number(1)).unwrap();
    assert_service_unavailable(&right, NUMBER);
    assert_eq!(left.get(NUMBER).unwrap().0, 1);
}

#[tokio::test]
async fn effect_dispose_removes_extended_nodes_from_diagnostics() {
    let runtime = runtime();
    let root = runtime.root();
    let before = runtime.diagnostics().contexts.len();
    let owner = root.effect().unwrap();
    let child = owner.extend().unwrap();
    child.provide(NUMBER, Number(1)).unwrap();
    child.on(PING, |_| Ok(())).unwrap();
    assert_eq!(runtime.diagnostics().contexts.len(), before + 1);
    assert!(!runtime.diagnostics().providers.is_empty());
    owner.dispose();
    assert_eq!(runtime.diagnostics().contexts.len(), before);
    assert!(runtime.diagnostics().providers.is_empty());
    root.emit(PING, &Ping(1)).unwrap();
}

#[tokio::test]
async fn isolate_derives_view_without_mutating_parent_or_siblings() {
    let runtime = runtime();
    let root = runtime.root();
    root.provide(NUMBER, Number(1)).unwrap();
    let sibling = root.extend().unwrap();

    let (isolated, label) = root.isolate(NUMBER).unwrap();
    assert_eq!(root.get(NUMBER).unwrap().0, 1);
    assert_eq!(sibling.get(NUMBER).unwrap().0, 1);
    assert_service_unavailable(&isolated, NUMBER);

    let owner = isolated.effect().unwrap();
    owner.provide(NUMBER, Number(2)).unwrap();
    let nested = isolated.extend().unwrap();
    let shared = root.isolate_with(NUMBER, label).unwrap();
    assert_eq!(isolated.get(NUMBER).unwrap().0, 2);
    assert_eq!(nested.get(NUMBER).unwrap().0, 2);
    assert_eq!(shared.get(NUMBER).unwrap().0, 2);
    assert_eq!(root.get(NUMBER).unwrap().0, 1);
    assert_eq!(sibling.get(NUMBER).unwrap().0, 1);

    owner.dispose();
    assert_service_unavailable(&isolated, NUMBER);
    assert_eq!(root.get(NUMBER).unwrap().0, 1);
}

#[tokio::test]
async fn isolation_label_shares_service_across_sibling_contexts() {
    let rt = runtime();
    let root = rt.root();
    let room_base = root.extend().unwrap();
    let (room, label) = room_base.isolate(NUMBER).unwrap();
    room.provide(NUMBER, Number(11)).unwrap();

    let a = root.isolate_with(NUMBER, label.clone()).unwrap();
    assert_eq!(a.get(NUMBER).unwrap().0, 11);

    let b = root.isolate_with(NUMBER, label).unwrap();
    assert_eq!(b.get(NUMBER).unwrap().0, 11);

    let plain = root.extend().unwrap();
    assert_service_unavailable(&plain, NUMBER);

    let other_rt = runtime();
    let (_, foreign) = other_rt.root().isolate(NUMBER).unwrap();
    assert!(matches!(
        room.isolate_with(NUMBER, foreign),
        Err(CoreError::IsolationRuntimeMismatch)
    ));
}

#[tokio::test]
async fn intercept_overrides_config_without_mutating_parent() {
    use cordis_core::ConfigKey;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Theme(&'static str);

    static THEME: ConfigKey<Theme> = ConfigKey::new("test.theme@1");

    let runtime = runtime();
    let root = runtime.root();
    assert!(matches!(
        root.config(THEME),
        Err(CoreError::ConfigUnavailable { .. })
    ));

    let child = root.intercept(THEME, Theme("dark")).unwrap();
    assert_eq!(child.config(THEME).unwrap().0, "dark");
    assert!(matches!(
        root.config(THEME),
        Err(CoreError::ConfigUnavailable { .. })
    ));

    let nested = child.intercept(THEME, Theme("oled")).unwrap();
    assert_eq!(nested.config(THEME).unwrap().0, "oled");
    assert_eq!(child.config(THEME).unwrap().0, "dark");

    let snap = runtime.diagnostics();
    assert!(
        snap.contexts
            .iter()
            .any(|ctx| ctx.config_keys.contains(&"test.theme@1"))
    );
    assert!(!format!("{snap:?}").contains("oled"));
}

#[tokio::test]
async fn intercept_type_conflict_and_plugin_can_read_config() {
    use cordis_core::ConfigKey;

    #[derive(Debug)]
    struct Flag(bool);
    #[derive(Debug)]
    struct OtherFlag;

    static FLAG: ConfigKey<Flag> = ConfigKey::new("test.flag@1");
    static FLAG_AS_OTHER: ConfigKey<OtherFlag> = ConfigKey::new("test.flag@1");

    let runtime = runtime();
    let root = runtime.root();
    let scoped = root.intercept(FLAG, Flag(true)).unwrap();
    let context_count = runtime.diagnostics().contexts.len();
    assert!(matches!(
        root.intercept(FLAG_AS_OTHER, OtherFlag),
        Err(CoreError::ConfigKeyTypeConflict { .. })
    ));
    assert_eq!(runtime.diagnostics().contexts.len(), context_count);
    for _ in 0..3 {
        assert!(matches!(
            root.intercept(FLAG_AS_OTHER, OtherFlag),
            Err(CoreError::ConfigKeyTypeConflict { .. })
        ));
    }
    assert_eq!(runtime.diagnostics().contexts.len(), context_count);
    assert!(matches!(
        scoped.config(FLAG_AS_OTHER),
        Err(CoreError::ConfigTypeMismatch { .. })
    ));

    struct ConfigReader;
    #[async_trait]
    impl Plugin for ConfigReader {
        fn key(&self) -> cordis_core::PluginKey {
            cordis_core::PluginKey::new("test.configreader")
        }
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            assert!(ctx.config(FLAG)?.0);
            Ok(())
        }
    }
    let mut fiber = scoped.plugin(Arc::new(ConfigReader)).await.unwrap();
    assert_eq!(fiber.state(), FiberState::Active);
    fiber.dispose();
}
