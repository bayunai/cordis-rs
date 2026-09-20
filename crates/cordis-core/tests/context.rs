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
async fn effect_dispose_removes_resources_registered_from_extended_view() {
    let runtime = runtime();
    let root = runtime.root();
    let owner = root.effect().unwrap();
    let child = owner.extend().unwrap();
    child.provide(NUMBER, Number(1)).unwrap();
    child.on(PING, |_| Ok(())).unwrap();
    assert!(!runtime.diagnostics().providers.is_empty());
    owner.dispose();
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

    // 纯派生 Context 不进入 Runtime diagnostics，也不暴露配置载荷。
    assert!(!format!("{:?}", runtime.diagnostics()).contains("oled"));
}

#[tokio::test]
async fn intercept_shares_service_domain_with_parent_and_siblings() {
    use cordis_core::ConfigKey;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Theme(&'static str);

    static THEME: ConfigKey<Theme> = ConfigKey::new("test.theme-share@1");

    let runtime = runtime();
    let root = runtime.root();
    let a = root.intercept(THEME, Theme("dark")).unwrap();
    let b = root.intercept(THEME, Theme("light")).unwrap();

    let owner = a.effect().unwrap();
    owner.provide(NUMBER, Number(42)).unwrap();

    assert_eq!(root.get(NUMBER).unwrap().0, 42);
    assert_eq!(b.get(NUMBER).unwrap().0, 42);
    assert_eq!(a.config(THEME).unwrap().0, "dark");
    assert_eq!(b.config(THEME).unwrap().0, "light");

    owner.dispose();
    assert_service_unavailable(&root, NUMBER);
    assert_service_unavailable(&b, NUMBER);
}

#[tokio::test]
async fn extend_and_isolate_still_isolate_services() {
    let runtime = runtime();
    let root = runtime.root();

    let extended = root.extend().unwrap();
    let owner = extended.effect().unwrap();
    owner.provide(NUMBER, Number(2)).unwrap();
    assert_eq!(extended.get(NUMBER).unwrap().0, 2);
    // 子服务域的 provide 对父不可见。
    assert_service_unavailable(&root, NUMBER);

    root.provide(NUMBER, Number(1)).unwrap();
    // 未隔离的子域仍可沿父链看到父 Provider。
    let sibling = root.extend().unwrap();
    assert_eq!(sibling.get(NUMBER).unwrap().0, 1);

    let (isolated, _) = root.isolate(NUMBER).unwrap();
    assert_service_unavailable(&isolated, NUMBER);
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
    assert!(matches!(
        root.intercept(FLAG_AS_OTHER, OtherFlag),
        Err(CoreError::ConfigKeyTypeConflict { .. })
    ));
    for _ in 0..3 {
        assert!(matches!(
            root.intercept(FLAG_AS_OTHER, OtherFlag),
            Err(CoreError::ConfigKeyTypeConflict { .. })
        ));
    }
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

#[tokio::test]
async fn discarded_root_views_do_not_create_runtime_resources() {
    use cordis_core::ConfigKey;

    #[derive(Debug)]
    struct Flag;
    static FLAG: ConfigKey<Flag> = ConfigKey::new("test.discarded-view@1");

    let runtime = runtime();
    let root = runtime.root();
    let before = runtime.diagnostics();
    for _ in 0..1_000 {
        let view = root.extend().unwrap();
        let (view, label) = view.isolate(NUMBER).unwrap();
        let _ = view.isolate_with(NUMBER, label).unwrap();
        let _ = root.intercept(FLAG, Flag).unwrap();
    }
    let after = runtime.diagnostics();
    assert_eq!(after.providers.len(), before.providers.len());
    assert_eq!(after.effects.len(), before.effects.len());
    assert_eq!(after.plugin_fibers.len(), before.plugin_fibers.len());
    assert_eq!(after.inject_fibers.len(), before.inject_fibers.len());
}
