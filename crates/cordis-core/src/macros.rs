//! 插件定义的声明辅助；展开后使用既有的身份与延迟初始化机制。

/// 声明一个共享的具名插件定义。
///
/// 展开为 `std::sync::LazyLock<PluginDefinition>`，首次访问时初始化一次。
/// 每个声明拥有独立身份，即使名称相同；克隆定义句柄则共享身份。
/// 调用方无需导入 `LazyLock` 或 `PluginDefinition`。
///
/// ```
/// cordis_core::plugin_definition!(
///     /// 此插件在当前进程内共享的定义。
///     pub static PLUGIN_DEFINITION = "example.storage";
/// );
///
/// let cloned = PLUGIN_DEFINITION.clone();
/// assert_eq!(cloned.id(), PLUGIN_DEFINITION.id());
/// assert_eq!(PLUGIN_DEFINITION.name(), Some("example.storage"));
/// ```
///
/// 支持 Rust 可见性、属性和函数内声明；声明末尾的分号可省略。
/// 动态名称与匿名定义继续使用 [`crate::PluginDefinition`] 的构造函数。
/// 名称须为字符串字面量，不接受运行时表达式或其他类型：
///
/// ```compile_fail
/// cordis_core::plugin_definition!(static INVALID = 123;);
/// ```
///
/// ```compile_fail
/// let name = String::from("example.storage");
/// cordis_core::plugin_definition!(static INVALID = name;);
/// ```
///
/// 只接受单条 `static` 定义，不接受 `const` 或批量声明：
///
/// ```compile_fail
/// cordis_core::plugin_definition!(pub const INVALID = "example.storage";);
/// ```
#[macro_export]
macro_rules! plugin_definition {
    ($(#[$meta:meta])* $vis:vis static $name:ident = $label:literal $(;)?) => {
        $(#[$meta])*
        $vis static $name: ::std::sync::LazyLock<$crate::PluginDefinition> =
            ::std::sync::LazyLock::new(|| $crate::PluginDefinition::named($label));
    };
}
