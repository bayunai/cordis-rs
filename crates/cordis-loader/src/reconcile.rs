//! Entry/子树差分 reconcile：目标索引、计划与生命周期执行。

use crate::{
    catalog::ExtensionCatalog,
    config::{EntryOptions, ExtensionsConfig},
    error::{LoaderError, format_panic_message},
    plugin::{LoaderInner, MountedEntry},
    snapshot::{EntryId, LoaderSnapshot},
};
use async_trait::async_trait;
use cordis_core::{Context, CoreError, FiberState, Plugin, PluginKey, ServiceId};
use std::{
    collections::{BTreeMap, BTreeSet},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

/// 目标树节点（预检索引；Plugin 按需填充）。
pub(crate) struct DesiredNode {
    parent: Option<EntryId>,
    options: EntryOptions,
    children: Vec<EntryId>,
    plugin: Option<Arc<dyn Plugin>>,
    plugin_key: Option<PluginKey>,
}

pub(crate) struct ReconcilePlan {
    /// 整节点删除（含子树根）；执行时后序释放并移出 map。
    remove: BTreeSet<EntryId>,
    /// 仅释放 Fiber，保留 map 槽位（禁用）。
    dispose_fiber: BTreeSet<EntryId>,
    /// `Fiber::replace`。
    replace: BTreeSet<EntryId>,
    /// 新建或重新挂载 Fiber（含 Group 重建）。
    mount: BTreeSet<EntryId>,
    /// 需 `factory.build` 的普通启用 Entry。
    build: BTreeSet<EntryId>,
    /// 目标前序。
    desired_order: Vec<EntryId>,
}

impl LoaderInner {
    pub(crate) async fn execute_reconcile(
        &self,
        desired: BTreeMap<EntryId, DesiredNode>,
        plan: ReconcilePlan,
    ) -> Result<LoaderSnapshot, LoaderError> {
        // 1) 后序删除整节点
        let remove_order = postorder_among(&plan.remove, &self.order.lock().expect("entry order"));
        for path in remove_order {
            self.dispose_and_remove(&path).await?;
        }

        // 2) 后序仅释放 Fiber（禁用）
        let dispose_order = postorder_among(
            &plan.dispose_fiber,
            &self.order.lock().expect("entry order"),
        );
        for path in dispose_order {
            self.dispose_fiber_only(&path).await?;
        }

        // 3) 同步保留节点上的 options / enabled（含 Group disabled 开关）
        {
            let mut entries = self.entries.lock().expect("entries");
            for (path, node) in &desired {
                if let Some(entry) = entries.get_mut(path) {
                    let parent_disabled = ancestor_disabled(path, &desired);
                    entry.options = node.options.clone();
                    entry.name = node.options.name.clone();
                    entry.enabled = !parent_disabled && !node.options.disabled;
                    if entry.group {
                        entry.factory = None;
                    } else {
                        entry.factory = Some(node.options.name.clone());
                    }
                }
            }
        }

        // 4) replace
        for path in &plan.replace {
            self.replace_entry(path, &desired).await?;
        }

        // 5) 前序挂载
        for path in &plan.desired_order {
            if plan.mount.contains(path) {
                self.mount_one(path, &desired).await?;
            }
        }

        // 6) 纯排序 / 最终前序
        *self.order.lock().expect("entry order") = plan.desired_order;
        Ok(self.snapshot())
    }

    async fn dispose_and_remove(&self, path: &EntryId) -> Result<(), LoaderError> {
        let mut fiber = {
            let mut entries = self.entries.lock().expect("entries");
            let Some(mut entry) = entries.remove(path) else {
                return Ok(());
            };
            entry.fiber.take()
        };
        if let Some(mut fiber) = fiber.take() {
            fiber
                .dispose_wait()
                .await
                .map_err(|source| LoaderError::Lifecycle {
                    instance: path.to_string(),
                    source,
                })?;
        }
        self.order
            .lock()
            .expect("entry order")
            .retain(|item| item != path);
        Ok(())
    }

    async fn dispose_fiber_only(&self, path: &EntryId) -> Result<(), LoaderError> {
        let mut fiber = {
            let mut entries = self.entries.lock().expect("entries");
            let Some(entry) = entries.get_mut(path) else {
                return Ok(());
            };
            entry.enabled = false;
            entry.plugin_key = None;
            entry.fiber.take()
        };
        if let Some(mut fiber) = fiber.take() {
            fiber
                .dispose_wait()
                .await
                .map_err(|source| LoaderError::Lifecycle {
                    instance: path.to_string(),
                    source,
                })?;
        }
        Ok(())
    }

    async fn replace_entry(
        &self,
        path: &EntryId,
        desired: &BTreeMap<EntryId, DesiredNode>,
    ) -> Result<(), LoaderError> {
        let node = desired.get(path).expect("replace target");
        let plugin = node.plugin.clone().expect("replace plugin");
        let dependencies = {
            let entries = self.entries.lock().expect("entries");
            let entry = entries.get(path).expect("mounted replace target");
            let (_, dependencies) = self.catalog.resolve_injections(
                node.options.inject.as_ref(),
                path.as_str(),
                entry.context.clone(),
            )?;
            dependencies
        };
        let mut fiber = {
            let mut entries = self.entries.lock().expect("entries");
            let entry = entries.get_mut(path).expect("mounted replace target");
            entry.options = node.options.clone();
            entry.fiber.take().expect("replace requires existing fiber")
        };
        let replace_result = fiber
            .replace(Arc::new(ConfiguredPlugin::new(plugin, dependencies)))
            .await;
        let failed = fiber.state() == FiberState::Failed;
        let last_error = fiber.last_error();
        {
            let mut entries = self.entries.lock().expect("entries");
            let entry = entries.get_mut(path).expect("mounted replace target");
            if replace_result.is_ok() && !failed {
                entry.plugin_key = node.plugin_key;
            }
            entry.fiber = Some(fiber);
        }
        replace_result.map_err(|source| LoaderError::Lifecycle {
            instance: path.to_string(),
            source,
        })?;
        if failed {
            return Err(LoaderError::Lifecycle {
                instance: path.to_string(),
                source: CoreError::PluginApply(
                    last_error.unwrap_or_else(|| "plugin replace failed".into()),
                ),
            });
        }
        Ok(())
    }

    async fn mount_one(
        &self,
        path: &EntryId,
        desired: &BTreeMap<EntryId, DesiredNode>,
    ) -> Result<(), LoaderError> {
        let node = desired.get(path).expect("mount target");
        let parent_disabled = ancestor_disabled(path, desired);
        let enabled = !parent_disabled && !node.options.disabled;

        let parent_context = {
            let entries = self.entries.lock().expect("entries");
            match &node.parent {
                Some(parent) => entries.get(parent).map(|entry| entry.context.clone()),
                None => Some(self.context.clone()),
            }
        };
        let parent_context = parent_context.ok_or_else(|| LoaderError::UnknownInstance {
            instance: node
                .parent
                .as_ref()
                .map(|parent| parent.to_string())
                .unwrap_or_default(),
        })?;

        // 已存在槽位（例如禁用后重新启用）：先丢掉旧 fiber（应已空）。
        let stale = {
            let mut entries = self.entries.lock().expect("entries");
            entries.get_mut(path).and_then(|entry| entry.fiber.take())
        };
        if let Some(mut fiber) = stale {
            fiber
                .dispose_wait()
                .await
                .map_err(|source| LoaderError::Lifecycle {
                    instance: path.to_string(),
                    source,
                })?;
        }

        let domain = {
            let mut labels = self.named_labels.lock().expect("named labels");
            self.catalog.resolve_isolations(
                node.options.isolate.as_ref(),
                path.as_str(),
                parent_context,
                &mut labels,
            )?
        };
        let (context, dependencies) =
            self.catalog
                .resolve_injections(node.options.inject.as_ref(), path.as_str(), domain)?;

        let (fiber, plugin_key, mount_failed, mount_error) = if node.options.group {
            let fiber = context
                .plugin(Arc::new(ConfiguredPlugin::new(
                    Arc::new(GroupPlugin),
                    dependencies,
                )))
                .await
                .map_err(|source| LoaderError::Lifecycle {
                    instance: path.to_string(),
                    source,
                })?;
            let failed = fiber.state() == FiberState::Failed;
            let last_error = fiber.last_error();
            (
                Some(fiber),
                Some(PluginKey::new("cordis.loader.group")),
                failed,
                last_error,
            )
        } else if enabled {
            let plugin = node
                .plugin
                .clone()
                .expect("enabled ordinary entry must be built");
            let fiber = context
                .plugin(Arc::new(ConfiguredPlugin::new(plugin, dependencies)))
                .await
                .map_err(|source| LoaderError::Lifecycle {
                    instance: path.to_string(),
                    source,
                })?;
            let failed = fiber.state() == FiberState::Failed;
            let last_error = fiber.last_error();
            (Some(fiber), node.plugin_key, failed, last_error)
        } else {
            (None, None, false, None)
        };

        let mut entries = self.entries.lock().expect("entries");
        entries.insert(
            path.clone(),
            MountedEntry {
                parent: node.parent.clone(),
                name: node.options.name.clone(),
                group: node.options.group,
                enabled,
                fiber,
                plugin_key,
                context,
                factory: (!node.options.group).then(|| node.options.name.clone()),
                options: node.options.clone(),
            },
        );
        drop(entries);
        if mount_failed {
            return Err(LoaderError::Lifecycle {
                instance: path.to_string(),
                source: CoreError::PluginApply(
                    mount_error.unwrap_or_else(|| "plugin apply failed".into()),
                ),
            });
        }
        Ok(())
    }
}

pub(crate) fn index_desired_tree(
    catalog: &ExtensionCatalog,
    config: &ExtensionsConfig,
) -> Result<(BTreeMap<EntryId, DesiredNode>, Vec<EntryId>), LoaderError> {
    config.validate(catalog)?;
    let mut out = BTreeMap::new();
    let roots = index_entries(&config.extensions, None, &mut out)?;
    Ok((out, roots))
}

fn index_entries(
    entries: &[EntryOptions],
    parent: Option<&str>,
    out: &mut BTreeMap<EntryId, DesiredNode>,
) -> Result<Vec<EntryId>, LoaderError> {
    let mut child_ids = Vec::new();
    for options in entries {
        let path = EntryId::from(parent.map_or_else(
            || options.id.clone(),
            |parent| format!("{parent}:{id}", id = options.id),
        ));
        let children = if options.group {
            index_entries(&options.children()?, Some(path.as_str()), out)?
        } else {
            Vec::new()
        };
        child_ids.push(path.clone());
        out.insert(
            path.clone(),
            DesiredNode {
                parent: parent.map(EntryId::from),
                options: options.clone(),
                children,
                plugin: None,
                plugin_key: None,
            },
        );
    }
    Ok(child_ids)
}

pub(crate) fn plan_reconcile(
    mounted: &BTreeMap<EntryId, MountedEntry>,
    desired: &BTreeMap<EntryId, DesiredNode>,
    roots: &[EntryId],
) -> Result<ReconcilePlan, LoaderError> {
    let mut remove = BTreeSet::new();
    let mut dispose_fiber = BTreeSet::new();
    let mut replace = BTreeSet::new();
    let mut mount = BTreeSet::new();
    let mut build = BTreeSet::new();
    let mut rebuild_subtrees = BTreeSet::new();

    let desired_order = preorder_from_desired(desired, roots);

    for path in mounted.keys() {
        if !desired.contains_key(path) {
            remove.insert(path.clone());
        }
    }

    for (path, node) in desired {
        let Some(current) = mounted.get(path) else {
            mount.insert(path.clone());
            if !node.options.group {
                let parent_disabled = ancestor_disabled(path, desired);
                if !parent_disabled && !node.options.disabled {
                    build.insert(path.clone());
                }
            } else {
                // Group 新建时整棵子树由 mount 前序创建；子节点也会在本循环中各自加入 mount。
            }
            continue;
        };

        if current.group != node.options.group {
            return Err(LoaderError::InvalidEntry {
                path: path.to_string(),
                message: "不允许在同一路径上将普通 Entry 与 Group 互换".into(),
            });
        }

        if !node.options.group {
            let factory = current.factory.as_deref().unwrap_or("");
            if factory != node.options.name {
                return Err(LoaderError::FactoryChanged {
                    instance: path.to_string(),
                    from: factory.into(),
                    to: node.options.name.clone(),
                });
            }
        }

        if node.options.group {
            if current.options.inject != node.options.inject
                || current.options.isolate != node.options.isolate
            {
                rebuild_subtrees.insert(path.clone());
            } else {
                let parent_disabled = ancestor_disabled(path, desired);
                let want_enabled = !parent_disabled && !node.options.disabled;
                if current.options.disabled != node.options.disabled
                    || current.enabled != want_enabled
                {
                    plan_group_disable_toggle(
                        path,
                        desired,
                        mounted,
                        &mut dispose_fiber,
                        &mut mount,
                        &mut build,
                    );
                }
            }
        } else {
            let parent_disabled = ancestor_disabled(path, desired);
            let want_enabled = !parent_disabled && !node.options.disabled;
            let inject_changed = current.options.inject != node.options.inject;
            let isolate_changed = current.options.isolate != node.options.isolate;
            let config_changed = current.options.config != node.options.config;
            let failed = current
                .fiber
                .as_ref()
                .is_some_and(|fiber| fiber.state() == FiberState::Failed);
            let disposed = current
                .fiber
                .as_ref()
                .is_some_and(|fiber| fiber.is_disposed());

            if inject_changed
                || isolate_changed
                || current.enabled != want_enabled
                || (want_enabled && (current.fiber.is_none() || disposed))
            {
                if current.fiber.is_some() {
                    dispose_fiber.insert(path.clone());
                }
                if want_enabled {
                    mount.insert(path.clone());
                    build.insert(path.clone());
                }
            } else if want_enabled && (config_changed || failed) {
                replace.insert(path.clone());
                build.insert(path.clone());
            }
        }
    }

    let rebuild_paths: Vec<_> = rebuild_subtrees.iter().cloned().collect();
    for root in rebuild_paths {
        for path in subtree_paths_mounted(&root, mounted) {
            remove.insert(path);
        }
        for path in subtree_paths_desired(&root, desired) {
            mount.insert(path.clone());
            dispose_fiber.remove(&path);
            replace.remove(&path);
            if let Some(node) = desired.get(&path)
                && !node.options.group
            {
                let parent_disabled = ancestor_disabled(&path, desired);
                if !parent_disabled && !node.options.disabled {
                    build.insert(path);
                } else {
                    build.remove(&path);
                }
            }
        }
    }

    for path in remove.iter().cloned().collect::<Vec<_>>() {
        if !mount.contains(&path) {
            replace.remove(&path);
            dispose_fiber.remove(&path);
            build.remove(&path);
        }
    }

    // dispose_fiber 与 mount 同 path：先释放再挂载（启停/inject 变更）。
    // remove 与 mount 同 path：子树重建，先删后挂。

    Ok(ReconcilePlan {
        remove,
        dispose_fiber,
        replace,
        mount,
        build,
        desired_order,
    })
}

fn plan_group_disable_toggle(
    group: &EntryId,
    desired: &BTreeMap<EntryId, DesiredNode>,
    mounted: &BTreeMap<EntryId, MountedEntry>,
    dispose_fiber: &mut BTreeSet<EntryId>,
    mount: &mut BTreeSet<EntryId>,
    build: &mut BTreeSet<EntryId>,
) {
    for path in subtree_paths_desired(group, desired) {
        if &path == group {
            continue;
        }
        let Some(node) = desired.get(&path) else {
            continue;
        };
        let parent_disabled = ancestor_disabled(&path, desired);
        let want_enabled = !parent_disabled && !node.options.disabled;
        let currently_enabled = mounted.get(&path).is_some_and(|entry| entry.enabled);
        if currently_enabled && !want_enabled {
            dispose_fiber.insert(path);
        } else if !currently_enabled && want_enabled {
            mount.insert(path.clone());
            if !node.options.group {
                build.insert(path);
            }
        }
    }
}

fn subtree_paths_mounted(
    root: &EntryId,
    mounted: &BTreeMap<EntryId, MountedEntry>,
) -> Vec<EntryId> {
    let mut out = Vec::new();
    fn walk(path: &EntryId, mounted: &BTreeMap<EntryId, MountedEntry>, out: &mut Vec<EntryId>) {
        let children: Vec<_> = mounted
            .iter()
            .filter(|(_, entry)| entry.parent.as_ref() == Some(path))
            .map(|(child, _)| child.clone())
            .collect();
        for child in children {
            walk(&child, mounted, out);
        }
        out.push(path.clone());
    }
    walk(root, mounted, &mut out);
    out
}

fn subtree_paths_desired(root: &EntryId, desired: &BTreeMap<EntryId, DesiredNode>) -> Vec<EntryId> {
    let mut out = Vec::new();
    fn walk(path: &EntryId, desired: &BTreeMap<EntryId, DesiredNode>, out: &mut Vec<EntryId>) {
        if let Some(node) = desired.get(path) {
            for child in &node.children {
                walk(child, desired, out);
            }
        }
        out.push(path.clone());
    }
    walk(root, desired, &mut out);
    out
}

fn preorder_from_desired(
    desired: &BTreeMap<EntryId, DesiredNode>,
    roots: &[EntryId],
) -> Vec<EntryId> {
    let mut out = Vec::new();
    fn walk(path: &EntryId, desired: &BTreeMap<EntryId, DesiredNode>, out: &mut Vec<EntryId>) {
        out.push(path.clone());
        if let Some(node) = desired.get(path) {
            for child in &node.children {
                walk(child, desired, out);
            }
        }
    }
    for root in roots {
        walk(root, desired, &mut out);
    }
    out
}

pub(crate) fn build_plan_plugins(
    catalog: &ExtensionCatalog,
    desired: &mut BTreeMap<EntryId, DesiredNode>,
    plan: &ReconcilePlan,
    mounted: &Mutex<BTreeMap<EntryId, MountedEntry>>,
) -> Result<(), LoaderError> {
    let mounted = mounted.lock().expect("entries");
    for path in &plan.build {
        let node = desired.get_mut(path).expect("build target");
        let factory = catalog.get(&node.options.name).expect("validated factory");
        let plugin = catch_unwind(AssertUnwindSafe(|| factory.build(&node.options.config)))
            .map_err(|payload| LoaderError::FactoryPanic {
                message: format_panic_message("extension factory build", payload),
            })?
            .map_err(|error| LoaderError::PluginBuild {
                instance: path.to_string(),
                factory: node.options.name.clone(),
                message: error.to_string(),
            })?;
        let plugin_key = catch_unwind(AssertUnwindSafe(|| plugin.key())).map_err(|payload| {
            LoaderError::PluginPanic {
                message: format_panic_message("plugin key", payload),
            }
        })?;
        if let Some(current) = mounted.get(path)
            && !current.group
            && current.factory.as_deref() == Some(node.options.name.as_str())
            && let Some(expected) = current.plugin_key
            && expected != plugin_key
        {
            return Err(LoaderError::PluginKeyChanged {
                instance: path.to_string(),
                expected,
                actual: plugin_key,
            });
        }
        node.plugin = Some(plugin);
        node.plugin_key = Some(plugin_key);
    }
    Ok(())
}

fn ancestor_disabled(path: &EntryId, desired: &BTreeMap<EntryId, DesiredNode>) -> bool {
    let mut current = desired.get(path).and_then(|node| node.parent.clone());
    while let Some(parent) = current {
        let Some(node) = desired.get(&parent) else {
            break;
        };
        if node.options.disabled {
            return true;
        }
        current = node.parent.clone();
    }
    false
}

fn postorder_among(paths: &BTreeSet<EntryId>, current_order: &[EntryId]) -> Vec<EntryId> {
    let mut result = current_order
        .iter()
        .rev()
        .filter(|path| paths.contains(path))
        .cloned()
        .collect::<Vec<_>>();
    // 失败 reconcile 可能已插入节点、但尚未来得及提交 order。它们也必须在
    // 下一次收敛中被释放；按路径深度倒序保证父节点晚于后代处理。
    let mut unlisted = paths
        .iter()
        .filter(|path| !result.contains(path))
        .cloned()
        .collect::<Vec<_>>();
    unlisted.sort_by(|left, right| {
        right
            .as_str()
            .matches(':')
            .count()
            .cmp(&left.as_str().matches(':').count())
            .then_with(|| left.cmp(right))
    });
    result.extend(unlisted);
    result
}

struct GroupPlugin;

#[async_trait]
impl Plugin for GroupPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("cordis.loader.group")
    }

    async fn apply(&self, _: &Context) -> Result<(), CoreError> {
        Ok(())
    }
}

struct ConfiguredPlugin {
    inner: Arc<dyn Plugin>,
    dependencies: Vec<ServiceId>,
}

impl ConfiguredPlugin {
    fn new(inner: Arc<dyn Plugin>, dependencies: Vec<ServiceId>) -> Self {
        Self {
            inner,
            dependencies,
        }
    }
}

#[async_trait]
impl Plugin for ConfiguredPlugin {
    fn key(&self) -> PluginKey {
        self.inner.key()
    }

    fn inject(&self) -> Vec<ServiceId> {
        let mut dependencies = self.dependencies.clone();
        for dependency in self.inner.inject() {
            if !dependencies.contains(&dependency) {
                dependencies.push(dependency);
            }
        }
        dependencies
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        self.inner.apply(ctx).await
    }
}
