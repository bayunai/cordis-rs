//! Entry/子树差分 reconcile：目标索引、计划与生命周期执行。

use crate::{
    EntryTreeSource, SourceRevision,
    catalog::ExtensionCatalog,
    config::{EntryOptions, ExtensionsConfig},
    config_control::ConfigRevision,
    error::{LoaderError, format_panic_message},
    plugin::RuntimeEntry,
    prepared::{ConfigBinding, PreparedOutcome},
    snapshot::{EntryId, LoaderSnapshot},
    tree::{PreparedTreePlan, RuntimeTree},
};
use async_trait::async_trait;
use cordis_core::{Context, CoreError, FiberState, Plugin, PluginKey, ServiceId};
use std::{
    collections::{BTreeMap, BTreeSet},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

/// 目标树节点（预检索引；Plugin 按需填充）。
pub(crate) struct DesiredNode {
    parent: Option<EntryId>,
    options: EntryOptions,
    children: Vec<EntryId>,
    plugin: Option<Arc<dyn Plugin>>,
    plugin_key: Option<PluginKey>,
    binding: Option<Arc<dyn ConfigBinding>>,
}

pub(crate) struct ReconcilePlan {
    tree_updates: BTreeMap<EntryId, TreeCommit>,
    /// 整节点删除（含子树根）；执行时后序释放并移出 map。
    remove: BTreeSet<EntryId>,
    /// 仅释放 Fiber，保留 map 槽位（禁用）。
    dispose_fiber: BTreeSet<EntryId>,
    /// `Fiber::replace`。
    replace: BTreeSet<EntryId>,
    /// 新建或重新挂载 Fiber（含 Group 重建）。
    mount: BTreeSet<EntryId>,
    /// 需准备配置并选择构造/热提交的普通启用 Entry。
    build: BTreeSet<EntryId>,
    /// 目标前序。
    desired_order: Vec<EntryId>,
    hot: BTreeMap<EntryId, HotCommit>,
}

struct TreeCommit {
    state: Arc<crate::prepared::TreeState>,
    fiber_id: u64,
    binding: Arc<dyn ConfigBinding>,
    tree: Arc<RuntimeTree>,
    expected: ConfigRevision,
    old_revision: SourceRevision,
    source: Arc<dyn EntryTreeSource>,
    prepared: PreparedTreePlan,
    commit: Box<dyn FnOnce() + Send>,
}

struct HotCommit {
    fiber_id: u64,
    binding: Arc<dyn ConfigBinding>,
    commit: Box<dyn FnOnce() + Send>,
}

impl RuntimeTree {
    pub(crate) async fn execute_reconcile(
        &self,
        desired: BTreeMap<EntryId, DesiredNode>,
        plan: ReconcilePlan,
    ) -> Result<LoaderSnapshot, LoaderError> {
        let logger = self
            .owner
            .logger()
            .map_err(|source| LoaderError::Runtime { source })?;
        let mut lifecycle_error = None;
        // 1) 后序删除整节点
        let remove_order = postorder_among(&plan.remove, &self.order.lock().expect("entry order"));
        for path in remove_order {
            record_lifecycle_result(
                self.dispose_and_remove(&path).await,
                &mut lifecycle_error,
                &logger,
            )?;
        }

        // 2) 后序仅释放 Fiber（禁用）
        let dispose_order = postorder_among(
            &plan.dispose_fiber,
            &self.order.lock().expect("entry order"),
        );
        for path in dispose_order {
            record_lifecycle_result(
                self.dispose_fiber_only(&path).await,
                &mut lifecycle_error,
                &logger,
            )?;
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
            record_lifecycle_result(
                self.replace_entry(path, &desired).await,
                &mut lifecycle_error,
                &logger,
            )?;
        }

        // 5) 前序挂载
        for path in &plan.desired_order {
            if plan.mount.contains(path) {
                let result = self.mount_one(path, &desired).await;
                record_lifecycle_result(result, &mut lifecycle_error, &logger)?;
            }
        }

        // 独立条目均已尝试；失败不提交热值、desired 或文件 revision。
        if let Some(error) = lifecycle_error {
            return Err(error);
        }

        // 来源定义更新不是 watch 通知：必须等待子树协调完成，才能提交父树声明。
        for (path, update) in plan.tree_updates {
            let result = async {
                {
                    let entries = self.entries.lock().expect("entries");
                    let valid = entries.get(&path).is_some_and(|entry| {
                        entry.fiber.as_ref().is_some_and(|fiber| {
                            fiber.id() == update.fiber_id && fiber.state() == FiberState::Active
                        }) && entry
                            .binding
                            .as_ref()
                            .is_some_and(|binding| Arc::ptr_eq(binding, &update.binding))
                    });
                    if !valid {
                        return Err(LoaderError::HotConfigStale {
                            instance: path.to_string(),
                        });
                    }
                }
                let _operation = update.tree.operations.lock().await;
                update.tree.ensure_open()?;
                if update.expected != update.tree.config_revision() {
                    return Err(LoaderError::HotConfigStale {
                        instance: path.to_string(),
                    });
                }
                let old_source = update
                    .tree
                    .source
                    .lock()
                    .expect("source")
                    .clone()
                    .ok_or(LoaderError::NoConfigSource)?;
                old_source.check_revision(&update.old_revision)?;
                update.source.check_revision(
                    &update
                        .prepared
                        .revision
                        .clone()
                        .ok_or(LoaderError::NoConfigSource)?,
                )?;
                // 失败可能已改变部分子实例。相同旧定义重试也必须重新协调，不能误报无变化。
                update.state.dirty.store(true, Ordering::Release);
                update.tree.apply_plan(update.prepared).await?;
                // 只有成功协调后才激活来源自身的共享上下文策略，不提前发布候选元数据。
                update.source.context(&update.tree.owner)?;
                *update.tree.source.lock().expect("source") = Some(update.source);
                (update.commit)();
                update.state.dirty.store(false, Ordering::Release);
                Ok(())
            }
            .await;
            // 某个来源子树的业务失败仍须隔离，不能阻止其他独立来源定义更新。
            record_lifecycle_result(result, &mut lifecycle_error, &logger)?;
        }
        if let Some(error) = lifecycle_error {
            return Err(error);
        }

        // 所有生命周期成功后才提交热值，不等待业务订阅者。
        for (path, hot) in plan.hot {
            let entries = self.entries.lock().expect("entries");
            let current = entries.get(&path);
            let valid = !self.owner.is_disposed()
                && current.is_some_and(|entry| {
                    entry.fiber.as_ref().is_some_and(|fiber| {
                        fiber.id() == hot.fiber_id && fiber.state() == FiberState::Active
                    }) && entry
                        .binding
                        .as_ref()
                        .is_some_and(|binding| Arc::ptr_eq(binding, &hot.binding))
                });
            if !valid {
                return Err(LoaderError::HotConfigStale {
                    instance: path.to_string(),
                });
            }
            catch_unwind(AssertUnwindSafe(hot.commit)).map_err(|payload| {
                LoaderError::FactoryPanic {
                    message: format_panic_message("hot config commit", payload),
                }
            })?;
        }

        // 6) 纯排序 / 最终前序
        *self.order.lock().expect("entry order") = plan.desired_order;
        Ok(self.local_snapshot())
    }

    pub(crate) async fn dispose_and_remove(&self, path: &EntryId) -> Result<(), LoaderError> {
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
            entry.binding = None;
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
        let replacement = Arc::new(ConfiguredPlugin::new(plugin, dependencies));
        let replace_result = fiber.replace(replacement.clone()).await;
        let failed = fiber.state() == FiberState::Failed;
        let last_error = fiber.last_error();
        {
            let mut entries = self.entries.lock().expect("entries");
            let entry = entries.get_mut(path).expect("mounted replace target");
            // Busy / 旧 disposer 失败时，Core 可能尚未安装候选插件。
            // 成功返回（包括 Pending）或已进入候选 apply 才切换绑定。
            if replace_result.is_ok() || replacement.applied.load(Ordering::Acquire) {
                entry.binding = node.binding.clone();
            }
            if fiber.is_disposed() {
                entry.binding = None;
            }
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
                Some(parent) => entries
                    .get(parent)
                    .filter(|entry| {
                        !entry.context.is_disposed()
                            && entry.fiber.as_ref().is_some_and(|fiber| {
                                !matches!(fiber.state(), FiberState::Failed | FiberState::Disposed)
                            })
                    })
                    .map(|entry| entry.context.clone()),
                None => Some(self.owner.clone()),
            }
        };
        let parent_context = parent_context.ok_or_else(|| LoaderError::Lifecycle {
            instance: path.to_string(),
            source: CoreError::PluginApply(format!(
                "父条目 {} 未建立有效挂载上下文，不能挂载后代",
                node.parent
                    .as_ref()
                    .map(EntryId::as_str)
                    .unwrap_or("<root>"),
            )),
        })?;

        let stale = {
            let mut entries = self.entries.lock().expect("entries");
            entries.get_mut(path).and_then(|entry| {
                entry.binding = None;
                entry.fiber.take()
            })
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
        let context = self.inject_entry_location(path, context);

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
            RuntimeEntry {
                parent: node.parent.clone(),
                name: node.options.name.clone(),
                group: node.options.group,
                enabled,
                fiber,
                plugin_key,
                context,
                factory: (!node.options.group).then(|| node.options.name.clone()),
                options: node.options.clone(),
                binding: enabled.then(|| node.binding.clone()).flatten(),
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

// --- plan helpers ---

/// 每个业务错误均记录，调用方返回首个错误；非业务故障立即传播。
fn record_lifecycle_result(
    result: Result<(), LoaderError>,
    first_error: &mut Option<LoaderError>,
    logger: &cordis_core::Logger,
) -> Result<(), LoaderError> {
    match result {
        Err(error) if error.is_entry_lifecycle_failure() => {
            logger.error(error.to_string());
            first_error.get_or_insert(error);
            Ok(())
        }
        result => result,
    }
}

pub(crate) fn index_desired_tree(
    catalog: &ExtensionCatalog,
    config: &ExtensionsConfig,
    prefix: Option<&str>,
) -> Result<(BTreeMap<EntryId, DesiredNode>, Vec<EntryId>), LoaderError> {
    config.validate(catalog)?;
    let mut out = BTreeMap::new();
    let roots = index_entries(&config.extensions, None, prefix, &mut out)?;
    Ok((out, roots))
}

fn index_entries(
    entries: &[EntryOptions],
    parent: Option<&str>,
    prefix: Option<&str>,
    out: &mut BTreeMap<EntryId, DesiredNode>,
) -> Result<Vec<EntryId>, LoaderError> {
    let mut child_ids = Vec::new();
    for options in entries {
        let path = EntryId::from(match parent {
            Some(parent) => format!("{parent}:{id}", id = options.id),
            None => match prefix {
                Some(prefix) if !prefix.is_empty() => {
                    format!("{prefix}:{id}", id = options.id)
                }
                _ => options.id.clone(),
            },
        });
        // 子树顶层条目的 parent 为 None：mount 时挂到 RuntimeTree.owner。
        let node_parent = parent.map(EntryId::from);
        let children = if options.group {
            index_entries(&options.children()?, Some(path.as_str()), prefix, out)?
        } else {
            Vec::new()
        };
        child_ids.push(path.clone());
        out.insert(
            path.clone(),
            DesiredNode {
                parent: node_parent,
                options: options.clone(),
                children,
                plugin: None,
                plugin_key: None,
                binding: None,
            },
        );
    }
    Ok(child_ids)
}

pub(crate) fn plan_reconcile(
    mounted: &BTreeMap<EntryId, RuntimeEntry>,
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
            let unavailable = current.fiber.as_ref().is_none_or(|fiber| {
                matches!(fiber.state(), FiberState::Failed | FiberState::Disposed)
            });
            if unavailable
                || current.options.inject != node.options.inject
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
        tree_updates: BTreeMap::new(),
        remove,
        dispose_fiber,
        replace,
        mount,
        build,
        desired_order,
        hot: BTreeMap::new(),
    })
}

fn plan_group_disable_toggle(
    group: &EntryId,
    desired: &BTreeMap<EntryId, DesiredNode>,
    mounted: &BTreeMap<EntryId, RuntimeEntry>,
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
    mounted: &BTreeMap<EntryId, RuntimeEntry>,
) -> Vec<EntryId> {
    let mut out = Vec::new();
    fn walk(path: &EntryId, mounted: &BTreeMap<EntryId, RuntimeEntry>, out: &mut Vec<EntryId>) {
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
    plan: &mut ReconcilePlan,
    mounted: &Mutex<BTreeMap<EntryId, RuntimeEntry>>,
    mut prepared: Option<(EntryId, crate::PreparedExtension)>,
) -> Result<(), LoaderError> {
    // 只复制预检元数据，不复制拥有释放语义的 Fiber。调用扩展时不持有条目锁。
    struct BuildEntry {
        binding: Option<Arc<dyn ConfigBinding>>,
        active_fiber: Option<u64>,
        group: bool,
        factory: Option<String>,
        plugin_key: Option<PluginKey>,
    }
    let mounted: BTreeMap<_, _> = mounted
        .lock()
        .expect("entries")
        .iter()
        .map(|(path, entry)| {
            (
                path.clone(),
                BuildEntry {
                    binding: entry.binding.clone(),
                    active_fiber: entry
                        .fiber
                        .as_ref()
                        .filter(|fiber| fiber.state() == FiberState::Active)
                        .map(|fiber| fiber.id()),
                    group: entry.group,
                    factory: entry.factory.clone(),
                    plugin_key: entry.plugin_key,
                },
            )
        })
        .collect();
    let mut candidates = plan.build.clone();
    for (path, entry) in mounted.iter() {
        if entry.binding.is_some()
            && desired.contains_key(path)
            && !plan.remove.contains(path)
            && !plan.mount.contains(path)
            && !plan.dispose_fiber.contains(path)
            && entry.active_fiber.is_some()
        {
            candidates.insert(path.clone());
        }
    }
    for path in &candidates {
        let node = desired.get_mut(path).expect("build target");
        let factory = catalog.get(&node.options.name).expect("validated factory");
        let current = mounted.get(path).filter(|entry| {
            !plan.mount.contains(path)
                && !plan.dispose_fiber.contains(path)
                && !plan.remove.contains(path)
                && entry.active_fiber.is_some()
        });
        let result = catch_unwind(AssertUnwindSafe(|| {
            // 表单编辑已校验过目标配置；消费同一份预备结果，避免再次执行 prepare。
            // 禁用或无需构造的目标不会进入 candidates，其闭包随预备结果释放。
            let candidate = if prepared.as_ref().is_some_and(|(id, _)| id == path) {
                prepared.take().expect("prepared target").1
            } else {
                factory.prepare(&node.options.config)?
            };
            candidate.resolve(current.and_then(|entry| entry.binding.as_ref()))
        }))
        .map_err(|payload| LoaderError::FactoryPanic {
            message: format_panic_message("extension factory prepare/construct", payload),
        })?
        .map_err(|error| LoaderError::PluginBuild {
            instance: path.to_string(),
            factory: node.options.name.clone(),
            message: error.to_string(),
        })?;
        let (plugin, binding) = match result {
            PreparedOutcome::TreeUpdate {
                binding,
                state,
                open,
                commit,
            } => {
                let (ctx, subtree) = state
                    .mounted
                    .lock()
                    .expect("tree mounted")
                    .clone()
                    .ok_or_else(|| LoaderError::HotConfigStale {
                        instance: path.to_string(),
                    })?;
                let tree = subtree.tree.clone();
                tree.ensure_open()?;
                let expected = tree.config_revision();
                let old_revision = tree
                    .revision
                    .lock()
                    .expect("revision")
                    .clone()
                    .ok_or(LoaderError::NoConfigSource)?;
                let old_source = tree
                    .source
                    .lock()
                    .expect("source")
                    .clone()
                    .ok_or(LoaderError::NoConfigSource)?;
                old_source.check_revision(&old_revision)?;
                let source = open(&ctx)?;
                if Some(source.identity()) != tree.source_id {
                    return Err(crate::SourceError::new(
                        crate::SourceErrorKind::Conflict,
                        "来源身份改变，必须重建树载体",
                    )
                    .into());
                }
                let document = source.read()?;
                let prepared = tree.prepare_plan(document.config, Some(document.revision), None)?;
                let fiber_id = current
                    .and_then(|entry| entry.active_fiber)
                    .expect("active tree fiber");
                plan.replace.remove(path);
                plan.tree_updates.insert(
                    path.clone(),
                    TreeCommit {
                        state,
                        fiber_id,
                        binding,
                        tree,
                        expected,
                        old_revision,
                        source,
                        prepared,
                        commit,
                    },
                );
                continue;
            }
            PreparedOutcome::Hot { binding, commit } => {
                let fiber_id = current
                    .and_then(|entry| entry.active_fiber)
                    .expect("hot active fiber");
                plan.replace.remove(path);
                if let Some(commit) = commit {
                    plan.hot.insert(
                        path.clone(),
                        HotCommit {
                            fiber_id,
                            binding,
                            commit,
                        },
                    );
                }
                continue;
            }
            PreparedOutcome::Plugin { plugin, binding } => (plugin, binding),
        };
        // Binding 不同但原始 options 相同仍必须替换（失败后的实际配置差分）。
        if !plan.mount.contains(path) {
            plan.replace.insert(path.clone());
        }
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
        node.binding = binding;
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
    applied: AtomicBool,
}

impl ConfiguredPlugin {
    fn new(inner: Arc<dyn Plugin>, dependencies: Vec<ServiceId>) -> Self {
        Self {
            inner,
            dependencies,
            applied: AtomicBool::new(false),
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
        self.applied.store(true, Ordering::Release);
        self.inner.apply(ctx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cordis_core::Runtime;

    struct FailedParent;
    #[async_trait]
    impl Plugin for FailedParent {
        fn key(&self) -> PluginKey {
            PluginKey::new("failed.parent")
        }
        async fn apply(&self, _: &Context) -> Result<(), CoreError> {
            Err(CoreError::PluginApply("parent failed".into()))
        }
    }

    #[tokio::test]
    async fn missing_or_failed_parent_never_mounts_descendant_in_root_context() {
        let runtime = Runtime::new().unwrap();
        let parent = runtime.root().plugin(Arc::new(FailedParent)).await.unwrap();
        let tree = RuntimeTree::new_root(ExtensionCatalog::new(), runtime.root()).unwrap();
        tree.entries.lock().unwrap().insert(
            EntryId::from("blocked"),
            RuntimeEntry {
                parent: None,
                name: "cordis:group".into(),
                group: true,
                enabled: true,
                plugin_key: Some(PluginKey::new("failed.parent")),
                fiber: Some(parent),
                context: runtime.root(),
                factory: None,
                options: EntryOptions::group("blocked", vec![]).unwrap(),
                binding: None,
            },
        );
        let path = EntryId::from("blocked:child");
        let desired = BTreeMap::from([(
            path.clone(),
            DesiredNode {
                parent: Some(EntryId::from("blocked")),
                options: EntryOptions::new("child", "unused"),
                children: vec![],
                plugin: None,
                plugin_key: None,
                binding: None,
            },
        )]);
        for _ in 0..2 {
            let error = tree.mount_one(&path, &desired).await.unwrap_err();
            assert!(error.is_entry_lifecycle_failure());
            assert!(error.to_string().contains("父条目 blocked"));
            assert!(!tree.entries.lock().unwrap().contains_key(&path));
            tree.entries
                .lock()
                .unwrap()
                .remove(&EntryId::from("blocked"));
        }
        runtime.shutdown().await.unwrap();
    }
}
