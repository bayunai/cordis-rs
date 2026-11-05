//! 在一个取消安全的控制操作中协调两棵树的归属、释放和写回。
use super::*;
use crate::{SourceError, SourceErrorKind};
use std::sync::atomic::Ordering;

struct SnapshotBatch(Arc<LoaderInner>);
impl SnapshotBatch {
    fn new(loader: Arc<LoaderInner>) -> Self {
        *loader.batching_snapshot.lock().expect("snapshot batch") = true;
        Self(loader)
    }
}
impl Drop for SnapshotBatch {
    fn drop(&mut self) {
        *self.0.batching_snapshot.lock().expect("snapshot batch") = false;
        self.0.publish_snapshot_changed();
    }
}

pub(super) async fn update(
    loader: Arc<LoaderInner>,
    path: String,
    patch: EntryUpdate,
    parent: Option<Option<String>>,
    position: Option<usize>,
    expected_origin: Option<Arc<RuntimeTree>>,
) -> Result<LoaderSnapshot, LoaderControlError> {
    tokio::spawn(async move {
        let _control = loader.config_operations.lock().await;
        if !loader.alive.load(Ordering::Acquire) {
            return Err(LoaderControlError::LoaderUnavailable);
        }
        let (origin, local) = resolve_entry_path(&loader, &path);
        if expected_origin
            .as_ref()
            .is_some_and(|expected| !Arc::ptr_eq(expected, &origin))
        {
            return Err(LoaderControlError::LoaderUnavailable);
        }
        let (destination, target_parent) = match &parent {
            None => (origin.clone(), None),
            Some(None) => (loader.root.clone(), None),
            Some(Some(parent)) => resolve_parent_path(&loader, parent),
        };
        // 固定树实例顺序；同树只取一次锁。
        let (first, second) = if origin.config_identity <= destination.config_identity {
            (&origin, &destination)
        } else {
            (&destination, &origin)
        };
        let _first = first.operations.lock().await;
        let _second = if Arc::ptr_eq(first, second) {
            None
        } else {
            Some(second.operations.lock().await)
        };
        origin.ensure_open()?;
        destination.ensure_open()?;
        origin.await_idle().await?;
        destination.await_idle().await?;
        ensure_tree_revision_control(&origin).await?;
        if !Arc::ptr_eq(&origin, &destination) {
            ensure_tree_revision_control(&destination).await?;
        }
        let mut source_config = origin.desired.lock().expect("desired").clone();
        if Arc::ptr_eq(&origin, &destination) {
            if parent.is_some() {
                move_local(
                    &mut source_config,
                    &local,
                    target_parent.as_deref(),
                    position,
                )?;
            }
            patch_entry(&mut source_config.extensions, &local, patch)?;
            if parent.is_none() {
                return commit_tree_locked(&origin, source_config, None, None)
                    .await
                    .map(|_| loader.aggregate_snapshot());
            }
            validate_write(&origin, &source_config).await?;
            let plan = origin
                .prepare_plan(source_config.clone(), None, None, None, false, false)
                .await?;
            let _batch = SnapshotBatch::new(loader.clone());
            let result = origin.apply_plan(plan).await;
            *origin.desired.lock().expect("desired") = source_config.clone();
            let lifecycle_errors: Vec<_> = result.err().into_iter().collect();
            if !lifecycle_errors.is_empty() {
                origin.mark_unsettled();
            }
            let destination = persist(&origin, &source_config).await;
            let snapshot = loader.aggregate_snapshot();
            return if !lifecycle_errors.is_empty()
                || matches!(destination, MovePersistence::Failed { .. })
            {
                Err(LoaderControlError::Move {
                    snapshot: Box::new(snapshot),
                    origin: MovePersistence::NotRequired,
                    destination,
                    lifecycle_errors,
                })
            } else {
                Ok(snapshot)
            };
        }
        let mut target_config = destination.desired.lock().expect("desired").clone();
        let moving = find_entry(&source_config.extensions, &local)?;
        reject_descendant_tree(&origin, &destination, &moving, &path)?;
        let entry = remove_entry(&mut source_config.extensions, &local)?;
        insert_entry(
            &mut target_config.extensions,
            target_parent.as_deref(),
            position,
            entry,
        )?;
        patch_entry(&mut target_config.extensions, &local, patch)?;
        source_config.validate(&origin.catalog)?;
        target_config.validate(&destination.catalog)?;
        validate_write(&origin, &source_config).await?;
        validate_write(&destination, &target_config).await?;
        // 两侧工厂构造、Schema 与已有子树更新均在释放前完成预检。
        let source_plan = origin
            .prepare_plan(source_config.clone(), None, None, None, false, false)
            .await?;
        let target_plan = destination
            .prepare_plan(target_config.clone(), None, None, None, false, false)
            .await?;
        origin.ensure_open()?;
        destination.ensure_open()?;
        let _batch = SnapshotBatch::new(loader.clone());
        let mut lifecycle_errors = Vec::new();
        if let Err(error) = origin.apply_plan(source_plan).await {
            lifecycle_errors.push(error);
        }
        if let Err(error) = destination.apply_plan(target_plan).await {
            lifecycle_errors.push(error);
        }
        // 一旦通过预检并进入释放阶段，声明归属只保留在目标树，失败实例可显式恢复。
        *origin.desired.lock().expect("desired") = source_config.clone();
        *destination.desired.lock().expect("desired") = target_config.clone();
        if !lifecycle_errors.is_empty() {
            origin.mark_unsettled();
            destination.mark_unsettled();
        }
        let destination_status = persist(&destination, &target_config).await;
        let origin_status = if matches!(destination_status, MovePersistence::Failed { .. }) {
            origin.mark_unsettled();
            match origin.source.lock().expect("source").as_ref() {
                Some(source) => MovePersistence::NotAttempted {
                    source_id: source.identity(),
                },
                None => MovePersistence::NotRequired,
            }
        } else {
            persist(&origin, &source_config).await
        };
        let failed = !lifecycle_errors.is_empty()
            || matches!(destination_status, MovePersistence::Failed { .. })
            || matches!(
                origin_status,
                MovePersistence::Failed { .. } | MovePersistence::NotAttempted { .. }
            );
        let snapshot = loader.aggregate_snapshot();
        if failed {
            Err(LoaderControlError::Move {
                snapshot: Box::new(snapshot),
                origin: origin_status,
                destination: destination_status,
                lifecycle_errors,
            })
        } else {
            Ok(snapshot)
        }
    })
    .await
    .map_err(|_| LoaderControlError::ConfigOperationAborted)?
}

async fn validate_write(
    tree: &RuntimeTree,
    config: &ExtensionsConfig,
) -> Result<(), LoaderControlError> {
    let source = tree.source.lock().expect("source").clone();
    if let Some(source) = source {
        source
            .validate_write(config)
            .await
            .map_err(|error| LoaderControlError::from_source(source.identity(), error))?;
    }
    Ok(())
}

async fn persist(tree: &RuntimeTree, config: &ExtensionsConfig) -> MovePersistence {
    let source = tree.source.lock().expect("source").clone();
    let Some(source) = source else {
        return MovePersistence::NotRequired;
    };
    let source_id = source.identity();
    let revision = tree.revision.lock().expect("revision").clone();
    let result = match revision {
        Some(expected) => source.write(config, &expected).await,
        None => Err(SourceError::new(
            SourceErrorKind::InvalidData,
            "来源缺少版本令牌",
        )),
    };
    match result {
        Ok(revision) => {
            *tree.revision.lock().expect("revision") = Some(revision);
            MovePersistence::Written { source_id }
        }
        Err(error) => {
            tree.mark_unsettled();
            MovePersistence::Failed {
                source_id,
                error: Box::new(error),
            }
        }
    }
}

fn reject_descendant_tree(
    origin: &Arc<RuntimeTree>,
    destination: &Arc<RuntimeTree>,
    moving: &EntryOptions,
    path: &str,
) -> Result<(), LoaderControlError> {
    fn collect(entry: &EntryOptions, ids: &mut Vec<String>) -> Result<(), LoaderError> {
        ids.push(entry.id.clone());
        if entry.group {
            for child in entry.children()? {
                collect(&child, ids)?;
            }
        }
        Ok(())
    }
    let mut ids = Vec::new();
    collect(moving, &mut ids)?;
    let mut cursor = Some(destination.clone());
    while let Some(tree) = cursor {
        if tree
            .parent_tree
            .upgrade()
            .is_some_and(|parent| Arc::ptr_eq(&parent, origin))
        {
            let prefix = tree.prefix.as_str();
            let local = if origin.prefix.as_str().is_empty() {
                prefix
            } else {
                prefix
                    .strip_prefix(&format!("{}:", origin.prefix))
                    .ok_or_else(|| LoaderControlError::InvalidMove { path: path.into() })?
            };
            if ids.iter().any(|id| id == local) {
                return Err(LoaderControlError::InvalidMove { path: path.into() });
            }
        }
        cursor = tree.parent_tree.upgrade();
    }
    // 注册树祖先链是权威关系；无需按 Group 地址前缀猜测后代。
    Ok(())
}
