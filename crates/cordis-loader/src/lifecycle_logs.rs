//! 所属来源树的生命周期信息日志；状态订阅与显式释放共用同一记录，避免重复。
use crate::{snapshot::EntryId, tree::RuntimeTree};
use cordis_core::{FiberState, FiberStateChange, Logger};
use std::{collections::HashMap, sync::Mutex};

struct TrackedFiber {
    path: EntryId,
    factory: String,
    active: bool,
}

pub(crate) struct LifecycleLogs {
    logger: Logger,
    fibers: Mutex<HashMap<u64, TrackedFiber>>,
}

impl LifecycleLogs {
    pub(crate) fn new(logger: Logger) -> Self {
        Self {
            logger,
            fibers: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn error(&self, error: &crate::LoaderError) {
        self.logger.error(error.to_string());
    }

    pub(crate) fn warn_event_gap(&self, missed: u64) {
        self.logger.warn(format!(
            "生命周期事件丢失 {missed} 条，日志存在缺口；历史事件无法恢复，正在重建当前状态基线"
        ));
    }
}

impl RuntimeTree {
    /// 事件滞后后只按实际 Fiber 建立新基线，不补造丢失的历史操作。
    pub(crate) fn resync_lifecycle(&self) {
        // 与条目发布采用相同的 entries -> logs 顺序，不持锁调用日志导出器。
        let entries = self.entries.lock().expect("entries");
        let current = entries
            .iter()
            .filter(|(_, entry)| !entry.group)
            .filter_map(|(path, entry)| {
                let fiber = entry.fiber.as_ref()?;
                let state = fiber.state();
                if state == FiberState::Disposed || fiber.is_disposed() {
                    return None;
                }
                Some((
                    fiber.id(),
                    TrackedFiber {
                        path: path.clone(),
                        factory: entry.options.name.clone(),
                        active: state == FiberState::Active,
                    },
                ))
            })
            .collect();
        *self.lifecycle_logs.fibers.lock().expect("lifecycle logs") = current;
    }

    fn log_lifecycle(&self, operation: &str, path: &EntryId, factory: &str) {
        let source = self.source.lock().expect("source").clone();
        if source.is_some_and(|source| source.enable_logs()) {
            self.lifecycle_logs
                .logger
                .info(format!("{operation} {path} ({factory})"));
        }
    }

    pub(crate) fn track_lifecycle(
        &self,
        id: u64,
        path: &EntryId,
        factory: &str,
        state: FiberState,
    ) {
        let active = state == FiberState::Active;
        self.lifecycle_logs
            .fibers
            .lock()
            .expect("lifecycle logs")
            .insert(
                id,
                TrackedFiber {
                    path: path.clone(),
                    factory: factory.into(),
                    active,
                },
            );
        // 最初的状态事件可能早于挂载返回；用完成状态登记，后到的 Active 事件不重复输出。
        if active {
            self.log_lifecycle("挂载", path, factory);
        }
    }

    pub(crate) fn observe_lifecycle(&self, change: &FiberStateChange) -> bool {
        // 创建时的 Pending 可能在 track 的完成快照之后才被消费，不代表依赖卸载。
        if change.previous.is_none() {
            return false;
        }
        self.record_lifecycle_state(change.fiber_id, change.current)
    }

    pub(crate) fn record_lifecycle_state(&self, id: u64, state: FiberState) -> bool {
        let action = {
            let mut fibers = self.lifecycle_logs.fibers.lock().expect("lifecycle logs");
            let Some(tracked) = fibers.get_mut(&id) else {
                return false;
            };
            let operation = match state {
                FiberState::Active if !tracked.active => {
                    tracked.active = true;
                    Some("挂载")
                }
                FiberState::Unloading
                | FiberState::Pending
                | FiberState::Failed
                | FiberState::Disposed
                    if tracked.active =>
                {
                    tracked.active = false;
                    Some("卸载")
                }
                _ => None,
            };
            let action = operation.map(|op| (op, tracked.path.clone(), tracked.factory.clone()));
            if state == FiberState::Disposed {
                fibers.remove(&id);
            }
            action
        };
        if let Some((operation, path, factory)) = action {
            self.log_lifecycle(operation, &path, &factory);
        }
        true
    }

    pub(crate) fn log_reload(&self, path: &EntryId, factory: &str) {
        self.log_lifecycle("重载", path, factory);
    }
}
