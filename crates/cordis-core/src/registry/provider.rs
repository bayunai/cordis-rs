//! Provider 的注册与撤销。

use crate::{
    Context, CoreError, ProviderAvailability, ServiceId,
    effect::EffectScope,
    error::format_panic_message,
    registry::{Registry, RegistryState},
    service::ErasedService,
};
use std::{
    any::TypeId,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Weak},
};

pub(crate) type ProviderCheck = Arc<dyn Fn() -> ProviderAvailability + Send + Sync>;

/// 供依赖重算比较的 Provider 版本。可用性翻转会改变 revision，即使 Provider
/// 随后在同一调度周期内恢复，也会迫使已激活消费者重新验证。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ProviderRevision {
    pub(crate) id: u64,
    pub(crate) availability_revision: u64,
}

/// 由 `provide_checked()` 返回的 Provider 状态刷新句柄。
#[derive(Clone)]
pub struct ProviderHandle {
    registry: Weak<Registry>,
    provider_id: u64,
    service: ServiceId,
}

impl ProviderHandle {
    /// 按注册时提供的无阻塞检查函数刷新可用性。
    ///
    /// 检查函数在 Registry 锁外执行；Provider 已撤销时返回明确错误。
    pub fn refresh(&self) -> Result<(), CoreError> {
        let registry = self.registry.upgrade().ok_or(CoreError::ProviderDisposed {
            service: self.service,
        })?;
        registry.refresh_checked_provider(self.provider_id, self.service)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum ProviderKey {
    Local { context: usize, service: ServiceId },
    Isolated { isolation: u64, service: ServiceId },
}

pub(crate) struct ProviderRecord {
    pub(crate) id: u64,
    pub(crate) availability_revision: u64,
    pub(crate) availability: ProviderAvailability,
    pub(crate) check: Option<ProviderCheck>,
    pub(crate) value: ErasedService,
    pub(crate) effect_id: Option<u64>,
    pub(crate) context: Context,
    pub(crate) isolation: Option<u64>,
}

pub(crate) struct EffectRecord {
    pub(crate) name: String,
    pub(crate) parent: Option<u64>,
    pub(crate) context: Option<Context>,
    pub(crate) fiber_id: Option<u64>,
    pub(crate) scope: EffectScope,
}

impl Registry {
    pub(crate) fn resolve_with_id(
        &self,
        context: &Context,
        key: ServiceId,
    ) -> Option<(ProviderRevision, ErasedService)> {
        let state = self.state.lock().ok()?;
        crate::service::resolver::resolve_provider(&state, context, key).map(|provider| {
            (
                ProviderRevision {
                    id: provider.id,
                    availability_revision: provider.availability_revision,
                },
                provider.value.clone(),
            )
        })
    }

    pub(crate) fn register_effect(
        self: &Arc<Self>,
        id: u64,
        name: String,
        parent: Option<u64>,
        context: Option<Context>,
        fiber_id: Option<u64>,
        scope: &EffectScope,
    ) {
        if let Ok(mut state) = self.state.lock() {
            state.effects.insert(
                id,
                EffectRecord {
                    name,
                    parent,
                    context,
                    fiber_id,
                    scope: scope.clone(),
                },
            );
        }
        let weak = Arc::downgrade(self);
        scope.on_dispose(move || {
            if let Some(registry) = weak.upgrade()
                && let Ok(mut state) = registry.state.lock()
            {
                state.effects.remove(&id);
            }
        });
    }

    fn lock_service_type(
        state: &mut RegistryState,
        key: ServiceId,
        type_id: TypeId,
    ) -> Result<(), CoreError> {
        match state.service_types.entry(key) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(type_id);
                Ok(())
            }
            std::collections::hash_map::Entry::Occupied(entry) if *entry.get() == type_id => Ok(()),
            std::collections::hash_map::Entry::Occupied(_) => {
                Err(CoreError::ServiceKeyTypeConflict { service: key })
            }
        }
    }

    pub(crate) fn provide(
        self: &Arc<Self>,
        context: Context,
        key: ServiceId,
        value: ErasedService,
        owner: EffectScope,
        availability: ProviderAvailability,
        check: Option<ProviderCheck>,
    ) -> Result<u64, CoreError> {
        if owner.is_disposed() {
            return Err(CoreError::ContextDisposed);
        }
        let provider_id = self.allocate_id();
        {
            let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
            Self::lock_service_type(&mut state, key, value.type_id)?;
            let isolation = context.nearest_isolation(key).map(|label| label.id());
            let slot = match isolation {
                Some(isolation) => ProviderKey::Isolated {
                    isolation,
                    service: key,
                },
                None => ProviderKey::Local {
                    context: context.identity(),
                    service: key,
                },
            };
            if state.providers.contains_key(&slot) {
                return Err(CoreError::ServiceConflict { service: key });
            }
            state.providers.insert(
                slot,
                ProviderRecord {
                    id: provider_id,
                    availability_revision: 0,
                    availability,
                    check,
                    value,
                    effect_id: Some(owner.id()),
                    context,
                    isolation,
                },
            );
        }
        let weak = Arc::downgrade(self);
        owner.on_dispose(move || {
            if let Some(registry) = weak.upgrade() {
                registry.remove_provider(provider_id);
            }
        });
        self.mark_dirty();
        Ok(provider_id)
    }

    pub(crate) fn checked_provider_handle(
        self: &Arc<Self>,
        provider_id: u64,
        service: ServiceId,
    ) -> ProviderHandle {
        ProviderHandle {
            registry: Arc::downgrade(self),
            provider_id,
            service,
        }
    }

    pub(crate) fn evaluate_check(check: &ProviderCheck) -> Result<ProviderAvailability, CoreError> {
        catch_unwind(AssertUnwindSafe(|| check())).map_err(|payload| {
            CoreError::ProviderCheck(format_panic_message("provider check", payload))
        })
    }

    pub(crate) fn refresh_checked_provider(
        self: &Arc<Self>,
        provider_id: u64,
        service: ServiceId,
    ) -> Result<(), CoreError> {
        let check = {
            let state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
            let Some(provider) = state.providers.values().find(|item| item.id == provider_id)
            else {
                return Err(CoreError::ProviderDisposed { service });
            };
            provider
                .check
                .clone()
                .ok_or(CoreError::ProviderDisposed { service })?
        };

        let result = Self::evaluate_check(&check);
        let availability = match &result {
            Ok(availability) => availability.clone(),
            Err(error) => ProviderAvailability::Unavailable {
                reason: Arc::from(error.to_string()),
            },
        };
        let changed = {
            let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
            let Some(provider) = state
                .providers
                .values_mut()
                .find(|item| item.id == provider_id)
            else {
                return Err(CoreError::ProviderDisposed { service });
            };
            let ready_changed = provider.availability.is_ready() != availability.is_ready();
            provider.availability = availability;
            if ready_changed {
                provider.availability_revision = provider.availability_revision.wrapping_add(1);
            }
            ready_changed
        };
        if changed {
            self.mark_dirty();
        }
        result.map(|_| ())
    }

    fn remove_provider(self: &Arc<Self>, provider_id: u64) {
        let removed = if let Ok(mut state) = self.state.lock() {
            let key = state
                .providers
                .iter()
                .find_map(|(key, value)| (value.id == provider_id).then_some(*key));
            key.is_some_and(|key| state.providers.remove(&key).is_some())
        } else {
            false
        };
        if removed {
            self.mark_dirty();
        }
    }

    pub(crate) fn resolve(&self, context: &Context, key: ServiceId) -> Option<ErasedService> {
        let state = self.state.lock().ok()?;
        crate::service::resolver::resolve_provider(&state, context, key)
            .map(|provider| provider.value.clone())
    }
}
