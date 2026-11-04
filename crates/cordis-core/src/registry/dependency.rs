//! 已解析服务与执行 Scope 的绑定，以及资源释放的等待关系。

use super::{Registry, RegistryState, provider::ProviderRevision};
use crate::service::resolver::resolve_provider;
use crate::{Context, CoreError, ServiceId, Services, effect::EffectScope};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub(crate) struct DependencyBinding {
    consumer: EffectScope,
    providers: Vec<(u64, EffectScope)>,
}

impl Registry {
    /// 同锁解析并登记绑定；调用方须先为 consumer 持有执行屏障。
    pub(crate) fn bind_dependencies(
        &self,
        context: &Context,
        deps: &[ServiceId],
        consumer: &EffectScope,
    ) -> Result<Option<(Vec<ProviderRevision>, Services)>, CoreError> {
        let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
        if consumer.is_disposed() {
            return Err(CoreError::ContextDisposed);
        }
        let mut values = HashMap::new();
        let mut revisions = Vec::new();
        let mut providers = Vec::new();
        for key in deps {
            let Some(provider) = resolve_provider(&state, context, *key) else {
                return Ok(None);
            };
            values.insert(*key, provider.value.clone());
            revisions.push(ProviderRevision {
                id: provider.id,
                availability_revision: provider.availability_revision,
            });
            if !providers.iter().any(|(id, _)| *id == provider.id) {
                providers.push((provider.id, provider.owner.clone()));
            }
        }
        if !providers.is_empty() {
            assert!(
                !state.bindings.contains_key(&consumer.id()),
                "execution already bound"
            );
            state.bindings.insert(
                consumer.id(),
                DependencyBinding {
                    consumer: consumer.clone(),
                    providers,
                },
            );
            // Candidate bindings change resource barriers throughout their owners' subtrees.
            // Validate the entire resulting graph, including newly introduced inherited edges.
            if let Some(scopes) = dependency_cycle(&state) {
                state.bindings.remove(&consumer.id());
                return Err(CoreError::DependencyCycle { scopes });
            }
        }
        Ok(Some((revisions, Services { values })))
    }

    /// 在 Provider 槽位被删除前关闭新绑定，并捕获必须先完成的消费者。
    pub(crate) fn retire_scope(self: &Arc<Self>, scope: &EffectScope) -> Vec<EffectScope> {
        let consumers = {
            let mut state = self.state.lock().expect("registry");
            for provider in state.providers.values_mut() {
                if scope.contains(&provider.owner) {
                    provider.retiring = true;
                }
            }
            waiting_consumers(&state, scope, true)
        };
        self.mark_dirty();
        consumers
    }

    pub(crate) fn release_bindings(self: &Arc<Self>, scope: &EffectScope) {
        let mut state = self.state.lock().expect("registry");
        state.bindings.remove(&scope.id());
        state
            .providers
            .retain(|_, provider| !provider.owner.ptr_eq(scope));
        state.effects.remove(&scope.id());
        drop(state);
    }

    /// 等待 target 的释放是否会反过来等待当前用户生命周期回调。
    pub(crate) fn check_lifecycle_wait(&self, target: &EffectScope) -> Result<(), CoreError> {
        let Some(current) = crate::callback_context::current_lifecycle_scope() else {
            return Ok(());
        };
        let state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
        if reaches(&state, target, &current) {
            return Err(CoreError::LifecycleReentrant {
                scope: current.id(),
            });
        }
        Ok(())
    }
}

/// A consumer branch drains itself; other resource branches retain ancestor services.
fn waiting_consumers(
    state: &RegistryState,
    scope: &EffectScope,
    retiring_only: bool,
) -> Vec<EffectScope> {
    let consumer_owners = upstream_owners(state, scope);
    let retiring = state
        .providers
        .values()
        .filter(|provider| provider.retiring)
        .map(|provider| provider.id)
        .collect::<HashSet<_>>();
    state
        .bindings
        .values()
        .filter(|binding| {
            binding.providers.iter().any(|(id, owner)| {
                (!retiring_only || retiring.contains(id))
                    && (scope.contains(owner)
                        || (owner.contains(scope) && !consumer_owners.contains(&owner.id())))
            })
        })
        .map(|binding| (binding.consumer.id(), binding.consumer.clone()))
        .collect::<HashMap<_, _>>()
        .into_values()
        .collect()
}

/// Direct and transitive consumers are drainable branches of their upstream providers.
fn upstream_owners(state: &RegistryState, scope: &EffectScope) -> HashSet<u64> {
    let mut owners = HashSet::new();
    let mut pending = vec![scope.clone()];
    let mut visited = HashSet::new();
    while let Some(scope) = pending.pop() {
        if !visited.insert(scope.id()) {
            continue;
        }
        for binding in state
            .bindings
            .values()
            .filter(|binding| binding.consumer.contains(&scope))
        {
            for (_, owner) in &binding.providers {
                owners.insert(owner.id());
                pending.push(owner.clone());
            }
        }
    }
    owners
}

fn reaches(state: &RegistryState, start: &EffectScope, target: &EffectScope) -> bool {
    let mut pending = vec![start.clone()];
    let mut visited = HashSet::new();
    while let Some(scope) = pending.pop() {
        if !visited.insert(scope.id()) {
            continue;
        }
        if scope.contains(target) {
            return true;
        }
        pending.extend(waiting_consumers(state, &scope, false));
    }
    false
}

/// Adding one binding can introduce an inherited cycle away from that binding's owner.
fn dependency_cycle(state: &RegistryState) -> Option<Vec<u64>> {
    let mut scopes = state
        .effects
        .values()
        .map(|effect| (effect.scope.id(), effect.scope.clone()))
        .collect::<HashMap<_, _>>();
    for binding in state.bindings.values() {
        scopes.insert(binding.consumer.id(), binding.consumer.clone());
        for (_, owner) in &binding.providers {
            scopes.insert(owner.id(), owner.clone());
        }
    }
    let edges = scopes
        .values()
        .map(|scope| {
            let mut next = waiting_consumers(state, scope, false)
                .into_iter()
                .map(|scope| scope.id())
                .collect::<HashSet<_>>();
            next.extend(
                scopes
                    .values()
                    .filter(|child| child.id() != scope.id() && scope.contains(child))
                    .map(|child| child.id()),
            );
            (scope.id(), next.into_iter().collect::<Vec<_>>())
        })
        .collect::<HashMap<_, _>>();
    fn visit(
        id: u64,
        edges: &HashMap<u64, Vec<u64>>,
        done: &mut HashSet<u64>,
        path: &mut Vec<u64>,
    ) -> Option<Vec<u64>> {
        if let Some(index) = path.iter().position(|value| *value == id) {
            let mut cycle = path[index..].to_vec();
            cycle.push(id);
            return Some(cycle);
        }
        if !done.insert(id) {
            return None;
        }
        path.push(id);
        for next in &edges[&id] {
            if let Some(cycle) = visit(*next, edges, done, path) {
                return Some(cycle);
            }
        }
        path.pop();
        None
    }
    let mut done = HashSet::new();
    for id in scopes.keys() {
        if let Some(cycle) = visit(*id, &edges, &mut done, &mut Vec::new()) {
            return Some(cycle);
        }
    }
    None
}
