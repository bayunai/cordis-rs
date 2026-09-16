//! 计算并预构造 reconcile 计划。预检失败时调用方不得变更运行状态。

use crate::{
    catalog::ExtensionCatalog,
    config::{ExtensionEntry, ExtensionsConfig},
    error::{HostError, format_panic_message},
    host::MountedInstance,
    snapshot::InstanceId,
};
use cordis_core::{Plugin, PluginKey};
use std::{
    collections::HashMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

pub(crate) struct PreparedInstance {
    pub instance: InstanceId,
    pub factory: String,
    pub config: toml::Value,
    pub plugin: Arc<dyn Plugin>,
    pub plugin_key: PluginKey,
}

pub(crate) struct ReconcilePlan {
    pub removes: Vec<InstanceId>,
    pub replaces: Vec<PreparedInstance>,
    pub adds: Vec<PreparedInstance>,
}

pub(crate) fn prepare(
    catalog: &ExtensionCatalog,
    mounted: &HashMap<InstanceId, MountedInstance>,
    order: &[InstanceId],
    config: &ExtensionsConfig,
) -> Result<ReconcilePlan, HostError> {
    config.validate(catalog)?;
    for entry in &config.extensions {
        let id = InstanceId::from(entry.instance.as_str());
        if let Some(existing) = mounted.get(&id)
            && existing.factory != entry.factory
        {
            return Err(HostError::FactoryChanged {
                instance: entry.instance.clone(),
                from: existing.factory.clone(),
                to: entry.factory.clone(),
            });
        }
    }

    let enabled: HashMap<&str, &ExtensionEntry> = config
        .extensions
        .iter()
        .filter(|entry| entry.enabled)
        .map(|entry| (entry.instance.as_str(), entry))
        .collect();

    let removes = order
        .iter()
        .rev()
        .filter(|id| !enabled.contains_key(id.as_str()))
        .cloned()
        .collect::<Vec<_>>();

    let mut replaces = Vec::new();
    let mut adds = Vec::new();
    for entry in &config.extensions {
        if !entry.enabled {
            continue;
        }
        let id = InstanceId::from(entry.instance.as_str());
        match mounted.get(&id) {
            Some(existing) if existing.config == entry.config => {}
            Some(existing) => {
                let prepared = build_instance(catalog, entry)?;
                if prepared.plugin_key != existing.plugin_key {
                    return Err(HostError::PluginKeyChanged {
                        instance: entry.instance.clone(),
                        expected: existing.plugin_key,
                        actual: prepared.plugin_key,
                    });
                }
                replaces.push(prepared);
            }
            None => adds.push(build_instance(catalog, entry)?),
        }
    }

    Ok(ReconcilePlan {
        removes,
        replaces,
        adds,
    })
}

fn build_instance(
    catalog: &ExtensionCatalog,
    entry: &ExtensionEntry,
) -> Result<PreparedInstance, HostError> {
    let factory = catalog
        .get(&entry.factory)
        .ok_or_else(|| HostError::UnknownFactory {
            instance: entry.instance.clone(),
            factory: entry.factory.clone(),
        })?;
    let built =
        catch_unwind(AssertUnwindSafe(|| factory.build(&entry.config))).map_err(|payload| {
            HostError::FactoryPanic {
                message: format_panic_message("extension factory build", payload),
            }
        })?;
    let plugin = built.map_err(|error| HostError::PluginBuild {
        instance: entry.instance.clone(),
        factory: entry.factory.clone(),
        message: error.to_string(),
    })?;
    let plugin_key = catch_unwind(AssertUnwindSafe(|| plugin.key())).map_err(|payload| {
        HostError::PluginPanic {
            message: format_panic_message("plugin key", payload),
        }
    })?;
    Ok(PreparedInstance {
        instance: InstanceId::from(entry.instance.as_str()),
        factory: entry.factory.clone(),
        config: entry.config.clone(),
        plugin,
        plugin_key,
    })
}
