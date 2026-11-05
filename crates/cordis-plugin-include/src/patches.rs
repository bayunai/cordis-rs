use crate::EntryPatch;
use cordis_loader::{
    CONFIG_VERSION, EntryOptions, ExtensionsConfig, GROUP_NAME, SourceError, SourceErrorKind,
};
use std::collections::HashSet;

fn invalid(message: &'static str) -> SourceError {
    SourceError::new(SourceErrorKind::InvalidData, message)
}

/// 返回独立副本；依次执行并立即验证，不让被覆盖的非法输入潜伏到后面的 patch。
pub fn apply_patches(
    config: &ExtensionsConfig,
    patches: &[EntryPatch],
) -> Result<ExtensionsConfig, SourceError> {
    validate(config)?;
    let mut next = config.clone();
    for patch in patches {
        if let Some(insert) = &patch.insert {
            if patch.config.is_some()
                || patch.disabled.is_some()
                || patch.group.is_some()
                || patch.inject.is_some()
                || patch.isolate.is_some()
            {
                return Err(invalid("插入 patch 不能同时覆盖条目字段"));
            }
            if let Some(path) = &patch.id {
                modify(&mut next.extensions, path, &mut |entry| {
                    check_name(entry, patch)?;
                    if !entry.group {
                        return Err(invalid("插入 patch 的目标不是 Group"));
                    }
                    let mut children = entry
                        .children()
                        .map_err(|_| invalid("Group 子条目结构无效"))?;
                    children.extend(insert.iter().cloned());
                    entry.config =
                        toml::Value::try_from(children).map_err(|_| invalid("无法表示插入条目"))?;
                    Ok(())
                })?;
            } else {
                if patch.name.is_some() {
                    return Err(invalid("根插入 patch 不接受 Factory 名称校验"));
                }
                next.extensions.extend(insert.iter().cloned());
            }
        } else {
            let path = patch
                .id
                .as_deref()
                .ok_or_else(|| invalid("覆盖 patch 必须指定 id"))?;
            modify(&mut next.extensions, path, &mut |entry| {
                check_name(entry, patch)?;
                if let Some(config) = &patch.config {
                    entry.config = config.clone();
                }
                if let Some(disabled) = patch.disabled {
                    entry.disabled = disabled;
                }
                if let Some(group) = patch.group {
                    entry.group = group;
                }
                if let Some(inject) = &patch.inject {
                    entry.inject = Some(inject.clone());
                }
                if let Some(isolate) = &patch.isolate {
                    entry.isolate = Some(isolate.clone());
                }
                Ok(())
            })?;
        }
        validate(&next)?;
    }
    Ok(next)
}

fn check_name(entry: &EntryOptions, patch: &EntryPatch) -> Result<(), SourceError> {
    if patch.name.as_deref().is_some_and(|name| name != entry.name) {
        return Err(invalid("patch 的 Factory 名称与目标不一致"));
    }
    Ok(())
}

fn modify(
    entries: &mut [EntryOptions],
    id: &str,
    change: &mut dyn FnMut(&mut EntryOptions) -> Result<(), SourceError>,
) -> Result<(), SourceError> {
    if id.is_empty() || id.contains(':') {
        return Err(invalid("patch 必须使用当前 EntryTree 内的单段 ID"));
    }
    if modify_entry(entries, id, change)? {
        Ok(())
    } else {
        Err(invalid("patch 的目标条目不存在"))
    }
}

fn modify_entry(
    entries: &mut [EntryOptions],
    id: &str,
    change: &mut dyn FnMut(&mut EntryOptions) -> Result<(), SourceError>,
) -> Result<bool, SourceError> {
    for entry in entries {
        if entry.id == id {
            change(entry)?;
            return Ok(true);
        }
        if entry.group {
            let mut children = entry
                .children()
                .map_err(|_| invalid("Group 子条目结构无效"))?;
            if modify_entry(&mut children, id, change)? {
                entry.config =
                    toml::Value::try_from(children).map_err(|_| invalid("无法表示 Group 条目"))?;
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub(crate) fn validate(config: &ExtensionsConfig) -> Result<(), SourceError> {
    if config.version != CONFIG_VERSION {
        return Err(invalid("不支持的配置版本；当前要求 version = 3"));
    }
    validate_entries(&config.extensions, &mut HashSet::new())
}
fn validate_entries(
    entries: &[EntryOptions],
    ids: &mut HashSet<String>,
) -> Result<(), SourceError> {
    for entry in entries {
        if entry.id.is_empty() || entry.id.contains(':') || !ids.insert(entry.id.clone()) {
            return Err(invalid("条目 ID 为空、含分隔符或在 EntryTree 内重复"));
        }
        if entry.name.is_empty() {
            return Err(invalid("条目 Factory 名称不能为空"));
        }
        if entry.group {
            if entry.name != GROUP_NAME {
                return Err(invalid("Group 的 Factory 名称必须为 cordis:group"));
            }
            let children = entry
                .children()
                .map_err(|_| invalid("Group 子条目结构无效"))?;
            validate_entries(&children, ids)?;
        } else if entry.name == GROUP_NAME || !entry.config.is_table() {
            return Err(invalid(
                "普通条目 config 必须是配置表且不能使用 Group Factory",
            ));
        }
    }
    Ok(())
}
