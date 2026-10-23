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
    path: &str,
    change: &mut dyn FnMut(&mut EntryOptions) -> Result<(), SourceError>,
) -> Result<(), SourceError> {
    let (head, tail) = path
        .split_once(':')
        .map_or((path, None), |(head, tail)| (head, Some(tail)));
    if head.is_empty() {
        return Err(invalid("patch 路径不能为空或包含空段"));
    }
    let entry = entries
        .iter_mut()
        .find(|entry| entry.id == head)
        .ok_or_else(|| invalid("patch 的目标条目不存在"))?;
    if let Some(tail) = tail {
        if !entry.group {
            return Err(invalid("patch 路径穿过非 Group 条目"));
        }
        let mut children = entry
            .children()
            .map_err(|_| invalid("Group 子条目结构无效"))?;
        modify(&mut children, tail, change)?;
        entry.config =
            toml::Value::try_from(children).map_err(|_| invalid("无法表示 Group 条目"))?;
        Ok(())
    } else {
        change(entry)
    }
}

pub(crate) fn validate(config: &ExtensionsConfig) -> Result<(), SourceError> {
    if config.version != CONFIG_VERSION {
        return Err(invalid("不支持的配置版本；当前要求 version = 3"));
    }
    validate_entries(&config.extensions)
}
fn validate_entries(entries: &[EntryOptions]) -> Result<(), SourceError> {
    let mut ids = HashSet::new();
    for entry in entries {
        if entry.id.is_empty() || entry.id.contains(':') || !ids.insert(&entry.id) {
            return Err(invalid("条目 ID 为空、含分隔符或同组重复"));
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
            validate_entries(&children)?;
        } else if entry.name == GROUP_NAME || !entry.config.is_table() {
            return Err(invalid(
                "普通条目 config 必须是配置表且不能使用 Group Factory",
            ));
        }
    }
    Ok(())
}
