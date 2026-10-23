//! 每个扩展名只有一个编解码器；解析失败不会尝试其他格式。
use cordis_loader::{ExtensionsConfig, SourceError, SourceErrorKind};
use std::path::Path;

#[derive(Clone, Copy)]
pub(crate) enum Format {
    Toml,
    Json,
    Yaml,
}
fn invalid() -> SourceError {
    SourceError::new(
        SourceErrorKind::InvalidData,
        "Include 文件格式、类型或文档结构无效",
    )
}

impl Format {
    pub(crate) fn for_path(path: &Path) -> Result<Self, SourceError> {
        match path.extension().and_then(|extension| extension.to_str()) {
            Some("toml") => Ok(Self::Toml),
            Some("json") => Ok(Self::Json),
            Some("yaml" | "yml") => Ok(Self::Yaml),
            _ => Err(SourceError::new(
                SourceErrorKind::InvalidData,
                "Include 仅支持 .toml、.json、.yaml 或 .yml 文件",
            )),
        }
    }
    pub(crate) fn parse(self, bytes: &[u8]) -> Result<ExtensionsConfig, SourceError> {
        let value = match self {
            Self::Toml => {
                toml::from_str::<toml::Value>(std::str::from_utf8(bytes).map_err(|_| invalid())?)
                    .map_err(|_| invalid())?
            }
            Self::Json => json(serde_json::from_slice(bytes).map_err(|_| invalid())?)?,
            Self::Yaml => yaml(serde_yaml_ng::from_slice(bytes).map_err(|_| invalid())?)?,
        };
        finite(&value)?;
        let config: ExtensionsConfig = value.try_into().map_err(|_| invalid())?;
        crate::patches::validate(&config)?;
        Ok(config)
    }
    pub(crate) fn serialize(self, config: &ExtensionsConfig) -> Result<Vec<u8>, SourceError> {
        crate::patches::validate(config)?;
        let bytes = match self {
            Self::Toml => toml::to_string_pretty(config)
                .map(String::into_bytes)
                .map_err(|_| invalid())?,
            Self::Json => serde_json::to_vec_pretty(config).map_err(|_| invalid())?,
            Self::Yaml => serde_yaml_ng::to_string(config)
                .map(String::into_bytes)
                .map_err(|_| invalid())?,
        };
        // 不允许格式转换悄悄丢失 TOML 日期类型或不可表示的数值。
        if self.parse(&bytes)? != *config {
            return Err(SourceError::new(
                SourceErrorKind::InvalidData,
                "目标配置无法在所选文件格式中无损往返",
            ));
        }
        Ok(bytes)
    }
}

fn finite(value: &toml::Value) -> Result<(), SourceError> {
    match value {
        toml::Value::Float(value) if !value.is_finite() => Err(invalid()),
        toml::Value::Array(values) => values.iter().try_for_each(finite),
        toml::Value::Table(values) => values.values().try_for_each(finite),
        _ => Ok(()),
    }
}
fn json(value: serde_json::Value) -> Result<toml::Value, SourceError> {
    Ok(match value {
        serde_json::Value::Null => return Err(invalid()),
        serde_json::Value::Bool(value) => toml::Value::Boolean(value),
        serde_json::Value::String(value) => toml::Value::String(value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                toml::Value::Integer(value)
            } else if value.is_u64() {
                return Err(invalid());
            } else {
                toml::Value::Float(
                    value
                        .as_f64()
                        .filter(|value| value.is_finite())
                        .ok_or_else(invalid)?,
                )
            }
        }
        serde_json::Value::Array(values) => {
            toml::Value::Array(values.into_iter().map(json).collect::<Result<_, _>>()?)
        }
        serde_json::Value::Object(values) => toml::Value::Table(
            values
                .into_iter()
                .map(|(key, value)| Ok((key, json(value)?)))
                .collect::<Result<_, SourceError>>()?,
        ),
    })
}
fn yaml(value: serde_yaml_ng::Value) -> Result<toml::Value, SourceError> {
    Ok(match value {
        serde_yaml_ng::Value::Null | serde_yaml_ng::Value::Tagged(_) => return Err(invalid()),
        serde_yaml_ng::Value::Bool(value) => toml::Value::Boolean(value),
        serde_yaml_ng::Value::String(value) => toml::Value::String(value),
        serde_yaml_ng::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                toml::Value::Integer(value)
            } else if value.as_u64().is_some() {
                return Err(invalid());
            } else {
                toml::Value::Float(
                    value
                        .as_f64()
                        .filter(|value| value.is_finite())
                        .ok_or_else(invalid)?,
                )
            }
        }
        serde_yaml_ng::Value::Sequence(values) => {
            toml::Value::Array(values.into_iter().map(yaml).collect::<Result<_, _>>()?)
        }
        serde_yaml_ng::Value::Mapping(values) => {
            let mut table = toml::Table::new();
            for (key, value) in values {
                let serde_yaml_ng::Value::String(key) = key else {
                    return Err(invalid());
                };
                if table.insert(key, yaml(value)?).is_some() {
                    return Err(invalid());
                }
            }
            toml::Value::Table(table)
        }
    })
}
