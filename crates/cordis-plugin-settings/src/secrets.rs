//! 管理配置描述的保守秘密投影；不根据当前值挑选较宽松的 Schema 分支。

use serde_json::{Map, Value};
use std::collections::BTreeMap;

type SecretPaths = Vec<(Vec<String>, bool)>;
type Redacted = (Value, toml::Value, SecretPaths);

/// 返回管理 Schema、脱敏配置以及秘密字段的存在状态。
///
/// 数组按整体编辑，因此数组元素中的秘密使整个数组成为秘密。
pub(crate) fn redact(schema: &Value, config: &toml::Value) -> Result<Redacted, String> {
    let expanded = expand(schema, schema, &mut Vec::new())?;
    let mut secrets = BTreeMap::new();
    collect(&expanded, Some(config), &mut Vec::new(), &mut secrets)?;
    let paths: Vec<Vec<String>> = secrets.keys().cloned().collect();
    secrets.retain(|path, _| {
        !paths
            .iter()
            .any(|ancestor| ancestor.len() < path.len() && path.starts_with(ancestor))
    });
    let mut value = config.clone();
    for path in secrets.keys() {
        remove(&mut value, path);
    }
    let mut management_schema = expanded;
    sanitize(&mut management_schema, &paths, &mut Vec::new(), false);
    Ok((management_schema, value, secrets.into_iter().collect()))
}

const SCHEMA_MAPS: &[&str] = &[
    "properties",
    "$defs",
    "definitions",
    "patternProperties",
    "dependentSchemas",
];
const SCHEMA_ARRAYS: &[&str] = &["allOf", "anyOf", "oneOf", "prefixItems"];
const SCHEMA_SINGLE: &[&str] = &[
    "additionalProperties",
    "unevaluatedProperties",
    "items",
    "additionalItems",
    "contains",
    "not",
    "if",
    "then",
    "else",
    "propertyNames",
    "unevaluatedItems",
    "contentSchema",
];

/// 展开本地引用，同时验证所有 Schema 子节点。拒绝递归引用而非遗漏递归秘密。
fn expand(schema: &Value, root: &Value, refs: &mut Vec<String>) -> Result<Value, String> {
    if schema.is_boolean() {
        return Ok(schema.clone());
    }
    let object = schema
        .as_object()
        .ok_or("秘密字段 Schema 必须是对象或布尔值")?;
    if object.contains_key("$dynamicRef") || object.contains_key("$recursiveRef") {
        return Err("秘密字段 Schema 不支持动态或递归引用".into());
    }
    if object.contains_key("$id") {
        return Err("秘密字段 Schema 不支持改变引用作用域的 $id".into());
    }
    if object.contains_key("dependencies") {
        return Err("秘密字段 Schema 不支持旧版 dependencies；请使用 dependentSchemas".into());
    }
    if let Some(flag) = object.get("writeOnly")
        && !flag.is_boolean()
    {
        return Err("秘密字段 Schema 的 writeOnly 必须是布尔值".into());
    }
    let mut result = object.clone();
    for key in SCHEMA_MAPS {
        if let Some(value) = result.get_mut(*key) {
            let map = value
                .as_object_mut()
                .ok_or("秘密字段 Schema 的属性映射非法")?;
            for child in map.values_mut() {
                *child = expand(child, root, refs)?;
            }
        }
    }
    for key in SCHEMA_ARRAYS {
        if let Some(value) = result.get_mut(*key) {
            let array = value
                .as_array_mut()
                .ok_or("秘密字段 Schema 的组合分支非法")?;
            for child in array {
                *child = expand(child, root, refs)?;
            }
        }
    }
    for key in SCHEMA_SINGLE {
        if let Some(value) = result.get_mut(*key) {
            *value = expand(value, root, refs)?;
        }
    }
    if let Some(reference) = result.remove("$ref") {
        let reference = reference
            .as_str()
            .ok_or("秘密字段 Schema 的引用必须是字符串")?;
        if !reference.starts_with('#') {
            return Err("秘密字段 Schema 不支持远程引用".into());
        }
        if refs.iter().any(|item| item == reference) {
            return Err("秘密字段 Schema 含无法安全展开的循环引用".into());
        }
        let pointer = &reference[1..];
        if !pointer.is_empty() && !pointer.starts_with('/') {
            return Err("秘密字段 Schema 仅支持本地 JSON Pointer 引用".into());
        }
        let target = root
            .pointer(pointer)
            .ok_or("秘密字段 Schema 的本地引用不存在")?;
        refs.push(reference.to_owned());
        let target = expand(target, root, refs)?;
        refs.pop();
        return Ok(serde_json::json!({"allOf": [target, Value::Object(result)]}));
    }
    Ok(Value::Object(result))
}

fn branches(schema: &Value) -> impl Iterator<Item = &Value> {
    ["allOf", "anyOf", "oneOf"]
        .into_iter()
        .flat_map(move |key| {
            schema
                .get(key)
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .chain(
            ["not", "if", "then", "else"]
                .into_iter()
                .filter_map(move |key| schema.get(key)),
        )
        .chain(
            schema
                .get("dependentSchemas")
                .and_then(Value::as_object)
                .into_iter()
                .flat_map(Map::values),
        )
}

fn contains_secret(schema: &Value) -> bool {
    if schema.get("writeOnly") == Some(&Value::Bool(true)) {
        return true;
    }
    SCHEMA_MAPS.iter().any(|key| {
        schema
            .get(*key)
            .and_then(Value::as_object)
            .is_some_and(|map| map.values().any(contains_secret))
    }) || SCHEMA_ARRAYS.iter().any(|key| {
        schema
            .get(*key)
            .and_then(Value::as_array)
            .is_some_and(|array| array.iter().any(contains_secret))
    }) || SCHEMA_SINGLE
        .iter()
        .any(|key| schema.get(*key).is_some_and(contains_secret))
}

fn collect(
    schema: &Value,
    value: Option<&toml::Value>,
    path: &mut Vec<String>,
    secrets: &mut BTreeMap<Vec<String>, bool>,
) -> Result<(), String> {
    if schema.get("writeOnly") == Some(&Value::Bool(true)) {
        secrets.insert(path.clone(), value.is_some());
        return Ok(());
    }
    // Any branch may describe an array; do not disclose its elements through another branch.
    let array_secret = ["items", "additionalItems", "contains", "unevaluatedItems"]
        .into_iter()
        .any(|key| schema.get(key).is_some_and(contains_secret))
        || schema
            .get("prefixItems")
            .and_then(Value::as_array)
            .is_some_and(|items| items.iter().any(contains_secret));
    if array_secret {
        secrets.insert(path.clone(), value.is_some());
        return Ok(());
    }
    if schema.get("propertyNames").is_some_and(contains_secret)
        || schema.get("contentSchema").is_some_and(contains_secret)
        || schema
            .get("patternProperties")
            .and_then(Value::as_object)
            .is_some_and(|patterns| patterns.values().any(contains_secret))
    {
        return Err("秘密字段 Schema 含无法安全投影的属性模式或内容 Schema".into());
    }
    let table = value.and_then(toml::Value::as_table);
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        for (name, child) in properties {
            path.push(name.clone());
            collect(
                child,
                table.and_then(|table| table.get(name)),
                path,
                secrets,
            )?;
            path.pop();
        }
    }
    for key in ["additionalProperties", "unevaluatedProperties"] {
        if let Some(child) = schema.get(key)
            && contains_secret(child)
            && let Some(table) = table
        {
            for (name, value) in table {
                // For unevaluatedProperties, conservative treatment includes declared keys.
                if key == "additionalProperties"
                    && schema
                        .get("properties")
                        .and_then(Value::as_object)
                        .is_some_and(|map| map.contains_key(name))
                {
                    continue;
                }
                path.push(name.clone());
                collect(child, Some(value), path, secrets)?;
                path.pop();
            }
        }
    }
    for branch in branches(schema) {
        collect(branch, value, path, secrets)?;
    }
    Ok(())
}

fn remove(value: &mut toml::Value, path: &[String]) {
    if path.is_empty() {
        *value = toml::Value::Table(toml::Table::new());
    } else if let Some(table) = value.as_table_mut() {
        if path.len() == 1 {
            table.remove(&path[0]);
        } else if let Some(child) = table.get_mut(&path[0]) {
            remove(child, &path[1..]);
        }
    }
}

fn sanitize(schema: &mut Value, secrets: &[Vec<String>], path: &mut Vec<String>, inherited: bool) {
    let whole_secret = inherited || secrets.iter().any(|secret| path.starts_with(secret));
    let descendant_secret = secrets.iter().any(|secret| secret.starts_with(path));
    let secret = whole_secret || descendant_secret || contains_secret(schema);
    let Some(object) = schema.as_object_mut() else {
        return;
    };
    if secret {
        for key in ["default", "examples", "const", "enum"] {
            object.remove(key);
        }
    }
    // All references have been expanded. Keeping their unused definitions could leak
    // a payload from a definition whose use site adds writeOnly.
    object.remove("$defs");
    object.remove("definitions");
    if let Some(properties) = object.get_mut("properties").and_then(Value::as_object_mut) {
        for (name, child) in properties {
            path.push(name.clone());
            sanitize(child, secrets, path, whole_secret);
            path.pop();
        }
    }
    for key in ["patternProperties", "dependentSchemas"] {
        if let Some(map) = object.get_mut(key).and_then(Value::as_object_mut) {
            for child in map.values_mut() {
                sanitize(
                    child,
                    secrets,
                    path,
                    whole_secret || (key == "patternProperties" && descendant_secret),
                );
            }
        }
    }
    for key in SCHEMA_ARRAYS {
        if let Some(array) = object.get_mut(*key).and_then(Value::as_array_mut) {
            for child in array {
                sanitize(child, secrets, path, whole_secret);
            }
        }
    }
    for key in SCHEMA_SINGLE {
        if let Some(child) = object.get_mut(*key) {
            let dynamic = matches!(*key, "additionalProperties" | "unevaluatedProperties");
            sanitize(
                child,
                secrets,
                path,
                whole_secret || (dynamic && descendant_secret),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn config(input: &str) -> toml::Value {
        toml::from_str(input).unwrap()
    }

    #[test]
    fn nested_secrets_and_missing_fields() {
        let schema = json!({"properties":{"public":{"enum":["a","b"]},"auth":{"properties":{
            "token":{"type":"string","writeOnly":true,"default":"hidden"},
            "absent":{"writeOnly":true}
        }}}});
        let (schema, value, secrets) =
            redact(&schema, &config("public='a'\n[auth]\ntoken='hidden'\n")).unwrap();
        assert_eq!(value, config("public='a'\n[auth]\n"));
        assert_eq!(
            secrets,
            vec![
                (vec!["auth".into(), "absent".into()], false),
                (vec!["auth".into(), "token".into()], true)
            ]
        );
        assert_eq!(schema["properties"]["public"]["enum"], json!(["a", "b"]));
        assert!(!schema.to_string().contains("hidden"));
    }

    #[test]
    fn local_refs_and_union_are_conservative() {
        let schema = json!({"$defs":{"secret":{"writeOnly":true,"examples":["hidden"]}},
            "anyOf":[{"properties":{"token":{"type":"string"}}},{"properties":{"token":{"$ref":"#/$defs/secret"}}}]});
        let (schema, value, secrets) = redact(&schema, &config("token='hidden'\nother=1")).unwrap();
        assert_eq!(value, config("other=1"));
        assert_eq!(secrets, vec![(vec!["token".into()], true)]);
        assert!(!schema.to_string().contains("hidden"));
    }

    #[test]
    fn secret_values_cannot_leak_through_other_branches_or_definitions() {
        let schema = json!({
            "$defs":{"value":{"type":"string","default":"hidden"}},
            "anyOf":[
                {"properties":{"token":{"writeOnly":true}}},
                {"properties":{"token":{"$ref":"#/$defs/value","enum":["hidden"]},"public":{"enum":["a","b"]}}}
            ]
        });
        let (schema, _, _) = redact(&schema, &config("token='hidden'")).unwrap();
        assert!(!schema.to_string().contains("hidden"));
        assert_eq!(
            schema["anyOf"][1]["properties"]["public"]["enum"],
            json!(["a", "b"])
        );
        let schema = json!({"writeOnly":true,"properties":{"token":{"default":"hidden"}}});
        let (schema, _, _) = redact(&schema, &config("token='hidden'")).unwrap();
        assert!(!schema.to_string().contains("hidden"));
    }

    #[test]
    fn array_element_secrets_hide_whole_array() {
        let schema =
            json!({"properties":{"rows":{"items":{"properties":{"token":{"writeOnly":true}}}}}});
        let (_, value, secrets) = redact(
            &schema,
            &config("rows=[{token='hidden',name='visible'}]\nother=true"),
        )
        .unwrap();
        assert_eq!(value, config("other=true"));
        assert_eq!(secrets, vec![(vec!["rows".into()], true)]);
    }

    #[test]
    fn parent_payloads_are_removed_and_parent_secret_deduplicates() {
        let schema = json!({"default":{"auth":{"token":"hidden"}},"properties":{"auth":{
            "allOf":[{"writeOnly":true},{"properties":{"token":{"writeOnly":true}}}],
            "const":{"token":"hidden"},"enum":[{"token":"hidden"}]
        }}});
        let (schema, value, secrets) = redact(&schema, &config("[auth]\ntoken='hidden'")).unwrap();
        assert_eq!(value, config(""));
        assert_eq!(secrets, vec![(vec!["auth".into()], true)]);
        assert!(!schema.to_string().contains("hidden"));
    }

    #[test]
    fn arbitrary_map_keys_and_conditional_secrets() {
        let schema = json!({"properties":{"map":{"additionalProperties":{"writeOnly":true}}},
            "if":{"properties":{"flag":{"const":true}}},"then":{"properties":{"token":{"writeOnly":true}}}});
        let (_, value, secrets) = redact(
            &schema,
            &config("flag=false\ntoken='hidden'\n[map]\na='hidden'\nb='hidden'"),
        )
        .unwrap();
        assert_eq!(value, config("flag=false\n[map]\n"));
        assert_eq!(secrets.len(), 3);
    }

    #[test]
    fn unsafe_schema_fails_explicitly_without_values() {
        for schema in [
            json!({"$ref":"https://example.com/schema"}),
            json!({"$defs":{"loop":{"$ref":"#/$defs/loop"}},"$ref":"#/$defs/loop"}),
            json!({"$ref":"#/$defs/missing"}),
            json!({"$dynamicRef":"#node"}),
            json!({"patternProperties":{".*":{"writeOnly":true}}}),
            json!({"writeOnly":"true"}),
        ] {
            let error = redact(&schema, &config("token='hidden'")).unwrap_err();
            assert!(!error.contains("hidden"));
        }
    }
}
