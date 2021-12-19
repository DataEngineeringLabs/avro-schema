use std::{collections::HashMap, fmt};

use serde::{
    de::{MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::Value;

use super::*;

fn to_primitive(v: &str) -> Option<Schema> {
    use Schema::*;
    Some(match v {
        "null" => Null,
        "boolean" => Boolean,
        "string" => String,
        "bytes" => Bytes,
        "int" => Int,
        "long" => Long,
        "float" => Float,
        "double" => Double,
        _ => return None,
    })
}

fn get_type<E: serde::de::Error>(map: &mut HashMap<String, Value>) -> Result<String, E> {
    if let Some(v) = map.remove("type") {
        if let Value::String(v) = v {
            Ok(v)
        } else {
            Err(serde::de::Error::custom("type must be a string"))
        }
    } else {
        Err(serde::de::Error::missing_field("type"))
    }
}

fn as_string<E: serde::de::Error>(v: Value, helper: &str) -> Result<String, E> {
    if let Value::String(v) = v {
        Ok(v)
    } else {
        Err(serde::de::Error::custom(format!(
            "{} must be a string",
            helper
        )))
    }
}

fn remove_string<E: serde::de::Error>(
    data: &mut HashMap<String, Value>,
    key: &str,
) -> Result<Option<String>, E> {
    match data.remove(key) {
        Some(s) => as_string(s, key).map(Some),
        None => Ok(None),
    }
}

fn remove_vec_string<E: serde::de::Error>(
    data: &mut HashMap<String, Value>,
    key: &str,
) -> Result<Vec<String>, E> {
    match data.remove(key) {
        Some(s) => {
            if let Value::Array(x) = s {
                x.into_iter().map(|x| as_string(x, key)).collect()
            } else {
                Err(serde::de::Error::custom(format!(
                    "{} must be a string",
                    key
                )))
            }
        }
        None => Ok(vec![]),
    }
}

fn to_enum<E: serde::de::Error>(mut data: HashMap<String, Value>) -> Result<Schema, E> {
    Ok(Schema::Enum {
        name: remove_string(&mut data, "name")?
            .ok_or_else(|| serde::de::Error::custom("name is required in enum"))?,
        namespace: remove_string(&mut data, "namespace")?,
        aliases: remove_vec_string(&mut data, "aliases")?,
        doc: remove_string(&mut data, "doc")?,
        symbols: remove_vec_string(&mut data, "symbols")?,
        default: remove_string(&mut data, "default")?,
    })
}

struct SchemaVisitor {}

impl<'de> Visitor<'de> for SchemaVisitor {
    type Value = Schema;

    // Format a message stating what data this Visitor expects to receive.
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string, array or map describing an Avro schema")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        to_primitive(v)
            .ok_or_else(|| serde::de::Error::custom("string must be a valid primitive Schema"))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut vec = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(item) = seq.next_element::<Value>()? {
            let schema: Schema = serde_json::from_value(item).map_err(|e| {
                println!("{}", e);
                serde::de::Error::custom("Each item of a union must be a valid Schema")
            })?;
            vec.push(schema)
        }
        Ok(Schema::Union(vec))
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut map = HashMap::<String, Value>::with_capacity(access.size_hint().unwrap_or(0));

        // While there are entries remaining in the input, add them
        // into our map.
        while let Some((key, value)) = access.next_entry()? {
            map.insert(key, value);
        }

        let (schema, type_) = get_type(&mut map).map(|x| (to_primitive(&x), x))?;

        if let Some(schema) = schema {
            Ok(schema)
        } else {
            match type_.as_ref() {
                "enum" => to_enum(map),
                _ => todo!(),
            }
        }
    }
}

impl<'de> Deserialize<'de> for Schema {
    fn deserialize<D>(deserializer: D) -> Result<Schema, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(SchemaVisitor {})
    }
}
