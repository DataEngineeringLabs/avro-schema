use serde_json::Result;

use avro_schema::Schema;

fn cases() -> Vec<(&'static str, Schema)> {
    use Schema::*;
    vec![
        (r#"{"type": "null"}"#, Null),
        (r#""null""#, Null),
        (r#""boolean""#, Boolean),
        (r#"{"type": "boolean"}"#, Boolean),
        (r#""string""#, String),
        (r#"{"type": "string"}"#, String),
        (r#""bytes""#, Bytes),
        (r#"{"type": "bytes"}"#, Bytes),
        (r#""int""#, Int),
        (r#"{"type": "int"}"#, Int),
        (r#""long""#, Long),
        (r#"{"type": "long"}"#, Long),
        (r#""float""#, Float),
        (r#"{"type": "float"}"#, Float),
        (r#""double""#, Double),
        (r#"{"type": "double"}"#, Double),
        (
            r#"{"type": "enum", "name": "Test", "symbols": ["A", "B"]}"#,
            Enum {
                name: "Test".to_string(),
                namespace: None,
                doc: None,
                aliases: vec![],
                symbols: vec!["A".to_string(), "B".to_string()],
                default: None,
            },
        ),
        (r#"["null", "string"]"#, Union(vec![Null, String])),
        (
            r#"[{"type": "null"}, {"type": "string"}]"#,
            Union(vec![Null, String]),
        ),
    ]
}

#[test]
fn test_deserialize() -> Result<()> {
    for (data, expected) in cases() {
        let v: avro_schema::Schema = serde_json::from_str(data)?;
        assert_eq!(v, expected);
    }
    Ok(())
}
