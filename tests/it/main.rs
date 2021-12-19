use serde_json::Result;

use avro_schema::{BytesLogical, Field, LongLogical, Schema};

fn cases() -> Vec<(&'static str, Schema)> {
    use Schema::*;
    vec![
        (r#"null"#, Null),
        (r#"{"type": "null"}"#, Null),
        (r#"{"type": null}"#, Null),
        (r#""null""#, Null),
        (r#""boolean""#, Boolean),
        (r#"{"type": "boolean"}"#, Boolean),
        (r#""string""#, String(None)),
        (r#"{"type": "string"}"#, String(None)),
        (r#""bytes""#, Bytes(None)),
        (r#"{"type": "bytes"}"#, Bytes(None)),
        (
            r#"{"type": "bytes", "logicalType": "decimal", "precision": 10}"#,
            Bytes(Some(BytesLogical::Decimal(10, 0))),
        ),
        (r#""int""#, Int(None)),
        (r#"{"type": "int"}"#, Int(None)),
        (r#""long""#, Long(None)),
        (r#"{"type": "long"}"#, Long(None)),
        (
            r#"{"type": "long", "logicalType": "timestamp-millis"}"#,
            Long(Some(LongLogical::TimestampMillis)),
        ),
        (r#""float""#, Float),
        (r#"{"type": "float"}"#, Float),
        (r#""double""#, Double),
        (r#"{"type": "double"}"#, Double),
        (
            r#"{"type": "enum", "name": "Test", "symbols": ["A", "B"]}"#,
            Enum(avro_schema::Enum::new(
                "Test",
                vec!["A".to_string(), "B".to_string()],
            )),
        ),
        (r#"["null", "string"]"#, Union(vec![Null, String(None)])),
        (
            r#"[{"type": "null"}, {"type": "string"}]"#,
            Union(vec![Null, String(None)]),
        ),
        (
            r#"{"type": "map", "values": "long"}"#,
            Map(Box::new(Long(None))),
        ),
        (
            r#"{
                "type": "map",
                "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}
            }"#,
            Map(Box::new(Enum(avro_schema::Enum::new(
                "Test",
                vec!["A".to_string(), "B".to_string()],
            )))),
        ),
        (
            r#"{"type": "array", "items": "long"}"#,
            Array(Box::new(Long(None))),
        ),
        (
            r#"{
                    "type": "array",
                    "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}
                }"#,
            Array(Box::new(
                avro_schema::Enum::new("Test", vec!["A".to_string(), "B".to_string()]).into(),
            )),
        ),
        (
            r#"{
                "type":"record",
                "name":"HandshakeResponse",
                "namespace":"org.apache.avro.ipc",
                "fields":[
                    {
                        "name":"match",
                        "type":{
                            "type":"enum",
                            "name":"HandshakeMatch",
                            "symbols":["BOTH", "CLIENT", "NONE"]
                        }
                    },
                    {"name":"serverProtocol", "type":["null", "string"]},
                    {
                        "name":"serverHash",
                        "type":["null", {"name":"MD5", "size":16, "type":"fixed"}]
                    },
                    {
                        "name":"meta",
                        "type":["null", {"type":"map", "values":"bytes"}]
                    }
                ]
            }"#,
            Record(avro_schema::Record {
                name: "HandshakeResponse".to_string(),
                namespace: Some("org.apache.avro.ipc".to_string()),
                doc: None,
                aliases: vec![],
                fields: vec![
                    Field::new(
                        "match",
                        avro_schema::Enum::new(
                            "HandshakeMatch",
                            vec!["BOTH".to_string(), "CLIENT".to_string(), "NONE".to_string()],
                        )
                        .into(),
                    ),
                    Field::new("serverProtocol", Union(vec![Null, String(None)])),
                    Field::new(
                        "serverHash",
                        Union(vec![Null, avro_schema::Fixed::new("MD5", 16).into()]),
                    ),
                    Field::new("meta", Union(vec![Null, Map(Box::new(Bytes(None)))])),
                ],
            }),
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

#[test]
fn test_round_trip() -> Result<()> {
    for (_, expected) in cases() {
        let serialized = serde_json::to_string(&expected)?;
        let v: avro_schema::Schema = serde_json::from_str(&serialized)?;
        assert_eq!(expected, v);
    }
    Ok(())
}
