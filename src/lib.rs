mod de;

#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum Order {
    Ascending,
    Descending,
    Ignore,
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct InnerField {
    name: String,
    doc: Option<String>,
    type_: Schema,
    default: Option<Schema>,
    order: Option<Order>,
    aliases: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum Schema {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Record {
        name: String,
        namespace: Option<String>,
        doc: Option<String>,
        aliases: Vec<String>,
        fields: Vec<InnerField>,
    },
    Enum {
        name: String,
        namespace: Option<String>,
        aliases: Vec<String>,
        doc: Option<String>,
        symbols: Vec<String>,
        default: Option<String>,
    },
    Array {
        items: Box<Schema>,
    },
    Map {
        values: Box<Schema>,
    },
    Union(Vec<Schema>),
    Fixed {
        name: String,
        namespace: Option<String>,
        doc: Option<String>,
        aliases: Vec<String>,
        size: usize,
    },
}
