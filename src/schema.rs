#[derive(Debug, Clone, PartialEq, Hash)]
pub enum Schema {
    Null,
    Boolean,
    Int(Option<IntLogical>),
    Long(Option<LongLogical>),
    Float,
    Double,
    Bytes,
    String(Option<StringLogical>),
    Record(Record),
    Enum(Enum),
    Array(Box<Schema>),
    Map(Box<Schema>),
    Union(Vec<Schema>),
    Fixed(Fixed),
}

#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum Order {
    Ascending,
    Descending,
    Ignore,
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct Field {
    pub name: String,
    pub doc: Option<String>,
    pub schema: Schema,
    pub default: Option<Schema>,
    pub order: Option<Order>,
    pub aliases: Vec<String>,
}

impl Field {
    pub fn new<I: Into<String>>(name: I, schema: Schema) -> Self {
        Self {
            name: name.into(),
            doc: None,
            schema,
            default: None,
            order: None,
            aliases: vec![],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct Record {
    pub name: String,
    pub namespace: Option<String>,
    pub doc: Option<String>,
    pub aliases: Vec<String>,
    pub fields: Vec<Field>,
}

impl Record {
    pub fn new<I: Into<String>>(name: I, fields: Vec<Field>) -> Self {
        Self {
            name: name.into(),
            namespace: None,
            doc: None,
            fields,
            aliases: vec![],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct Fixed {
    pub name: String,
    pub namespace: Option<String>,
    pub doc: Option<String>,
    pub aliases: Vec<String>,
    pub size: usize,
    pub logical: Option<FixedLogical>,
}

impl Fixed {
    pub fn new<I: Into<String>>(name: I, size: usize) -> Self {
        Self {
            name: name.into(),
            namespace: None,
            doc: None,
            size,
            aliases: vec![],
            logical: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct Enum {
    pub name: String,
    pub namespace: Option<String>,
    pub aliases: Vec<String>,
    pub doc: Option<String>,
    pub symbols: Vec<String>,
    pub default: Option<String>,
}

impl Enum {
    pub fn new<I: Into<String>>(name: I, symbols: Vec<String>) -> Self {
        Self {
            name: name.into(),
            namespace: None,
            doc: None,
            symbols,
            aliases: vec![],
            default: None,
        }
    }
}

impl From<Enum> for Schema {
    fn from(enum_: Enum) -> Self {
        Schema::Enum(enum_)
    }
}

impl From<Record> for Schema {
    fn from(record: Record) -> Self {
        Schema::Record(record)
    }
}

impl From<Fixed> for Schema {
    fn from(fixed: Fixed) -> Self {
        Schema::Fixed(fixed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IntLogical {
    Date,
    Time,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LongLogical {
    Time,
    TimestampMillis,
    TimestampMicros,
    LocalTimestampMillis,
    LocalTimestampMicros,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StringLogical {
    Uuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FixedLogical {
    Decimal(usize, usize),
    Duration,
}
