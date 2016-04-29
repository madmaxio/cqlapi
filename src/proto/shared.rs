use chrono::*;
use rustcql::shared::Column;

// Rules:
// fields with QueryType Storaged, Value, Substring must form a set without duplicates (check not implemented)
// fields with QueryType Substring must be Field::Text(_) only (check not implemented)

// (group id) f1 f2 ... fn                                  main
// (group f1 id) f2 ... fn                                  Value
// (group f1 id) f2 ... fn + (group f1_substring f1)        Substring
// (group entity_id id) f1 f2 ... fn                        by_entity
// (group entity_id id row) f1 f2 ... fn                    by_many



//pub static HOST: &'static str = "10.0.2.15:9042";
pub static HOST: &'static str = "127.0.0.1:9042";

pub fn now() -> Column {
    Column::Timestamp(UTC::now().timestamp() * 1000 + (UTC::now().nanosecond() / 1000000) as i64)
}


pub enum QueryType {
    Storaged,
    Value,
    Substring
}

pub enum Field<'a> {
    Bigint(&'a str),
    Timestamp(&'a str),
    Text(&'a str),
    Double(&'a str)
}

pub struct FieldConf<'a> {
    pub f: Field<'a>,
    pub qt: QueryType
}

pub fn new_fc(f: Field, qt: QueryType) -> FieldConf {
    FieldConf {
        f: f,
        qt: qt
    }
}

impl<'a> Field<'a> {
    pub fn unwrap(&self) -> &'a str  {
        match self {
            &Field::Bigint(name) => name,
            &Field::Timestamp(name) => name,
            &Field::Text(name) => name,
            &Field::Double(name) => name
        }
    }
    pub fn get_order(&self) -> &'a str  {
        match self {
            &Field::Bigint(_) => "desc",
            &Field::Timestamp(_) => "desc",
            &Field::Text(_) => "asc",
            _ => "asc"
        }
    }
}

pub fn add_field(query: String, f: &Field) -> String {
    match f {
        &Field::Bigint(name) => query + name + " bigint,",
        &Field::Timestamp(name) => query + name + " timestamp,",
        &Field::Text(name) => query + name + " text,",
        &Field::Double(name) => query + name + " double,"
    }
}