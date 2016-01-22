#[cfg(test)]

use chrono::*;

use rustcql::Connection;
use rustcql::shared::Consistency;
use rustcql::shared::Consistency::Quorum;
use rustcql::shared::Column;
use rustcql::shared::Response;
use rustcql::shared::ResultBody;
use rustcql::shared::BatchQuery;
use rustcql::connect;

use super::*;



pub struct Entity<'a> {
    pub test1: FieldConf<'a>,
    pub test2: FieldConf<'a>,
    pub test3: FieldConf<'a>,
    pub test4: FieldConf<'a>
}

fn get_entity<'a>() -> Entity<'a> {
    Entity {
        test1: FieldConf {
            f: Field::Text("test1"), qt: QueryType::Substring
        },
        test2: FieldConf {
            f: Field::Datetime("test2"), qt: QueryType::Value
        },
        test3: FieldConf {
            f: Field::Text("test3"), qt: QueryType::Storaged
        },
        test4: FieldConf {
            f: Field::Double("test4"), qt: QueryType::Value
        }
    }
}

pub fn get_conf<'a>() -> Conf<'a, Entity<'a>> {

    let e = get_entity();

    Conf {
        e : get_entity(),
        name: "test",
        fields: Some(vec![
            e.test1,
            e.test2,
            e.test3,
            e.test4
        ]),
        by_entity: Some(vec![
            "test_union"
        ]),
        by_many: Some(vec![
            "test_paper"
        ])
    }
}

#[test]
fn test_get_entitiy() {
    let e = get_entity();
}

#[test]
fn test_get_conf() {
    let c = get_conf();
}

#[test]
fn test_get_schema() {
    let c = get_conf();
    let s = c.get_schema();
}

#[test]
pub fn test_create_schema() {

    let mut conn = connect(HOST.to_string()).unwrap();

    create_schema(&mut conn, vec![
        get_conf().get_schema()
    ]);
}

#[test]
fn test_insert_all() {

    let c = get_conf();

    let now = UTC::now().to_rfc3339();

    let mut conn = connect(HOST.to_string()).unwrap();

    c.insert_all(&mut conn, 1, 1, vec![
        Column::String("asd".to_string()),
        Column::String(now.clone()),
        Column::String("qwe".to_string()),
        Column::Double(1.333333)
    ], Quorum);

    let r = c.first_by_id(&mut conn, 1, 1).unwrap();
}