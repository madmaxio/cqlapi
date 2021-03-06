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

use proto::shared::*;
use proto::conf::*;
use proto::schema::*;

pub struct Entity<'a> {
    pub test1: FieldConf<'a>,
    pub test2: FieldConf<'a>,
    pub test3: FieldConf<'a>,
    pub test4: FieldConf<'a>,
    pub timestamp_test1: FieldConf<'a>,
    pub timestamp_test2: FieldConf<'a>
}

fn get_entity<'a>() -> Entity<'a> {
    Entity {
        test1: new_fc(Field::Text("test1"), QueryType::Substring),
        test2: new_fc(Field::Timestamp("test2"), QueryType::Value),
        test3: new_fc(Field::Text("test3"), QueryType::Storaged),
        test4: new_fc(Field::Double("test4"), QueryType::Value),
        timestamp_test1: new_fc(Field::Timestamp("timestamp_test1"), QueryType::Value),
        timestamp_test2: new_fc(Field::Timestamp("timestamp_test2"), QueryType::Value)
    }
}

pub fn get_conf<'a>() -> Conf<'a, Entity<'a>> {

    let e = get_entity();

    new_conf("test", get_entity(),
        Some(vec![
             e.test1,
             e.test2,
             e.test3,
             e.test4,
             e.timestamp_test1,
             e.timestamp_test2
             ]),
         Some(vec!["test_union"]),
         Some(vec!["test_paper"]))

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

//#[ignore]
#[test]
pub fn test_create_schema() {

    let mut conn = connect(HOST.to_string()).unwrap();

    create_schema(&mut conn, vec![
        get_conf().get_schema()
    ], 1);
}

//#[ignore]
#[test]
fn test_insert_all() {

    let c = get_conf();

    let mut conn = connect(HOST.to_string()).unwrap();

    c.insert_all(&mut conn, 1, 1, vec![
        Column::String("asd".to_string()),
        now(),
        Column::String("qwe".to_string()),
        Column::Double(1.333333),
        now(),
        now()
    ], Quorum);

    let r = c.first_by_id(&mut conn, 1, 1).unwrap();
}