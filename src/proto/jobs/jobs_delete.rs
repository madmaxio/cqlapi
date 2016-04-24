use std::collections::HashMap;
use std::io::Result;

use chrono::*;


use rustcql::Connection;
use rustcql::shared::Consistency;
use rustcql::shared::Response;
use rustcql::shared::ResultBody;
use rustcql::shared::Row;
use rustcql::shared::BatchQuery;
use rustcql::shared::Column;

use proto::shared::*;
use proto::conf::*;

use proto::schema::Schema;



pub fn field_delete_job<T>(conf: &Conf<T>, fc: &FieldConf, group: i64, id: i64, mut values: Vec<Column>, state: HashMap<String, Column>, fs: Column) -> Option<BatchQuery> {
    match fc.qt {
        QueryType::Storaged => {
            storaged_delete_job(conf, &fc.f, group, id, values.clone());
            None
        }
        QueryType::Value =>  {
            Some(by_field_delete_job(conf, &fc.f, group, id, values.clone(), state.clone(), fs.clone()))
        }
        QueryType::Substring => {
            by_substring_delete_job(conf, &fc.f, group, id, values.clone());
            Some(by_field_delete_job(conf, &fc.f, group, id, values.clone(), state.clone(), fs.clone()))
        }
    }
}

pub fn storaged_delete_job<T>(conf: &Conf<T>, f: &Field, group: i64, id: i64, mut values: Vec<Column>) {
}
pub fn by_field_delete_job<T>(conf: &Conf<T>, f: &Field, group: i64, id: i64, mut values: Vec<Column>, state: HashMap<String, Column>, fs: Column) -> BatchQuery {

    // (group id) f1 f2 ... fn                                  main
    // (group f1 id) f2 ... fn                                  Value

    let field_name = f.unwrap();

    let mut query = "delete from test1.".to_string() + conf.name + "_by_field_"
    + field_name + " where group = ? and " + field_name + " = ? and id = ?";

    //println!("{}", query);

    BatchQuery::SimpleWithParams(query, vec![
    Column::Bigint(group),
    fs,
    Column::Bigint(id)
    ])

}
pub fn by_substring_delete_job<T>(conf: &Conf<T>, f: &Field, group: i64, id: i64, mut values: Vec<Column>) {
    // (group id) f1 f2 ... fn                                  main
    // (group f1 id) f2 ... fn + (group f1_substring f1)        Substring


}
pub fn by_entity_delete_job<T>(conf: &Conf<T>, schema: &mut Schema, entity_name: &str) {
    // (group id) f1 f2 ... fn                                  main
    // (group entity_id id) f1 f2 ... fn                        by_entity

}
pub fn by_many_delete_job<T>(conf: &Conf<T>, schema: &mut Schema, entity_name: &str) {
    // (group id) f1 f2 ... fn                                  main
    // (group entity_id id row) f1 f2 ... fn                    by_many

}