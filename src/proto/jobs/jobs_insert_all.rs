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



pub fn field_insert_all_job<T>(conf: &Conf<T>, fc: &FieldConf, group: i64, id: i64, mut values: Vec<Column>) -> Option<BatchQuery> {
    match fc.qt {
        QueryType::Storaged => {
            storaged_insert_all_job(conf, &fc.f, group, id, values.clone());
            None
        }
        QueryType::Value =>  {
            Some(by_field_insert_all_job(conf, &fc.f, group, id, values.clone()))
        }
        QueryType::Substring => {
            by_substring_insert_all_job(conf, &fc.f, group, id, values.clone());
            Some(by_field_insert_all_job(conf, &fc.f, group, id, values.clone()))
        }
    }
}

pub fn storaged_insert_all_job<T>(conf: &Conf<T>, f: &Field, group: i64, id: i64, mut values: Vec<Column>) {
}
pub fn by_field_insert_all_job<T>(conf: &Conf<T>, f: &Field, group: i64, id: i64, mut values: Vec<Column>) -> BatchQuery {

    // (group id) f1 f2 ... fn                                  main
    // (group f1 id) f2 ... fn                                  Value

    let field_name = f.get_name();

    let mut query = "insert into test1.".to_string() + conf.name + "_by_field_" + field_name + " (group,id,";

    if let Some(ref f) = conf.fields {

        if f.len() != values.len() {
            panic!("fields and values count don't match");
        }

        query = f.iter().fold(query, |query, x| {
            query + &x.f.get_name() + ","
        });
    }


    let len = query.len();

    query.truncate(len - 1);

    query = query + ") values (?,?,";

    for i in 0..values.len() {
        query = query + "?,";
    }

    let len = query.len();

    query.truncate(len - 1);

    query = query + ")";

    //println!("{}", query);

    values.insert(0, Column::Bigint(id));
    values.insert(0, Column::Bigint(group));

    //println!("{:?}", values);

    BatchQuery::SimpleWithParams(query, values)

}
pub fn by_substring_insert_all_job<T>(conf: &Conf<T>, f: &Field, group: i64, id: i64, mut values: Vec<Column>) {
    // (group id) f1 f2 ... fn                                  main
    // (group f1 id) f2 ... fn + (group f1_substring f1)        Substring


}
pub fn by_entity_insert_all_job<T>(conf: &Conf<T>, schema: &mut Schema, entity_name: &str) {
    // (group id) f1 f2 ... fn                                  main
    // (group entity_id id) f1 f2 ... fn                        by_entity

}
pub fn by_many_insert_all_job<T>(conf: &Conf<T>, schema: &mut Schema, entity_name: &str) {
    // (group id) f1 f2 ... fn                                  main
    // (group entity_id id row) f1 f2 ... fn                    by_many

}