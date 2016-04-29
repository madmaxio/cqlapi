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



pub fn field_create_job<T>(conf: &Conf<T>, schema: &mut Schema, fc: &FieldConf) {
    match fc.qt {
        QueryType::Storaged => {
            storaged_create_job(conf, schema, &fc.f);
        }
        QueryType::Value => {
            by_field_create_job(conf, schema, &fc.f);
        }
        QueryType::Substring => {
            by_field_create_job(conf, schema, &fc.f);
            by_substring_create_job(conf, schema, &fc.f);
        }
    }
}

pub fn storaged_create_job<T>(conf: &Conf<T>, schema: &mut Schema, f: &Field) {
}
pub fn by_field_create_job<T>(conf: &Conf<T>, schema: &mut Schema, f: &Field) {
    // (group id) f1 f2 ... fn                                  main
    // (group f1 id) f2 ... fn                                  Value
    let field_name = f.get_name();
    let mut query = "create table ".to_string() + conf.name + "_by_field_" + field_name + " (group bigint,
        id bigint, created_at timestamp, updated_at timestamp,";
    match conf.fields {
        Some(ref f) => {
            query = f.iter().fold(query, |query, x| {
                add_field(query, &x.f)
            });
        }
        _ => {}
    }

    query = query + "primary key (group," + field_name + ",id)
        )
        with clustering order by (" + field_name + " " + f.get_order() + ",id desc) and gc_grace_seconds = 86400";

    //println!("");
    //println!("{}", query);

    schema.queries.push(query);
}
pub fn by_substring_create_job<T>(conf: &Conf<T>, schema: &mut Schema, f: &Field) {
    // (group id) f1 f2 ... fn                                  main
    // (group f1 id) f2 ... fn + (group f1_substring f1)        Substring

    let field_name = f.get_name();
    let mut query = "create table ".to_string()
    + conf.name + "_" + field_name
    + "_substring (group bigint,substring text,value text, primary key (group,substring,value)) with gc_grace_seconds = 86400";

    //println!("");
    //println!("{}", query);

    schema.queries.push(query);
}
pub fn by_entity_create_job<T>(conf: &Conf<T>, schema: &mut Schema, entity_name: &str) {
    // (group id) f1 f2 ... fn                                  main
    // (group entity_id id) f1 f2 ... fn                        by_entity
    let mut query = "create table ".to_string() + conf.name + "_by_entity_" + entity_name
    + " (group bigint, entity bigint, id bigint, created_at timestamp, updated_at timestamp,";
    match conf.fields {
        Some(ref f) => {
            query = f.iter().fold(query, |query, x| {
                add_field(query, &x.f)
            });
        }
        _ => {}
    }

    query = query + "primary key (group,entity,id)
        )
        with clustering order by (entity desc,id desc) and gc_grace_seconds = 86400";

    //println!("");
    //println!("{}", query);

    schema.queries.push(query);
}
pub fn by_many_create_job<T>(conf: &Conf<T>, schema: &mut Schema, entity_name: &str) {
    // (group id) f1 f2 ... fn                                  main
    // (group entity_id id row) f1 f2 ... fn                    by_many
    let mut query = "create table ".to_string() + conf.name + "_by_many_" + entity_name
    + " (group bigint,entity bigint,id bigint,row bigint,created_at timestamp,updated_at timestamp,";
    match conf.fields {
        Some(ref f) => {
            query = f.iter().fold(query, |query, x| {
                add_field(query, &x.f)
            });
        }
        _ => {}
    }

    query = query + "primary key (group,entity,id,row)
        )
        with clustering order by (entity desc,id desc, row desc) and gc_grace_seconds = 86400";

    //println!("");
    //println!("{}", query);

    schema.queries.push(query);
}