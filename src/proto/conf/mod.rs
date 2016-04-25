pub mod conf_schema;
pub mod conf_create;
pub mod conf_insert;
pub mod conf_first;
pub mod conf_list;

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

use proto::jobs::jobs_create::*;
use proto::jobs::jobs_insert::*;
use proto::jobs::jobs_insert_all::*;
use proto::jobs::jobs_delete::*;

use proto::schema::Schema;



// (group id) f1 f2 ... fn                                  main
// (group f1 id) f2 ... fn                                  Value
// (group f1 id) f2 ... fn + (group f1_substring f1)        Substring
// (group entity_id id) f1 f2 ... fn                        by_entity
// (group entity_id id row) f1 f2 ... fn                    by_many



pub struct Conf<'a, T> {
    pub name: &'a str,
    pub e: T,
    pub fields: Option<Vec<FieldConf<'a>>>,
    pub by_entity: Option<Vec<&'a str>>,
    pub by_many: Option<Vec<&'a str>>
}

pub fn new_conf<'a, E: 'a>(name: &'a str, e: E, fields: Option<Vec<FieldConf<'a>>>, by_entity: Option<Vec<&str>>, by_many: Option<Vec<&str>>) -> Conf<'a, E> {
    Conf {
        e : e,
        name: name,
        fields: fields,
        by_entity: None,
        by_many: None
    }
}