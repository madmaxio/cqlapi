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

use proto::conf::*;
use proto::schema::Schema;


// (group id) f1 f2 ... fn                                  main
// (group f1 id) f2 ... fn                                  Value
// (group f1 id) f2 ... fn + (group f1_substring f1)        Substring
// (group entity_id id) f1 f2 ... fn                        by_entity
// (group entity_id id row) f1 f2 ... fn                    by_many

impl<'a, T> Conf<'a, T> {
    pub fn create(&self, schema: &mut Schema) {

        let mut query = "create table ".to_string() + self.name + " (group bigint,
        id bigint, created_at timestamp, updated_at timestamp,";

        match self.fields {
            Some(ref f) => {
                query = f.iter().fold(query, |query, x| {
                    field_create_job(self, schema, x);
                    add_field(query, &x.f)
                });
            }
            _ => {}
        }

        match self.by_entity {
            Some(ref fields) => {
                for x in fields.iter() {
                    by_entity_create_job(self, schema, x);
                }
            }
            _ => {}
        }

        match self.by_many {
            Some(ref fields) => {
                for x in fields.iter() {
                    by_many_create_job(self, schema, x);
                }
            }
            _ => {}
        }

        query = query + "primary key (group, id)
        )
        with clustering order by (id desc) and gc_grace_seconds = 86400";

        //println!("");
        //println!("{}", query);

        schema.queries.push(query);
    }
}