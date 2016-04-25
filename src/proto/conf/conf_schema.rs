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
    pub fn get_schema(&self) -> Schema {

        let mut s = Schema {
            queries: vec![]
        };

        self.create(&mut s);

        s
    }
}