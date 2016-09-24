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
    pub fn first(&self, conn: &mut Connection, group: i64, fc: &FieldConf, key: Column) -> Option<HashMap<String, Column>> {

        match fc.qt {
            QueryType::Storaged => {}
            QueryType::Value | QueryType::Substring => {

                let mut values = vec![];

                values.push(Column::Bigint(group));
                values.push(key);

                let field_name = fc.f.get_name();

                let query = "select * from test1.".to_string() + self.name + "_by_field_" + field_name + " where group = ? and " + field_name + " = ? limit 1";

                let result = conn.prm_query(query, values, Consistency::Quorum).unwrap();

                //println!("result of first is {:?}", result);

                match result {
                    Response::Result(rb) => {
                        match rb {
                            ResultBody::Rows(rows, paging_state) => {
                                if rows.len() > 0 {
                                    return Some(rows[0].columns.clone())
                                }
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
        }

        None
    }

    pub fn first_by_id(&self, conn: &mut Connection, group: i64, id: i64) -> Option<HashMap<String, Column>> {

        let mut values = vec![];

        values.push(Column::Bigint(group));
        values.push(Column::Bigint(id));

        let query = "select * from test1.".to_string() + self.name + " where group = ? and id = ? limit 1";

        let result = conn.prm_query(query, values, Consistency::Quorum).unwrap();

        //println!("result of first is {:?}", result);

        match result {
            Response::Result(rb) => {
                match rb {
                    ResultBody::Rows(rows, paging_state) => {
                        if rows.len() > 0 {
                            return Some(rows[0].columns.clone())
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }

        None
    }
}