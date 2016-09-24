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
    pub fn list_with_limit_by_id(conn: &mut Connection, group: i64, last_item_id: Option<i64>) -> Vec<HashMap<String, Column>> {
        let mut values = vec![];
        values.push(Column::Bigint(group));

        let mut query: String;

        match last_item_id {
            Some(id) => {
                values.push(Column::Bigint(id));
                query = "select * from test1.company where group = ? and id < ? limit 10".to_string();
            }
            None => {
                query = "select * from test1.company where group = ? limit 10".to_string();
            }
        }

        //println!("query is {}", query);

        let result = conn.prm_query(query, values, Consistency::Quorum).unwrap();

        //println!("result of proto list is {:?}", result);

        let mut res = vec![];

        match result {
            Response::Result(rb) => {
                match rb {
                    ResultBody::Rows(rows, paging_state) => {
                        for row in rows.iter() {
                            res.push(row.columns.clone());
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }

        res
    }
}

// (group id) f1 f2 ... fn                                  main
// (group f1 id) f2 ... fn                                  Value


/*
pub fn prepare(conn: &mut Connection) ->Result<Vec<u8>, ()> {
    let result = conn.prepare("
		insert into test1.person_first_name_index (group, first_name_index, first_name) values (?, ?, ?)
		".to_string());

    if let Response::Result(result) = result.unwrap() {
        if let ResultBody::Prepared(id) = result {
            return Ok(id)
        }
    }

    Err(())
}

pub fn add(prepared_id: Vec<u8>, group: i64, index: String, value: String) -> BatchQuery {
    let mut values = vec![];

    values.push(Column::Bigint(group));
    values.push(Column::String(index));
    values.push(Column::String(value));

    BatchQuery::Prepared(prepared_id, values)
}

*/


/*
pub fn list_with_id_in<T>(conn: &mut Connection, from: &str, group: i64, values: &Vec<i64>, some_closure: &Fn(&Row) -> T) -> Vec<T> {

    let mut query = values.iter().fold(
        "select * from ".to_string() + from + " where group = ? and id in (",
        |query, x| query + "?,");

    let len = query.len();

    query.truncate(len - 1);

    query.push(')');

    let mut values = values.iter().map(|x| Column::Bigint(*x)).collect::<Vec<Column>>();
    values.insert(0, Column::Bigint(group));

    println!("dat query {}", values.len());

    let result = conn.prm_query(query, values, Consistency::Quorum).unwrap();

    //println!("result of id_range is {:?}", result);

    let mut res = vec![];

    match result {
        Response::Result(rb) => {
            match rb {
                ResultBody::Rows(rows) => {
                    for row in rows.iter() {
                        res.push(some_closure(row));
                    }
                }
                _ => {}
            }
        }
        _ => {}
    }

    res
}
*/