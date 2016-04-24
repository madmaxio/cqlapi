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

impl<'a, T> Conf<'a, T> {

    // (group id) f1 f2 ... fn                                  main
    // (group f1 id) f2 ... fn                                  Value
    // (group f1 id) f2 ... fn + (group f1_substring f1)        Substring
    // (group entity_id id) f1 f2 ... fn                        by_entity
    // (group entity_id id row) f1 f2 ... fn                    by_many

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
    pub fn get_schema(&self) -> Schema {

        let mut s = Schema {
            queries: vec![]
        };

        self.create(&mut s);

        s
    }
    pub fn insert(&self, mut conn: &mut Connection, group: i64, id: i64, f_v: Vec<(&FieldConf, Column)>, consistency: Consistency) -> Result<Response> {
        let state = self.first_by_id(&mut conn, group, id);

        let mut f = vec![];

        let mut values = vec![];


        for &(fc, ref v) in f_v.iter() {
            f.push(fc);
            values.push(v.clone());
        }

        conn.execute_batch(self.get_batch_for_insert(group, id, f, values, state), consistency)
    }
    pub fn get_batch_for_insert(&self, group: i64, id: i64, f: Vec<&FieldConf>, mut values: Vec<Column>, state: Option<HashMap<String, Column>>) -> Vec<BatchQuery> {

        let mut batch = vec![];

        if let Some(state) = state {
            if f.len() != values.len() {
                panic!("fields and values count don't match");
            }

            for i in 0..f.len() {
                let c = state.get(f[i].f.unwrap()).unwrap();
                if *c != values[i] {
                    if let Some(bq) = field_delete_job(self, &f[i], group, id, values.clone(), state.clone(), c.clone()) {
                        batch.push(bq);
                    }
                }
            }
        }


        let name = self.name;

        let mut query = "insert into test1.".to_string() + name + " (group,id,";

        if f.len() != values.len() {
            panic!("fields and values count don't match");
        }

        query = f.iter().fold(query, |query, x| {
            if let Some(bq) = field_insert_job(self, x, &f, group, id, values.clone()) {
                batch.push(bq);
            }
            query + &x.f.unwrap() + ","
        });

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

        batch.push(BatchQuery::SimpleWithParams(query, values));

        batch
    }
    pub fn insert_all(&self, mut conn: &mut Connection, group: i64, id: i64, mut values: Vec<Column>, consistency: Consistency) -> Result<Response> {
        let state = self.first_by_id(&mut conn, group, id);
        conn.execute_batch(self.get_batch_for_insert_all(group, id, values, state), consistency)
    }
    pub fn get_batch_for_insert_all(&self, group: i64, id: i64, mut values: Vec<Column>, state: Option<HashMap<String, Column>>) -> Vec<BatchQuery> {

        let mut batch = vec![];

        if let Some(state) = state {
            if let Some(ref f) = self.fields {

                if f.len() != values.len() {
                    panic!("fields and values count don't match");
                }

                for i in 0..f.len() {
                    let c = state.get(f[i].f.unwrap()).unwrap();
                    if *c != values[i] {
                        if let Some(bq) = field_delete_job(self, &f[i], group, id, values.clone(), state.clone(), c.clone()) {
                            batch.push(bq);
                        }
                    }
                }
            }
        }

        let name = self.name;
        let mut query = "insert into test1.".to_string() + name + " (group,id,";

        if let Some(ref f) = self.fields {

            if f.len() != values.len() {
                panic!("fields and values count don't match");
            }

            query = f.iter().fold(query, |query, x| {
                if let Some(bq) = field_insert_all_job(self, x, group, id, values.clone()) {
                    batch.push(bq);
                }
                query + &x.f.unwrap() + ","
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

        batch.push(BatchQuery::SimpleWithParams(query, values));

        batch
    }
    pub fn first(&self, conn: &mut Connection, group: i64, fc: &FieldConf, key: Column) -> Option<HashMap<String, Column>> {

        match fc.qt {
            QueryType::Storaged => {}
            QueryType::Value | QueryType::Substring => {

                let mut values = vec![];

                values.push(Column::Bigint(group));
                values.push(key);

                let field_name = fc.f.unwrap();

                let query = "select * from test1.".to_string() + self.name + "_by_field_" + field_name + " where group = ? and " + field_name + " = ? limit 1";

                let result = conn.prm_query(query, values, Consistency::Quorum).unwrap();

                //println!("result of first is {:?}", result);

                match result {
                    Response::Result(rb) => {
                        match rb {
                            ResultBody::Rows(rows) => {
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
                    ResultBody::Rows(rows) => {
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
                    ResultBody::Rows(rows) => {
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