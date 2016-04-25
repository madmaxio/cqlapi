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
}