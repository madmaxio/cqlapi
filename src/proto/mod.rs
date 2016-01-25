mod tests;


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


//pub static HOST: &'static str = "10.0.2.15:9042";
pub static HOST: &'static str = "127.0.0.1:9042";



pub enum QueryType {
    Storaged,
    Value,
    Substring
}

pub enum Field<'a> {
    Bigint(&'a str),
    Timestamp(&'a str),
    Text(&'a str),
    Double(&'a str),
    Datetime(&'a str)
}

pub struct FieldConf<'a> {
    pub f: Field<'a>,
    pub qt: QueryType
}



// Rules:
// fields with QueryType Storaged, Value, Substring must form a set without duplicates (check not implemented)
// fields with QueryType Substring must be Field::Text(_) only (check not implemented)

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

pub struct Schema {
    pub queries: Vec<String>
}

pub fn create_schema(conn: &mut Connection, context: Vec<Schema>, replication_factor: u32) {
    let result = conn.query("DROP KEYSPACE IF EXISTS test1".to_string(), Consistency::Quorum);
    println!("Result of DROP KEYSPACE was {:?}", result);

    let query = "CREATE KEYSPACE test1
               WITH replication = {
                 'class' : 'SimpleStrategy',
                 'replication_factor' : ".to_string() + &replication_factor.to_string() + "}";

    let result = conn.query(query, Consistency::Quorum);

    println!("Result of CREATE KEYSPACE was {:?}", result);

    let result = conn.query("USE test1".to_string(), Consistency::Quorum);
    println!("Result of USE was {:?}", result);

    for s in context.iter() {
        for query in s.queries.iter() {
            let result = conn.query(query.to_string(), Consistency::Quorum);
            println!("Result was {:?}", result);
        }
    }
}

impl<'a> Field<'a> {
    pub fn unwrap(&self) -> &'a str  {
        match self {
            &Field::Bigint(name) => name,
            &Field::Timestamp(name) => name,
            &Field::Text(name) => name,
            &Field::Double(name) => name,
            &Field::Datetime(name) => name
        }
    }
    pub fn get_order(&self) -> &'a str  {
        match self {
            &Field::Bigint(_) => "desc",
            &Field::Timestamp(_) => "desc",
            &Field::Text(_) => "asc",
            &Field::Datetime(_) => "desc",
            _ => "asc"
        }
    }
}

fn add_field(query: String, f: &Field) -> String {
    match f {
        &Field::Bigint(name) => query + name + " bigint,",
        &Field::Timestamp(name) => query + name + " timestamp,",
        &Field::Text(name) => query + name + " text,",
        &Field::Double(name) => query + name + " double,",
        &Field::Datetime(name) => query + name + " text,"
    }
}

fn field_create_job<T>(conf: &Conf<T>, schema: &mut Schema, fc: &FieldConf) {
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

fn storaged_create_job<T>(conf: &Conf<T>, schema: &mut Schema, f: &Field) {
}
fn by_field_create_job<T>(conf: &Conf<T>, schema: &mut Schema, f: &Field) {
    // (group id) f1 f2 ... fn                                  main
    // (group f1 id) f2 ... fn                                  Value
    let field_name = f.unwrap();
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
fn by_substring_create_job<T>(conf: &Conf<T>, schema: &mut Schema, f: &Field) {
    // (group id) f1 f2 ... fn                                  main
    // (group f1 id) f2 ... fn + (group f1_substring f1)        Substring

    let field_name = f.unwrap();
    let mut query = "create table ".to_string()
    + conf.name + "_" + field_name
    + "_substring (group bigint,substring text,value text, primary key (group,substring,value)) with gc_grace_seconds = 86400";

    //println!("");
    //println!("{}", query);

    schema.queries.push(query);
}
fn by_entity_create_job<T>(conf: &Conf<T>, schema: &mut Schema, entity_name: &str) {
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
fn by_many_create_job<T>(conf: &Conf<T>, schema: &mut Schema, entity_name: &str) {
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

fn field_insert_job<T>(conf: &Conf<T>, fc: &FieldConf, fields: &Vec<&FieldConf>, group: i64, id: i64, mut values: Vec<Column>) -> Option<BatchQuery> {
    match fc.qt {
        QueryType::Storaged => {
            storaged_insert_job(conf, &fc.f, fields, group, id, values.clone());
            None
        }
        QueryType::Value =>  {
            Some(by_field_insert_job(conf, &fc.f, fields, group, id, values.clone()))
        }
        QueryType::Substring => {
            by_substring_insert_job(conf, &fc.f, fields, group, id, values.clone());
            Some(by_field_insert_job(conf, &fc.f, fields, group, id, values.clone()))
        }
    }
}

fn storaged_insert_job<T>(conf: &Conf<T>, f: &Field, fields: &Vec<&FieldConf>, group: i64, id: i64, mut values: Vec<Column>) {
}
fn by_field_insert_job<T>(conf: &Conf<T>, f: &Field, fields: &Vec<&FieldConf>, group: i64, id: i64, mut values: Vec<Column>) -> BatchQuery {

    // (group id) f1 f2 ... fn                                  main
    // (group f1 id) f2 ... fn                                  Value

    let field_name = f.unwrap();

    let mut query = "insert into test1.".to_string() + conf.name + "_by_field_" + field_name + " (group,id,";

    if fields.len() != values.len() {
        panic!("fields and values count don't match");
    }

    query = fields.iter().fold(query, |query, x| {
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

    BatchQuery::SimpleWithParams(query, values)

}
fn by_substring_insert_job<T>(conf: &Conf<T>, f: &Field, fields: &Vec<&FieldConf>, group: i64, id: i64, mut values: Vec<Column>) {
    // (group id) f1 f2 ... fn                                  main
    // (group f1 id) f2 ... fn + (group f1_substring f1)        Substring


}
fn by_entity_insert_job<T>(conf: &Conf<T>, schema: &mut Schema, entity_name: &str) {
    // (group id) f1 f2 ... fn                                  main
    // (group entity_id id) f1 f2 ... fn                        by_entity

}
fn by_many_insert_job<T>(conf: &Conf<T>, schema: &mut Schema, entity_name: &str) {
    // (group id) f1 f2 ... fn                                  main
    // (group entity_id id row) f1 f2 ... fn                    by_many

}

fn field_insert_all_job<T>(conf: &Conf<T>, fc: &FieldConf, group: i64, id: i64, mut values: Vec<Column>) -> Option<BatchQuery> {
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

fn storaged_insert_all_job<T>(conf: &Conf<T>, f: &Field, group: i64, id: i64, mut values: Vec<Column>) {
}
fn by_field_insert_all_job<T>(conf: &Conf<T>, f: &Field, group: i64, id: i64, mut values: Vec<Column>) -> BatchQuery {

    // (group id) f1 f2 ... fn                                  main
    // (group f1 id) f2 ... fn                                  Value

    let field_name = f.unwrap();

    let mut query = "insert into test1.".to_string() + conf.name + "_by_field_" + field_name + " (group,id,";

    if let Some(ref f) = conf.fields {

        if f.len() != values.len() {
            panic!("fields and values count don't match");
        }

        query = f.iter().fold(query, |query, x| {
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

    BatchQuery::SimpleWithParams(query, values)

}
fn by_substring_insert_all_job<T>(conf: &Conf<T>, f: &Field, group: i64, id: i64, mut values: Vec<Column>) {
    // (group id) f1 f2 ... fn                                  main
    // (group f1 id) f2 ... fn + (group f1_substring f1)        Substring


}
fn by_entity_insert_all_job<T>(conf: &Conf<T>, schema: &mut Schema, entity_name: &str) {
    // (group id) f1 f2 ... fn                                  main
    // (group entity_id id) f1 f2 ... fn                        by_entity

}
fn by_many_insert_all_job<T>(conf: &Conf<T>, schema: &mut Schema, entity_name: &str) {
    // (group id) f1 f2 ... fn                                  main
    // (group entity_id id row) f1 f2 ... fn                    by_many

}

fn field_delete_job<T>(conf: &Conf<T>, fc: &FieldConf, group: i64, id: i64, mut values: Vec<Column>, state: HashMap<String, Column>, fs: Column) -> Option<BatchQuery> {
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

fn storaged_delete_job<T>(conf: &Conf<T>, f: &Field, group: i64, id: i64, mut values: Vec<Column>) {
}
fn by_field_delete_job<T>(conf: &Conf<T>, f: &Field, group: i64, id: i64, mut values: Vec<Column>, state: HashMap<String, Column>, fs: Column) -> BatchQuery {

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
fn by_substring_delete_job<T>(conf: &Conf<T>, f: &Field, group: i64, id: i64, mut values: Vec<Column>) {
    // (group id) f1 f2 ... fn                                  main
    // (group f1 id) f2 ... fn + (group f1_substring f1)        Substring


}
fn by_entity_delete_job<T>(conf: &Conf<T>, schema: &mut Schema, entity_name: &str) {
    // (group id) f1 f2 ... fn                                  main
    // (group entity_id id) f1 f2 ... fn                        by_entity

}
fn by_many_delete_job<T>(conf: &Conf<T>, schema: &mut Schema, entity_name: &str) {
    // (group id) f1 f2 ... fn                                  main
    // (group entity_id id row) f1 f2 ... fn                    by_many

}

pub fn new_fc(f: Field, qt: QueryType) -> FieldConf {
    FieldConf {
        f: f,
        qt: qt
    }
}

pub fn new_conf<'a, E: 'a>(name: &'a str, e: E, fields: Option<Vec<FieldConf<'a>>>,
    by_entity: Option<Vec<&str>>, by_many: Option<Vec<&str>>) -> Conf<'a, E> {
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
    fn get_batch_for_insert(&self, f: Vec<&FieldConf>, group: i64, id: i64, mut values: Vec<Column>, state: Option<HashMap<String, Column>>) -> Vec<BatchQuery> {

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
    pub fn insert(&self, mut conn: &mut Connection, f: Vec<&FieldConf>, group: i64, id: i64, mut values: Vec<Column>, consistency: Consistency) -> Result<Response> {
        let state = self.first_by_id(&mut conn, group, id);
        conn.execute_batch(self.get_batch_for_insert(f, group, id, values, state), consistency)
    }
    fn get_batch_for_insert_all(&self, group: i64, id: i64, mut values: Vec<Column>, state: Option<HashMap<String, Column>>) -> Vec<BatchQuery> {

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
    pub fn insert_all(&self, mut conn: &mut Connection, group: i64, id: i64, mut values: Vec<Column>, consistency: Consistency) -> Result<Response> {
        let state = self.first_by_id(&mut conn, group, id);
        conn.execute_batch(self.get_batch_for_insert_all(group, id, values, state), consistency)
    }
    pub fn first(&self, conn: &mut Connection, fc: &FieldConf, group: i64, key: Column) -> Option<HashMap<String, Column>> {

        match fc.qt {
            QueryType::Storaged => {}
            QueryType::Value | QueryType::Substring => {

                let mut values = vec![];

                values.push(Column::Bigint(group));
                values.push(key);

                let field_name = fc.f.unwrap();

                let query = "select * from test1.".to_string() + self.name + "_by_" + field_name + " where group = ? and " + field_name + " = ? limit 1";

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