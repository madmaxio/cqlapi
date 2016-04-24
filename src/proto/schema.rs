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


// Rules:
// fields with QueryType Storaged, Value, Substring must form a set without duplicates (check not implemented)
// fields with QueryType Substring must be Field::Text(_) only (check not implemented)

// (group id) f1 f2 ... fn                                  main
// (group f1 id) f2 ... fn                                  Value
// (group f1 id) f2 ... fn + (group f1_substring f1)        Substring
// (group entity_id id) f1 f2 ... fn                        by_entity
// (group entity_id id row) f1 f2 ... fn                    by_many

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