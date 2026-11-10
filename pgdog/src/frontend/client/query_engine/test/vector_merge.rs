//! Does a cross-shard pgvector top-k query use the streaming merge?
//!
//! Routes the query through the real query engine (rewrite, parse, route,
//! post-route projection rewrite), then checks merge eligibility and feeds
//! simulated shard responses through the multi-shard read loop.

use super::prelude::*;
use super::test_sharded_client;
use crate::backend::pool::connection::multi_shard::test_support::{
    can_merge, simulated_merge_binding, supported_order,
};
use crate::frontend::router::parser::{OrderBy, order_by::OrderByColumn};
use crate::net::{
    CommandComplete, DataRow, Decoder, Field, FromBytes, Message, ReadyForQuery, RowDescription,
    ToBytes,
};

const SQL: &str =
    "SELECT id, embedding FROM sharded ORDER BY embedding <-> '[0.1,0.5,0.9]' LIMIT 3";

/// A pgvector column: the type OID is assigned at CREATE EXTENSION time, so
/// it is never a builtin OID.
fn vector_field(name: &str) -> Field {
    vector_field_oid(name, 16_385)
}

fn vector_field_oid(name: &str, type_oid: i32) -> Field {
    Field {
        name: name.into(),
        table_oid: 0,
        column: 0,
        type_oid,
        type_size: -1,
        type_modifier: 3,
        format: 0,
    }
}

async fn routed(messages: Vec<ProtocolMessage>) -> ClientRequest {
    let mut client = test_sharded_client();
    client.client_request = ClientRequest::from(messages);
    let mut engine = QueryEngine::from_client(&client).unwrap();
    let (mut context, request) = QueryEngineContext::new(&mut client);
    engine
        .rewrite_extended(&mut context, &mut request.messages)
        .unwrap();
    let rewrite = engine
        .parse_and_rewrite(&mut context, request)
        .await
        .unwrap();
    assert!(
        engine
            .route_query(&mut context, request, rewrite.as_ref())
            .await
            .unwrap()
    );
    request.clone()
}

fn helper_sort() -> Vec<OrderBy> {
    vec![OrderBy::Column(OrderByColumn::Asc(
        "__pgdog_order_col0".into(),
    ))]
}

#[tokio::test]
async fn vector_topk_simple_protocol_is_merge_eligible() {
    let request = routed(vec![Query::new(SQL).into()]).await;
    // The vector distance is projected as a float8 helper column and the
    // route sorts on that helper rather than on the vector itself.
    assert_eq!(request.route().order_by(), helper_sort().as_slice());
    assert!(can_merge(&request));
}

#[tokio::test]
async fn vector_topk_extended_protocol_is_merge_eligible() {
    let request = routed(vec![
        Parse::new_anonymous(SQL).into(),
        Bind::new_statement("").into(),
        Describe::new_portal("").into(),
        Execute::new().into(),
        Sync.into(),
    ])
    .await;
    assert_eq!(request.route().order_by(), helper_sort().as_slice());
    assert!(can_merge(&request));
}

#[tokio::test]
async fn vector_topk_row_description_supports_streaming_merge() {
    let request = routed(vec![Query::new(SQL).into()]).await;
    let rd = RowDescription::new(&[
        Field::bigint("id"),
        vector_field("embedding"),
        Field::double("__pgdog_order_col0"),
    ]);
    assert!(supported_order(request.route(), &Decoder::from(rd)));
}

fn shard_response(rows: &[(i64, &str, f64)]) -> Vec<Message> {
    shard_response_oid(rows, 16_385)
}

fn shard_response_oid(rows: &[(i64, &str, f64)], vector_oid: i32) -> Vec<Message> {
    let rd = RowDescription::new(&[
        Field::bigint("id"),
        vector_field_oid("embedding", vector_oid),
        Field::double("__pgdog_order_col0"),
    ]);
    let mut messages = vec![rd.message()];
    for &(id, vector, distance) in rows {
        let mut row = DataRow::new();
        row.add(id).add(vector.to_owned()).add(distance.to_string());
        messages.push(row.message());
    }
    messages.push(CommandComplete::from_str(&format!("SELECT {}", rows.len())).message());
    messages.push(ReadyForQuery::idle().message());
    messages
}

#[tokio::test]
async fn vector_topk_streams_through_merge_buffer() {
    let request = routed(vec![Query::new(SQL).into()]).await;
    let route = request.route().clone();
    let mut binding = simulated_merge_binding(
        &route,
        vec![
            (
                0,
                shard_response(&[
                    (1, "[0,0,0]", 0.1),
                    (3, "[1,1,1]", 0.3),
                    (5, "[2,2,2]", 0.5),
                ]),
            ),
            (
                1,
                shard_response(&[
                    (2, "[0,0,1]", 0.2),
                    (4, "[1,1,2]", 0.4),
                    (6, "[2,2,3]", 0.6),
                ]),
            ),
        ],
    );
    let mut ids = vec![];
    let mut emitted_before_all_complete = false;
    while let Some(message) = binding.read().await.unwrap() {
        if message.code() == 'D' {
            let row = DataRow::from_bytes(message.to_bytes()).unwrap();
            assert_eq!(row.len(), 2, "helper column dropped");
            assert_eq!(binding.result_buffer_kind(), "Merge");
            emitted_before_all_complete |= binding.command_complete_count() < 2;
            ids.push(row.get_text(0).unwrap().parse::<i64>().unwrap());
        }
    }
    assert_eq!(ids, [1, 2, 3]);
    assert!(emitted_before_all_complete);
}

/// Each database gets its own OID for the pgvector type at CREATE EXTENSION
/// time, so shards that are separate databases usually disagree on it. Only the
/// sort keys' types matter to the merge.
#[tokio::test]
async fn vector_topk_differing_vector_oids_still_stream() {
    let request = routed(vec![Query::new(SQL).into()]).await;
    let route = request.route().clone();
    let mut binding = simulated_merge_binding(
        &route,
        vec![
            (
                0,
                shard_response_oid(
                    &[
                        (1, "[0,0,0]", 0.1),
                        (3, "[1,1,1]", 0.3),
                        (5, "[2,2,2]", 0.5),
                    ],
                    16_385,
                ),
            ),
            (
                1,
                shard_response_oid(
                    &[
                        (2, "[0,0,1]", 0.2),
                        (4, "[1,1,2]", 0.4),
                        (6, "[2,2,3]", 0.6),
                    ],
                    16_390,
                ),
            ),
        ],
    );
    let mut ids = vec![];
    while let Some(message) = binding.read().await.unwrap() {
        if message.code() == 'D' {
            let row = DataRow::from_bytes(message.to_bytes()).unwrap();
            assert_eq!(binding.result_buffer_kind(), "Merge");
            assert!(
                binding.command_complete_count() < 2,
                "emit before all shards finish"
            );
            ids.push(row.get_text(0).unwrap().parse::<i64>().unwrap());
        }
    }
    assert_eq!(ids, [1, 2, 3]);
}
