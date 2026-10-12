use crate::frontend::router::parser::order_by::OrderByColumn;
use crate::{
    frontend::router::parser::{
        DistinctBy, OrderBy, Shard, ShardWithPriority,
        rewrite::statement::projection::{OrderByHelper, OrderBySource, ProjectionRewritePlan},
    },
    net::{BindComplete, DataRow, Field, Format},
};

use super::*;

fn read_strategy_servers(active: &[bool]) -> Vec<LinkedServer> {
    use crate::backend::{Pool, Server, pool::Guard};
    use tokio::time::Instant;

    active
        .iter()
        .enumerate()
        .map(|(index, &active)| {
            let mut server = Server::default();
            if active {
                server.prepared_statements_mut().state_mut().add('C');
            }
            // These protocol-only fixtures have no network connection to reuse.
            server.force_close();
            LinkedServer {
                server: Guard::new(Pool::new_test(), Box::new(server), Instant::now()),
                shard: index + 10,
                linked: false,
            }
        })
        .collect()
}

#[tokio::test]
async fn specific_read_strategy_reads_once_per_pass_and_resets() {
    let mut servers = read_strategy_servers(&[true, true, true]);
    let mut state = MultiShard::new(servers.len(), &Route::default());
    state.read_from(11);
    assert_eq!(state.read_strategy_mut().next_server(&servers), Some(1));
    // Yield control so the result-processing state can emit or choose again.
    assert_eq!(state.read_strategy_mut().next_server(&servers), None);
    state.read_strategy_mut().reset();
    // Selection follows the shard even if its position changes.
    servers.swap(0, 1);
    assert_eq!(state.read_strategy_mut().next_server(&servers), Some(0));

    // A query reset restores the default read policy.
    state.query_complete();
    assert_eq!(state.read_strategy_mut().next_server(&servers), Some(0));
    assert_eq!(state.read_strategy_mut().next_server(&servers), Some(1));
    assert_eq!(state.read_strategy_mut().next_server(&servers), Some(2));
    assert_eq!(state.read_strategy_mut().next_server(&servers), None);
}

#[tokio::test]
async fn specific_read_strategy_does_not_fall_back_to_other_servers() {
    let servers = read_strategy_servers(&[true, false, true]);
    let mut state = MultiShard::new(servers.len(), &Route::default());
    // Shard 11 is idle; shard 1 is absent even though vector index 1 exists.
    for shard_id in [11, 1] {
        state.read_from(shard_id);
        assert_eq!(state.read_strategy_mut().next_server(&servers), None);
    }
    state.read_from(12);
    assert_eq!(state.read_strategy_mut().next_server(&servers), Some(2));
}

#[test]
fn test_inconsistent_row_descriptions() {
    let route = Route::default();
    let mut multi_shard = MultiShard::new(2, &route);

    // Create two different row descriptions
    let rd1 = RowDescription::new(&[Field::text("name"), Field::bigint("id")]);
    let rd2 = RowDescription::new(&[Field::text("name")]); // Missing column

    // First row description should be processed successfully
    let result = multi_shard.handle_server_message(0, rd1.message()).unwrap();
    assert!(result.is_none()); // Not forwarded until all shards respond

    // Second inconsistent row description should cause an error
    let result = multi_shard.handle_server_message(0, rd2.message());
    assert!(result.is_err());

    if let Err(error) = result {
        let error_str = format!("{}", error);
        assert!(error_str.contains("inconsistent row descriptions"));
        assert!(error_str.contains("expected 2 columns, got 1 columns"));
    }
}

#[test]
fn test_inconsistent_data_rows() {
    let route = Route::default();
    let mut multi_shard = MultiShard::new(2, &route);

    // Set up row description first
    let rd = RowDescription::new(&[Field::text("name"), Field::bigint("id")]);
    multi_shard.handle_server_message(0, rd.message()).unwrap();

    // Create data rows with different column counts
    let mut dr1 = DataRow::new();
    dr1.add("test").add(123_i64);

    let mut dr2 = DataRow::new();
    dr2.add("only_name"); // Missing id column

    // First data row should be processed successfully
    let result = multi_shard.handle_server_message(0, dr1.message()).unwrap();
    assert!(result.is_none()); // Buffered, not forwarded immediately

    // Second inconsistent data row should cause an error
    let result = multi_shard.handle_server_message(0, dr2.message());
    assert!(result.is_err());

    if let Err(error) = result {
        let error_str = format!("{}", error);
        assert!(error_str.contains("inconsistent column count in data rows"));
        assert!(error_str.contains("expected 2 columns, got 1 columns"));
    }
}

#[test]
fn test_order_by_helper_after_star_expansion_is_dropped_after_sorting() {
    let mut plan = ProjectionRewritePlan::default();
    plan.order_by_helpers.push(OrderByHelper {
        sort_position: 0,
        source: OrderBySource::Column("price".into()),
        alias: "__pgdog_order_col0".into(),
        injected: true,
    });
    let mut route = Route::select(
        ShardWithPriority::new_default_unset(Shard::All),
        vec![OrderBy::Column(OrderByColumn::Asc(
            "__pgdog_order_col0".into(),
        ))],
        Default::default(),
        Default::default(),
        None,
    );
    route.projection_rewrite_plan = plan;
    let mut multi_shard = MultiShard::new(2, &route);

    let row_description = RowDescription::new(&[
        Field::bigint("id"),
        Field::text("value"),
        Field::timestamp("created_at"),
        Field::bigint("__pgdog_order_col0"),
    ]);
    assert!(
        multi_shard
            .handle_server_message(0, row_description.message())
            .unwrap()
            .is_none()
    );
    let client_description = multi_shard
        .handle_server_message(0, row_description.message())
        .unwrap()
        .unwrap();
    let client_description = RowDescription::from_bytes(client_description.to_bytes()).unwrap();
    assert_eq!(
        client_description
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        ["id", "value", "created_at"]
    );

    let mut first = DataRow::new();
    first
        .add(1_i64)
        .add("first")
        .add("2026-01-01 00:00:00")
        .add(20_i64);
    let mut second = DataRow::new();
    second
        .add(2_i64)
        .add("second")
        .add("2026-01-02 00:00:00")
        .add(10_i64);
    multi_shard
        .handle_server_message(0, first.message())
        .unwrap();
    multi_shard
        .handle_server_message(0, second.message())
        .unwrap();

    for _ in 0..2 {
        multi_shard
            .handle_server_message(0, CommandComplete::from_str("SELECT 1").message())
            .unwrap();
    }

    for expected in [2_i64, 1_i64] {
        let message = multi_shard.get_server_message().unwrap().unwrap();
        let row = DataRow::from_bytes(message.to_bytes()).unwrap();
        assert_eq!(row.len(), 3);
        assert_eq!(row.get::<i64>(0, Format::Text).unwrap(), expected);
    }
}

#[test]
fn test_rd_before_dr() {
    let mut multi_shard = MultiShard::new(
        3,
        &Route::read(ShardWithPriority::new_default_unset(Shard::All)),
    );
    let rd = RowDescription::new(&[Field::bigint("id")]);
    let mut dr = DataRow::new();
    dr.add(1i64);
    for _ in 0..2 {
        let result = multi_shard
            .handle_server_message(0, rd.message().backend(BackendPid::for_test(1)))
            .unwrap();
        assert!(result.is_none()); // dropped
        let result = multi_shard
            .handle_server_message(0, dr.message().backend(BackendPid::for_test(1)))
            .unwrap();
        assert!(result.is_none()); // buffered.
    }

    let result = multi_shard.handle_server_message(0, rd.message()).unwrap();
    assert_eq!(result, Some(rd.message()));
    let result = multi_shard.get_server_message().unwrap();
    // Waiting for command complete
    assert!(result.is_none());

    for _ in 0..3 {
        let result = multi_shard
            .handle_server_message(
                0,
                CommandComplete::from_str("SELECT 1")
                    .message()
                    .backend(BackendPid::for_test(1)),
            )
            .unwrap();
        assert!(result.is_none());
    }

    for _ in 0..2 {
        let result = multi_shard.get_server_message().unwrap();
        let id = BackendPid::for_test(1);
        assert_eq!(
            result.map(|m| m.backend(id)),
            Some(dr.message().backend(id))
        );
    }

    let result = multi_shard
        .get_server_message()
        .unwrap()
        .map(|m| m.backend(BackendPid::for_test(1)));
    assert_eq!(
        result,
        Some(
            CommandComplete::from_str("SELECT 3")
                .message()
                .backend(BackendPid::for_test(1))
        )
    );

    // Buffer is empty.
    assert!(multi_shard.get_server_message().unwrap().is_none());
}

#[test]
fn test_distinct_state_resets_between_requests() {
    let route = Route::select(
        ShardWithPriority::new_default_unset(Shard::All),
        vec![],
        Default::default(),
        Default::default(),
        Some(DistinctBy::Row),
    );
    let mut multi_shard = MultiShard::new(2, &route);
    let row_description = RowDescription::new(&[Field::bigint("id")]);
    let mut data_row = DataRow::new();
    data_row.add(1_i64);

    // The same DISTINCT query returning the same row in consecutive requests must
    // produce the row both times. Deduplication state is scoped to one request.
    for request in 1..=2 {
        for _ in 0..2 {
            multi_shard
                .handle_server_message(0, row_description.message())
                .unwrap();
            multi_shard
                .handle_server_message(0, data_row.message())
                .unwrap();
            multi_shard
                .handle_server_message(0, CommandComplete::from_str("SELECT 1").message())
                .unwrap();
        }

        let message = multi_shard
            .get_server_message()
            .unwrap()
            .unwrap_or_else(|| panic!("request {request} should return its distinct row"));
        assert_eq!(message.code(), 'D', "request {request}");

        let row = DataRow::from_bytes(message.to_bytes()).unwrap();
        assert_eq!(row.get::<i64>(0, Format::Text).unwrap(), 1);

        let message = multi_shard
            .get_server_message()
            .unwrap()
            .unwrap_or_else(|| panic!("request {request} should return CommandComplete"));
        let complete = CommandComplete::from_bytes(message.to_bytes()).unwrap();
        assert_eq!(complete.rows().unwrap(), Some(1), "request {request}");
        assert!(multi_shard.get_server_message().unwrap().is_none());

        multi_shard.query_complete();
    }
}

#[test]
fn test_ready_for_query_error_preservation() {
    let route = Route::default();
    let mut multi_shard = MultiShard::new(2, &route);

    // Create ReadyForQuery messages - one with transaction error, one normal
    let rfq_error = ReadyForQuery::error();
    let rfq_normal = ReadyForQuery::in_transaction(false);

    // Forward first ReadyForQuery message with error state
    let result = multi_shard
        .handle_server_message(0, rfq_error.message())
        .unwrap();
    assert!(result.is_none()); // Should not be forwarded yet (waiting for second shard)

    // Forward second normal ReadyForQuery message
    let result = multi_shard
        .handle_server_message(0, rfq_normal.message())
        .unwrap();

    // Should return the error message, not the normal one
    assert!(result.is_some());
    let returned_message = result.unwrap();
    let returned_rfq = ReadyForQuery::from_bytes(returned_message.to_bytes()).unwrap();
    assert!(returned_rfq.is_transaction_aborted());
}

#[test]
fn test_omni_command_complete_not_summed() {
    // For omni-sharded tables, we should NOT sum row counts across shards.
    let route = Route::write(ShardWithPriority::new_table_omni(Shard::All)).with_omnisharded(true);
    let mut multi_shard = MultiShard::new(3, &route);

    let backend1 = BackendPid::for_test(1);
    let backend2 = BackendPid::for_test(2);
    let backend3 = BackendPid::for_test(3);

    // All shards report UPDATE 5
    multi_shard
        .handle_server_message(
            0,
            CommandComplete::from_str("UPDATE 5")
                .message()
                .backend(backend1),
        )
        .unwrap();
    multi_shard
        .handle_server_message(
            0,
            CommandComplete::from_str("UPDATE 5")
                .message()
                .backend(backend2),
        )
        .unwrap();
    multi_shard
        .handle_server_message(
            0,
            CommandComplete::from_str("UPDATE 5")
                .message()
                .backend(backend3),
        )
        .unwrap();

    let result = multi_shard.get_server_message().unwrap();
    let cc = CommandComplete::from_bytes(result.unwrap().to_bytes()).unwrap();
    // Should be 5 (from one shard), not 15 (sum of all shards)
    assert_eq!(cc.rows().unwrap(), Some(5));
}

#[test]
fn test_omni_command_complete_uses_first_shard_row_count() {
    // For omni, we use the first shard's row count for consistency with DataRow behavior.
    let route = Route::write(ShardWithPriority::new_table_omni(Shard::All)).with_omnisharded(true);
    let mut multi_shard = MultiShard::new(2, &route);

    let backend1 = BackendPid::for_test(1);
    let backend2 = BackendPid::for_test(2);

    // First shard reports 7 rows
    multi_shard
        .handle_server_message(
            0,
            CommandComplete::from_str("UPDATE 7")
                .message()
                .backend(backend1),
        )
        .unwrap();

    // Second shard reports 9 rows (different, to distinguish first vs last)
    multi_shard
        .handle_server_message(
            0,
            CommandComplete::from_str("UPDATE 9")
                .message()
                .backend(backend2),
        )
        .unwrap();

    let result = multi_shard.get_server_message().unwrap();
    let cc = CommandComplete::from_bytes(result.unwrap().to_bytes()).unwrap();
    // Should be 7 (from FIRST shard), not 9 (from last)
    assert_eq!(cc.rows().unwrap(), Some(7));
}

#[test]
fn test_omni_data_rows_only_from_first_server() {
    // For omni-sharded tables with RETURNING, only forward DataRows from the first server.
    let route = Route::write(ShardWithPriority::new_table_omni(Shard::All)).with_omnisharded(true);
    let mut multi_shard = MultiShard::new(2, &route);

    let backend1 = BackendPid::for_test(1);
    let backend2 = BackendPid::for_test(2);

    // Setup: send RowDescription from both shards
    let rd = RowDescription::new(&[Field::bigint("id")]);
    multi_shard
        .handle_server_message(0, rd.message().backend(backend1))
        .unwrap();
    let rd_result = multi_shard
        .handle_server_message(0, rd.message().backend(backend2))
        .unwrap();
    assert!(rd_result.is_some()); // RowDescription forwarded after all shards

    // DataRow from first shard (backend1) - should be forwarded
    let mut dr1 = DataRow::new();
    dr1.add(100_i64);
    let result = multi_shard
        .handle_server_message(0, dr1.message().backend(backend1))
        .unwrap();
    assert!(result.is_some()); // Should be forwarded

    // DataRow from second shard (backend2) - should NOT be forwarded
    let mut dr2 = DataRow::new();
    dr2.add(200_i64);
    let result = multi_shard
        .handle_server_message(0, dr2.message().backend(backend2))
        .unwrap();
    assert!(result.is_none()); // Should be dropped

    // Another DataRow from first shard - should be forwarded
    let mut dr3 = DataRow::new();
    dr3.add(101_i64);
    let result = multi_shard
        .handle_server_message(0, dr3.message().backend(backend1))
        .unwrap();
    assert!(result.is_some()); // Should be forwarded
}

/// Statements pipelined in one exchange each get their own RowDescription,
/// and they may describe different result sets.
#[test]
fn test_pipelined_describe_forwards_every_group() {
    for shards in [1, 2] {
        let mut multi_shard = MultiShard::new(
            shards,
            &Route::read(ShardWithPriority::new_default_unset(Shard::All)),
        );

        let id = RowDescription::new(&[Field::bigint("id")]);
        let name = RowDescription::new(&[Field::text("name"), Field::bigint("id")]);
        let mut forwarded = vec![];

        for description in [&id, &name] {
            for _ in 0..shards {
                if let Some(message) = multi_shard
                    .handle_server_message(0, description.message())
                    .unwrap()
                {
                    forwarded.push(message);
                }
            }
        }

        assert_eq!(
            forwarded,
            vec![id.message(), name.message()],
            "{shards} shard(s)",
        );
    }
}

/// Each Bind in a pipelined exchange decides the wire format of its own rows,
/// and a server RowDescription must not take that over.
#[test]
fn test_bind_result_formats_apply_per_statement() {
    let mut multi_shard = MultiShard::new(
        2,
        &Route::read(ShardWithPriority::new_default_unset(Shard::All)),
    );

    let binary = Bind::new_params_codes_results("b1", &[], &[], &[1]);
    let text = Bind::new_statement("b2");
    let rd = RowDescription::new(&[Field::bigint("id")]);

    multi_shard.push_bind(&binary);
    multi_shard.push_bind(&text);

    for _ in 0..2 {
        multi_shard
            .handle_server_message(0, BindComplete.message())
            .unwrap();
    }
    for _ in 0..2 {
        multi_shard.handle_server_message(0, rd.message()).unwrap();
    }
    assert_eq!(multi_shard.decoder.get_format(0), Format::Binary);

    // The second statement asked for no formats.
    for _ in 0..2 {
        multi_shard
            .handle_server_message(0, BindComplete.message())
            .unwrap();
    }
    for _ in 0..2 {
        multi_shard.handle_server_message(0, rd.message()).unwrap();
    }
    assert_eq!(multi_shard.decoder.get_format(0), Format::Text);
}

/// A Bind that never completed is dropped at ReadyForQuery, so the next
/// exchange cannot pop it and type its rows after the wrong statement.
#[test]
fn test_ready_for_query_drops_pending_binds() {
    let mut multi_shard = MultiShard::new(
        2,
        &Route::read(ShardWithPriority::new_default_unset(Shard::All)),
    );

    multi_shard.push_bind(&Bind::new_statement("b1"));
    multi_shard.push_bind(&Bind::new_statement("b2"));

    // Only the first statement binds. The second is abandoned.
    for _ in 0..2 {
        multi_shard
            .handle_server_message(0, BindComplete.message())
            .unwrap();
    }
    assert_eq!(multi_shard.bound_statements.len(), 1);

    for _ in 0..2 {
        multi_shard
            .handle_server_message(0, ReadyForQuery::idle().message())
            .unwrap();
    }
    assert!(multi_shard.bound_statements.is_empty());
}

/// Exercise the actual read loop without a database or network connection.
fn simulated_merge_binding(route: &Route, responses: Vec<(usize, Vec<Message>)>) -> MultiBinding {
    use crate::backend::{Pool, Server, pool::Guard};
    use tokio::time::Instant;
    let servers: Vec<_> = responses
        .into_iter()
        .map(|(shard, messages)| {
            let mut server = Server::default();
            for message in messages {
                server
                    .prepared_statements_mut()
                    .state_mut()
                    .add_simulated(message);
            }
            server.force_close();
            LinkedServer {
                server: Guard::new(Pool::new_test(), Box::new(server), Instant::now()),
                shard,
                linked: false,
            }
        })
        .collect();
    let mut state = MultiShard::new(servers.len(), route);
    state.start_merge(servers.iter().map(|server| server.shard));
    MultiBinding {
        servers,
        state: state.boxed(),
        transaction_stmt: None,
        is_read: true,
    }
}

fn merge_route(limit: Option<usize>, offset: Option<usize>) -> Route {
    use crate::frontend::router::parser::{Limit, order_by::OrderByIdx};
    Route::select(
        ShardWithPriority::new_table(Shard::All),
        vec![OrderBy::Index(OrderByIdx::Asc(1))],
        Default::default(),
        Limit { limit, offset },
        None,
    )
}

fn merge_responses(values: &[i64]) -> Vec<Message> {
    let rd = RowDescription::new(&[Field::bigint("id")]);
    let mut messages = vec![rd.message()];
    for value in values {
        let mut row = DataRow::new();
        row.add(*value);
        messages.push(row.message());
    }
    messages.push(CommandComplete::from_str(&format!("SELECT {}", values.len())).message());
    messages.push(ReadyForQuery::idle().message());
    messages
}

#[tokio::test]
async fn merge_binding_streams_offset_limit_and_drains_using_specific() {
    let route = merge_route(Some(3), Some(2));
    // Sparse shard IDs, deliberately not in shard order; one shard is empty.
    let mut binding = simulated_merge_binding(
        &route,
        vec![
            (42, merge_responses(&[1, 4, 7, 10])),
            (8, merge_responses(&[])),
            (19, merge_responses(&[2, 5, 8, 11])),
        ],
    );
    assert_eq!(binding.read().await.unwrap().unwrap().code(), 'T');
    for (index, expected) in [4_i64, 5, 7].into_iter().enumerate() {
        let message = binding.read().await.unwrap().unwrap();
        assert_eq!(message.code(), 'D');
        let row = DataRow::from_bytes(message.to_bytes()).unwrap();
        assert_eq!(row.get::<i64>(0, Format::Text).unwrap(), expected);
        assert!(matches!(
            binding.state.read_strategy,
            ReadStrategy::Specific { .. }
        ));
        if index == 0 {
            assert!(
                binding.state.request_state.command_complete_count < 3,
                "emit before all shards finish"
            );
        }
    }
    let ResultBuffer::Merge(merge) = &binding.state.buffer else {
        panic!("merge buffer");
    };
    assert!(merge.draining);
    assert!(
        merge.buffer.is_none(),
        "release remaining candidates at LIMIT"
    );
    let message = binding.read().await.unwrap().unwrap();
    assert_eq!(message.code(), 'C');
    assert_eq!(
        CommandComplete::from_bytes(message.to_bytes())
            .unwrap()
            .rows()
            .unwrap(),
        Some(3)
    );
    assert!(matches!(
        binding.state.read_strategy,
        ReadStrategy::Specific { .. }
    ));
    assert_eq!(binding.read().await.unwrap().unwrap().code(), 'Z');
    assert!(!binding.has_more_messages());
    assert!(binding.read().await.unwrap().is_none());
}

#[tokio::test]
async fn merge_binding_handles_zero_limit_offset_exhaustion_and_unlimited_results() {
    for (limit, offset, expected) in [
        (Some(0), None, vec![]),
        (Some(5), Some(10), vec![]),
        (None, Some(1), vec![2_i64, 3, 4]),
    ] {
        let route = merge_route(limit, offset);
        let mut binding = simulated_merge_binding(
            &route,
            vec![(9, merge_responses(&[1, 3])), (3, merge_responses(&[2, 4]))],
        );
        let mut rows = vec![];
        let mut codes = vec![];
        while let Some(message) = binding.read().await.unwrap() {
            codes.push(message.code());
            match message.code() {
                'D' => rows.push(
                    DataRow::from_bytes(message.to_bytes())
                        .unwrap()
                        .get::<i64>(0, Format::Text)
                        .unwrap(),
                ),
                'C' => assert_eq!(
                    CommandComplete::from_bytes(message.to_bytes())
                        .unwrap()
                        .rows()
                        .unwrap(),
                    Some(expected.len())
                ),
                _ => {}
            }
        }
        assert_eq!(rows, expected);
        assert_eq!(codes.first(), Some(&'T'));
        assert_eq!(&codes[codes.len() - 2..], &['C', 'Z']);
        assert!(!binding.has_more_messages());
    }
}

#[tokio::test]
async fn merge_binding_uses_bind_formats_and_drops_helpers_after_ordering() {
    let mut route = merge_route(Some(2), None);
    route
        .projection_rewrite_plan
        .order_by_helpers
        .push(OrderByHelper {
            sort_position: 0,
            source: OrderBySource::Column("id".into()),
            alias: "id".into(),
            injected: true,
        });
    let rd = RowDescription::new(&[Field::bigint("id"), Field::bigint("value")]);
    let responses = |values: &[(i64, i64)]| {
        let mut messages = vec![BindComplete.message()];
        for &(id, value) in values {
            let mut row = DataRow::new();
            row.add(bytes::Bytes::copy_from_slice(&id.to_be_bytes()))
                .add(bytes::Bytes::copy_from_slice(&value.to_be_bytes()));
            messages.push(row.message());
        }
        messages.push(CommandComplete::from_str(&format!("SELECT {}", values.len())).message());
        // Execute/Flush response has no ReadyForQuery; a later Sync completes it.
        messages
    };
    let mut binding = simulated_merge_binding(
        &route,
        vec![
            (22, responses(&[(2, 20), (4, 40)])),
            (13, responses(&[(1, 10), (3, 30)])),
        ],
    );
    binding.state.decoder.set_row_description(rd);
    binding
        .state
        .push_bind(&Bind::new_params_codes_results("", &[], &[], &[1]));
    assert_eq!(binding.read().await.unwrap().unwrap().code(), '2');
    for expected in [10_i64, 20] {
        let message = binding.read().await.unwrap().unwrap();
        let row = DataRow::from_bytes(message.to_bytes()).unwrap();
        assert_eq!(row.len(), 1);
        assert_eq!(row.get::<i64>(0, Format::Binary).unwrap(), expected);
    }
    assert_eq!(binding.read().await.unwrap().unwrap().code(), 'C');
    assert!(binding.read().await.unwrap().is_none());
    assert!(matches!(
        binding.state.read_strategy,
        ReadStrategy::Specific { .. }
    ));
    // Sync-only updates retain the merge strategy through ReadyForQuery.
    binding.state.update_shards(2);
    for server in &mut binding.servers {
        server
            .prepared_statements_mut()
            .state_mut()
            .add_simulated(ReadyForQuery::idle().message());
    }
    assert_eq!(binding.read().await.unwrap().unwrap().code(), 'Z');
    assert!(!binding.has_more_messages());
}

#[tokio::test]
async fn merge_binding_propagates_errors_and_drains_remaining_servers() {
    use crate::net::ErrorResponse;
    let route = merge_route(Some(1), None);
    let mut failed = merge_responses(&[]);
    failed[1] = ErrorResponse {
        code: "57014".into(),
        message: "cancelled externally".into(),
        ..Default::default()
    }
    .message();
    let mut binding =
        simulated_merge_binding(&route, vec![(17, failed), (4, merge_responses(&[1, 2, 3]))]);
    assert_eq!(binding.read().await.unwrap().unwrap().code(), 'T');
    assert_eq!(binding.read().await.unwrap().unwrap().code(), 'E');
    assert_eq!(binding.read().await.unwrap().unwrap().code(), 'Z');
    assert!(!binding.has_more_messages());
    assert!(matches!(
        binding.state.read_strategy,
        ReadStrategy::Specific { .. }
    ));
    assert!(binding.read().await.unwrap().is_none());
}
