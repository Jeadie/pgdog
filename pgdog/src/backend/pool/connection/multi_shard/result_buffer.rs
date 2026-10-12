//! Per-query result buffering and streaming merge state.

use std::collections::BTreeSet;

use crate::{
    frontend::{ClientRequest, router::Route},
    net::{DataRow, Decoder, Message, Protocol, ProtocolMessage},
};
use pg_raw_parse::{
    Node,
    raw::{LimitOption, SortByDir, SortByNulls},
};

use super::super::{
    LinkedServer,
    buffer::{Buffer, SortColumn},
    presort_buffer::MergePreSortBuffer,
};

#[derive(Debug)]
pub(super) enum ResultBuffer {
    Full(Buffer),
    Merge(MergeState),
}

impl Default for ResultBuffer {
    fn default() -> Self {
        Self::Full(Buffer::default())
    }
}

impl ResultBuffer {
    pub(super) fn can_take(&self) -> bool {
        match self {
            Self::Full(buffer) => buffer.can_take(),
            Self::Merge(merge) => merge.can_take(),
        }
    }

    pub(super) fn take(&mut self, route: &Route) -> Option<Message> {
        match self {
            Self::Full(buffer) => buffer.take(),
            Self::Merge(merge) => merge.take(route).map(|row| row.message()),
        }
    }

    /// Map the actual shard to its dense buffer ID and track merge readiness.
    pub(super) fn observe_server_message(
        &mut self,
        shard_id: usize,
        code: char,
    ) -> Result<Option<usize>, super::Error> {
        match self {
            Self::Full(_) => Ok(None),
            Self::Merge(merge) => {
                let buffer_id = merge
                    .streams
                    .iter()
                    .position(|stream| stream.shard_id == shard_id)
                    .ok_or(super::Error::InvalidMergeShard(shard_id))?;
                match code {
                    'T' => merge.streams[buffer_id].described = true,
                    '2' => merge.streams[buffer_id].bound = true,
                    'E' => merge.drain(),
                    _ => {}
                }
                Ok(Some(buffer_id))
            }
        }
    }

    /// False means metadata required falling back from Merge to Full.
    pub(super) fn ensure_initialized(
        &mut self,
        route: &Route,
        decoder: &Decoder,
    ) -> Result<bool, super::Error> {
        if let Self::Merge(merge) = self
            && !merge.ensure_initialized(route, decoder)?
        {
            *self = Self::default();
            return Ok(false);
        }
        Ok(true)
    }
}

/// A row stream can finish before its server's protocol exchange does.
#[derive(Debug)]
struct MergeStream {
    shard_id: usize,
    described: bool,
    bound: bool,
}

#[derive(Debug)]
pub(super) struct MergeState {
    pub(super) buffer: Option<MergePreSortBuffer>,
    streams: Vec<MergeStream>,
    pub(super) returned: usize,
    skipped: usize,
    drop_columns: BTreeSet<usize>,
    pub(super) draining: bool,
}

impl MergeState {
    pub(super) fn new(shard_ids: impl Iterator<Item = usize>) -> Self {
        Self {
            buffer: None,
            streams: shard_ids
                .map(|shard_id| MergeStream {
                    shard_id,
                    described: false,
                    bound: false,
                })
                .collect(),
            returned: 0,
            skipped: 0,
            drop_columns: BTreeSet::new(),
            draining: false,
        }
    }

    /// Choose the stream needed for metadata or the next heap candidate.
    /// Once output ends, choose any server with protocol messages left to drain.
    pub(super) fn next_shard(
        &self,
        servers: &[LinkedServer],
        binds_pending: bool,
        descriptions_pending: bool,
    ) -> Result<Option<usize>, super::Error> {
        if !self.draining {
            for (buffer_id, stream) in self.streams.iter().enumerate() {
                let needs_row = if binds_pending {
                    !stream.bound
                } else if descriptions_pending {
                    !stream.described
                } else if let Some(buffer) = &self.buffer {
                    buffer.needs_row(buffer_id)?
                } else {
                    true
                };
                if needs_row
                    && servers
                        .iter()
                        .any(|server| server.shard == stream.shard_id && server.has_more_messages())
                {
                    return Ok(Some(stream.shard_id));
                }
            }
        }
        // CommandComplete closes a row stream, but ReadyForQuery may still be pending.
        Ok(servers
            .iter()
            .find(|server| server.has_more_messages())
            .map(|server| server.shard))
    }

    /// Delay construction until the result's columns and Bind formats are known.
    fn ensure_initialized(
        &mut self,
        route: &Route,
        decoder: &Decoder,
    ) -> Result<bool, super::Error> {
        if self.buffer.is_some() || self.draining {
            return Ok(true);
        }
        if !supported_order(route, decoder) {
            return Ok(false);
        }
        self.buffer = Some(MergePreSortBuffer::try_new(
            self.streams.len(),
            route.order_by(),
            decoder,
        )?);
        self.drop_columns = route
            .projection_rewrite_plan
            .drop_columns(decoder.row_description());
        Ok(true)
    }

    pub(super) fn drain(&mut self) {
        self.draining = true;
        self.buffer = None;
    }

    pub(super) fn can_take(&self) -> bool {
        self.buffer
            .as_ref()
            .is_some_and(MergePreSortBuffer::can_take)
    }

    pub(super) fn take(&mut self, route: &Route) -> Option<DataRow> {
        if route
            .limit()
            .limit
            .is_some_and(|limit| self.returned >= limit)
        {
            self.drain();
            return None;
        }
        while let Some(mut row) = self.buffer.as_mut()?.take() {
            if self.skipped < route.limit().offset.unwrap_or(0) {
                self.skipped += 1;
                continue;
            }
            row.drop_columns(&self.drop_columns);
            self.returned += 1;
            if route.limit().limit == Some(self.returned) {
                self.drain();
            }
            return Some(row);
        }
        None
    }
}

/// Check whether this request can use one streaming merge for its entire result set.
///
/// These checks cover SQL ordering and protocol lifecycle. Column resolution and
/// comparison compatibility are checked by `supported_order` once result metadata arrives.
pub(super) fn can_merge(request: &ClientRequest) -> bool {
    let route = request.route();
    // The heap needs sort keys. Aggregation and DISTINCT need global processing
    // before LIMIT can be applied, so they retain the full-buffer path.
    if route.order_by().is_empty() || !route.aggregate().is_empty() || route.distinct().is_some() {
        return false;
    }
    // Each stream currently has one `described` flag. Multiple Describe groups
    // would need separate readiness tracking before exposing their rows.
    if request
        .messages
        .iter()
        .filter(|message| matches!(message, ProtocolMessage::Describe(_)))
        .count()
        > 1
    {
        return false;
    }
    // Preparation-only requests have no row stream. Multiple executions need
    // separate heaps and OFFSET/LIMIT counters for their result sets.
    let executions = request
        .messages
        .iter()
        .filter(|message| {
            matches!(
                message,
                ProtocolMessage::Query(_) | ProtocolMessage::Execute(_)
            )
        })
        .count();
    // Multiple Binds also require tracking which formats belong to each result set.
    if executions != 1
        || request
            .messages
            .iter()
            .filter(|message| matches!(message, ProtocolMessage::Bind(_)))
            .count()
            > 1
    {
        return false;
    }
    // A bounded Execute can end with PortalSuspended instead of CommandComplete.
    // Resuming such a stream would require preserving merge state across requests.
    if request.messages.iter().any(
        |message| matches!(message, ProtocolMessage::Execute(execute) if execute.max_rows() != 0),
    ) {
        return false;
    }
    // Inspect the SQL itself: the route may omit ordering constructs it cannot
    // represent. Without the AST we cannot establish that every key is supported.
    let Some(ast) = &request.ast else {
        return false;
    };
    let mut statements = ast.ast.stmts();
    let Some(Node::SelectStmt(select)) = statements.next() else {
        return false;
    };
    // One merge state handles one SELECT. WITH TIES would need to continue past
    // LIMIT until all rows sharing the boundary's ordering keys have been returned.
    statements.next().is_none()
        && select.limit_option != LimitOption::LIMIT_OPTION_WITH_TIES
        // Every SQL sort key must have survived extraction into the route.
        && select.sort_clause().len() == route.order_by().len()
        && select.sort_clause().into_iter().all(|sort| {
            // Only columns, positional constants, and supported expressions are
            // represented by OrderBy; constructs such as COLLATE are excluded.
            matches!(sort.node(), Node::ColumnRef(_) | Node::A_Const(_) | Node::A_Expr(_))
                // OrderBy does not retain explicit NULLS FIRST/LAST overrides.
                && sort.sortby_nulls == SortByNulls::SORTBY_NULLS_DEFAULT
                // Custom USING operators need their own comparison semantics.
                && matches!(sort.sortby_dir, SortByDir::SORTBY_DEFAULT | SortByDir::SORTBY_ASC | SortByDir::SORTBY_DESC)
                // The supported expression sort is vector L2, represented as ascending.
                && !(matches!(sort.node(), Node::A_Expr(_)) && sort.sortby_dir == SortByDir::SORTBY_DESC)
        })
}

pub(super) fn supported_order(route: &Route, decoder: &Decoder) -> bool {
    if !decoder.has_row_description() {
        return false;
    }
    route.order_by().iter().all(|column| {
        SortColumn::try_new(column, decoder).is_ok_and(|column| column.supports_streaming_merge())
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        frontend::router::{
            Ast,
            parser::{DistinctBy, OrderBy, Shard, ShardWithPriority, order_by::OrderByIdx},
        },
        net::{Execute, Query},
    };
    use std::sync::Arc;

    fn request(sql: &str) -> ClientRequest {
        let mut request = ClientRequest::from(vec![ProtocolMessage::Query(Query::new(sql))]);
        request.ast = Some(Arc::new(Ast::new(pg_raw_parse::make::owned(|mem| {
            mem.parse(sql).unwrap()
        }))));
        request.route = Some(Route::select(
            ShardWithPriority::new_table(Shard::All),
            vec![OrderBy::Index(OrderByIdx::Asc(1))],
            Default::default(),
            Default::default(),
            None,
        ));
        request
    }

    #[test]
    fn merge_eligibility_requires_complete_supported_order_and_execution() {
        assert!(can_merge(&request(
            "SELECT id FROM items ORDER BY id LIMIT 3"
        )));
        assert!(can_merge(&request("SELECT id FROM items ORDER BY id DESC")));
        for sql in [
            "SELECT id FROM items ORDER BY id NULLS FIRST",
            "SELECT id FROM items ORDER BY id, random()",
            "SELECT id FROM items ORDER BY id COLLATE \"C\"",
            "SELECT id FROM items ORDER BY id USING >",
            "SELECT id FROM items ORDER BY id FETCH FIRST 3 ROWS WITH TIES",
            "SELECT embedding FROM items ORDER BY embedding <-> '[1,2,3]' DESC",
            "SELECT id FROM items ORDER BY id; SELECT 1",
        ] {
            assert!(!can_merge(&request(sql)), "{sql}");
        }
        let mut limited_portal = request("SELECT id FROM items ORDER BY id");
        limited_portal.messages = vec![ProtocolMessage::Execute(Execute::new_portal_limit("", 2))];
        assert!(!can_merge(&limited_portal));
        limited_portal.messages = vec![ProtocolMessage::Execute(Execute::new())];
        assert!(can_merge(&limited_portal));
        limited_portal
            .messages
            .push(ProtocolMessage::Execute(Execute::new()));
        assert!(!can_merge(&limited_portal));
        let mut distinct = request("SELECT DISTINCT id FROM items ORDER BY id");
        distinct.route = Some(Route::select(
            ShardWithPriority::new_table(Shard::All),
            vec![OrderBy::Index(OrderByIdx::Asc(1))],
            Default::default(),
            Default::default(),
            Some(DistinctBy::Row),
        ));
        assert!(!can_merge(&distinct));
        let text = Decoder::from(crate::net::RowDescription::new(&[crate::net::Field::text(
            "id",
        )]));
        assert!(
            !supported_order(distinct.route(), &text),
            "text requires PostgreSQL collation semantics"
        );
        let numeric = Decoder::from(crate::net::RowDescription::new(&[
            crate::net::Field::bigint("id"),
        ]));
        assert!(supported_order(distinct.route(), &numeric));
        assert!(!supported_order(distinct.route(), &Decoder::default()));
    }
}
