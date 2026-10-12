//! Multi-shard connection state.

use std::collections::VecDeque;

use crate::{
    frontend::router::Route,
    net::{
        BackendPid, Bind, Decoder, ReadyForQuery,
        messages::{
            DataRow, FromBytes, Message, Protocol, RowDescription, ToBytes,
            command_complete::CommandComplete,
        },
    },
};

use super::LinkedServer;
use result_buffer::{MergeState, ResultBuffer};

mod binding;
mod error;
mod result_buffer;
#[cfg(test)]
mod test;
mod upgrade;
mod validator;

pub(crate) use binding::MultiBinding;
pub(crate) use error::Error;
pub(crate) use upgrade::MultiShardUpgrade;
use validator::Validator;

/// Determine which [`LinkedServer`] to pull server messages from next.
#[derive(Debug)]
pub(super) enum ReadStrategy {
    /// Read one message per active server in each ordered pass.
    /// Each call starts at the first server, including after an early return.
    RoundRobin { next: usize },
    /// Read one message from the requested shard's server per pass.
    Specific { shard_id: usize, consumed: bool },
}

impl Default for ReadStrategy {
    fn default() -> Self {
        Self::RoundRobin { next: 0 }
    }
}

impl ReadStrategy {
    /// Start a fresh pass through the servers.
    fn reset(&mut self) {
        match self {
            Self::RoundRobin { next } => *next = 0,
            Self::Specific { consumed, .. } => *consumed = false,
        }
    }

    /// Select the next active server and advance the strategy's cursor.
    fn next_server(&mut self, servers: &[LinkedServer]) -> Option<usize> {
        match self {
            Self::RoundRobin { next } => {
                let index = servers
                    .iter()
                    .enumerate()
                    .skip(*next)
                    .find_map(|(index, server)| server.has_more_messages().then_some(index));
                *next = index.map_or(servers.len(), |index| index + 1);
                index
            }
            Self::Specific { shard_id, consumed } => {
                if std::mem::replace(consumed, true) {
                    return None;
                }
                servers
                    .iter()
                    .position(|server| server.shard == *shard_id && server.has_more_messages())
            }
        }
    }
}

#[derive(Default, Debug)]
struct RequestState {
    rows: usize,
    ready_for_query: usize,
    command_complete_count: usize,
    empty_query_response: usize,
    copy_in: usize,
    parse_complete: usize,
    parameter_description: usize,
    no_data: usize,
    row_description: usize,
    close_complete: usize,
    bind_complete: usize,
    command_complete: Option<Message>,
    transaction_error: bool,
    copy_done: usize,
    copy_out: usize,
    copy_data: usize,
    first_backend_data: Option<BackendPid>,
}

/// Multi-shard state.
#[derive(Default, Debug)]
pub(crate) struct MultiShard {
    /// Number of shards we are connected to.
    shards: usize,
    /// Route the query is taking.
    route: Route,
    /// In-flight request state.
    request_state: RequestState,
    /// Sorting/aggregate buffer.
    buffer: ResultBuffer,
    /// Server read policy for the current query.
    read_strategy: ReadStrategy,
    /// Row decoder.
    decoder: Decoder,
    /// Row consistency validator.
    validator: Validator,
    /// [`Bind`]s waiting for their [`crate::net::BindComplete`], in the order they were sent.
    bound_statements: VecDeque<Bind>,
}

impl MultiShard {
    /// New multi-shard state given the actual shard indices connected.
    pub(super) fn new(shards: usize, route: &Route) -> Self {
        Self {
            shards,
            route: route.clone(),
            ..Default::default()
        }
    }

    /// Update multi-shard state.
    pub(super) fn update(&mut self, shards: usize, route: &Route) {
        self.reset();
        self.shards = shards;
        self.route = route.clone();
    }

    /// Update only the shards count without resetting message counters.
    ///
    /// Used for [`crate::net::Sync`]-only requests where we need correct [`ReadyForQuery`] counting
    /// but must preserve buffered [`CommandComplete`] from previous queries.
    ///
    pub(super) fn update_shards(&mut self, shards: usize) {
        self.shards = shards;
    }

    /// Query finished execution, reset any query state.
    pub(super) fn query_complete(&mut self) {
        self.reset();
        // Don't reset:
        //  1. Route to keep routing decision
        //  2. Number of shards
        //  3. Decoder
        //  4. Pending Binds, pushed before this run
    }

    fn reset(&mut self) {
        self.request_state = RequestState::default();
        self.buffer = ResultBuffer::default();
        self.read_strategy = ReadStrategy::default();
        self.validator.reset();

        // Intentionally does not reset:
        //  1. Route, to keep routing decision
        //  2. Number of shards
        //  3. Decoder state
        //  4. Pending Binds, pushed before this run
    }

    pub(super) fn read_strategy_mut(&mut self) -> &mut ReadStrategy {
        &mut self.read_strategy
    }

    /// Request one message per pass from a particular shard's server.
    pub(super) fn read_from(&mut self, shard_id: usize) {
        self.read_strategy = ReadStrategy::Specific {
            shard_id,
            consumed: false,
        };
    }

    /// Choose a merge buffer once the binding knows the participating shard IDs.
    pub(super) fn start_merge(&mut self, shard_ids: impl Iterator<Item = usize>) {
        if self.should_buffer() {
            self.buffer = ResultBuffer::Merge(MergeState::new(shard_ids));
        }
    }

    /// Merge reads request a missing candidate; cleanup reads still use Specific.
    pub(super) fn prepare_read(&mut self, servers: &[LinkedServer]) -> Result<(), Error> {
        if let ResultBuffer::Merge(merge) = &self.buffer {
            let descriptions_pending = self.request_state.row_description > 0
                && !self
                    .request_state
                    .row_description
                    .is_multiple_of(self.shards);
            let shard_id = merge.next_shard(
                servers,
                !self.bound_statements.is_empty(),
                descriptions_pending,
            )?;
            if let Some(shard_id) = shard_id {
                self.read_from(shard_id);
            }
        }
        Ok(())
    }

    /// Check if the message we received from Postgres should be sent to the client, skipped,
    /// or modified.
    ///
    /// # Arguments
    ///
    /// * `message`: [`Message`] received from Postgres.
    ///
    /// # Returns
    ///
    /// [`Some`] if the message should be sent to the client, [`None`] if it should be dropped.
    ///
    pub(super) fn handle_server_message(
        &mut self,
        shard_id: usize,
        message: Message,
    ) -> Result<Option<Message>, Error> {
        let buffer_id = self
            .buffer
            .observe_server_message(shard_id, message.code())?;

        Ok(match message.code() {
            'Z' => self.handle_ready_for_query(message)?,
            'C' => self.handle_command_complete(message, buffer_id)?,
            'T' => self.handle_row_description(message)?,
            'I' => self.handle_empty_query_response(message)?,
            'D' => self.handle_data_row(message, buffer_id)?,
            'G' => Self::handle_passthrough(message, &mut self.request_state.copy_in, self.shards),
            'n' => Self::handle_passthrough(message, &mut self.request_state.no_data, self.shards),
            '1' => Self::handle_passthrough(
                message,
                &mut self.request_state.parse_complete,
                self.shards,
            ),
            '3' => Self::handle_passthrough(
                message,
                &mut self.request_state.close_complete,
                self.shards,
            ),
            'c' => {
                Self::handle_passthrough(message, &mut self.request_state.copy_done, self.shards)
            }
            'H' => Self::handle_passthrough(message, &mut self.request_state.copy_out, self.shards),
            't' => Self::handle_passthrough(
                message,
                &mut self.request_state.parameter_description,
                self.shards,
            ),
            '2' => self.handle_bind(message),
            'd' => self.handle_copy_data(message),
            _ => Some(message),
        })
    }

    /// Get the next available server message to send to the client.
    ///
    /// Used when buffering and transforming messages. This returns [`None`] until
    /// the state is ready, i.e., we received everything we need to sort/limit/offset, etc.
    ///
    pub(super) fn get_server_message(&mut self) -> Result<Option<Message>, Error> {
        // Do not expose rows before all shard descriptions have been validated.
        if self.request_state.row_description > 0
            && !self
                .request_state
                .row_description
                .is_multiple_of(self.shards)
        {
            return Ok(None);
        }
        let row = self.buffer.take(&self.route);
        if row.is_some() {
            return Ok(row);
        }
        if let Some(message) = self.request_state.command_complete.take() {
            if let ResultBuffer::Merge(merge) = &self.buffer {
                let cc = CommandComplete::from_bytes(message.to_bytes())?;
                return Ok(Some(cc.rewrite(merge.returned)?.message()));
            }
            return Ok(Some(message));
        }
        Ok(None)
    }

    /// The state has more messages for the client.
    pub(super) fn has_more_messages(&self) -> bool {
        self.buffer.can_take() || self.request_state.command_complete.is_some()
    }

    /// Push [`Bind`] message into decoding queue. This is used with multi-[`Bind`] extended
    /// protocol requests to make sure we process them in the right order and with correct decoding.
    pub(super) fn push_bind(&mut self, bind: &Bind) {
        self.bound_statements.push_back(bind.clone());
    }

    fn handle_ready_for_query(&mut self, message: Message) -> Result<Option<Message>, Error> {
        self.request_state.ready_for_query += 1;

        if message.transaction_error() {
            self.request_state.transaction_error = true;
        }

        Ok(
            if self
                .request_state
                .ready_for_query
                .is_multiple_of(self.shards)
            {
                self.bound_statements.clear();

                if self.request_state.transaction_error {
                    Some(ReadyForQuery::error().message())
                } else {
                    Some(message)
                }
            } else {
                None
            },
        )
    }

    // Count [`CommandComplete`] messages.
    //
    // Once all shards finished executing the command,
    // we can start aggregating and sorting.
    fn handle_command_complete(
        &mut self,
        message: Message,
        buffer_id: Option<usize>,
    ) -> Result<Option<Message>, Error> {
        if !self.buffer.ensure_initialized(&self.route, &self.decoder)? {
            self.read_strategy = ReadStrategy::default();
        }
        if let (ResultBuffer::Merge(merge), Some(buffer_id)) = (&mut self.buffer, buffer_id)
            && let Some(buffer) = &mut merge.buffer
        {
            buffer.mark_complete(buffer_id)?;
        }
        let cc = CommandComplete::from_bytes(message.to_bytes())?;

        // DDL commands don't return rows.
        let has_rows = if let Some(rows) = cc.rows()? {
            if self.route.is_omnisharded() {
                // Only use the first shard's row count for consistency with DataRow.
                if self.request_state.command_complete_count == 0 {
                    self.request_state.rows = rows;
                }
            } else {
                self.request_state.rows += rows;
            }
            true
        } else {
            false
        };

        self.request_state.command_complete_count += 1;

        if self
            .request_state
            .command_complete_count
            .is_multiple_of(self.shards)
        {
            if let ResultBuffer::Full(buffer) = &mut self.buffer {
                buffer.mark_full();

                if !buffer.is_empty() {
                    // Helpers remain in the internal row through aggregation and
                    // sorting, then are removed before client-visible operations.
                    buffer
                        .aggregate(
                            self.route.aggregate(),
                            &self.decoder,
                            &self.route.projection_rewrite_plan,
                        )
                        .map_err(Error::from)?;

                    buffer.sort(self.route.order_by(), &self.decoder);
                    buffer.drop_helper_columns(&self.route.projection_rewrite_plan, &self.decoder);
                    buffer.distinct(self.route.distinct(), &self.decoder);
                    buffer.limit(self.route.limit());
                }
            }

            if has_rows {
                let rows = match &self.buffer {
                    ResultBuffer::Full(buffer) if self.should_buffer() => buffer.len(),
                    ResultBuffer::Merge(_) => {
                        // The merge may still contain rows. Rewrite the count when consumed.
                        self.request_state.command_complete = Some(cc.message());
                        return Ok(None);
                    }
                    _ => self.request_state.rows,
                };
                self.request_state.command_complete = Some(cc.rewrite(rows)?.message());
            } else {
                return Ok(Some(cc.message()));
            }
        }

        Ok(None)
    }

    fn handle_row_description(&mut self, message: Message) -> Result<Option<Message>, Error> {
        let mut forward = None;

        self.request_state.row_description += 1;
        let rd = RowDescription::from_bytes(message.to_bytes())?;

        // Validate row description consistency inside the group.
        // we reset after shards processed the group of the same RD
        let is_first_in_group = self.validator.validate_row_description(&rd)?;

        // Set it as soon as we have it, so the aggregator and sorter
        // can use it.
        if is_first_in_group {
            self.decoder.set_row_description(rd.clone());
        } else if matches!(self.buffer, ResultBuffer::Merge(_))
            && self
                .decoder
                .row_description()
                .fields
                .iter()
                .zip(rd.fields.iter())
                .any(|(left, right)| left.data_type() != right.data_type())
        {
            // The legacy validator permits differing types. Preserve its buffered behavior.
            self.buffer = ResultBuffer::default();
            self.read_strategy = ReadStrategy::default();
        }

        if self
            .request_state
            .row_description
            .is_multiple_of(self.shards)
        {
            // Only send it to the client once all shards sent it,
            // so we don't get early requests from clients.
            let plan = &self.route.projection_rewrite_plan;
            if plan.is_noop() {
                forward = Some(message);
            } else {
                let client_rd = rd.drop_columns(plan.drop_columns(&rd));
                forward = Some(client_rd.message());
            }

            // The next statement describes a different result set.
            self.validator.reset();
        }

        Ok(forward)
    }

    fn handle_empty_query_response(&mut self, message: Message) -> Result<Option<Message>, Error> {
        self.request_state.empty_query_response += 1;

        Ok(
            if self
                .request_state
                .empty_query_response
                .is_multiple_of(self.shards)
            {
                Some(message)
            } else {
                None
            },
        )
    }

    fn handle_data_row(
        &mut self,
        message: Message,
        buffer_id: Option<usize>,
    ) -> Result<Option<Message>, Error> {
        if !self.buffer.ensure_initialized(&self.route, &self.decoder)? {
            self.read_strategy = ReadStrategy::default();
        }
        let mut forward = None;

        if self.shards > 1 {
            // Validate data row consistency.
            let data_row = DataRow::from_bytes(message.to_bytes())?;
            self.validator.validate_data_row(&data_row)?;
        }

        // INVARIANT: omni dedup relies on Source::Backend carrying a
        // process-unique BackendPid (see BackendPid::seq). Never tag messages
        // with a non-unique value; doing so silently corrupts row deduplication.
        if self.request_state.first_backend_data.is_none() {
            self.request_state.first_backend_data = message.source().backend_id();
        }

        if !self.should_buffer()
            && self
                .request_state
                .row_description
                .is_multiple_of(self.shards)
        {
            if self.route.is_omnisharded() {
                if self.request_state.first_backend_data == message.source().backend_id() {
                    forward = Some(message);
                }
            } else {
                forward = Some(message);
            }
        } else {
            match &mut self.buffer {
                ResultBuffer::Full(buffer) => buffer.add(message).map_err(Error::from)?,
                ResultBuffer::Merge(merge) if !merge.draining => {
                    if let (Some(buffer), Some(buffer_id)) = (&mut merge.buffer, buffer_id) {
                        buffer.add(buffer_id, message)?;
                    }
                }
                ResultBuffer::Merge(_) => {}
            }
        }

        Ok(forward)
    }

    fn handle_passthrough(message: Message, counter: &mut usize, shards: usize) -> Option<Message> {
        *counter += 1;

        if counter.is_multiple_of(shards) {
            Some(message)
        } else {
            None
        }
    }

    fn handle_bind(&mut self, message: Message) -> Option<Message> {
        let mut forward = None;
        self.request_state.bind_complete += 1;

        if self.request_state.bind_complete.is_multiple_of(self.shards) {
            forward = Some(message);

            if let Some(bind) = self.bound_statements.pop_front() {
                self.decoder.set_formats(&bind);
            }
        }

        forward
    }

    fn handle_copy_data(&mut self, message: Message) -> Option<Message> {
        self.request_state.copy_data += 1;
        Some(message)
    }

    /// Return true if we need to buffer [`DataRow`] messages
    /// received from the servers because we need to post-process them.
    fn should_buffer(&self) -> bool {
        // 1. We are talking to more than one shard (cross-shard query)
        // 2. The route contains transformations we need to perform, e.g., aggregates, sorting, etc.
        // 3. The route does not concern omnisharded tables which have the same data on all shards
        //    anyway.
        self.shards > 1 && self.route.requires_post_processing() && !self.route.is_omnisharded()
    }

    /// Box me up, Scotty.
    pub(super) fn boxed(self) -> Box<Self> {
        Box::new(self)
    }
}
