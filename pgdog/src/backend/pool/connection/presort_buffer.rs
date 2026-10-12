//! Merge individually sorted buffer streams before all results arrive.

use std::{cmp::Ordering, collections::VecDeque};

use crate::{
    frontend::router::parser::OrderBy,
    net::{
        Decoder,
        messages::{DataRow, FromBytes, Message, Protocol, ToBytes},
    },
};

use super::buffer::{Error, Result, SortColumn, compare_data_row};

#[derive(Debug, Default)]
struct BufferState {
    rows: VecDeque<DataRow>,

    // No new DataRow will arrive.
    closed: bool,
}

/// Sort and merge rows pre-ordered buffers.
///
/// [`MergePreSortBuffer`] expects each buffer to be presorted by the same set of [`OrderBy`].
///
/// Unlike [`super::buffer::Buffer`], [`MergePreSortBuffer`] need not buffer all results
/// before it can be consumed [`MergePreSortBuffer::take`]. Once the first result from
/// each buffer is received, results can be returned (at least one row).
///
/// Stream IDs are dense indices into this buffer, not database shard numbers.
#[derive(Debug)]
pub(super) struct MergePreSortBuffer {
    // Per-buffer state.
    buffers: Vec<BufferState>,

    heap: MergeHeap,
    // Unfinished streams whose next row is still unknown.
    missing: usize,
}

impl MergePreSortBuffer {
    pub(super) fn try_new(
        num_buffers: usize,
        order_cols: &[OrderBy],
        decoder: &Decoder,
    ) -> Result<Self> {
        let order_cols = order_cols
            .iter()
            .map(|o| SortColumn::try_new(o, decoder))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            buffers: (0..num_buffers).map(|_| BufferState::default()).collect(),
            heap: MergeHeap::new(num_buffers, order_cols),
            missing: num_buffers,
        })
    }

    pub(super) fn take(&mut self) -> Option<DataRow> {
        if !self.can_take() {
            return None;
        }

        let MergeHead { buffer_id, row } = self.heap.pop_min()?;

        // Push next from `buffer_id`'s buffer to heap (if possible).
        let state = &mut self.buffers[buffer_id];
        if let Some(next) = state.rows.pop_front() {
            self.push_head(buffer_id, next);
        } else if !state.closed {
            self.missing += 1;
        }

        Some(row)
    }

    pub(super) fn can_take(&self) -> bool {
        self.missing == 0 && !self.heap.is_empty()
    }

    /// Whether the reader must obtain this stream's next row or completion.
    pub(super) fn needs_row(&self, buffer_id: usize) -> Result<bool> {
        let state = self
            .buffers
            .get(buffer_id)
            .ok_or(Error::InvalidBuffer(buffer_id))?;
        Ok(!state.closed && !self.heap.contains(buffer_id))
    }

    /// Mark a stream exhausted. Already buffered rows remain available.
    pub(super) fn mark_complete(&mut self, buffer_id: usize) -> Result<()> {
        let state = self
            .buffers
            .get_mut(buffer_id)
            .ok_or(Error::InvalidBuffer(buffer_id))?;

        if !state.closed {
            if !self.heap.contains(buffer_id) {
                self.missing -= 1;
            }
            state.closed = true;
        }
        Ok(())
    }

    /// Add a new [`Message`] for a given `buffer_id`.
    ///
    /// Caller must ensure consistency of this message with previous given the defined ordering.
    pub(super) fn add(&mut self, buffer_id: usize, message: Message) -> Result<()> {
        let needs_row = self.needs_row(buffer_id)?;
        if self.buffers[buffer_id].closed {
            return Err(Error::BufferComplete(buffer_id));
        }
        let dr = DataRow::from_bytes(message.to_bytes())?;
        if needs_row {
            self.missing -= 1;
            self.push_head(buffer_id, dr);
        } else {
            self.buffers[buffer_id].rows.push_back(dr);
        }

        Ok(())
    }

    /// Set a new [`DataRow`] as the next value from a buffer.
    fn push_head(&mut self, buffer_id: usize, row: DataRow) {
        self.heap.push(MergeHead { buffer_id, row });
    }
}

#[derive(Debug)]
struct MergeHead {
    buffer_id: usize,
    row: DataRow,
}

/// Min-heap of buffer candidates under the query's ordering.
#[derive(Debug)]
struct MergeHeap {
    heads: Vec<MergeHead>,
    has_head: Vec<bool>,
    order_cols: Vec<SortColumn>,
}

impl MergeHeap {
    fn new(capacity: usize, order_cols: Vec<SortColumn>) -> Self {
        Self {
            heads: Vec::with_capacity(capacity),
            has_head: vec![false; capacity],
            order_cols,
        }
    }

    fn is_empty(&self) -> bool {
        self.heads.is_empty()
    }

    fn contains(&self, buffer_id: usize) -> bool {
        self.has_head.get(buffer_id).copied().unwrap_or(false)
    }

    fn pop_min(&mut self) -> Option<MergeHead> {
        if self.is_empty() {
            return None;
        }
        let head = self.heads.swap_remove(0);
        self.has_head[head.buffer_id] = false;
        self.sift_down();
        Some(head)
    }

    fn compare_heads(&self, left: usize, right: usize) -> Ordering {
        compare_data_row(
            &self.order_cols,
            &self.heads[left].row,
            &self.heads[right].row,
        )
        .then_with(|| self.heads[left].buffer_id.cmp(&self.heads[right].buffer_id))
    }

    fn push(&mut self, head: MergeHead) {
        self.has_head[head.buffer_id] = true;
        self.heads.push(head);
        let mut child = self.heads.len() - 1;
        while child > 0 {
            let parent = (child - 1) / 2;
            if self.compare_heads(parent, child) != Ordering::Greater {
                break;
            }
            self.heads.swap(parent, child);
            child = parent;
        }
    }

    fn sift_down(&mut self) {
        let mut parent = 0;
        while parent * 2 + 1 < self.heads.len() {
            let mut child = parent * 2 + 1;
            if child + 1 < self.heads.len()
                && self.compare_heads(child + 1, child) == Ordering::Less
            {
                child += 1;
            }
            if self.compare_heads(parent, child) != Ordering::Greater {
                break;
            }
            self.heads.swap(parent, child);
            parent = child;
        }
    }
}

#[cfg(test)]
mod test {
    use super::super::buffer::Buffer;
    use super::*;
    use crate::{
        frontend::router::parser::order_by::{OrderByColumn, OrderByIdx},
        net::{Field, Format, RowDescription, messages::Vector},
    };
    use bytes::Bytes;

    fn merge_row(value: i64) -> Message {
        let mut row = DataRow::new();
        row.add(value);
        row.message()
    }

    fn take_merge_value(buffer: &mut MergePreSortBuffer) -> i64 {
        let message = buffer.take().expect("a globally ordered row is ready");
        DataRow::from_bytes(message.to_bytes())
            .expect("valid row")
            .get::<i64>(0, Format::Text)
            .expect("bigint column")
    }

    #[test]
    fn eager_merge_waits_for_missing_heads() {
        let decoder = Decoder::from(RowDescription::new(&[Field::bigint("id")]));
        let mut buffer =
            MergePreSortBuffer::try_new(2, &[OrderBy::column("id".into(), true)], &decoder)
                .expect("valid ordering");

        buffer.add(0, merge_row(1)).expect("valid stream");
        assert!(!buffer.can_take());
        assert!(buffer.take().is_none());
        assert!(buffer.needs_row(1).expect("valid stream"));

        buffer.add(1, merge_row(3)).expect("valid stream");
        assert_eq!(take_merge_value(&mut buffer), 1);
        // Buffer 0's unseen next row could precede buffer 1's buffered row.
        assert!(buffer.take().is_none());
        assert!(buffer.needs_row(0).expect("valid stream"));

        buffer.add(0, merge_row(2)).expect("valid stream");
        assert_eq!(take_merge_value(&mut buffer), 2);
        buffer.mark_complete(0).expect("valid stream");
        assert_eq!(take_merge_value(&mut buffer), 3);
        buffer.mark_complete(1).expect("valid stream");
        assert!(!buffer.can_take());
        assert!(buffer.take().is_none());
    }

    #[test]
    fn eager_merge_handles_empty_and_completed_streams() {
        let decoder = Decoder::from(RowDescription::new(&[Field::bigint("id")]));
        let mut buffer =
            MergePreSortBuffer::try_new(3, &[OrderBy::Index(OrderByIdx::Asc(1))], &decoder)
                .expect("valid ordering");
        buffer.mark_complete(0).expect("empty stream");
        buffer.mark_complete(0).expect("completion is idempotent");
        buffer.add(1, merge_row(2)).expect("valid stream");
        buffer.add(1, merge_row(4)).expect("valid stream");
        buffer.mark_complete(1).expect("buffered stream");
        assert!(buffer.take().is_none());
        buffer.add(2, merge_row(3)).expect("valid stream");
        assert_eq!(take_merge_value(&mut buffer), 2);
        assert_eq!(take_merge_value(&mut buffer), 3);
        assert!(buffer.take().is_none());
        buffer.mark_complete(2).expect("valid stream");
        assert_eq!(take_merge_value(&mut buffer), 4);
        assert!(buffer.take().is_none());
        assert!(!buffer.needs_row(1).expect("valid stream"));
    }

    #[test]
    fn eager_merge_matches_full_sort_with_multiple_keys() {
        let decoder = Decoder::from(RowDescription::new(&[
            Field::bigint("group"),
            Field::bigint("id"),
        ]));
        let columns = [
            OrderBy::column("group".into(), true),
            OrderBy::Index(OrderByIdx::Desc(2)),
        ];
        let mut eager = MergePreSortBuffer::try_new(7, &columns, &decoder).expect("valid ordering");
        let mut full = Buffer::default();
        for buffer_id in (0..7).rev() {
            for group in 0..buffer_id {
                for id in (0..3).rev() {
                    let mut row = DataRow::new();
                    row.add(group as i64).add(id as i64);
                    full.add(row.message()).expect("valid row");
                    eager.add(buffer_id, row.message()).expect("valid stream");
                }
            }
            eager.mark_complete(buffer_id).expect("valid stream");
        }
        full.sort(&columns, &decoder);
        full.mark_full();
        let mut count = 0;
        while let Some(expected) = full.take() {
            let actual = eager.take().expect("merged row");
            assert_eq!(actual.to_bytes(), expected.to_bytes());
            count += 1;
        }
        assert_eq!(count, 63);
        assert!(eager.take().is_none());
    }

    #[test]
    fn eager_merge_preserves_binary_formats_and_null_ordering() {
        let mut field = Field::bigint("id");
        field.format = 1;
        let decoder = Decoder::from(RowDescription::new(&[field]));
        for ascending in [true, false] {
            let columns = [OrderBy::column("id".into(), ascending)];
            let mut eager =
                MergePreSortBuffer::try_new(2, &columns, &decoder).expect("valid ordering");
            let mut full = Buffer::default();
            for buffer_id in 0..2 {
                let values = if ascending {
                    vec![Some(buffer_id as i64), Some(buffer_id as i64 + 2), None]
                } else {
                    vec![None, Some(buffer_id as i64 + 2), Some(buffer_id as i64)]
                };
                for value in values {
                    let mut row = DataRow::new();
                    row.insert(
                        0,
                        Bytes::copy_from_slice(&value.unwrap_or(0).to_be_bytes()),
                        value.is_none(),
                    );
                    full.add(row.message()).expect("valid row");
                    eager.add(buffer_id, row.message()).expect("valid stream");
                }
                eager.mark_complete(buffer_id).expect("valid stream");
            }
            full.sort(&columns, &decoder);
            full.mark_full();
            while let Some(expected) = full.take() {
                assert_eq!(
                    eager.take().expect("merged row").to_bytes(),
                    expected.to_bytes()
                );
            }
            assert!(eager.take().is_none());
        }
    }

    #[test]
    fn eager_merge_orders_buffer_heads_by_vector_distance() {
        use crate::net::Bind;
        use pgdog_postgres_types::FromDataType;

        for format in [Format::Text, Format::Binary] {
            let mut field = Field::text("embedding");
            // Extension OIDs can differ from built-in types; decode using the vector sort key.
            field.type_oid = 99_999;
            let mut decoder = Decoder::from(RowDescription::new(&[field]));
            decoder.set_formats(&Bind::new_params_codes_results(
                "",
                &[],
                &[],
                &[if format == Format::Text { 0 } else { 1 }],
            ));
            let columns = [OrderBy::Column(OrderByColumn::AscVectorL2(
                "embedding".into(),
                Vector::from(vec![0.0_f32, 0.0, 0.0]),
            ))];
            let mut buffer =
                MergePreSortBuffer::try_new(3, &columns, &decoder).expect("valid ordering");
            drop(decoder);
            for (buffer_id, vectors) in [(0, ["[-1,0,0]", "[-4,0,0]"]), (1, ["[2,0,0]", "[3,0,0]"])]
            {
                for vector in vectors {
                    let mut row = DataRow::new();
                    row.add(
                        Vector::decode(vector.as_bytes(), Format::Text)
                            .unwrap()
                            .encode(format)
                            .unwrap(),
                    );
                    buffer.add(buffer_id, row.message()).expect("valid stream");
                }
                buffer.mark_complete(buffer_id).expect("valid stream");
            }
            let mut null = DataRow::new();
            null.add(None::<i64>);
            buffer.add(2, null.message()).unwrap();
            buffer.mark_complete(2).unwrap();
            for expected in ["[-1,0,0]", "[2,0,0]", "[3,0,0]", "[-4,0,0]"] {
                let row = buffer.take().expect("merged row");
                assert_eq!(
                    row.get::<Vector>(0, format).unwrap(),
                    Vector::decode(expected.as_bytes(), Format::Text).unwrap()
                );
            }
            assert!(buffer.take().unwrap().get_raw(0).unwrap().is_null);
            assert!(buffer.take().is_none());
        }
    }

    #[test]
    fn eager_merge_validates_streams_and_column_names() {
        let decoder = Decoder::from(RowDescription::new(&[Field::bigint("id")]));
        assert!(matches!(
            MergePreSortBuffer::try_new(1, &[OrderBy::column("missing".into(), true)], &decoder),
            Err(Error::ColumnByNameNotFound(_))
        ));
        let mut buffer = MergePreSortBuffer::try_new(1, &[], &decoder).expect("valid buffer");
        assert!(matches!(
            buffer.add(1, merge_row(1)),
            Err(Error::InvalidBuffer(1))
        ));
        assert!(matches!(
            buffer.mark_complete(1),
            Err(Error::InvalidBuffer(1))
        ));
        buffer.mark_complete(0).expect("valid stream");
        assert!(matches!(
            buffer.add(0, merge_row(1)),
            Err(Error::BufferComplete(0))
        ));
        let mut empty = MergePreSortBuffer::try_new(0, &[], &decoder).expect("zero streams");
        assert!(empty.take().is_none());
    }
}
