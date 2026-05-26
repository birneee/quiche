// Copyright (C) 2023, Cloudflare, Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
// IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
// THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
// PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

use std::cmp;

use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use fast_list::LinkedList;
use fast_list::LinkedListIndex;

use crate::stream::RecvAction;
use crate::stream::RecvBufResetReturn;
use crate::stream::StreamIdHashMap;
use crate::Error;
use crate::Result;

use crate::flowcontrol;

use crate::range_buf::RangeBuf;

/// Identity-hashed map from stream offset → data chunk.
type OffsetMap = StreamIdHashMap<Bytes>;

/// A half-open byte interval `[start, end)`.
#[derive(Debug, Clone, Copy)]
struct ByteInterval {
    start: u64,
    end:   u64,
}

/// Ordered list of byte ranges not yet received (quic-go gap tracking).
///
/// Initialized with a single all-covering gap `[0, u64::MAX)` so that
/// `#[derive(Default)]` on `RecvBuf` produces a ready-to-use instance.
#[derive(Debug)]
struct Gaps(LinkedList<ByteInterval>);

impl Default for Gaps {
    fn default() -> Self {
        let mut list = LinkedList::new();
        list.push_back(ByteInterval { start: 0, end: u64::MAX });
        Gaps(list)
    }
}

impl std::ops::Deref for Gaps {
    type Target = LinkedList<ByteInterval>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Gaps {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Receive-side stream buffer.
///
/// Incoming stream frames are stored in `data`, an offset-keyed hash map
/// (ported from quic-go's frameSorter). A `gaps` list tracks which byte
/// ranges are still missing. `off` is the next offset to deliver to the
/// application — it doubles as the sorter's read position, so no separate
/// struct or sync helpers are needed.
#[derive(Debug, Default)]
pub struct RecvBuf {
    /// Buffered chunks keyed by start offset.
    data: OffsetMap,

    /// The lowest data offset that has yet to be read by the application.
    off: u64,

    /// Ordered list of byte ranges not yet received (quic-go gap tracking).
    gaps: Gaps,

    /// The total length of data received on this stream.
    len: u64,

    /// Receiver flow controller.
    flow_control: flowcontrol::FlowControl,

    /// The final stream offset received from the peer, if any.
    fin_off: Option<u64>,

    /// The error code received via RESET_STREAM.
    error: Option<u64>,

    /// Whether incoming data is validated but not buffered.
    drain: bool,
}

impl RecvBuf {
    /// Creates a new receive buffer.
    pub fn new(max_data: u64, initial_window: u64, max_window: u64) -> RecvBuf {
        RecvBuf {
            flow_control: flowcontrol::FlowControl::new(
                max_data,
                initial_window,
                max_window,
            ),
            ..RecvBuf::default()
        }
    }

    /// Inserts the given chunk of data in the buffer.
    ///
    /// This also takes care of enforcing stream flow control limits, as well
    /// as handling incoming data that overlaps data that is already in the
    /// buffer.
    pub fn write(&mut self, buf: RangeBuf) -> Result<()> {
        if buf.max_off() > self.max_data() {
            return Err(Error::FlowControl);
        }

        if let Some(fin_off) = self.fin_off {
            // Stream's size is known, forbid data beyond that point.
            if buf.max_off() > fin_off {
                return Err(Error::FinalSize);
            }

            // Stream's size is already known, forbid changing it.
            if buf.fin() && fin_off != buf.max_off() {
                return Err(Error::FinalSize);
            }
        }

        // Stream's known size is lower than data already received.
        if buf.fin() && buf.max_off() < self.len {
            return Err(Error::FinalSize);
        }

        // We already saved the final offset, so there's nothing else we
        // need to keep from the RangeBuf if it's empty.
        if self.fin_off.is_some() && buf.is_empty() {
            return Ok(());
        }

        if buf.fin() {
            self.fin_off = Some(buf.max_off());
        }

        // No need to store empty buffer that doesn't carry the fin flag.
        if !buf.fin() && buf.is_empty() {
            return Ok(());
        }

        self.len = cmp::max(self.len, buf.max_off());

        if !self.drain {
            if !buf.is_empty() {
                self.sorter_push(buf.off(), Bytes::copy_from_slice(&buf[..]))?;
            } else if buf.fin() && !self.ready() {
                // Empty fin with no data at the read position: insert a
                // zero-length sentinel so the application gets one
                // Ok((0, true)) notification.
                self.data.insert(self.off, Bytes::new());
            }
        } else {
            // we are not storing any data, off == len
            self.off = self.len;
        }

        Ok(())
    }

    /// Reads contiguous data from the receive buffer.
    ///
    /// Data is written into the given `out` buffer, up to the length of `out`.
    ///
    /// Only contiguous data is removed, starting from offset 0. The offset is
    /// incremented as data is taken out of the receive buffer. If there is no
    /// data at the expected read offset, the `Done` error is returned.
    ///
    /// On success the amount of data read and a flag indicating
    /// if there is no more data in the buffer, are returned as a tuple.
    #[inline]
    pub fn emit(&mut self, mut out: &mut [u8]) -> Result<(usize, bool)> {
        self.emit_or_discard(RecvAction::Emit { out: &mut out })
    }

    /// Reads or discards contiguous data from the receive buffer.
    ///
    /// Passing an `action` of `StreamRecvAction::Emit` results in data being
    /// written into the provided buffer, up to its length.
    ///
    /// Passing an `action` of `StreamRecvAction::Discard` results in up to
    /// the indicated number of bytes being discarded without copying.
    ///
    /// Only contiguous data is removed, starting from offset 0. The offset is
    /// incremented as data is taken out of the receive buffer. If there is no
    /// data at the expected read offset, the `Done` error is returned.
    ///
    /// On success the amount of data read or discarded, and a flag indicating
    /// if there is no more data in the buffer, are returned as a tuple.
    pub fn emit_or_discard<B: bytes::BufMut>(
        &mut self, mut action: RecvAction<B>,
    ) -> Result<(usize, bool)> {
        let mut len = 0;
        let mut cap = match &action {
            RecvAction::Emit { out } => out.remaining_mut(),
            RecvAction::Discard { len } => *len,
        };

        if !self.ready() {
            return Err(Error::Done);
        }

        // The stream was reset, so clear its data and return the error code
        // instead.
        if let Some(e) = self.error {
            self.data.clear();
            return Err(Error::StreamReset(e));
        }

        while cap > 0 && self.ready() {
            let chunk = self.sorter_pop(cap)?;
            let chunk_len = chunk.len();

            // Only copy data if we're emitting, not discarding.
            if let RecvAction::Emit { ref mut out } = action {
                // Note: `BufMut::remaining_mut()` cannot "shrink", but BufMut
                // impls are allowed to grow the buffer, so we
                // check here that we still have at least
                // `cap` bytes, but we can't require equality
                debug_assert!(
                    cap <= out.remaining_mut(),
                    "We updated `cap` incorrectly"
                );
                out.put_slice(&chunk[..]);
            }

            len += chunk_len;
            cap -= chunk_len;
        }

        // Update consumed bytes for flow control.
        self.flow_control.add_consumed(len as u64);

        Ok((len, self.is_fin()))
    }

    /// Resets the stream at the given offset.
    pub fn reset(
        &mut self, error_code: u64, final_size: u64,
    ) -> Result<RecvBufResetReturn> {
        // Stream's size is already known, forbid changing it.
        if let Some(fin_off) = self.fin_off {
            if fin_off != final_size {
                return Err(Error::FinalSize);
            }
        }

        // Stream's known size is lower than data already received.
        if final_size < self.len {
            return Err(Error::FinalSize);
        }

        // Final size must not exceed the stream-level flow control limit.
        if final_size > self.max_data() {
            return Err(Error::FlowControl);
        }

        if self.error.is_some() {
            // We already verified that the final size matches
            return Ok(RecvBufResetReturn::zero());
        }

        // Calculate how many bytes need to be removed from the connection flow
        // control.
        let result = RecvBufResetReturn {
            max_data_delta: final_size - self.len,
            consumed_flowcontrol: final_size - self.off,
        };

        self.error = Some(error_code);

        // Clear all data already buffered.
        self.off = final_size;

        self.data.clear();
        self.fin_off = Some(final_size);
        self.len = cmp::max(self.len, final_size);

        // Insert a sentinel so ready() returns true and the application
        // receives exactly one StreamReset notification via emit_or_discard.
        // Skip if already draining — shutdown() means the app is done reading.
        if !self.drain {
            self.data.insert(self.off, Bytes::new());
        }

        Ok(result)
    }

    /// Commits the new max_data limit.
    pub fn update_max_data(&mut self, now: Instant) {
        self.flow_control.update_max_data(now);
    }

    /// Return the new max_data limit.
    pub fn max_data_next(&mut self) -> u64 {
        self.flow_control.max_data_next()
    }

    /// Return the current flow control limit.
    pub fn max_data(&self) -> u64 {
        self.flow_control.max_data()
    }

    /// Return the current window.
    pub fn window(&self) -> u64 {
        self.flow_control.window()
    }

    /// Autotune the window size.
    pub fn autotune_window(&mut self, now: Instant, rtt: Duration) {
        self.flow_control.autotune_window(now, rtt);
    }

    /// Shuts down receiving data and returns the number of bytes
    /// that should be returned to the connection level flow
    /// control
    pub fn shutdown(&mut self) -> Result<u64> {
        if self.drain {
            return Err(Error::Done);
        }

        self.drain = true;

        self.data.clear();

        let consumed = self.max_off() - self.off;
        self.off = self.max_off();

        Ok(consumed)
    }

    /// Returns the lowest offset of data buffered.
    // Used by qlog (feature-gated); suppress the warning when qlog is disabled.
    #[allow(dead_code)]
    pub fn off_front(&self) -> u64 {
        self.off
    }

    /// Returns true if we need to update the local flow control limit.
    pub fn almost_full(&self) -> bool {
        self.fin_off.is_none() && self.flow_control.should_update_max_data()
    }

    /// Returns the largest offset ever received.
    pub fn max_off(&self) -> u64 {
        self.len
    }

    /// Returns true if the receive-side of the stream is complete.
    ///
    /// This happens when the stream's receive final size is known, and the
    /// application has read all data from the stream.
    pub fn is_fin(&self) -> bool {
        if self.fin_off == Some(self.off) {
            return true;
        }

        false
    }

    /// Returns true if the stream is not storing incoming data.
    pub fn is_draining(&self) -> bool {
        self.drain
    }

    /// Returns true if the stream has data to be read.
    pub fn ready(&self) -> bool {
        self.data.contains_key(&self.off)
    }

    #[cfg(test)]
    pub(crate) fn flow_control_for_tests(&self) -> &flowcontrol::FlowControl {
        &self.flow_control
    }

    // -----------------------------------------------------------------------
    // Frame-sorter internals (ported from quic-go's frameSorter)
    // -----------------------------------------------------------------------

    /// Remove and return up to `max_size` bytes starting at `self.off`.
    fn sorter_pop(&mut self, max_size: usize) -> Result<Bytes> {
        let mut chunk = match self.data.remove(&self.off) {
            Some(b) => b,
            None => return Err(Error::Done),
        };
        if max_size < chunk.len() {
            let out = chunk.split_to(max_size);
            self.off += max_size as u64;
            self.data.insert(self.off, chunk);
            Ok(out)
        } else {
            self.off += chunk.len() as u64;
            Ok(chunk)
        }
    }

    /// Insert a new frame, trimming or discarding bytes that overlap with
    /// already-received data. Longer frames replace shorter ones at the same
    /// position (quic-go semantics).
    fn sorter_push(&mut self, offset: u64, mut buf: Bytes) -> Result<()> {
        debug_assert!(!buf.is_empty());
        debug_assert!(self.gaps.head().is_some());

        let mut start = offset;
        let mut end = offset + buf.len() as u64;

        if end <= self.gaps.head().unwrap().value.start {
            return Ok(());
        }

        let (start_gap, starts_in_gap) = self.find_start_gap(start);
        let (end_gap, ends_in_gap) = self.find_end_gap(start_gap, end);

        let start_gap_equals_end_gap = start_gap == end_gap;

        // Read all fields we need from the two gap nodes upfront — one lookup
        // each, matching quic-go's single pointer-dereference per element.
        let sg = self.gaps.get(start_gap).unwrap();
        let start_gap_next  = sg.next_index;
        let start_gap_start = sg.value.start;
        let start_gap_end   = sg.value.end;
        let (end_gap_start, end_gap_end) = if start_gap_equals_end_gap {
            (start_gap_start, start_gap_end)
        } else {
            let eg = self.gaps.get(end_gap).unwrap().value;
            (eg.start, eg.end)
        };

        if (start_gap_equals_end_gap && end <= start_gap_start) ||
            (!start_gap_equals_end_gap &&
                start_gap_end >= end_gap_start &&
                end <= start_gap_start)
        {
            return Ok(()); // duplicate
        }

        let mut adjusted_start_gap_end = false;

        let mut pos = start;
        let mut has_replaced_at_least_one = false;

        // Replace shorter existing frames with this longer one.
        while let Some(old) = self.data.remove(&pos) {
            let old_len = old.len() as u64;
            if end - pos > old_len ||
                (has_replaced_at_least_one && end - pos == old_len)
            {
                pos += old_len;
                has_replaced_at_least_one = true;
            } else {
                if !has_replaced_at_least_one {
                    return Ok(());
                }
                // Cut new frame so it ends where the existing frame starts.
                let _ = buf.split_off((pos - start) as usize);
                end = pos;
                break;
            }
        }

        // Trim leading bytes that fall before the gap start.
        if !starts_in_gap && !has_replaced_at_least_one {
            buf = buf.split_off((start_gap_start - start) as usize);
            start = start_gap_start;
        }

        // Update / remove the start gap.
        if start <= start_gap_start {
            if end >= start_gap_end {
                self.gaps.remove(start_gap);
            } else {
                self.gaps.get_mut(start_gap).unwrap().value.start = end;
            }
        } else if !has_replaced_at_least_one {
            self.gaps.get_mut(start_gap).unwrap().value.end = start;
            adjusted_start_gap_end = true;
        }

        // Remove intermediate gaps and their associated frames.
        if !start_gap_equals_end_gap {
            self.delete_consecutive(start_gap_end);
            let mut gap = start_gap_next;
            while let Some(idx) = gap {
                let item = self.gaps.get(idx).unwrap();
                if item.value.end >= end_gap_start {
                    break;
                }
                let item_end = item.value.end;
                let next = item.next_index;
                self.delete_consecutive(item_end);
                self.gaps.remove(idx);
                gap = next;
            }
        }

        // Trim trailing bytes that fall past the end gap.
        if !ends_in_gap && start != end_gap_end && end > end_gap_end {
            let _ = buf.split_off((end_gap_end - start) as usize);
            end = end_gap_end;
        }

        // Update / remove the end gap.
        if end == end_gap_end {
            if !start_gap_equals_end_gap {
                self.gaps.remove(end_gap);
            }
        } else if start_gap_equals_end_gap && adjusted_start_gap_end {
            self.gaps.insert_after(
                start_gap,
                ByteInterval { start: end, end: start_gap_end },
            );
        } else if !start_gap_equals_end_gap {
            self.gaps.get_mut(end_gap).unwrap().value.start = end;
        }

        const MAX_GAPS: usize = 1000;
        if self.gaps.len() > MAX_GAPS {
            panic!("too many gaps in received stream data");
        }

        self.data.insert(start, buf);
        Ok(())
    }

    /// Returns the handle of the gap that contains or precedes `offset`, and
    /// whether `offset` falls inside that gap.
    fn find_start_gap(&self, offset: u64) -> (LinkedListIndex, bool) {
        for item in self.gaps.iter() {
            if offset >= item.value.start && offset <= item.value.end {
                return (item.index, true);
            }
            if offset < item.value.start {
                return (item.index, false);
            }
        }
        panic!("no gap found for offset {offset}");
    }

    /// Returns the handle of the gap that contains or follows `offset`, and
    /// whether `offset` falls inside that gap.
    fn find_end_gap(
        &self, start_gap: LinkedListIndex, offset: u64,
    ) -> (LinkedListIndex, bool) {
        let mut prev = start_gap;
        for item in self.gaps.iter_next(start_gap) {
            if offset >= item.value.start && offset < item.value.end {
                return (item.index, true);
            }
            if offset < item.value.start {
                return (prev, false);
            }
            prev = item.index;
        }
        panic!("no gap found for offset {offset}");
    }

    fn delete_consecutive(&mut self, mut pos: u64) {
        while let Some(entry) = self.data.remove(&pos) {
            pos += entry.len() as u64;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::DEFAULT_STREAM_WINDOW;
    use bytes::BufMut as _;
    use rstest::rstest;

    // Helper function for testing either buffer emit or discard.
    //
    // The `emit` parameter controls whether data is emitted or discarded from
    // `recv`.
    //
    // The `target_len` parameter controls the maximum amount of bytes that
    // could be read, up to the capacity of `recv`. The `result_len` is the
    // actual number of bytes that were taken out of `recv`. An assert is
    // performed on `result_len` to ensure the number of bytes read meets the
    // caller expectations.
    //
    // The `is_fin` parameter relates to the buffer's finished status. An assert
    // is performed on it to ensure the status meet the caller expectations.
    //
    // The `test_bytes` parameter carries an optional slice of bytes. Is set, an
    // assert is performed against the bytes that were read out of the buffer,
    // to ensure caller expectations are met.
    fn assert_emit_discard(
        recv: &mut RecvBuf, emit: bool, target_len: usize, result_len: usize,
        is_fin: bool, test_bytes: Option<&[u8]>,
    ) {
        let mut buf = Vec::<u8>::with_capacity(512).limit(target_len);
        let action = if emit {
            RecvAction::Emit { out: &mut buf }
        } else {
            RecvAction::Discard { len: target_len }
        };

        let (read, fin) = recv.emit_or_discard(action).unwrap();

        let buf = buf.into_inner();
        if emit {
            assert_eq!(buf.len(), read);
            if let Some(v) = test_bytes {
                assert_eq!(&buf, v);
            }
        }

        assert_eq!(read, result_len);
        assert_eq!(is_fin, fin);
    }

    // Helper function for testing buffer status for either emit or discard.
    fn assert_emit_discard_done(recv: &mut RecvBuf, emit: bool) {
        let mut buf = [0u8; 32];
        let action = if emit {
            RecvAction::Emit {
                out: &mut buf.as_mut_slice(),
            }
        } else {
            RecvAction::Discard { len: 32 }
        };
        assert_eq!(recv.emit_or_discard(action), Err(Error::Done));
    }

    #[rstest]
    fn empty_read(#[values(true, false)] emit: bool) {
        let mut recv =
            RecvBuf::new(u64::MAX, DEFAULT_STREAM_WINDOW, DEFAULT_STREAM_WINDOW);
        assert_eq!(recv.len, 0);

        assert_emit_discard_done(&mut recv, emit);
    }

    #[rstest]
    fn empty_stream_frame(#[values(true, false)] emit: bool) {
        let mut recv =
            RecvBuf::new(15, DEFAULT_STREAM_WINDOW, DEFAULT_STREAM_WINDOW);
        assert_eq!(recv.len, 0);

        let buf = RangeBuf::from(b"hello", 0, false);
        assert!(recv.write(buf).is_ok());
        assert_eq!(recv.len, 5);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 1);

        assert_emit_discard(&mut recv, emit, 32, 5, false, None);

        // Don't store non-fin empty buffer.
        let buf = RangeBuf::from(b"", 10, false);
        assert!(recv.write(buf).is_ok());
        assert_eq!(recv.len, 5);
        assert_eq!(recv.off, 5);
        assert_eq!(recv.data.len(), 0);

        // Check flow control for empty buffer.
        let buf = RangeBuf::from(b"", 16, false);
        assert_eq!(recv.write(buf), Err(Error::FlowControl));

        // Store fin empty buffer.
        let buf = RangeBuf::from(b"", 5, true);
        assert!(recv.write(buf).is_ok());
        assert_eq!(recv.len, 5);
        assert_eq!(recv.off, 5);
        assert_eq!(recv.data.len(), 1);

        // Don't store additional fin empty buffers.
        let buf = RangeBuf::from(b"", 5, true);
        assert!(recv.write(buf).is_ok());
        assert_eq!(recv.len, 5);
        assert_eq!(recv.off, 5);
        assert_eq!(recv.data.len(), 1);

        // Don't store additional fin non-empty buffers.
        let buf = RangeBuf::from(b"aa", 3, true);
        assert!(recv.write(buf).is_ok());
        assert_eq!(recv.len, 5);
        assert_eq!(recv.off, 5);
        assert_eq!(recv.data.len(), 1);

        // Validate final size with fin empty buffers.
        let buf = RangeBuf::from(b"", 6, true);
        assert_eq!(recv.write(buf), Err(Error::FinalSize));
        let buf = RangeBuf::from(b"", 4, true);
        assert_eq!(recv.write(buf), Err(Error::FinalSize));

        assert_emit_discard(&mut recv, emit, 32, 0, true, None);
    }

    #[rstest]
    fn ordered_read(#[values(true, false)] emit: bool) {
        let mut recv =
            RecvBuf::new(u64::MAX, DEFAULT_STREAM_WINDOW, DEFAULT_STREAM_WINDOW);
        assert_eq!(recv.len, 0);

        let first = RangeBuf::from(b"hello", 0, false);
        let second = RangeBuf::from(b"world", 5, false);
        let third = RangeBuf::from(b"something", 10, true);

        assert!(recv.write(second).is_ok());
        assert_eq!(recv.len, 10);
        assert_eq!(recv.off, 0);

        assert_emit_discard_done(&mut recv, emit);

        assert!(recv.write(third).is_ok());
        assert_eq!(recv.len, 19);
        assert_eq!(recv.off, 0);

        assert_emit_discard_done(&mut recv, emit);

        assert!(recv.write(first).is_ok());
        assert_eq!(recv.len, 19);
        assert_eq!(recv.off, 0);

        assert_emit_discard(
            &mut recv,
            emit,
            32,
            19,
            true,
            Some(b"helloworldsomething"),
        );
        assert_eq!(recv.len, 19);
        assert_eq!(recv.off, 19);

        assert_emit_discard_done(&mut recv, emit);
    }

    /// Test shutdown behavior
    #[rstest]
    fn shutdown(#[values(true, false)] emit: bool) {
        let mut recv =
            RecvBuf::new(u64::MAX, DEFAULT_STREAM_WINDOW, DEFAULT_STREAM_WINDOW);
        assert_eq!(recv.len, 0);

        let first = RangeBuf::from(b"hello", 0, false);
        let second = RangeBuf::from(b"world", 5, false);
        let third = RangeBuf::from(b"something", 10, false);

        assert!(recv.write(second).is_ok());
        assert_eq!(recv.len, 10);
        assert_eq!(recv.off, 0);

        assert_emit_discard_done(&mut recv, emit);

        // shutdown the buffer. Buffer is dropped.
        assert_eq!(recv.shutdown(), Ok(10));
        assert_eq!(recv.len, 10);
        assert_eq!(recv.off, 10);
        assert_eq!(recv.data.len(), 0);

        assert_emit_discard_done(&mut recv, emit);

        // subsequent writes are validated but not added to the buffer
        assert!(recv.write(first).is_ok());
        assert_eq!(recv.len, 10);
        assert_eq!(recv.off, 10);
        assert_eq!(recv.data.len(), 0);

        // the max offset of received data can increase and
        // the recv.off must increase with it
        assert!(recv.write(third).is_ok());
        assert_eq!(recv.len, 19);
        assert_eq!(recv.off, 19);
        assert_eq!(recv.data.len(), 0);

        // Send a reset
        assert_emit_discard_done(&mut recv, emit);
        assert_eq!(
            recv.reset(42, 123),
            Ok(RecvBufResetReturn {
                max_data_delta: 104,
                consumed_flowcontrol: 104,
            })
        );
        assert_eq!(recv.len, 123);
        assert_eq!(recv.off, 123);
        assert_eq!(recv.data.len(), 0);

        assert_emit_discard_done(&mut recv, emit);
    }

    #[rstest]
    fn split_read(#[values(true, false)] emit: bool) {
        let mut recv =
            RecvBuf::new(u64::MAX, DEFAULT_STREAM_WINDOW, DEFAULT_STREAM_WINDOW);
        assert_eq!(recv.len, 0);

        let first = RangeBuf::from(b"something", 0, false);
        let second = RangeBuf::from(b"helloworld", 9, true);

        assert!(recv.write(first).is_ok());
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 0);

        assert!(recv.write(second).is_ok());
        assert_eq!(recv.len, 19);
        assert_eq!(recv.off, 0);

        assert_emit_discard(&mut recv, emit, 10, 10, false, Some(b"somethingh"));
        assert_eq!(recv.len, 19);
        assert_eq!(recv.off, 10);

        assert_emit_discard(&mut recv, emit, 5, 5, false, Some(b"ellow"));
        assert_eq!(recv.len, 19);
        assert_eq!(recv.off, 15);

        assert_emit_discard(&mut recv, emit, 5, 4, true, Some(b"orld"));
        assert_eq!(recv.len, 19);
        assert_eq!(recv.off, 19);
    }

    #[test]
    fn split_read_incremental_buf() {
        let mut recv =
            RecvBuf::new(u64::MAX, DEFAULT_STREAM_WINDOW, DEFAULT_STREAM_WINDOW);
        assert_eq!(recv.len, 0);

        let first = RangeBuf::from(b"something", 0, false);
        let second = RangeBuf::from(b"helloworld", 9, true);

        assert!(recv.write(first).is_ok());
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 0);

        assert!(recv.write(second).is_ok());
        assert_eq!(recv.len, 19);
        assert_eq!(recv.off, 0);

        let mut buf = Vec::new().limit(10);
        assert_eq!(
            recv.emit_or_discard(RecvAction::Emit { out: &mut buf }),
            Ok((10, false))
        );
        assert_eq!(recv.len, 19);
        assert_eq!(recv.off, 10);
        assert_eq!(buf.get_ref().len(), 10);
        assert_eq!(buf.get_ref().as_slice(), b"somethingh");

        buf.set_limit(5);
        assert_eq!(
            recv.emit_or_discard(RecvAction::Emit { out: &mut buf }),
            Ok((5, false))
        );
        assert_eq!(recv.len, 19);
        assert_eq!(recv.off, 15);
        assert_eq!(buf.get_ref().len(), 15);
        assert_eq!(buf.get_ref().as_slice(), b"somethinghellow");

        buf.set_limit(42);
        assert_eq!(
            recv.emit_or_discard(RecvAction::Emit { out: &mut buf }),
            Ok((4, true))
        );
        assert_eq!(recv.len, 19);
        assert_eq!(recv.off, 19);
        assert_eq!(buf.get_ref().len(), 19);
        assert_eq!(buf.get_ref().as_slice(), b"somethinghelloworld");
    }

    #[rstest]
    fn incomplete_read(#[values(true, false)] emit: bool) {
        let mut recv =
            RecvBuf::new(u64::MAX, DEFAULT_STREAM_WINDOW, DEFAULT_STREAM_WINDOW);
        assert_eq!(recv.len, 0);

        let mut buf = [0u8; 32];

        let first = RangeBuf::from(b"something", 0, false);
        let second = RangeBuf::from(b"helloworld", 9, true);

        assert!(recv.write(second).is_ok());
        assert_eq!(recv.len, 19);
        assert_eq!(recv.off, 0);

        let action = if emit {
            RecvAction::Emit {
                out: &mut buf.as_mut_slice(),
            }
        } else {
            RecvAction::Discard { len: 32 }
        };
        assert_eq!(recv.emit_or_discard(action), Err(Error::Done));

        assert!(recv.write(first).is_ok());
        assert_eq!(recv.len, 19);
        assert_eq!(recv.off, 0);

        assert_emit_discard(
            &mut recv,
            emit,
            32,
            19,
            true,
            Some(b"somethinghelloworld"),
        );
        assert_eq!(recv.len, 19);
        assert_eq!(recv.off, 19);
    }

    #[rstest]
    fn zero_len_read(#[values(true, false)] emit: bool) {
        let mut recv =
            RecvBuf::new(u64::MAX, DEFAULT_STREAM_WINDOW, DEFAULT_STREAM_WINDOW);
        assert_eq!(recv.len, 0);

        let first = RangeBuf::from(b"something", 0, false);
        let second = RangeBuf::from(b"", 9, true);

        assert!(recv.write(first).is_ok());
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 1);

        assert!(recv.write(second).is_ok());
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 1);

        assert_emit_discard(&mut recv, emit, 32, 9, true, Some(b"something"));
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 9);
    }

    #[rstest]
    fn past_read(#[values(true, false)] emit: bool) {
        let mut recv =
            RecvBuf::new(u64::MAX, DEFAULT_STREAM_WINDOW, DEFAULT_STREAM_WINDOW);
        assert_eq!(recv.len, 0);

        let first = RangeBuf::from(b"something", 0, false);
        let second = RangeBuf::from(b"hello", 3, false);
        let third = RangeBuf::from(b"ello", 4, true);
        let fourth = RangeBuf::from(b"ello", 5, true);

        assert!(recv.write(first).is_ok());
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 1);

        assert_emit_discard(&mut recv, emit, 32, 9, false, Some(b"something"));
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 9);

        assert!(recv.write(second).is_ok());
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 9);
        assert_eq!(recv.data.len(), 0);

        assert_eq!(recv.write(third), Err(Error::FinalSize));

        assert!(recv.write(fourth).is_ok());
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 9);
        assert_eq!(recv.data.len(), 0);

        assert_emit_discard_done(&mut recv, emit);
    }

    #[rstest]
    fn fully_overlapping_read(#[values(true, false)] emit: bool) {
        let mut recv =
            RecvBuf::new(u64::MAX, DEFAULT_STREAM_WINDOW, DEFAULT_STREAM_WINDOW);
        assert_eq!(recv.len, 0);

        let first = RangeBuf::from(b"something", 0, false);
        let second = RangeBuf::from(b"hello", 4, false);

        assert!(recv.write(first).is_ok());
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 1);

        assert!(recv.write(second).is_ok());
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 1);

        assert_emit_discard(&mut recv, emit, 32, 9, false, Some(b"something"));
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 9);
        assert_eq!(recv.data.len(), 0);

        assert_emit_discard_done(&mut recv, emit);
    }

    #[rstest]
    fn fully_overlapping_read2(#[values(true, false)] emit: bool) {
        let mut recv =
            RecvBuf::new(u64::MAX, DEFAULT_STREAM_WINDOW, DEFAULT_STREAM_WINDOW);
        assert_eq!(recv.len, 0);

        let first = RangeBuf::from(b"something", 0, false);
        let second = RangeBuf::from(b"hello", 4, false);

        assert!(recv.write(second).is_ok());
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 1);

        assert!(recv.write(first).is_ok());
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 1); // sorter: longer "something" replaces shorter "hello"

        assert_emit_discard(&mut recv, emit, 32, 9, false, Some(b"something"));
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 9);
        assert_eq!(recv.data.len(), 0);

        assert_emit_discard_done(&mut recv, emit);
    }

    #[rstest]
    fn fully_overlapping_read3(#[values(true, false)] emit: bool) {
        let mut recv =
            RecvBuf::new(u64::MAX, DEFAULT_STREAM_WINDOW, DEFAULT_STREAM_WINDOW);
        assert_eq!(recv.len, 0);

        let first = RangeBuf::from(b"something", 0, false);
        let second = RangeBuf::from(b"hello", 3, false);

        assert!(recv.write(second).is_ok());
        assert_eq!(recv.len, 8);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 1);

        assert!(recv.write(first).is_ok());
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 1); // sorter: longer "something" replaces shorter "hello"

        assert_emit_discard(&mut recv, emit, 32, 9, false, Some(b"something"));
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 9);
        assert_eq!(recv.data.len(), 0);

        assert_emit_discard_done(&mut recv, emit);
    }

    #[rstest]
    fn fully_overlapping_read_multi(#[values(true, false)] emit: bool) {
        let mut recv =
            RecvBuf::new(u64::MAX, DEFAULT_STREAM_WINDOW, DEFAULT_STREAM_WINDOW);
        assert_eq!(recv.len, 0);

        let first = RangeBuf::from(b"somethingsomething", 0, false);
        let second = RangeBuf::from(b"hello", 3, false);
        let third = RangeBuf::from(b"hello", 12, false);

        assert!(recv.write(second).is_ok());
        assert_eq!(recv.len, 8);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 1);

        assert!(recv.write(third).is_ok());
        assert_eq!(recv.len, 17);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 2);

        assert!(recv.write(first).is_ok());
        assert_eq!(recv.len, 18);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 1); // sorter: longer "somethingsomething" replaces both "hello" frames

        assert_emit_discard(
            &mut recv,
            emit,
            32,
            18,
            false,
            Some(b"somethingsomething"),
        );
        assert_eq!(recv.len, 18);
        assert_eq!(recv.off, 18);
        assert_eq!(recv.data.len(), 0);

        assert_emit_discard_done(&mut recv, emit);
    }

    #[rstest]
    fn overlapping_start_read(#[values(true, false)] emit: bool) {
        let mut recv =
            RecvBuf::new(u64::MAX, DEFAULT_STREAM_WINDOW, DEFAULT_STREAM_WINDOW);
        assert_eq!(recv.len, 0);

        let first = RangeBuf::from(b"something", 0, false);
        let second = RangeBuf::from(b"hello", 8, true);

        assert!(recv.write(first).is_ok());
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 1);

        assert!(recv.write(second).is_ok());
        assert_eq!(recv.len, 13);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 2);

        assert_emit_discard(
            &mut recv,
            emit,
            32,
            13,
            true,
            Some(b"somethingello"),
        );

        assert_eq!(recv.len, 13);
        assert_eq!(recv.off, 13);

        assert_emit_discard_done(&mut recv, emit);
    }

    #[rstest]
    fn overlapping_end_read(#[values(true, false)] emit: bool) {
        let mut recv =
            RecvBuf::new(u64::MAX, DEFAULT_STREAM_WINDOW, DEFAULT_STREAM_WINDOW);
        assert_eq!(recv.len, 0);

        let first = RangeBuf::from(b"hello", 0, false);
        let second = RangeBuf::from(b"something", 3, true);

        assert!(recv.write(second).is_ok());
        assert_eq!(recv.len, 12);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 1);

        assert!(recv.write(first).is_ok());
        assert_eq!(recv.len, 12);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 2);

        assert_emit_discard(&mut recv, emit, 32, 12, true, Some(b"helsomething"));
        assert_eq!(recv.len, 12);
        assert_eq!(recv.off, 12);

        assert_emit_discard_done(&mut recv, emit);
    }

    #[rstest]
    fn overlapping_end_twice_read(#[values(true, false)] emit: bool) {
        let mut recv =
            RecvBuf::new(u64::MAX, DEFAULT_STREAM_WINDOW, DEFAULT_STREAM_WINDOW);
        assert_eq!(recv.len, 0);

        let first = RangeBuf::from(b"he", 0, false);
        let second = RangeBuf::from(b"ow", 4, false);
        let third = RangeBuf::from(b"rl", 7, false);
        let fourth = RangeBuf::from(b"helloworld", 0, true);

        assert!(recv.write(third).is_ok());
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 1);

        assert!(recv.write(second).is_ok());
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 2);

        assert!(recv.write(first).is_ok());
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 3);

        assert!(recv.write(fourth).is_ok());
        assert_eq!(recv.len, 10);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 1); // sorter: longer "helloworld" replaces "he", "ow", "rl"

        assert_emit_discard(&mut recv, emit, 32, 10, true, Some(b"helloworld"));
        assert_eq!(recv.len, 10);
        assert_eq!(recv.off, 10);

        assert_emit_discard_done(&mut recv, emit);
    }

    #[rstest]
    fn overlapping_end_twice_and_contained_read(
        #[values(true, false)] emit: bool,
    ) {
        let mut recv =
            RecvBuf::new(u64::MAX, DEFAULT_STREAM_WINDOW, DEFAULT_STREAM_WINDOW);
        assert_eq!(recv.len, 0);

        let first = RangeBuf::from(b"hellow", 0, false);
        let second = RangeBuf::from(b"barfoo", 10, true);
        let third = RangeBuf::from(b"rl", 7, false);
        let fourth = RangeBuf::from(b"elloworldbarfoo", 1, true);

        assert!(recv.write(third).is_ok());
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 1);

        assert!(recv.write(second).is_ok());
        assert_eq!(recv.len, 16);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 2);

        assert!(recv.write(first).is_ok());
        assert_eq!(recv.len, 16);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 3);

        assert!(recv.write(fourth).is_ok());
        assert_eq!(recv.len, 16);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 2); // sorter: "elloworldbarfoo" trims to "orldbarfoo"@6; "hellow"@0 stays

        assert_emit_discard(
            &mut recv,
            emit,
            32,
            16,
            true,
            Some(b"helloworldbarfoo"),
        );
        assert_eq!(recv.len, 16);
        assert_eq!(recv.off, 16);

        assert_emit_discard_done(&mut recv, emit);
    }

    #[rstest]
    fn partially_multi_overlapping_reordered_read(
        #[values(true, false)] emit: bool,
    ) {
        let mut recv =
            RecvBuf::new(u64::MAX, DEFAULT_STREAM_WINDOW, DEFAULT_STREAM_WINDOW);
        assert_eq!(recv.len, 0);

        let first = RangeBuf::from(b"hello", 8, false);
        let second = RangeBuf::from(b"something", 0, false);
        let third = RangeBuf::from(b"moar", 11, true);

        assert!(recv.write(first).is_ok());
        assert_eq!(recv.len, 13);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 1);

        assert!(recv.write(second).is_ok());
        assert_eq!(recv.len, 13);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 2);

        assert!(recv.write(third).is_ok());
        assert_eq!(recv.len, 15);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 3);

        assert_emit_discard(
            &mut recv,
            emit,
            32,
            15,
            true,
            Some(b"somethinhelloar"),
        );
        assert_eq!(recv.len, 15);
        assert_eq!(recv.off, 15);
        assert_eq!(recv.data.len(), 0);

        assert_emit_discard_done(&mut recv, emit);
    }

    #[rstest]
    fn partially_multi_overlapping_reordered_read2(
        #[values(true, false)] emit: bool,
    ) {
        let mut recv =
            RecvBuf::new(u64::MAX, DEFAULT_STREAM_WINDOW, DEFAULT_STREAM_WINDOW);
        assert_eq!(recv.len, 0);

        let first = RangeBuf::from(b"aaa", 0, false);
        let second = RangeBuf::from(b"bbb", 2, false);
        let third = RangeBuf::from(b"ccc", 4, false);
        let fourth = RangeBuf::from(b"ddd", 6, false);
        let fifth = RangeBuf::from(b"eee", 9, false);
        let sixth = RangeBuf::from(b"fff", 11, false);

        assert!(recv.write(second).is_ok());
        assert_eq!(recv.len, 5);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 1);

        assert!(recv.write(fourth).is_ok());
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 2);

        assert!(recv.write(third).is_ok());
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 3);

        assert!(recv.write(first).is_ok());
        assert_eq!(recv.len, 9);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 4);

        assert!(recv.write(sixth).is_ok());
        assert_eq!(recv.len, 14);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 5);

        assert!(recv.write(fifth).is_ok());
        assert_eq!(recv.len, 14);
        assert_eq!(recv.off, 0);
        assert_eq!(recv.data.len(), 6);

        assert_emit_discard(
            &mut recv,
            emit,
            32,
            14,
            false,
            Some(b"aabbbcdddeefff"),
        );
        assert_eq!(recv.len, 14);
        assert_eq!(recv.off, 14);
        assert_eq!(recv.data.len(), 0);

        assert_emit_discard_done(&mut recv, emit);
    }

    #[test]
    fn mixed_read_actions() {
        let mut recv =
            RecvBuf::new(u64::MAX, DEFAULT_STREAM_WINDOW, DEFAULT_STREAM_WINDOW);
        assert_eq!(recv.len, 0);

        let first = RangeBuf::from(b"hello", 0, false);
        let second = RangeBuf::from(b"world", 5, false);
        let third = RangeBuf::from(b"something", 10, true);

        assert!(recv.write(second).is_ok());
        assert_eq!(recv.len, 10);
        assert_eq!(recv.off, 0);

        assert_emit_discard_done(&mut recv, true);
        assert_emit_discard_done(&mut recv, false);

        assert!(recv.write(third).is_ok());
        assert_eq!(recv.len, 19);
        assert_eq!(recv.off, 0);

        assert_emit_discard_done(&mut recv, true);
        assert_emit_discard_done(&mut recv, false);

        assert!(recv.write(first).is_ok());
        assert_eq!(recv.len, 19);
        assert_eq!(recv.off, 0);

        assert_emit_discard(&mut recv, true, 5, 5, false, Some(b"hello"));
        assert_eq!(recv.len, 19);
        assert_eq!(recv.off, 5);

        assert_emit_discard(&mut recv, false, 5, 5, false, None);
        assert_eq!(recv.len, 19);
        assert_eq!(recv.off, 10);

        assert_emit_discard(&mut recv, true, 9, 9, true, Some(b"something"));
        assert_eq!(recv.len, 19);
        assert_eq!(recv.off, 19);

        assert_emit_discard_done(&mut recv, true);
        assert_emit_discard_done(&mut recv, false);
    }
}
