// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use super::ZERO_CHUNK;
use crate::buffer::reader;
use crate::buffer::writer::Storage as _;
use crate::buffer::Reader;
use crate::varint::VarInt;
use bytes::Buf;
use bytes::BufMut;
use bytes::BytesMut;
use core::fmt;

/// Possible states for slots in the [`Reassembler`]'s queue
#[derive(PartialEq, Eq)]
pub struct Slot {
    start: u64,
    end: u64,
    data: BytesMut,
    virtual_len: Option<usize>,
}

impl fmt::Debug for Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Slot")
            .field("start", &self.start)
            .field("end", &self.end())
            .field("end_allocated", &self.end_allocated())
           // .field("len", &self.data.len())
           // .field("capacity", &self.data.capacity())
            .field("len", &self.buffered_len())
            .field("capacity", &self.capacity())
            .field("is_virtual", &self.is_virtual())
            .finish()
    }
}

impl Slot {
    #[inline]
    pub fn new(start: u64, end: u64, data: BytesMut) -> Self {
        super::probe::alloc(start, data.capacity());
        let v = Self {
            start,
            end,
            data,
            virtual_len: None,
        };
        v.invariants();
        v
    }

    #[inline]
    pub fn new_virtual(start: u64, end: u64) -> Self {
        let v = Self {
            start,
            end,
            data: BytesMut::new(),
            virtual_len: Some(0),
        };
        v.invariants();
        v
    }

    /// Creates a new `Slot` with only offset and length, without allocating
    /// payload data. This is useful for benchmarking where actual payload
    /// is not needed. The slot maintains all invariants but has no backing
    /// data allocation.
    #[inline]
    pub fn from_len(start: u64, len: usize) -> Self {
        let end = start + len as u64;
        // Previous approach for benchmarking:
        // let mut data = BytesMut::new();
        // unsafe {
        //     data.set_len(len);
        // }
        // Self { start, end, data }
        //
        // New approach: keep the original real-data path intact and represent
        // length-only writes with an explicit virtual length.
        Self {
            start,
            end,
            data: BytesMut::new(),
            virtual_len: Some(len),
        }
    }

    #[inline(always)]
    pub(crate) fn buffered_len(&self) -> usize {
        self.virtual_len.unwrap_or_else(|| self.data.len())
    }

    #[inline(always)]
    fn capacity(&self) -> usize {
        self.virtual_len
            .map(|_| (self.end - self.start) as usize)
            .unwrap_or_else(|| self.data.capacity())
    }

    #[inline(always)]
    fn is_virtual(&self) -> bool {
        self.virtual_len.is_some()
    }

    #[inline]
    fn ensure_real(&mut self) {
        let Some(len) = self.virtual_len.take() else {
            return;
        };

        let mut data = BytesMut::with_capacity((self.end - self.start) as usize);
        data.resize(len, 0);
        super::probe::alloc(self.start, data.capacity());
        self.data = data;
    }

    #[inline(always)]
    fn can_write_virtually<R>(&self, reader: &R) -> bool
    where
        R: Reader + ?Sized,
    {
        self.is_virtual() && reader.is_virtually_buffered()
    }

    #[inline(always)]
    fn advance_virtual_reader<R>(
        &mut self, reader: &mut R, len: usize,
    ) -> Result<(), R::Error>
    where
        R: Reader + ?Sized,
    {
        let target = reader
            .current_offset()
            .checked_add_usize(len)
            .expect("reader offsets were validated before slot writes");
        reader.skip_until(target)
    }

    #[inline(always)]
    pub fn try_write_reader<R>(
        &mut self, reader: &mut R, filled_slot: &mut bool,
    ) -> Result<Option<Slot>, R::Error>
    where
        R: Reader + ?Sized,
    {
        debug_assert!(self.start() <= reader.current_offset().as_u64());

        let end = self.end();

        if end < self.end_allocated() {
            // trim off chunks we've already copied
            reader.skip_until(unsafe { VarInt::new_unchecked(end) })?;
        } else {
            // we've already filled this slot so skip the entire thing on the
            // reader
            reader.skip_until(unsafe {
                VarInt::new_unchecked(self.end_allocated())
            })?;
            return Ok(None);
        }

        ensure!(!reader.buffer_is_empty(), Ok(None));

        // read the current offset
        let start = reader.current_offset().as_u64();

        // make sure this slot owns this range of data
        ensure!(start < self.end_allocated(), Ok(None));

        // if the current offsets match just do a straight copy on to the end of
        // the buffer
        if start == end {
            self.write_reader_append(reader, filled_slot)?;
            self.invariants();
            return Ok(None);
        }

        // copy and split off the filled data into another slot
        let filled = self.write_reader_split(reader, filled_slot)?;

        self.invariants();
        filled.invariants();

        Ok(Some(filled))
    }

    #[inline(always)]
    fn write_reader_split<R>(
        &mut self, reader: &mut R, filled_slot: &mut bool,
    ) -> Result<Self, R::Error>
    where
        R: Reader + ?Sized,
    {
        if self.can_write_virtually(reader) {
            let reader_start = reader.current_offset().as_u64();
            let chunk_len = (self.end_allocated() - reader_start) as usize;
            let filled_len = chunk_len.min(reader.buffered_len());

            self.advance_virtual_reader(reader, filled_len)?;
            super::probe::write(reader_start, filled_len);
            *filled_slot |= chunk_len == filled_len;

            let filled = Self {
                start: reader_start,
                end: self.end,
                data: BytesMut::new(),
                virtual_len: Some(filled_len),
            };
            filled.invariants();

            self.end = reader_start;
            self.invariants();

            return Ok(filled);
        }

        self.ensure_real();

        let reader_start = reader.current_offset().as_u64();

        unsafe {
            assume!(reader_start > self.end());
        }
        let offset = reader_start - self.end();

        let chunk = self.data.spare_capacity_mut();

        unsafe {
            // SAFETY: the data buffer should have at least one byte of spare
            // capacity if we got to this point
            assume!(chunk.len() as u64 > offset);
        }

        let chunk = &mut chunk[offset as usize..];
        let mut chunk = bytes::buf::UninitSlice::uninit(chunk);
        let chunk_len = chunk.len();
        let mut chunk = chunk.track_write();
        reader.copy_into(&mut chunk)?;
        let filled_len = chunk.written_len();

        super::probe::write(reader_start, filled_len);

        let filled = unsafe {
            // SAFETY: we should not have written more than the spare capacity
            let offset = offset as usize;

            assume!(self.data.len() + offset <= self.data.capacity());
            let mut filled = self.data.split_off(self.data.len() + offset);

            assume!(filled.is_empty());
            assume!(filled_len <= filled.capacity() - filled.len());
            filled.advance_mut(filled_len);
            filled
        };
        *filled_slot |= chunk_len == filled_len;

        let filled = Self {
            start: reader_start,
            end: self.end,
            data: filled,
            virtual_len: None,
        };
        filled.invariants();

        self.end = reader_start;
        self.invariants();

        Ok(filled)
    }

    #[inline(always)]
    fn write_reader_append<R>(
        &mut self, reader: &mut R, filled_slot: &mut bool,
    ) -> Result<(), R::Error>
    where
        R: Reader + ?Sized,
    {
        debug_assert_eq!(reader.current_offset().as_u64(), self.end());

        if self.can_write_virtually(reader) {
            let chunk_len = (self.end_allocated() - self.end()) as usize;
            let len = chunk_len.min(reader.buffered_len());

            self.advance_virtual_reader(reader, len)?;
            super::probe::write(self.end(), len);

            if let Some(virtual_len) = self.virtual_len.as_mut() {
                *virtual_len += len;
            }
            *filled_slot |= chunk_len == len;
            return Ok(());
        }

        self.ensure_real();

        unsafe {
            // SAFETY: the data buffer should have at least one byte of spare
            // capacity if we got to this point
            assume!(self.data.capacity() > self.data.len());
        }
        let chunk = self.data.spare_capacity_mut();
        let mut chunk = bytes::buf::UninitSlice::uninit(chunk);
        let chunk_len = chunk.len();
        let mut chunk = chunk.track_write();
        reader.copy_into(&mut chunk)?;
        let len = chunk.written_len();

        super::probe::write(self.end(), len);

        unsafe {
            // SAFETY: we should not have written more than the spare capacity
            assume!(self.data.len() + len <= self.data.capacity());
            self.data.advance_mut(len);
        }
        *filled_slot |= chunk_len == len;

        Ok(())
    }

    #[inline]
    pub fn unsplit(&mut self, next: Self) {
        if let (Some(current_len), Some(next_len)) =
            (self.virtual_len.as_mut(), next.virtual_len)
        {
            *current_len += next_len;
            self.end = next.end;
            self.invariants();
            return;
        }

        unsafe {
            assume!(self.end() == self.end_allocated());
            assume!(self.end() == next.start());
            assume!(!self.data.is_empty());
            assume!(self.data.capacity() > 0);
            assume!(!next.data.is_empty());
            assume!(next.data.capacity() > 0);
            assume!(
                self.data.as_ptr().add(self.data.len()) == next.data.as_ptr()
            );
        }
        self.data.unsplit(next.data);
        self.end = next.end;

        self.invariants();
    }

    #[inline]
    pub fn can_unsplit(&self, next: &Self) -> bool {
        if self.virtual_len.is_some() || next.virtual_len.is_some() {
            return self.virtual_len.is_some() && next.virtual_len.is_some();
        }

        unsafe { self.data.as_ptr().add(self.data.len()) == next.data.as_ptr() }
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.end() == self.end_allocated()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.buffered_len() == 0
    }

    #[inline(always)]
    pub fn is_occupied(&self, prev_offset: u64) -> bool {
        !self.is_empty() && self.start() == prev_offset
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        if let Some(len) = self.virtual_len {
            return &ZERO_CHUNK[..len];
        }

        &self.data
    }

    #[inline(always)]
    pub fn start(&self) -> u64 {
        self.start
    }

    #[inline(always)]
    pub fn end(&self) -> u64 {
        self.start + self.buffered_len() as u64
    }

    #[inline(always)]
    pub fn end_allocated(&self) -> u64 {
        self.end
    }

    #[inline(always)]
    pub fn consume(&mut self) -> BytesMut {
        if let Some(len) = self.virtual_len {
            let mut data = BytesMut::with_capacity(len);
            data.resize(len, 0);
            self.virtual_len = Some(0);
            self.start = self.end;
            return data;
        }

        let data = core::mem::replace(&mut self.data, BytesMut::new());
        self.start = self.end;
        data
    }

    #[inline(always)]
    pub fn consume_len(&mut self, watermark: usize) -> usize {
        let len = self.buffered_len().min(watermark);
        self.skip(len as u64);
        len
    }

    #[inline(always)]
    pub fn consume_all_buffered(&mut self) -> usize {
        let len = self.buffered_len();
        if self.virtual_len.is_some() {
            self.virtual_len = Some(0);
        } else {
            self.data = BytesMut::new();
        }
        self.start = self.end;
        self.invariants();
        len
    }

    #[inline]
    pub fn skip(&mut self, len: u64) {
        if let Some(buffered_len) = self.virtual_len.as_mut() {
            *buffered_len = buffered_len.saturating_sub(len as usize);
            self.start += len;
            self.invariants();
            return;
        }

        // trim off the data buffer
        unsafe {
            debug_assert!(len <= 1 << 16, "slot length should never exceed 2^16");
            let len = len as usize;

            // extend the write cursor if the length extends beyond the
            // initialized offset
            if let Some(to_advance) = len.checked_sub(self.data.len()) {
                assume!(to_advance <= self.data.remaining_mut());
                self.data.advance_mut(to_advance);
            }

            // consume `len` bytes
            let to_advance = self.data.remaining().min(len);
            self.data.advance(to_advance);
        }

        // advance the start position
        self.start += len;

        self.invariants();
    }

    /// Indicates the slot isn't capable of storing any more data and should be
    /// dropped
    #[inline(always)]
    pub fn should_drop(&self) -> bool {
        self.start() == self.end_allocated()
    }

    #[inline(always)]
    fn invariants(&self) {
        if cfg!(debug_assertions) {
            assert!(self.capacity() <= 1 << 16, "{self:?}");
            assert!(self.start() <= self.end(), "{self:?}");
            assert!(self.start() <= self.end_allocated(), "{self:?}");
            assert!(self.end() <= self.end_allocated(), "{self:?}");

            assert_eq!(
                self.capacity() as u64,
                self.end_allocated() - self.start(),
                "{self:?}"
            );

            assert_eq!(
                self.buffered_len() as u64,
                self.end() - self.start(),
                "{self:?}"
            );
        }
    }
}

impl reader::Storage for Slot {
    type Error = core::convert::Infallible;

    #[inline]
    fn buffered_len(&self) -> usize {
        self.buffered_len()
    }

    #[inline]
    fn buffer_is_empty(&self) -> bool {
        self.is_empty()
    }

    #[inline]
    fn read_chunk(
        &mut self, watermark: usize,
    ) -> Result<reader::storage::Chunk<'_>, Self::Error> {
        if let Some(buffered_len) = self.virtual_len.as_mut() {
            let len = (*buffered_len).min(watermark);
            if len == 0 {
                return Ok(reader::storage::Chunk::empty());
            }

            *buffered_len -= len;
            self.start += len as u64;
            return Ok((&ZERO_CHUNK[..len]).into());
        }

        let chunk = self.data.read_chunk(watermark)?;
        self.start += chunk.len() as u64;
        Ok(chunk)
    }

    #[inline]
    fn partial_copy_into<Dest>(
        &mut self, dest: &mut Dest,
    ) -> Result<reader::storage::Chunk<'_>, Self::Error>
    where
        Dest: crate::buffer::writer::Storage + ?Sized,
    {
        if let Some(buffered_len) = self.virtual_len.as_mut() {
            let len = (*buffered_len).min(dest.remaining_capacity());
            if len == 0 {
                return Ok(reader::storage::Chunk::empty());
            }

            *buffered_len -= len;
            self.start += len as u64;
            return Ok((&ZERO_CHUNK[..len]).into());
        }

        let mut dest = dest.track_write();
        let chunk = self.data.partial_copy_into(&mut dest)?;
        self.start += dest.written_len() as u64;
        self.start += chunk.len() as u64;
        Ok(chunk)
    }

    #[inline]
    fn copy_into<Dest>(&mut self, dest: &mut Dest) -> Result<(), Self::Error>
    where
        Dest: crate::buffer::writer::Storage + ?Sized,
    {
        if let Some(buffered_len) = self.virtual_len.as_mut() {
            let len = (*buffered_len).min(dest.remaining_capacity());
            if len == 0 {
                return Ok(());
            }

            dest.put_slice(&ZERO_CHUNK[..len]);
            *buffered_len -= len;
            self.start += len as u64;
            self.invariants();
            return Ok(());
        }

        let mut dest = dest.track_write();
        self.data.copy_into(&mut dest)?;
        self.start += dest.written_len() as u64;
        self.invariants();
        Ok(())
    }
}

impl Reader for Slot {
    #[inline]
    fn current_offset(&self) -> VarInt {
        unsafe { VarInt::new_unchecked(self.start) }
    }

    #[inline]
    fn final_offset(&self) -> Option<VarInt> {
        Some(unsafe { VarInt::new_unchecked(self.end) })
    }

    #[inline]
    fn skip_until(&mut self, offset: VarInt) -> Result<(), Self::Error> {
        if let Some(len) =
            offset.as_u64().checked_sub(self.current_offset().as_u64())
        {
            self.skip(len);
        }

        Ok(())
    }
}
