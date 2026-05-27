// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::buffer::duplex;
use crate::buffer::reader::Reader;
use crate::buffer::reader::Storage as _;
use crate::buffer::reader::{
    self,
};
use crate::buffer::writer::Writer;
use crate::buffer::writer::{
    self,
};
use crate::buffer::Error;
use crate::varint::VarInt;
use core::convert::Infallible;

/// A wrapper around an underlying buffer (`duplex`) which will prefer to
/// read/write from a user-provided temporary buffer (`storage`). The underlying
/// buffer (`duplex`)'s current position and total length are updated if needed.
pub struct Interposer<'a, S, D>
where
    S: writer::Storage + ?Sized,
    D: duplex::Skip<Error = Infallible> + ?Sized,
{
    storage: &'a mut S,
    duplex: &'a mut D,
}

impl<'a, S, D> Interposer<'a, S, D>
where
    S: writer::Storage + ?Sized,
    D: duplex::Skip<Error = Infallible> + ?Sized,
{
    #[inline]
    pub fn new(storage: &'a mut S, duplex: &'a mut D) -> Self {
        debug_assert!(
            !storage.has_remaining_capacity() || duplex.buffer_is_empty(),
            "`duplex` (len={}) should be drained into `storage` (cap={}) before constructing an Interposer",
            duplex.buffered_len(),
            storage.remaining_capacity()
        );

        Self { storage, duplex }
    }
}

/// Delegates to the inner Duplex
impl<S, D> reader::Storage for Interposer<'_, S, D>
where
    S: writer::Storage + ?Sized,
    D: duplex::Skip<Error = Infallible> + ?Sized,
{
    type Error = D::Error;

    #[inline]
    fn buffered_len(&self) -> usize {
        self.duplex.buffered_len()
    }

    #[inline]
    fn buffer_is_empty(&self) -> bool {
        self.duplex.buffer_is_empty()
    }

    #[inline]
    fn read_chunk(
        &mut self, watermark: usize,
    ) -> Result<reader::storage::Chunk<'_>, Self::Error> {
        self.duplex.read_chunk(watermark)
    }

    #[inline]
    fn partial_copy_into<Dest>(
        &mut self, dest: &mut Dest,
    ) -> Result<reader::storage::Chunk<'_>, Self::Error>
    where
        Dest: writer::Storage + ?Sized,
    {
        self.duplex.partial_copy_into(dest)
    }

    #[inline]
    fn copy_into<Dest>(&mut self, dest: &mut Dest) -> Result<(), Self::Error>
    where
        Dest: writer::Storage + ?Sized,
    {
        self.duplex.copy_into(dest)
    }
}

/// Delegates to the inner Duplex
impl<C, D> Reader for Interposer<'_, C, D>
where
    C: writer::Storage + ?Sized,
    D: duplex::Skip<Error = Infallible> + ?Sized,
{
    #[inline]
    fn current_offset(&self) -> VarInt {
        self.duplex.current_offset()
    }

    #[inline]
    fn final_offset(&self) -> Option<VarInt> {
        self.duplex.final_offset()
    }

    #[inline]
    fn has_buffered_fin(&self) -> bool {
        self.duplex.has_buffered_fin()
    }

    #[inline]
    fn is_consumed(&self) -> bool {
        self.duplex.is_consumed()
    }
}

impl<C, D> Writer for Interposer<'_, C, D>
where
    C: writer::Storage + ?Sized,
    D: duplex::Skip<Error = Infallible> + ?Sized,
{
    #[inline]
    fn read_from<R>(&mut self, reader: &mut R) -> Result<(), Error<R::Error>>
    where
        R: Reader + ?Sized,
    {
        let final_offset = reader.final_offset();

        {
            // if the storage specializes writing zero-copy Bytes/BytesMut, then
            // just write to the receive buffer, since that's what it
            // stores
            let mut should_delegate =
                C::SPECIALIZES_BYTES || C::SPECIALIZES_BYTES_MUT;

            // if the storage has no space left then write into the duplex
            should_delegate |= !self.storage.has_remaining_capacity();

            // if this packet is non-contiguous, then delegate to the wrapped
            // writer
            should_delegate |=
                reader.current_offset() != self.duplex.current_offset();

            // We want to decrypt into our own buffer (`duplex`) if that's
            // cheaper. Note that decrypt is itself an optional "free"
            // copy. Our choices are:
            //
            // 1. Decrypt in-place, copy head to `storage` and tail to `duplex`
            // 2. Decrypt to `duplex`, copy head to `storage`
            // 3. Decrypt to `storage`
            //
            // 1 and 2 are always possible (duplex can take any amount of data).
            // 1 is always more expensive than 2 in `# of bytes copied` but does
            // potentially save allocating large buffer(s) in
            // `duplex`, if most of the bytes go into storage.
            //
            // 3 is optimal and should essentially always be preferred. Maybe that
            // should even override SPECIALIZES_BYTES/_MUT?
            should_delegate |=
                self.storage.remaining_capacity() < (reader.buffered_len() / 2);

            if should_delegate {
                self.duplex.read_from(reader)?;

                // don't copy into `storage` here - let the caller do that later
                // since it can be more efficient to pull from
                // `duplex` all in one go.

                return Ok(());
            }
        }

        debug_assert!(
            self.storage.has_remaining_capacity(),
            "this code should only be executed if the storage has capacity"
        );

        {
            // track the number of consumed bytes
            let mut reader = reader.track_read();

            reader.copy_into(self.storage)?;

            let write_len = reader.consumed_len();
            let write_len =
                VarInt::try_from(write_len).map_err(|_| Error::OutOfRange)?;

            // notify the duplex that we bypassed it and should skip
            self.duplex
                .skip(write_len, final_offset)
                .map_err(Error::mapped)?;
        }

        // if we still have some remaining bytes consume the rest in the duplex
        if !reader.buffer_is_empty() {
            self.duplex.read_from(reader)?;
        }

        Ok(())
    }
}
