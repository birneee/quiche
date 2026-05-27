// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::buffer::reader::storage::Chunk;
use crate::buffer::reader::Reader;
use crate::buffer::reader::Storage;
use crate::buffer::writer;
use crate::buffer::Error;
use crate::varint::VarInt;

/// Wraps a single [`Storage`] instance as a [`Reader`].
///
/// This can be used for scenarios where the entire stream is buffered and known
/// up-front.
#[derive(Debug)]
pub struct Complete<'a, S> {
    storage: &'a mut S,
    current_offset: VarInt,
    final_offset: VarInt,
}

impl<'a, S> Complete<'a, S>
where
    S: Storage,
{
    #[inline]
    pub fn new(storage: &'a mut S) -> Result<Self, Error> {
        let final_offset = VarInt::try_from(storage.buffered_len())
            .ok()
            .ok_or(Error::OutOfRange)?;
        Ok(Self {
            storage,
            current_offset: VarInt::ZERO,
            final_offset,
        })
    }
}

impl<S> Storage for Complete<'_, S>
where
    S: Storage,
{
    type Error = S::Error;

    #[inline]
    fn buffered_len(&self) -> usize {
        self.storage.buffered_len()
    }

    #[inline]
    fn buffer_is_empty(&self) -> bool {
        self.storage.buffer_is_empty()
    }

    #[inline]
    fn read_chunk(&mut self, watermark: usize) -> Result<Chunk<'_>, Self::Error> {
        let chunk = self.storage.read_chunk(watermark)?;
        self.current_offset += chunk.len();
        Ok(chunk)
    }

    #[inline]
    fn partial_copy_into<Dest>(
        &mut self, dest: &mut Dest,
    ) -> Result<Chunk<'_>, Self::Error>
    where
        Dest: writer::Storage + ?Sized,
    {
        let mut dest = dest.track_write();
        let chunk = self.storage.partial_copy_into(&mut dest)?;
        self.current_offset += chunk.len();
        self.current_offset += dest.written_len();
        Ok(chunk)
    }

    #[inline]
    fn copy_into<Dest>(&mut self, dest: &mut Dest) -> Result<(), Self::Error>
    where
        Dest: writer::Storage + ?Sized,
    {
        let mut dest = dest.track_write();
        self.storage.copy_into(&mut dest)?;
        self.current_offset += dest.written_len();
        Ok(())
    }
}

impl<C> Reader for Complete<'_, C>
where
    C: Storage,
{
    #[inline]
    fn current_offset(&self) -> VarInt {
        self.current_offset
    }

    #[inline]
    fn final_offset(&self) -> Option<VarInt> {
        Some(self.final_offset)
    }

    #[inline]
    fn is_virtually_buffered(&self) -> bool {
        false
    }
}
