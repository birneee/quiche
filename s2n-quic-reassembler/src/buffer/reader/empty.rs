// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::buffer::reader::storage::Chunk;
use crate::buffer::reader::Reader;
use crate::buffer::reader::Storage;
use crate::buffer::writer;
use crate::varint::VarInt;

/// Returns an empty buffer for the current offset of an inner reader
#[derive(Debug)]
pub struct Empty<'a, R: Reader + ?Sized>(&'a R);

impl<'a, R: Reader + ?Sized> Empty<'a, R> {
    #[inline]
    pub fn new(reader: &'a R) -> Self {
        Self(reader)
    }
}

impl<R: Reader + ?Sized> Storage for Empty<'_, R> {
    type Error = core::convert::Infallible;

    #[inline(always)]
    fn buffered_len(&self) -> usize {
        0
    }

    #[inline(always)]
    fn read_chunk(
        &mut self, _watermark: usize,
    ) -> Result<Chunk<'_>, Self::Error> {
        Ok(Chunk::empty())
    }

    #[inline(always)]
    fn partial_copy_into<Dest>(
        &mut self, _dest: &mut Dest,
    ) -> Result<Chunk<'_>, Self::Error>
    where
        Dest: writer::Storage + ?Sized,
    {
        Ok(Chunk::empty())
    }
}

impl<R: Reader + ?Sized> Reader for Empty<'_, R> {
    #[inline]
    fn current_offset(&self) -> VarInt {
        self.0.current_offset()
    }

    #[inline]
    fn final_offset(&self) -> Option<VarInt> {
        self.0.final_offset()
    }

    #[inline]
    fn is_virtually_buffered(&self) -> bool {
        self.0.is_virtually_buffered()
    }
}
