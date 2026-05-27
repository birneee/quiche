// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use super::ZERO_CHUNK;
use crate::buffer::reader;
use crate::buffer::writer;
use crate::buffer::Error;
use crate::buffer::Reader;
use crate::varint::VarInt;
use core::fmt;

#[derive(PartialEq, Eq)]
pub struct Request<'a> {
    offset: u64,
    data: &'a [u8],
    is_fin: bool,
}

impl fmt::Debug for Request<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Request")
            .field("offset", &self.offset)
            .field("len", &self.data.len())
            .field("is_fin", &self.is_fin)
            .finish()
    }
}

impl<'a> Request<'a> {
    #[inline]
    pub fn new(
        offset: VarInt, data: &'a [u8], is_fin: bool,
    ) -> Result<Self, Error> {
        offset
            .checked_add_usize(data.len())
            .ok_or(Error::OutOfRange)?;
        Ok(Self {
            offset: offset.as_u64(),
            data,
            is_fin,
        })
    }
}

// Added for benchmarking.
#[derive(PartialEq, Eq)]
pub struct RequestLen {
    offset: u64,
    len: usize,
    is_fin: bool,
}

impl fmt::Debug for RequestLen {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RequestLen")
            .field("offset", &self.offset)
            .field("len", &self.len)
            .field("is_fin", &self.is_fin)
            .finish()
    }
}

impl RequestLen {
    #[inline]
    pub fn new(offset: VarInt, len: usize, is_fin: bool) -> Result<Self, Error> {
        offset.checked_add_usize(len).ok_or(Error::OutOfRange)?;
        Ok(Self {
            offset: offset.as_u64(),
            len,
            is_fin,
        })
    }

    #[inline]
    fn advance(&mut self, len: usize) {
        self.offset += len as u64;
        self.len -= len;
    }

    #[inline]
    fn put_zeroes<Dest>(dest: &mut Dest, len: usize)
    where
        Dest: writer::Storage + ?Sized,
    {
        let mut remaining = len;

        while remaining > 0 {
            let chunk_len = remaining.min(ZERO_CHUNK.len());
            dest.put_slice(&ZERO_CHUNK[..chunk_len]);
            remaining -= chunk_len;
        }
    }
}

impl Reader for Request<'_> {
    #[inline]
    fn current_offset(&self) -> VarInt {
        unsafe { VarInt::new_unchecked(self.offset) }
    }

    #[inline]
    fn final_offset(&self) -> Option<VarInt> {
        if self.is_fin {
            Some(self.current_offset() + self.data.len())
        } else {
            None
        }
    }
}

impl Reader for RequestLen {
    #[inline]
    fn current_offset(&self) -> VarInt {
        unsafe { VarInt::new_unchecked(self.offset) }
    }

    #[inline]
    fn final_offset(&self) -> Option<VarInt> {
        if self.is_fin {
            Some(self.current_offset() + self.len)
        } else {
            None
        }
    }

    #[inline]
    fn is_virtually_buffered(&self) -> bool {
        true
    }
}

impl reader::Storage for Request<'_> {
    type Error = core::convert::Infallible;

    #[inline]
    fn buffered_len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    fn read_chunk(
        &mut self, watermark: usize,
    ) -> Result<reader::storage::Chunk<'_>, Self::Error> {
        let chunk = self.data.read_chunk(watermark)?;
        self.offset += chunk.len() as u64;
        Ok(chunk)
    }

    #[inline]
    fn partial_copy_into<Dest>(
        &mut self, dest: &mut Dest,
    ) -> Result<reader::storage::Chunk<'_>, Self::Error>
    where
        Dest: writer::Storage + ?Sized,
    {
        let mut dest = dest.track_write();
        let chunk = self.data.partial_copy_into(&mut dest)?;
        self.offset += chunk.len() as u64;
        self.offset += dest.written_len() as u64;
        Ok(chunk)
    }

    #[inline]
    fn copy_into<Dest>(&mut self, dest: &mut Dest) -> Result<(), Self::Error>
    where
        Dest: writer::Storage + ?Sized,
    {
        let mut dest = dest.track_write();
        self.data.copy_into(&mut dest)?;
        self.offset += dest.written_len() as u64;
        Ok(())
    }
}

impl reader::Storage for RequestLen {
    type Error = core::convert::Infallible;

    #[inline]
    fn buffered_len(&self) -> usize {
        self.len
    }

    #[inline]
    fn read_chunk(
        &mut self, watermark: usize,
    ) -> Result<reader::storage::Chunk<'_>, Self::Error> {
        if self.len == 0 {
            return Ok(reader::storage::Chunk::empty());
        }

        let chunk_len = self.len.min(ZERO_CHUNK.len()).min(watermark);
        if chunk_len == 0 {
            return Ok(reader::storage::Chunk::empty());
        }

        self.advance(chunk_len);
        Ok((&ZERO_CHUNK[..chunk_len]).into())
    }

    #[inline]
    fn partial_copy_into<Dest>(
        &mut self, dest: &mut Dest,
    ) -> Result<reader::storage::Chunk<'_>, Self::Error>
    where
        Dest: writer::Storage + ?Sized,
    {
        let total_len = self.len.min(dest.remaining_capacity());
        if total_len == 0 {
            return Ok(reader::storage::Chunk::empty());
        }

        let chunk_len = total_len.min(ZERO_CHUNK.len());
        let direct_len = total_len - chunk_len;

        if direct_len > 0 {
            Self::put_zeroes(dest, direct_len);
            self.advance(direct_len);
        }

        self.advance(chunk_len);
        Ok((&ZERO_CHUNK[..chunk_len]).into())
    }

    #[inline]
    fn copy_into<Dest>(&mut self, dest: &mut Dest) -> Result<(), Self::Error>
    where
        Dest: writer::Storage + ?Sized,
    {
        let len = self.len.min(dest.remaining_capacity());
        if len == 0 {
            return Ok(());
        }

        Self::put_zeroes(dest, len);
        self.advance(len);
        Ok(())
    }
}
