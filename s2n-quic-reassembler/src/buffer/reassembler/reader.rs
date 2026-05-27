// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use super::Reassembler;
use crate::buffer::reader::storage::Chunk;
use crate::buffer::reader::storage::Infallible;
use crate::buffer::reader::Reader;
use crate::buffer::reader::Storage;
use crate::buffer::writer;
use crate::varint::VarInt;
use bytes::BytesMut;

impl Storage for Reassembler {
    type Error = core::convert::Infallible;

    #[inline]
    fn buffered_len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn buffer_is_empty(&self) -> bool {
        self.is_empty()
    }

    #[inline]
    fn read_chunk(&mut self, watermark: usize) -> Result<Chunk<'_>, Self::Error> {
        let (chunk, should_drop) = {
            let Some(slot) = self.slots.front_mut() else {
                return Ok(BytesMut::new().into());
            };

            // make sure the slot has some data
            ensure!(
                slot.is_occupied(self.cursors.start_offset),
                Ok(BytesMut::new().into())
            );

            // if we have a final size and this slot overlaps it then return the
            // entire thing
            let chunk = if self.cursors.final_size().is_some_and(|final_size| {
                final_size <= slot.end_allocated() &&
                    watermark >= slot.buffered_len()
            }) {
                slot.consume()
            } else {
                match slot.read_chunk(watermark)? {
                    Chunk::BytesMut(chunk) => chunk,
                    Chunk::Bytes(chunk) => BytesMut::from(&chunk[..]),
                    Chunk::Slice(chunk) => BytesMut::from(chunk),
                }
            };

            (chunk, slot.should_drop())
        };

        if should_drop {
            self.slots.pop_front();
        }

        super::probe::pop(self.cursors.start_offset, chunk.len());

        self.cursors.start_offset += chunk.len() as u64;

        self.invariants();

        Ok(chunk.into())
    }

    #[inline]
    fn partial_copy_into<Dest>(
        &mut self, dest: &mut Dest,
    ) -> Result<Chunk<'_>, Self::Error>
    where
        Dest: writer::Storage + ?Sized,
    {
        // ensure we have enough capacity in the destination buf
        ensure!(dest.has_remaining_capacity(), Ok(Default::default()));

        let mut prev = BytesMut::new();

        loop {
            let remaining = dest.remaining_capacity();
            unsafe {
                assume!(prev.len() <= remaining);
            }
            let watermark = remaining - prev.len();

            debug_assert!(remaining > 0);

            let Chunk::BytesMut(chunk) = self.infallible_read_chunk(watermark)
            else {
                unsafe { assume!(false) }
            };

            // if the chunk is empty then return the previous value
            ensure!(!chunk.is_empty(), Ok(prev.into()));

            // flush the previous chunk if needed
            if !prev.is_empty() {
                dest.put_bytes_mut(prev);
            }

            // if the chunk is exactly the same size as the watermark, then return
            // it
            if chunk.len() == watermark {
                return Ok(chunk.into());
            }

            // store the chunk for another iteration, in case we can pull more
            prev = chunk;
        }
    }

    #[inline]
    fn copy_into<Dest>(&mut self, dest: &mut Dest) -> Result<(), Self::Error>
    where
        Dest: writer::Storage + ?Sized,
    {
        // if the destination wants bytes then use the partial copy logic instead
        if Dest::SPECIALIZES_BYTES || Dest::SPECIALIZES_BYTES_MUT {
            let mut chunk = self.infallible_partial_copy_into(dest);
            chunk.infallible_copy_into(dest);
            return Ok(());
        }

        loop {
            // ensure we have enough capacity in the destination buf
            ensure!(dest.has_remaining_capacity(), Ok(()));

            let Some(slot) = self.slots.front_mut() else {
                return Ok(());
            };

            // make sure the slot has some data
            ensure!(slot.is_occupied(self.cursors.start_offset), Ok(()));

            // avoid refcounting if the destination wants slices
            let mut dest = dest.track_write();
            slot.infallible_copy_into(&mut dest);

            if slot.should_drop() {
                // remove empty buffers
                self.slots.pop_front();
            }

            super::probe::pop(self.cursors.start_offset, dest.written_len());

            self.cursors.start_offset += dest.written_len() as u64;

            self.invariants();
        }
    }
}

impl Reader for Reassembler {
    #[inline]
    fn current_offset(&self) -> VarInt {
        unsafe {
            // SAFETY: offset will always fit into a VarInt
            VarInt::new_unchecked(self.cursors.start_offset)
        }
    }

    #[inline]
    fn final_offset(&self) -> Option<VarInt> {
        self.final_size().map(|v| unsafe {
            // SAFETY: offset will always fit into a VarInt
            VarInt::new_unchecked(v)
        })
    }
}
