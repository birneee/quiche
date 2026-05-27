// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::buffer::reader::storage::Chunk;
use crate::buffer::writer::Storage;
use bytes::buf::UninitSlice;
use bytes::Bytes;
use bytes::BytesMut;

/// Only allows a single write into the storage. After that, no more writes are
/// allowed.
///
/// This can be used for very low latency scenarios where processing the single
/// read is more important than filling the entire storage with as much data as
/// possible.
pub struct WriteOnce<'a, S: Storage + ?Sized> {
    storage: &'a mut S,
    did_write: bool,
}

impl<'a, S: Storage + ?Sized> WriteOnce<'a, S> {
    #[inline]
    pub fn new(storage: &'a mut S) -> Self {
        Self {
            storage,
            did_write: false,
        }
    }
}

impl<S: Storage + ?Sized> Storage for WriteOnce<'_, S> {
    const SPECIALIZES_BYTES: bool = S::SPECIALIZES_BYTES;
    const SPECIALIZES_BYTES_MUT: bool = S::SPECIALIZES_BYTES_MUT;

    #[inline]
    fn put_slice(&mut self, bytes: &[u8]) {
        let did_write = !bytes.is_empty();
        self.storage.put_slice(bytes);
        self.did_write |= did_write;
    }

    #[inline(always)]
    fn put_uninit_slice<F, Error>(
        &mut self, payload_len: usize, f: F,
    ) -> Result<bool, Error>
    where
        F: FnOnce(&mut UninitSlice) -> Result<(), Error>,
    {
        let did_write = self.storage.put_uninit_slice(payload_len, f)?;
        self.did_write |= did_write && payload_len > 0;
        Ok(did_write)
    }

    #[inline]
    fn remaining_capacity(&self) -> usize {
        ensure!(!self.did_write, 0);
        self.storage.remaining_capacity()
    }

    #[inline]
    fn has_remaining_capacity(&self) -> bool {
        ensure!(!self.did_write, false);
        self.storage.has_remaining_capacity()
    }

    #[inline]
    fn put_bytes(&mut self, bytes: Bytes) {
        let did_write = !bytes.is_empty();
        self.storage.put_bytes(bytes);
        self.did_write |= did_write;
    }

    #[inline]
    fn put_bytes_mut(&mut self, bytes: BytesMut) {
        let did_write = !bytes.is_empty();
        self.storage.put_bytes_mut(bytes);
        self.did_write |= did_write;
    }

    #[inline]
    fn put_chunk(&mut self, chunk: Chunk) {
        let did_write = !chunk.is_empty();
        self.storage.put_chunk(chunk);
        self.did_write |= did_write;
    }
}
