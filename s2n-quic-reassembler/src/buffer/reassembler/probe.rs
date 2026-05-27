// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/// Records a reassembler allocation.
#[inline]
pub fn alloc(_offset: u64, _capacity: usize) {}

/// Records a chunk consumed from the front of the buffer.
#[inline]
pub fn pop(_offset: u64, _len: usize) {}

/// Records a chunk written at an offset.
#[inline]
pub fn write(_offset: u64, _len: usize) {}
