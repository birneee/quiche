// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Stream reassembly primitives vendored from s2n-quic.

extern crate alloc;

#[macro_use]
#[doc(hidden)]
pub mod macros;

pub mod buffer;
pub mod varint;

pub use buffer::Error;
pub use buffer::Reassembler;
pub use varint::VarInt;
