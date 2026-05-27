// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use core::fmt;
use core::ops::Add;
use core::ops::AddAssign;
use core::ops::Sub;

/// The largest value representable as a QUIC variable-length integer.
pub const MAX_VARINT_VALUE: u64 = 4_611_686_018_427_387_903;

/// Error returned when a value cannot fit in a QUIC variable-length integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VarIntError;

impl fmt::Display for VarIntError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "varint range exceeded")
    }
}

impl std::error::Error for VarIntError {}

/// A QUIC variable-length integer.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct VarInt(u64);

impl VarInt {
    /// The maximum valid QUIC variable-length integer.
    pub const MAX: Self = Self(MAX_VARINT_VALUE);
    /// The zero value.
    pub const ZERO: Self = Self(0);

    /// Creates a new [`VarInt`].
    #[inline(always)]
    pub fn new(v: u64) -> Result<Self, VarIntError> {
        if v > MAX_VARINT_VALUE {
            return Err(VarIntError);
        }

        Ok(Self(v))
    }

    /// Returns a [`VarInt`] without validating the input.
    ///
    /// # Safety
    ///
    /// Callers must ensure `value` is less than or equal to [`VarInt::MAX`].
    #[inline(always)]
    pub const unsafe fn new_unchecked(value: u64) -> Self {
        Self(value)
    }

    /// Creates a value from a `u8`.
    #[inline(always)]
    pub const fn from_u8(v: u8) -> Self {
        Self(v as u64)
    }

    /// Creates a value from a `u16`.
    #[inline(always)]
    pub const fn from_u16(v: u16) -> Self {
        Self(v as u64)
    }

    /// Creates a value from a `u32`.
    #[inline(always)]
    pub const fn from_u32(v: u32) -> Self {
        Self(v as u64)
    }

    /// Returns the integer value.
    #[inline(always)]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Adds two values, returning `None` on overflow.
    #[inline]
    pub fn checked_add(self, value: Self) -> Option<Self> {
        Self::new(self.0.checked_add(value.0)?).ok()
    }

    /// Adds a `usize`, returning `None` on overflow.
    #[inline]
    pub fn checked_add_usize(self, value: usize) -> Option<Self> {
        let value = value.try_into().ok()?;
        self.checked_add(value)
    }

    /// Saturating subtraction.
    #[inline]
    #[must_use]
    pub fn saturating_sub(self, value: Self) -> Self {
        Self(self.0.saturating_sub(value.0))
    }

    /// Checked subtraction.
    #[inline]
    pub fn checked_sub(self, value: Self) -> Option<Self> {
        Some(Self(self.0.checked_sub(value.0)?))
    }
}

impl fmt::Display for VarInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl TryFrom<usize> for VarInt {
    type Error = VarIntError;

    #[inline]
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Self::new(value as u64)
    }
}

impl From<u8> for VarInt {
    #[inline]
    fn from(value: u8) -> Self {
        Self::from_u8(value)
    }
}

impl From<u16> for VarInt {
    #[inline]
    fn from(value: u16) -> Self {
        Self::from_u16(value)
    }
}

impl From<u32> for VarInt {
    #[inline]
    fn from(value: u32) -> Self {
        Self::from_u32(value)
    }
}

impl TryFrom<u64> for VarInt {
    type Error = VarIntError;

    #[inline]
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<u128> for VarInt {
    type Error = VarIntError;

    #[inline]
    fn try_from(value: u128) -> Result<Self, Self::Error> {
        if value > MAX_VARINT_VALUE as u128 {
            return Err(VarIntError);
        }

        Ok(Self(value as u64))
    }
}

impl Add<VarInt> for VarInt {
    type Output = VarInt;

    #[inline]
    fn add(self, rhs: VarInt) -> Self::Output {
        self.checked_add(rhs).expect("VarInt overflow occurred")
    }
}

impl Add<usize> for VarInt {
    type Output = VarInt;

    #[inline]
    fn add(self, rhs: usize) -> Self::Output {
        self.checked_add_usize(rhs)
            .expect("VarInt overflow occurred")
    }
}

impl AddAssign<VarInt> for VarInt {
    #[inline]
    fn add_assign(&mut self, rhs: VarInt) {
        *self = self.checked_add(rhs).expect("VarInt overflow occurred");
    }
}

impl AddAssign<usize> for VarInt {
    #[inline]
    fn add_assign(&mut self, rhs: usize) {
        *self = self
            .checked_add_usize(rhs)
            .expect("VarInt overflow occurred");
    }
}

impl Sub<VarInt> for VarInt {
    type Output = VarInt;

    #[inline]
    fn sub(self, rhs: VarInt) -> Self::Output {
        self.checked_sub(rhs).expect("VarInt underflow occurred")
    }
}

impl PartialEq<u64> for VarInt {
    #[inline]
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

impl PartialEq<VarInt> for u64 {
    #[inline]
    fn eq(&self, other: &VarInt) -> bool {
        *self == other.0
    }
}

impl PartialOrd<u64> for VarInt {
    #[inline]
    fn partial_cmp(&self, other: &u64) -> Option<core::cmp::Ordering> {
        self.0.partial_cmp(other)
    }
}

impl PartialOrd<VarInt> for u64 {
    #[inline]
    fn partial_cmp(&self, other: &VarInt) -> Option<core::cmp::Ordering> {
        self.partial_cmp(&other.0)
    }
}
