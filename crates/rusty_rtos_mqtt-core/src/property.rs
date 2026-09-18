//! The MQTT 5 property primitives: bounded reads out of a packet.
//!
//! MQTT 5 adds properties to almost every packet, and every one of them is
//! decoded through the same handful of primitives — a one-, two- or four-byte
//! integer, a length-prefixed string, or a user property, which is two strings.
//!
//! They carry two rules between them, and both are protocol requirements:
//!
//! 1. **A property may appear once.** A repeat is a protocol error, not a
//!    last-one-wins. The caller tracks that per identifier and passes it in.
//! 2. **Every read is bounded by the property length**, which was itself
//!    decoded from the packet a moment earlier. That is the length-prefix
//!    attack surface: a string claiming more bytes than the property has left
//!    must be refused *before* the read, not after.
//!
//! # A failed read still moves the cursor
//!
//! This is the C's behaviour and it is reproduced exactly. [`PropertyReader::utf8`]
//! consumes its two length bytes and charges them to the budget *before* it
//! discovers the body does not fit, so a refusal leaves the cursor two bytes on
//! and the budget two smaller. A caller that retried from there would read
//! rubbish — which is why the only correct response to a refusal is to abandon
//! the packet, and why the differential compares the cursor and the budget
//! after every call rather than only the status.
//!
//! # Two variable-length decoders, not one
//!
//! [`decode_variable_length`] is the *property* length decoder.
//! [`process_incoming_packet_type_and_length`](crate::header) has its own, for
//! the fixed header, and they are not the same function: this one starts at
//! index 0 and is bounded by a buffer length, that one starts at index 1 and is
//! bounded by a count of bytes received. They also differ in how they treat a
//! value that is too large. Reusing either for the other would be wrong in a
//! way no single-function test would show, so both are transcribed.

use crate::header::{REMAINING_LENGTH_INVALID, variable_length_encoded_size};

/// Why a property read was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropertyError {
    /// `MQTTBadResponse`: the property repeats, or does not fit the budget.
    ///
    /// The C does not distinguish the two, and neither does this — both mean
    /// the packet is malformed and the only safe move is to drop it.
    BadResponse,
}

/// A cursor over a packet's property section, with a budget.
///
/// `remaining` is the property length decoded from the packet, and every read
/// is charged against it. It is deliberately **not** the same as the number of
/// bytes in the slice: the slice is what exists, the budget is what the packet
/// claimed, and a malformed packet is exactly one where they disagree.
#[derive(Debug)]
pub struct PropertyReader<'a> {
    bytes: &'a [u8],
    at: usize,
    remaining: u32,
}

impl<'a> PropertyReader<'a> {
    /// A reader over `bytes`, with `property_length` bytes claimed.
    #[must_use]
    pub const fn new(bytes: &'a [u8], property_length: u32) -> Self {
        Self {
            bytes,
            at: 0,
            remaining: property_length,
        }
    }

    /// How far into the buffer the cursor has moved.
    #[must_use]
    pub const fn position(&self) -> usize {
        self.at
    }

    /// How much of the property budget is left.
    #[must_use]
    pub const fn remaining(&self) -> u32 {
        self.remaining
    }

    /// Take `n` bytes from the cursor, if the SLICE has them.
    ///
    /// The budget check is the caller's, because it must happen first and its
    /// failure is a different thing from running off the end of the buffer.
    /// The C has no equivalent of this second check at all — it indexes — so
    /// this is where the transcription is strictly safer rather than equal.
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.at.checked_add(n)?;
        let slice = self.bytes.get(self.at..end)?;
        self.at = end;
        Some(slice)
    }

    /// `decodeUint8t`.
    ///
    /// # Errors
    ///
    /// [`PropertyError::BadResponse`] if `used` is already set, or the budget
    /// has fewer than one byte left.
    pub fn u8(&mut self, used: &mut bool) -> Result<u8, PropertyError> {
        if *used || self.remaining < 1 {
            return Err(PropertyError::BadResponse);
        }

        let Some(bytes) = self.take(1) else {
            return Err(PropertyError::BadResponse);
        };
        let Some(value) = bytes.first().copied() else {
            return Err(PropertyError::BadResponse);
        };

        self.remaining = self.remaining.saturating_sub(1);
        *used = true;
        Ok(value)
    }

    /// `decodeUint16t`.
    ///
    /// # Errors
    ///
    /// As [`u8`](Self::u8), with a two-byte budget.
    pub fn u16(&mut self, used: &mut bool) -> Result<u16, PropertyError> {
        if *used || self.remaining < 2 {
            return Err(PropertyError::BadResponse);
        }

        let Some(bytes) = self.take(2) else {
            return Err(PropertyError::BadResponse);
        };
        let Ok(array) = <[u8; 2]>::try_from(bytes) else {
            return Err(PropertyError::BadResponse);
        };

        self.remaining = self.remaining.saturating_sub(2);
        *used = true;
        Ok(u16::from_be_bytes(array))
    }

    /// `decodeUint32t`.
    ///
    /// # Errors
    ///
    /// As [`u8`](Self::u8), with a four-byte budget.
    pub fn u32(&mut self, used: &mut bool) -> Result<u32, PropertyError> {
        if *used || self.remaining < 4 {
            return Err(PropertyError::BadResponse);
        }

        let Some(bytes) = self.take(4) else {
            return Err(PropertyError::BadResponse);
        };
        let Ok(array) = <[u8; 4]>::try_from(bytes) else {
            return Err(PropertyError::BadResponse);
        };

        self.remaining = self.remaining.saturating_sub(4);
        *used = true;
        Ok(u32::from_be_bytes(array))
    }

    /// The property IDENTIFIER byte that introduces each property.
    ///
    /// The C does not call a helper for this — every property loop spells out
    /// `propertyId = *pLocalIndex; pLocalIndex++; propertyLength -= 1U;`, with
    /// the loop condition `propertyLength > 0` standing in for a budget check
    /// and nothing at all standing in for a buffer check. This charges the
    /// budget the same way and refuses rather than reading past the slice.
    ///
    /// It takes no `used` flag: an id may obviously repeat, since a property
    /// section is a sequence of them.
    ///
    /// # Errors
    ///
    /// [`PropertyError::BadResponse`] if the budget is empty or the byte is
    /// not there.
    pub fn property_id(&mut self) -> Result<u8, PropertyError> {
        if self.remaining < 1 {
            return Err(PropertyError::BadResponse);
        }

        let Some(bytes) = self.take(1) else {
            return Err(PropertyError::BadResponse);
        };
        let Some(value) = bytes.first().copied() else {
            return Err(PropertyError::BadResponse);
        };

        self.remaining = self.remaining.saturating_sub(1);
        Ok(value)
    }

    /// `decodeUtf8`: a two-byte big-endian length, then that many bytes.
    ///
    /// The bytes are handed back unvalidated, as the C does — MQTT calls these
    /// UTF-8 strings, and this primitive does not check that they are.
    ///
    /// # Errors
    ///
    /// [`PropertyError::BadResponse`] if `used` is set, if fewer than two bytes
    /// of budget remain, or if the length it reads exceeds what is left.
    ///
    /// **A refusal of the last kind still moves the cursor two bytes on** and
    /// charges them to the budget, because the C discovers the overrun after
    /// consuming the length. See the module note.
    pub fn utf8(&mut self, used: &mut bool) -> Result<&'a [u8], PropertyError> {
        if *used || self.remaining < 2 {
            return Err(PropertyError::BadResponse);
        }

        let Some(header) = self.take(2) else {
            return Err(PropertyError::BadResponse);
        };
        let Ok(array) = <[u8; 2]>::try_from(header) else {
            return Err(PropertyError::BadResponse);
        };

        let length = u16::from_be_bytes(array);

        // Charged before the body is known to fit -- this is the state the C
        // leaves behind on a refusal, and it is reproduced deliberately.
        self.remaining = self.remaining.saturating_sub(2);

        if self.remaining < u32::from(length) {
            return Err(PropertyError::BadResponse);
        }

        let Some(body) = self.take(usize::from(length)) else {
            return Err(PropertyError::BadResponse);
        };

        self.remaining = self.remaining.saturating_sub(u32::from(length));
        *used = true;
        Ok(body)
    }

    /// `decodeUserProp`: two strings, a key and a value.
    ///
    /// User properties are the one MQTT 5 property that MAY repeat, so this
    /// takes no `used` flag — the C passes a fresh `false` for each half.
    ///
    /// # Errors
    ///
    /// [`PropertyError::BadResponse`] if either string fails. A failure on the
    /// value leaves the key already consumed.
    pub fn user_property(&mut self) -> Result<(&'a [u8], &'a [u8]), PropertyError> {
        let mut used = false;
        let key = self.utf8(&mut used)?;

        used = false;
        let value = self.utf8(&mut used)?;

        Ok((key, value))
    }
}

/// `decodeVariableLength`: the PROPERTY length, from the start of a buffer.
///
/// Not the same as the fixed header's decoder — see the module note. This one
/// starts at index 0, is bounded by the buffer length it is given, and forces a
/// refusal when the accumulated value reaches
/// [`REMAINING_LENGTH_INVALID`](crate::header::REMAINING_LENGTH_INVALID)
/// rather than only when the multiplier overruns.
///
/// # Errors
///
/// [`PropertyError::BadResponse`] for a value that is too large, encoded in too
/// many bytes, encoded non-minimally, or truncated.
pub fn decode_variable_length(buffer: &[u8]) -> Result<u32, PropertyError> {
    let mut remaining_length: u32 = 0;
    let mut multiplier: u32 = 1;
    let mut bytes_decoded: usize = 0;

    loop {
        if multiplier > 2_097_152 {
            return Err(PropertyError::BadResponse);
        }

        let Some(byte) = buffer.get(bytes_decoded).copied() else {
            return Err(PropertyError::BadResponse);
        };

        remaining_length =
            remaining_length.saturating_add(u32::from(byte & 0x7F).saturating_mul(multiplier));
        multiplier = multiplier.saturating_mul(128);
        bytes_decoded = bytes_decoded.saturating_add(1);

        // This decoder refuses an out-of-range value inside the loop where the
        // fixed header's does not -- and the check is UNREACHABLE in both, for
        // the same reason: four bytes of seven bits reach exactly
        // 268,435,455, which is one less than the invalid value. The multiplier
        // guard above has already bounded it. Kept because the C keeps it, and
        // because the bound is a property of two constants that could move;
        // `the_in_loop_range_check_cannot_fire` below proves the arithmetic.
        if remaining_length >= REMAINING_LENGTH_INVALID {
            return Err(PropertyError::BadResponse);
        }

        if (byte & 0x80) == 0 {
            break;
        }
    }

    // The same non-minimal check the fixed header makes.
    if bytes_decoded != variable_length_encoded_size(remaining_length) as usize {
        return Err(PropertyError::BadResponse);
    }

    Ok(remaining_length)
}

/// `encodeString`: a two-byte big-endian length, then the bytes.
///
/// `source` may be `None`, which writes the length and **reserves** the body
/// without writing it — the C passes a NULL pointer to do that when it means to
/// copy a payload in separately. Returns how many bytes were written, or 0 if
/// the destination is too small.
#[must_use]
pub fn encode_string(destination: &mut [u8], source: Option<&[u8]>, length: u16) -> usize {
    let total = usize::from(length).saturating_add(2);

    if destination.len() < total {
        return 0;
    }

    let Some(header) = destination.get_mut(..2) else {
        return 0;
    };
    header.copy_from_slice(&length.to_be_bytes());

    if let Some(bytes) = source {
        let Some(body) = bytes.get(..usize::from(length)) else {
            return 0;
        };
        let Some(slot) = destination.get_mut(2..total) else {
            return 0;
        };
        slot.copy_from_slice(body);
    }

    total
}

#[cfg(test)]
// A test does its own arithmetic; the workspace's policy is for library code.
#[allow(clippy::arithmetic_side_effects)]
mod tests {
    use super::*;

    /// Why the in-loop range check in [`decode_variable_length`] never fires.
    ///
    /// This started as a poison that did not fire. The multiplier guard stops
    /// the loop after four bytes, and four bytes of seven bits reach exactly
    /// `268,435,455` -- one less than [`REMAINING_LENGTH_INVALID`]. So the
    /// accumulated value can never reach it.
    ///
    /// The check stays because the C has it and a differential arm does not
    /// tidy its oracle. It is pinned because the bound is a relationship
    /// between two constants, and a change to either would make it live again
    /// without anything failing.
    #[test]
    fn the_in_loop_range_check_cannot_fire() {
        let max: u64 = [1u64, 128, 16_384, 2_097_152].iter().map(|m| 127 * m).sum();

        assert_eq!(
            max, 268_435_455,
            "four bytes of seven bits no longer reach this"
        );
        assert_eq!(
            max + 1,
            u64::from(REMAINING_LENGTH_INVALID),
            "the largest four-byte value is no longer exactly one below the              invalid value, so the in-loop check may now be reachable and the              comment beside it is wrong"
        );

        // And the largest legal value really is accepted, so the bound is not
        // simply a decoder that refuses everything near the top.
        assert_eq!(
            decode_variable_length(&[0xFF, 0xFF, 0xFF, 0x7F]),
            Ok(268_435_455)
        );
    }

    /// The two rules the primitives enforce, stated once.
    #[test]
    fn a_property_may_appear_only_once() {
        let bytes = [0x01u8, 0x02, 0x03, 0x04];
        let mut reader = PropertyReader::new(&bytes, 4);
        let mut used = false;

        assert_eq!(reader.u8(&mut used), Ok(0x01));
        assert!(used, "a successful read must mark the property as seen");
        assert_eq!(
            reader.u8(&mut used),
            Err(PropertyError::BadResponse),
            "a repeated property was accepted"
        );
    }
}
