//! The buffer an outgoing property section is written into.
//!
//! Everything this crate writes takes a `&mut [u8]` and returns how many bytes
//! it used. A property section is the exception: MQTT 5 lets a caller add
//! properties one at a time, in any order, so the section needs somewhere to
//! accumulate and something to remember what has already been added.
//!
//! That is all `MQTTPropBuilder_t` is — a buffer, a cursor and a bitfield —
//! and `MQTTPropertyBuilder_Init` is the constructor. The writers that fill it
//! are `core_mqtt_prop_serializer.c`, which is the next slice.
//!
//! # Two numbers that must agree
//!
//! The C's constructor takes a **pointer and a length** and never checks them
//! against each other, so an eight-byte buffer with a length of a million is
//! accepted and the first write past the eighth byte is somebody else's memory.
//! [`PropertyBuilder::new`] takes one slice, which carries both, and the
//! question cannot be asked. Two of the C's three refusals therefore have no
//! reachable equivalent here and are absent from the differential rather than
//! failing in it — see [`BuilderError`].

use crate::header::REMAINING_LENGTH_INVALID;

/// Why a property builder could not be created.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuilderError {
    /// `MQTTBadParameter`: the buffer is empty.
    ///
    /// The C answers this for three conditions: a null builder, a null buffer,
    /// and a zero length. Only the last is reachable through a `&mut [u8]`.
    Empty,
    /// `MQTTBadParameter`: the buffer is at or above
    /// [`REMAINING_LENGTH_INVALID`], so a section filling it could not be
    /// length-prefixed.
    ///
    /// Reachable only with a buffer of 256 MB or more, which is why no case in
    /// `oracle/context.trace` reaches it and
    /// `the_bound_is_the_first_illegal_remaining_length` pins the arithmetic
    /// instead.
    TooLong,
}

/// `MQTTPropBuilder_t`: a section under construction.
#[derive(Debug)]
pub struct PropertyBuilder<'a> {
    buffer: &'a mut [u8],
    at: usize,
    fields: u32,
}

impl<'a> PropertyBuilder<'a> {
    /// `MQTTPropertyBuilder_Init`.
    ///
    /// # Errors
    ///
    /// [`BuilderError::Empty`] for an empty buffer, [`BuilderError::TooLong`]
    /// for one at or above [`REMAINING_LENGTH_INVALID`].
    pub fn new(buffer: &'a mut [u8]) -> Result<Self, BuilderError> {
        if buffer.is_empty() {
            return Err(BuilderError::Empty);
        }

        let Ok(length) = u32::try_from(buffer.len()) else {
            return Err(BuilderError::TooLong);
        };

        if length >= REMAINING_LENGTH_INVALID {
            return Err(BuilderError::TooLong);
        }

        Ok(Self {
            buffer,
            at: 0,
            fields: 0,
        })
    }

    /// How many bytes have been written: the C's `currentIndex`.
    ///
    /// This doubles as the section's **property length**, which is what every
    /// validator and serializer in the crate is handed.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.at
    }

    /// Whether anything has been written yet.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.at == 0
    }

    /// How much room there is: the C's `bufferLength`.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.buffer.len()
    }

    /// Which properties have been added, by bit: the C's `fieldSet`.
    ///
    /// The bit positions are the ones [`connack::field`](crate::connack::field)
    /// already carries — one table, two directions.
    #[must_use]
    pub const fn fields(&self) -> u32 {
        self.fields
    }

    /// The section so far, ready to hand to a serializer.
    #[must_use]
    pub fn section(&self) -> &[u8] {
        self.buffer.get(..self.at).unwrap_or(&[])
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]
mod tests {
    use super::*;

    /// The bound is the first illegal remaining length, and nothing below it.
    ///
    /// The C checks `length >= MQTT_REMAINING_LENGTH_INVALID` on a length
    /// ARGUMENT, which a caller can set to anything. Here it is the buffer's
    /// own length, so reaching it needs a 256 MB buffer and no differential
    /// case can. The arithmetic is pinned instead — the same treatment the
    /// package gives every bound it cannot reach with a plausible input.
    #[test]
    fn the_bound_is_the_first_illegal_remaining_length() {
        assert_eq!(REMAINING_LENGTH_INVALID, 268_435_456);
        assert_eq!(
            REMAINING_LENGTH_INVALID - 1,
            268_435_455,
            "the largest legal remaining length is no longer one below the bound"
        );

        // The bound is LIVE code, not dead: a `usize` holds it on every target
        // this crate builds for, so a big enough buffer really would reach it.
        // What makes it untestable is the buffer -- 256 MB, which no part this
        // crate targets has and no test should allocate. So it gets the same
        // treatment as every other bound the package cannot reach with a
        // plausible input: the arithmetic is pinned and the differential
        // stays quiet about it.
        assert!(usize::try_from(REMAINING_LENGTH_INVALID).is_ok());
    }

    /// A builder starts empty, and `len` is the property length.
    #[test]
    fn a_new_builder_is_empty_and_its_length_is_the_section_length() {
        let mut bytes = [0u8; 8];
        let builder = PropertyBuilder::new(&mut bytes).unwrap();

        assert_eq!(builder.len(), 0);
        assert!(builder.is_empty());
        assert_eq!(builder.capacity(), 8);
        assert_eq!(builder.fields(), 0);
        assert_eq!(builder.section(), &[] as &[u8]);
    }

    /// An empty buffer is refused, because a section needs somewhere to go.
    #[test]
    fn an_empty_buffer_is_refused() {
        let mut nothing: [u8; 0] = [];
        assert_eq!(
            PropertyBuilder::new(&mut nothing).map(|b| b.capacity()),
            Err(BuilderError::Empty)
        );
    }
}
