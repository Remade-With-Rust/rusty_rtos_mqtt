//! The MQTT fixed header: a packet type, and a variable-byte length.
//!
//! Every MQTT packet begins with one type byte and a **remaining length**
//! encoded as one to four bytes, seven bits at a time, with the top bit of each
//! byte saying whether another follows. This module is the codec for that, and
//! it is the first thing a device parses off a socket — before it knows what
//! kind of packet it is holding, and therefore before any other validation can
//! apply.
//!
//! # Three ways to lie about a length
//!
//! It is also the classic place to attack an MQTT implementation, and the C
//! refuses all three:
//!
//! 1. **Too many bytes.** The multiplier is checked *before* each byte, so a
//!    fifth continuation byte is refused rather than shifted off the top.
//! 2. **Too large.** Four bytes can express more than the 268,435,455 the
//!    specification allows, and the length check below catches the excess.
//! 3. **Non-minimal.** `0x80 0x00` decodes to zero, and so does `0x00` — but
//!    only the second is the minimal encoding. The C rejects the first by
//!    comparing the bytes it consumed against
//!    [`variable_length_encoded_size`] of the answer.
//!
//! The third is the interesting one and it is the same shape as the over-long
//! UTF-8 rule `rusty_rtos_json` had to get right: a decoder that accepts
//! non-minimal encodings gives an attacker two spellings of the same value,
//! which is how length checks get bypassed one layer up.

/// `MQTT_MAX_REMAINING_LENGTH`: the largest length the specification allows.
pub const MAX_REMAINING_LENGTH: u32 = 268_435_455;

/// `MQTT_REMAINING_LENGTH_INVALID`: one past the largest legal length.
pub const REMAINING_LENGTH_INVALID: u32 = 268_435_456;

/// The packet types that may arrive from a broker, masked to their high nibble.
///
/// Public because a caller who has taken bytes off a socket needs them to build
/// an [`ack::PacketInfo`](crate::ack::PacketInfo). Note that these are NIBBLES:
/// the ack deserializer matches whole bytes and spells `0x62` out for itself,
/// because a PUBREL with its reserved bit clear is a different packet.
pub mod packet {
    /// CONNECT, which only a client sends.
    pub const CONNECT: u8 = 0x10;
    /// CONNACK, the broker's answer to a CONNECT.
    pub const CONNACK: u8 = 0x20;
    /// PUBLISH. The low nibble carries the QoS, DUP and RETAIN flags.
    pub const PUBLISH: u8 = 0x30;
    /// PUBACK, a QoS 1 delivery acknowledged.
    pub const PUBACK: u8 = 0x40;
    /// PUBREC, the first half of a QoS 2 handshake.
    pub const PUBREC: u8 = 0x50;
    /// A PUBREL is `0x62`; masked to its nibble it is `0x60`, and the low bits
    /// are checked separately.
    pub const PUBREL_NIBBLE: u8 = 0x60;
    /// PUBCOMP, a QoS 2 handshake finished.
    pub const PUBCOMP: u8 = 0x70;
    /// SUBACK, one reason code per topic filter subscribed.
    pub const SUBACK: u8 = 0x90;
    /// UNSUBACK, one reason code per topic filter unsubscribed.
    pub const UNSUBACK: u8 = 0xB0;
    /// PINGRESP, which carries nothing.
    pub const PINGRESP: u8 = 0xD0;
    /// DISCONNECT, which either end may send.
    pub const DISCONNECT: u8 = 0xE0;
    /// PUBREL, whole: the nibble with its reserved bit, which MQTT requires
    /// to be set. [`PUBREL_NIBBLE`] is the same type with the bit masked off.
    pub const PUBREL: u8 = 0x62;
    /// SUBSCRIBE, whole, including its reserved bit.
    pub const SUBSCRIBE: u8 = 0x82;
    /// UNSUBSCRIBE, whole, including its reserved bit.
    pub const UNSUBSCRIBE: u8 = 0xA2;
    /// PINGREQ, which only a client sends.
    pub const PINGREQ: u8 = 0xC0;
    /// AUTH, MQTT 5's extended authentication exchange.
    pub const AUTH: u8 = 0xF0;
}

/// A decoded fixed header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PacketHeader {
    /// The type byte, flags and all — not masked, because a PUBLISH's flags
    /// carry its QoS, its DUP bit and its RETAIN bit.
    pub packet_type: u8,
    /// How many bytes follow the header.
    pub remaining_length: u32,
    /// How long the header itself was: 2 to 5 bytes.
    pub header_length: usize,
}

/// Why a fixed header could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderError {
    /// `MQTTNoDataAvailable`: nothing has arrived yet.
    NoDataAvailable,
    /// `MQTTNeedMoreBytes`: a header has started but is not complete. Read
    /// more and call again with the larger count.
    NeedMoreBytes,
    /// `MQTTBadResponse`: the type is not one a broker sends, or the length is
    /// malformed. **This is not a "read more" case** — the connection should
    /// go, because whatever is on it is not speaking MQTT.
    BadResponse,
}

/// `variableLengthEncodedSize`: how many bytes `length` needs.
///
/// This is also what makes a **non-minimal** encoding detectable: decode the
/// bytes, ask how many the answer should have taken, and refuse if they differ.
#[must_use]
pub const fn variable_length_encoded_size(length: u32) -> u32 {
    if length < 128 {
        1
    } else if length < 16_384 {
        2
    } else if length < 2_097_152 {
        3
    } else {
        4
    }
}

/// `encodeVariableLength`: write `length` into `destination`, seven bits at a
/// time, and return how many bytes were used.
///
/// Returns 0 if the destination is too small, which the C cannot do — it
/// asserts the caller has sized the buffer, having computed the size with
/// [`variable_length_encoded_size`] first. A library that must not panic
/// answers instead.
#[must_use]
pub fn encode_variable_length(destination: &mut [u8], length: u32) -> usize {
    let needed = variable_length_encoded_size(length) as usize;

    if destination.len() < needed {
        return 0;
    }

    let mut remaining = length;
    let mut written = 0usize;

    loop {
        let mut byte = (remaining % 128) as u8;
        remaining /= 128;

        // The top bit says "another byte follows".
        if remaining > 0 {
            byte |= 0x80;
        }

        let Some(slot) = destination.get_mut(written) else {
            return written;
        };
        *slot = byte;
        written = written.saturating_add(1);

        if remaining == 0 {
            break;
        }
    }

    written
}

/// `incomingPacketValid`: is this a packet type a broker may send us?
///
/// CONNECT, SUBSCRIBE, UNSUBSCRIBE and PINGREQ are client-to-server only, so
/// receiving one means the peer is confused or hostile.
#[must_use]
pub const fn incoming_packet_valid(packet_type: u8) -> bool {
    // A PUBREL's second bit is reserved and MUST be set, so `0x60` is
    // malformed where `0x62` is fine. That one bit is the difference between a
    // valid packet and a protocol violation, and it is why this nibble cannot
    // join the set below.
    if packet_type & 0xF0 == packet::PUBREL_NIBBLE {
        return (packet_type & 0x02) != 0;
    }

    // Every remaining arm asked the same question of the same quantity -- is
    // this high nibble in the set a broker may send -- so the ten-arm match is
    // set membership over sixteen values, and a `u16` answers it in a shift.
    const fn bit(type_byte: u8) -> u16 {
        1u16 << (type_byte >> 4)
    }
    const VALID: u16 = bit(packet::CONNACK)
        | bit(packet::PUBLISH)
        | bit(packet::PUBACK)
        | bit(packet::PUBREC)
        | bit(packet::PUBCOMP)
        | bit(packet::SUBACK)
        | bit(packet::UNSUBACK)
        | bit(packet::PINGRESP)
        | bit(packet::DISCONNECT)
        | bit(packet::AUTH);

    (VALID >> (packet_type >> 4)) & 1 == 1
}

/// `MQTT_ProcessIncomingPacketTypeAndLength`: read a fixed header from a buffer.
///
/// `available` is how many bytes have actually arrived, which may be fewer than
/// `buffer.len()` — that is the normal case when reading from a socket into a
/// larger buffer.
///
/// # Errors
///
/// [`HeaderError::NoDataAvailable`] when nothing has arrived,
/// [`HeaderError::NeedMoreBytes`] when the header is incomplete, and
/// [`HeaderError::BadResponse`] when the type is not one a broker sends or the
/// length is malformed. Only the middle one is worth retrying.
pub fn process_incoming_packet_type_and_length(
    buffer: &[u8],
    available: usize,
) -> Result<PacketHeader, HeaderError> {
    if available < 1 {
        return Err(HeaderError::NoDataAvailable);
    }

    let Some(packet_type) = buffer.first().copied() else {
        return Err(HeaderError::NoDataAvailable);
    };

    if !incoming_packet_valid(packet_type) {
        return Err(HeaderError::BadResponse);
    }

    let mut remaining_length: u32 = 0;
    let mut multiplier: u32 = 1;
    let mut bytes_decoded: usize = 0;

    loop {
        // Checked BEFORE consuming a byte, so a fifth continuation byte is
        // refused rather than shifted off the top of the multiplier.
        if multiplier > 2_097_152 {
            return Err(HeaderError::BadResponse);
        }

        // The header byte was read already, so the length bytes start at 1.
        if available <= bytes_decoded.saturating_add(1) {
            return Err(HeaderError::NeedMoreBytes);
        }

        let Some(byte) = buffer.get(bytes_decoded.saturating_add(1)).copied() else {
            return Err(HeaderError::NeedMoreBytes);
        };

        // Four bytes of seven bits cannot exceed 268,435,455, and the
        // multiplier guard above bounds it to four, so this cannot overflow.
        remaining_length =
            remaining_length.saturating_add(u32::from(byte & 0x7F).saturating_mul(multiplier));
        multiplier = multiplier.saturating_mul(128);
        bytes_decoded = bytes_decoded.saturating_add(1);

        // The C tests this at the top of a do/while with the byte
        // initialised to zero; testing it here after the assignment is the
        // same loop with one fewer dead store.
        if (byte & 0x80) == 0 {
            break;
        }
    }

    // The non-minimal check: the bytes consumed must be the bytes this value
    // needs. `0x80 0x00` decodes to zero in two bytes, and zero needs one.
    if bytes_decoded != variable_length_encoded_size(remaining_length) as usize {
        return Err(HeaderError::BadResponse);
    }

    Ok(PacketHeader {
        packet_type,
        remaining_length,
        header_length: bytes_decoded.saturating_add(1),
    })
}
