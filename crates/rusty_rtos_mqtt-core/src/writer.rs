//! The fixed header of every outgoing MQTT packet.
//!
//! [`property`](crate::property) is the reading side of the primitive layer.
//! This is the writing side: five functions that lay down the bytes at the
//! front of a CONNECT, SUBSCRIBE, UNSUBSCRIBE, DISCONNECT or publish
//! acknowledgement. They validate nothing and keep no state — the caller has
//! already computed the remaining length — which makes them exactly the sort of
//! code where a transcription is confidently and quietly wrong.
//!
//! # One byte does most of the work
//!
//! [`serialize_connect_fixed_header`] packs six independent decisions into a
//! single flags byte: clean session, will present, will QoS, will retain,
//! password present, username present. Get a bit position wrong and the broker
//! rejects a packet that looks fine in a hex dump, in a way that reads like a
//! network fault. The differential sweeps **every combination** rather than a
//! handful of samples, for that reason.
//!
//! # Sizes, not slices
//!
//! Each function returns how many bytes it wrote, and **0 if the destination is
//! too small**. The C returns a pointer to one past the end and asserts the
//! caller sized the buffer — it has computed the size a moment earlier, so in
//! its own use the assertion always holds. A library that must not panic
//! answers instead.

use crate::header::{encode_variable_length, variable_length_encoded_size};

/// `MQTT_VERSION_5`.
pub const VERSION_5: u8 = 5;

/// The packet types a client sends.
mod packet {
    pub const CONNECT: u8 = 0x10;
    pub const SUBSCRIBE: u8 = 0x82;
    pub const UNSUBSCRIBE: u8 = 0xA2;
    pub const PINGREQ: u8 = 0xC0;
    pub const DISCONNECT: u8 = 0xE0;
}

/// `MQTT_PACKET_TYPE_PINGREQ`, which has no body at all.
pub const PINGREQ: [u8; 2] = [packet::PINGREQ, 0];

/// The bit positions in the CONNECT flags byte.
///
/// Bit 0 is reserved and must be zero. The rest are the specification's, and
/// they are written out here rather than inlined because a wrong one is the
/// defect this module exists to avoid.
mod connect_flag {
    pub const CLEAN: u8 = 1;
    pub const WILL: u8 = 2;
    pub const WILL_QOS1: u8 = 3;
    pub const WILL_QOS2: u8 = 4;
    pub const WILL_RETAIN: u8 = 5;
    pub const PASSWORD: u8 = 6;
    pub const USERNAME: u8 = 7;
}

/// What a CONNECT's fixed header needs to know.
///
/// The credentials are present or absent rather than written here — the fixed
/// header only records *whether* they follow, and the bodies are appended by
/// the caller.
#[derive(Debug, Clone, Copy, Default)]
pub struct ConnectInfo<'a> {
    /// Start a fresh session, discarding anything the broker held.
    pub clean_session: bool,
    /// How long the broker should wait before declaring the client gone.
    pub keep_alive_seconds: u16,
    /// The username, if one is being sent.
    pub username: Option<&'a [u8]>,
    /// The password, if one is being sent.
    pub password: Option<&'a [u8]>,
}

/// A Last Will and Testament, as far as the fixed header is concerned.
#[derive(Debug, Clone, Copy, Default)]
pub struct WillInfo {
    /// The QoS the will should be published at.
    pub qos: crate::state::QoS,
    /// Whether the broker should retain it.
    pub retain: bool,
}

/// Write a byte, if there is room. Returns whether it fitted.
fn put(destination: &mut [u8], at: usize, value: u8) -> bool {
    if let Some(slot) = destination.get_mut(at) {
        *slot = value;
        true
    } else {
        false
    }
}

/// Write a big-endian `u16`, if there is room.
fn put_u16(destination: &mut [u8], at: usize, value: u16) -> bool {
    let end = at.saturating_add(2);
    if let Some(slot) = destination.get_mut(at..end) {
        slot.copy_from_slice(&value.to_be_bytes());
        true
    } else {
        false
    }
}

/// `serializeAckFixed`: a publish acknowledgement's header and reason code.
///
/// Writes the type byte, the remaining length, the packet id and one reason
/// code — four bytes plus however many the length needs. Returns 0 if the
/// destination is too small.
#[must_use]
pub fn serialize_ack_fixed(
    destination: &mut [u8],
    packet_type: u8,
    packet_id: u16,
    remaining_length: u32,
    reason_code: u8,
) -> usize {
    let length_bytes = variable_length_encoded_size(remaining_length) as usize;
    let total = 1usize
        .saturating_add(length_bytes)
        .saturating_add(2)
        .saturating_add(1);

    if destination.len() < total {
        return 0;
    }

    if !put(destination, 0, packet_type) {
        return 0;
    }

    let Some(tail) = destination.get_mut(1..) else {
        return 0;
    };
    if encode_variable_length(tail, remaining_length) != length_bytes {
        return 0;
    }

    let at = 1usize.saturating_add(length_bytes);
    if !put_u16(destination, at, packet_id) {
        return 0;
    }
    if !put(destination, at.saturating_add(2), reason_code) {
        return 0;
    }

    total
}

/// `serializeConnectFixedHeader`: a CONNECT up to and including the keep alive.
///
/// The protocol name and version follow the remaining length, then the flags
/// byte, then the keep alive. Everything after that — the client id, the will,
/// the credentials — is the caller's to append.
///
/// Returns 0 if the destination is too small.
#[must_use]
pub fn serialize_connect_fixed_header(
    destination: &mut [u8],
    connect: &ConnectInfo<'_>,
    will: Option<&WillInfo>,
    remaining_length: u32,
) -> usize {
    use crate::state::QoS;

    let length_bytes = variable_length_encoded_size(remaining_length) as usize;
    // type + length + "MQTT" as a 2-byte-prefixed string + version + flags + keep alive
    let total = 1usize
        .saturating_add(length_bytes)
        .saturating_add(6)
        .saturating_add(1)
        .saturating_add(1)
        .saturating_add(2);

    if destination.len() < total {
        return 0;
    }

    if !put(destination, 0, packet::CONNECT) {
        return 0;
    }

    let Some(tail) = destination.get_mut(1..) else {
        return 0;
    };
    if encode_variable_length(tail, remaining_length) != length_bytes {
        return 0;
    }

    let mut at = 1usize.saturating_add(length_bytes);

    // The protocol name is a length-prefixed string, always `MQTT`.
    if !put_u16(destination, at, 4) {
        return 0;
    }
    at = at.saturating_add(2);

    let end = at.saturating_add(4);
    let Some(name) = destination.get_mut(at..end) else {
        return 0;
    };
    name.copy_from_slice(b"MQTT");
    at = end;

    if !put(destination, at, VERSION_5) {
        return 0;
    }
    at = at.saturating_add(1);

    // Bit 0 is reserved and stays clear.
    let mut flags = 0u8;

    if connect.clean_session {
        flags |= 1 << connect_flag::CLEAN;
    }

    // The C tests its pointers for NULL; presence is the same question.
    if connect.username.is_some() {
        flags |= 1 << connect_flag::USERNAME;
    }
    if connect.password.is_some() {
        flags |= 1 << connect_flag::PASSWORD;
    }

    if let Some(will) = will {
        flags |= 1 << connect_flag::WILL;

        // QoS 0 sets neither bit, which is why this is not a shift of the
        // numeric QoS: the two bits are independent flags in the C.
        match will.qos {
            QoS::AtLeastOnce => flags |= 1 << connect_flag::WILL_QOS1,
            QoS::ExactlyOnce => flags |= 1 << connect_flag::WILL_QOS2,
            QoS::AtMostOnce => {}
        }

        if will.retain {
            flags |= 1 << connect_flag::WILL_RETAIN;
        }
    }

    if !put(destination, at, flags) {
        return 0;
    }
    at = at.saturating_add(1);

    if !put_u16(destination, at, connect.keep_alive_seconds) {
        return 0;
    }

    total
}

/// `serializeSubscribeHeader`: the type byte, the length and the packet id.
#[must_use]
pub fn serialize_subscribe_header(
    destination: &mut [u8],
    remaining_length: u32,
    packet_id: u16,
) -> usize {
    serialize_id_header(destination, packet::SUBSCRIBE, remaining_length, packet_id)
}

/// `serializeUnsubscribeHeader`: the same shape, a different type byte.
#[must_use]
pub fn serialize_unsubscribe_header(
    destination: &mut [u8],
    remaining_length: u32,
    packet_id: u16,
) -> usize {
    serialize_id_header(
        destination,
        packet::UNSUBSCRIBE,
        remaining_length,
        packet_id,
    )
}

/// The shape SUBSCRIBE and UNSUBSCRIBE share.
///
/// The C writes these out twice, identically but for the type byte. Sharing
/// them here is safe because the differential drives both and would catch the
/// two drifting apart — which, in the C, they could.
fn serialize_id_header(
    destination: &mut [u8],
    packet_type: u8,
    remaining_length: u32,
    packet_id: u16,
) -> usize {
    let length_bytes = variable_length_encoded_size(remaining_length) as usize;
    let total = 1usize.saturating_add(length_bytes).saturating_add(2);

    if destination.len() < total {
        return 0;
    }

    if !put(destination, 0, packet_type) {
        return 0;
    }

    let Some(tail) = destination.get_mut(1..) else {
        return 0;
    };
    if encode_variable_length(tail, remaining_length) != length_bytes {
        return 0;
    }

    if !put_u16(destination, 1usize.saturating_add(length_bytes), packet_id) {
        return 0;
    }

    total
}

/// `serializeDisconnectFixed`: the type byte, the length, and maybe a reason.
///
/// A DISCONNECT may carry no reason code at all, which is what the C's NULL
/// pointer means — a clean close with nothing to say.
#[must_use]
pub fn serialize_disconnect_fixed(
    destination: &mut [u8],
    reason_code: Option<u8>,
    remaining_length: u32,
) -> usize {
    let length_bytes = variable_length_encoded_size(remaining_length) as usize;
    let total = 1usize
        .saturating_add(length_bytes)
        .saturating_add(usize::from(reason_code.is_some()));

    if destination.len() < total {
        return 0;
    }

    if !put(destination, 0, packet::DISCONNECT) {
        return 0;
    }

    let Some(tail) = destination.get_mut(1..) else {
        return 0;
    };
    if encode_variable_length(tail, remaining_length) != length_bytes {
        return 0;
    }

    if let Some(reason) = reason_code {
        if !put(destination, 1usize.saturating_add(length_bytes), reason) {
            return 0;
        }
    }

    total
}

#[cfg(test)]
// A test does its own bit arithmetic and indexes its own output; the
// workspace's policy is for library code, where either would be a defect.
#[allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::state::QoS;

    /// Why the two will-QoS encodings are the same thing.
    ///
    /// This started as a poison that did not fire. The C sets two independent
    /// flags for will QoS 1 and 2; writing the numeric QoS shifted left by the
    /// QoS-1 bit position produces identical bytes, because the two bits are
    /// ADJACENT and the legal QoS values are 0, 1 and 2.
    ///
    /// The two-flag form is kept because it is the C's and because it says what
    /// the specification says. The equivalence is pinned because it rests on
    /// the bits being adjacent — move either constant and the two forms part
    /// company, with nothing else failing.
    #[test]
    fn the_two_will_qos_encodings_coincide() {
        assert_eq!(
            connect_flag::WILL_QOS2,
            connect_flag::WILL_QOS1 + 1,
            "the will-QoS bits are no longer adjacent, so a shifted-number \
             encoding is no longer equivalent to two flags"
        );

        for (qos, number) in [
            (QoS::AtMostOnce, 0u8),
            (QoS::AtLeastOnce, 1),
            (QoS::ExactlyOnce, 2),
        ] {
            let two_flags = match qos {
                QoS::AtLeastOnce => 1 << connect_flag::WILL_QOS1,
                QoS::ExactlyOnce => 1 << connect_flag::WILL_QOS2,
                QoS::AtMostOnce => 0,
            };
            let shifted = number << connect_flag::WILL_QOS1;

            assert_eq!(two_flags, shifted, "the two encodings differ at {qos:?}");
        }
    }

    /// Bit 0 of the CONNECT flags is reserved and must never be set.
    #[test]
    fn the_reserved_connect_bit_is_never_set() {
        for clean in [false, true] {
            for will in [
                None,
                Some(&WillInfo {
                    qos: QoS::ExactlyOnce,
                    retain: true,
                }),
            ] {
                let connect = ConnectInfo {
                    clean_session: clean,
                    keep_alive_seconds: 1,
                    username: Some(b"u"),
                    password: Some(b"p"),
                };
                let mut out = [0u8; 32];
                let n = serialize_connect_fixed_header(&mut out, &connect, will, 10);
                let flags = out[n - 3];

                assert_eq!(
                    flags & 1,
                    0,
                    "the reserved bit is set in {flags:08b} -- a broker must \
                     reject this packet"
                );
            }
        }
    }
}
