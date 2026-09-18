//! The DISCONNECT, both directions.
//!
//! MQTT 3.1.1's DISCONNECT was two bytes a client sent on the way out. MQTT 5
//! made it a real packet with a reason code and properties, and made it
//! **bidirectional** — a broker sends one to say why it is closing the socket,
//! and that is the packet [`ack`](crate::ack), [`connack`](crate::connack) and
//! [`publish`](crate::publish) between them do not cover.
//!
//! # One table, two directions, different answers
//!
//! `validateDisconnectResponse` takes an `incoming` flag, and the same byte is
//! read differently depending on it:
//!
//! * `0x04`, "disconnect with Will message", is a **client** code. Legal going
//!   out, refused coming in.
//! * Fifteen codes — server busy, server shutting down, session taken over,
//!   use another server, and the rest — are **server** codes. Legal coming in,
//!   refused going out.
//! * Thirteen are legal either way.
//!
//! So the reason code is swept 256 times in **each** direction, and the two
//! accepted sets are printed. The outgoing set turns out to be exactly MQTT 5.0
//! §3.14.2.1's client column; the incoming one is the server column **minus
//! `0x9F`**, which is a defect and is written up for filing.
//!
//! # And two different property tables
//!
//! An incoming DISCONNECT may carry a reason string, user properties and a
//! **server reference**; an outgoing one may carry a reason string, user
//! properties and a **session expiry interval**. Neither list is the other, and
//! nothing in the C names them together — so both are swept too.

use crate::ack::{AckError, PacketInfo, bounded};
use crate::header::{REMAINING_LENGTH_INVALID, variable_length_encoded_size};
use crate::property::{PropertyError, PropertyReader, decode_variable_length};

/// The property identifiers a DISCONNECT may carry, in either direction.
pub mod property {
    /// Session Expiry Interval, four bytes. **Outgoing only.**
    pub const SESSION_EXPIRY: u8 = 0x11;
    /// Server Reference, a string. **Incoming only.**
    pub const SERVER_REFERENCE: u8 = 0x1C;
    /// Reason String, a string. Either direction.
    pub const REASON_STRING: u8 = 0x1F;
    /// User Property, two strings. Either direction, and may repeat.
    pub const USER_PROPERTY: u8 = 0x26;
}

/// An incoming DISCONNECT, read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Disconnect<'a> {
    /// The broker's reason, if it gave one.
    ///
    /// `None` when the packet had no body at all — MQTT 5.0 §3.14.2.1 lets the
    /// whole variable header be omitted, which means a normal disconnection
    /// with nothing to add.
    pub reason_code: Option<u8>,
    /// The property section, without its length prefix.
    pub properties: &'a [u8],
}

/// The two numbers an outgoing DISCONNECT needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DisconnectSize {
    /// What goes in the fixed header.
    pub remaining_length: u32,
    /// The whole packet, header included.
    pub packet_size: u32,
}

/// Why a DISCONNECT was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisconnectError {
    /// `MQTTBadParameter`. On the way **out** this is the usual status,
    /// including for a reason code a client may not send. On the way **in** it
    /// covers a zero maximum packet size.
    BadParameter,
    /// `MQTTBadResponse`: an incoming packet was wrong.
    BadResponse,
    /// `MQTTNoMemory`: the caller's buffer is too small for the packet the size
    /// calculator already agreed to.
    NoMemory,
}

impl From<PropertyError> for DisconnectError {
    fn from(_: PropertyError) -> Self {
        Self::BadResponse
    }
}

impl From<AckError> for DisconnectError {
    fn from(error: AckError) -> Self {
        match error {
            AckError::BadParameter => Self::BadParameter,
            AckError::BadResponse => Self::BadResponse,
        }
    }
}

/// `validateDisconnectResponse`: may this reason code travel this way?
///
/// The one function in the library whose answer depends on **which direction a
/// packet is going**, and the reason this module covers both. See the module
/// note for the three groups.
///
/// # The incoming table is one short of the specification
///
/// MQTT 5.0 §3.14.2.1 lists `0x9F`, "Connection rate exceeded", among the codes
/// a server may send. It is in neither arm of the C's switch, so a client that
/// receives it answers `MQTTBadResponse` — treating a conformant disconnection
/// as a malformed packet. Transcribed exactly, because the C is the oracle, and
/// written up in `docs/upstream/` for filing.
#[must_use]
pub fn reason_code_allowed(reason_code: u8, incoming: bool) -> bool {
    match reason_code {
        // The client's own: "I am disconnecting and I want my Will sent."
        0x04 => !incoming,

        // Either direction.
        0x00 | 0x80 | 0x81 | 0x82 | 0x83 | 0x90 | 0x93 | 0x94 | 0x95 | 0x96 | 0x97 | 0x98
        | 0x99 => true,

        // The server's own. Note the absence of 0x9F, which the specification
        // puts here and the C does not.
        0x87 | 0x89 | 0x8B | 0x8C | 0x8D | 0x8E | 0x8F | 0x9A | 0x9B | 0x9C | 0x9D | 0x9E
        | 0xA0 | 0xA1 | 0xA2 => incoming,

        _ => false,
    }
}

/// `MQTT_DeserializeDisconnect`: read a DISCONNECT a broker sent.
///
/// # Errors
///
/// [`DisconnectError::BadParameter`] for a zero `max_packet_size`.
/// [`DisconnectError::BadResponse`] for a remaining length at or past MQTT's
/// maximum, a packet larger than `max_packet_size`, a reason code a server may
/// not send, a property section that does not fill the body exactly, or an
/// unknown or repeated property.
pub fn deserialize_disconnect<'a>(
    packet: &PacketInfo<'a>,
    max_packet_size: u32,
) -> Result<Disconnect<'a>, DisconnectError> {
    if max_packet_size == 0 {
        return Err(DisconnectError::BadParameter);
    }

    // Note the status: the C answers BadRESPONSE here and BadPARAMETER for the
    // same condition in `MQTT_DeserializePublish`. Transcribed as each has it.
    if packet.remaining_length >= REMAINING_LENGTH_INVALID {
        return Err(DisconnectError::BadResponse);
    }

    let packet_size = packet
        .remaining_length
        .saturating_add(variable_length_encoded_size(packet.remaining_length))
        .saturating_add(1);

    if packet_size > max_packet_size {
        return Err(DisconnectError::BadResponse);
    }

    // §3.14.2.1: the whole variable header may be omitted.
    if packet.remaining_length == 0 {
        return Ok(Disconnect {
            reason_code: None,
            properties: &[],
        });
    }

    let reason_code = *packet
        .remaining_data
        .first()
        .ok_or(DisconnectError::BadResponse)?;

    if !reason_code_allowed(reason_code, true) {
        return Err(DisconnectError::BadResponse);
    }

    // A one-byte body is the reason code and nothing else.
    if packet.remaining_length == 1 {
        return Ok(Disconnect {
            reason_code: Some(reason_code),
            properties: &[],
        });
    }

    let after_reason = bounded(
        packet.remaining_data,
        1,
        packet.remaining_length.saturating_sub(1),
    )?;

    let property_length = decode_variable_length(after_reason)?;
    let encoded = variable_length_encoded_size(property_length);

    // An EQUALITY: nothing follows a DISCONNECT's properties, so the body must
    // be the reason code, the encoded length, and exactly that many bytes.
    if packet.remaining_length != property_length.saturating_add(encoded).saturating_add(1) {
        return Err(DisconnectError::BadResponse);
    }

    let properties = bounded(after_reason, encoded as usize, property_length)?;
    walk_incoming_properties(properties, property_length)?;

    Ok(Disconnect {
        reason_code: Some(reason_code),
        properties,
    })
}

/// `validateIncomingDisconnectProperties`: a reason string, a server reference,
/// and any number of user properties.
///
/// **Not** the outgoing list — see [`validate_outgoing_properties`]. A server
/// reference tells a client where to reconnect, which is meaningless coming
/// from the client; a session expiry is the client's request, which is
/// meaningless coming from the server.
fn walk_incoming_properties(
    properties: &[u8],
    property_length: u32,
) -> Result<(), DisconnectError> {
    let mut reader = PropertyReader::new(properties, property_length);
    let mut reason_string = false;
    let mut server_reference = false;

    while reader.remaining() > 0 {
        match reader.property_id()? {
            property::REASON_STRING => {
                let _ = reader.utf8(&mut reason_string)?;
            }
            property::SERVER_REFERENCE => {
                let _ = reader.utf8(&mut server_reference)?;
            }
            property::USER_PROPERTY => {
                let _ = reader.user_property()?;
            }
            _ => return Err(DisconnectError::BadResponse),
        }
    }

    Ok(())
}

/// `MQTT_ValidateDisconnectProperties`: check what a client is about to send.
///
/// `connect_session_expiry` is what this client asked for in its CONNECT. MQTT
/// 5.0 §3.14.2.2.2 forbids setting a non-zero Session Expiry on the way out
/// when the connection began with zero — a session that was never going to
/// survive cannot be given a lifetime at the end of it.
///
/// # Errors
///
/// [`DisconnectError::BadParameter`] for an unknown property, a server
/// reference (which is an incoming-only property), or that session-expiry rule.
/// [`DisconnectError::BadResponse`] for a malformed value — note that the C
/// mixes the two statuses here, and both are transcribed as it has them.
pub fn validate_outgoing_properties(
    connect_session_expiry: u32,
    properties: &[u8],
) -> Result<(), DisconnectError> {
    let property_length =
        u32::try_from(properties.len()).map_err(|_| DisconnectError::BadParameter)?;
    let mut reader = PropertyReader::new(properties, property_length);

    while reader.remaining() > 0 {
        // The C declares `used` INSIDE its loop, so every property gets a fresh
        // flag and none of them can catch a duplicate. Reproduced, because a
        // transcription that hoisted the flags out would refuse packets the C
        // accepts -- and the duplicate is the caller's own doing here, not an
        // attacker's, which is presumably why nobody minded.
        let mut used = false;

        match reader.property_id()? {
            property::SESSION_EXPIRY => {
                let session_expiry = reader.u32(&mut used)?;

                if connect_session_expiry == 0 && session_expiry != 0 {
                    return Err(DisconnectError::BadParameter);
                }
            }
            property::REASON_STRING => {
                let _ = reader.utf8(&mut used)?;
            }
            property::USER_PROPERTY => {
                let _ = reader.user_property()?;
            }
            _ => return Err(DisconnectError::BadParameter),
        }
    }

    Ok(())
}

/// `MQTT_GetDisconnectPacketSize`: how big an outgoing DISCONNECT will be.
///
/// `reason_code` is optional, and `property_length` may only be non-zero when
/// there is one — the reason code comes first on the wire and cannot be
/// skipped.
///
/// # A bare DISCONNECT has a remaining length of ONE, not zero
///
/// With no reason code and no properties the C still charges a byte for the
/// encoded property length, so it produces `E0 01 00` where MQTT 5.0 §3.14.2.1
/// allows `E0 00`. Both are legal, and the three-byte form is correct **by
/// coincidence**: the byte the writer intends as a property length is read by a
/// broker as the reason code, and both are `0x00`. `a_bare_disconnect_is_three_bytes_by_coincidence`
/// records that, because it is the sort of thing a later simplification breaks.
///
/// # Errors
///
/// [`DisconnectError::BadParameter`] for a zero `max_packet_size`, a property
/// length with no reason code, a reason code a client may not send, a total
/// past MQTT's maximum remaining length, or a packet larger than
/// `max_packet_size`.
pub fn disconnect_packet_size(
    reason_code: Option<u8>,
    property_length: u32,
    max_packet_size: u32,
) -> Result<DisconnectSize, DisconnectError> {
    if reason_code.is_none() && property_length != 0 {
        return Err(DisconnectError::BadParameter);
    }

    if max_packet_size == 0 {
        return Err(DisconnectError::BadParameter);
    }

    let mut length = 0u32;

    if let Some(code) = reason_code {
        if !reason_code_allowed(code, false) {
            return Err(DisconnectError::BadParameter);
        }

        length = 1;
    }

    if property_length >= REMAINING_LENGTH_INVALID {
        return Err(DisconnectError::BadParameter);
    }

    let encoded = variable_length_encoded_size(property_length);

    // The C compares before adding, so the limit applies to the sum rather than
    // to what it stores. `<` and not `<=`: a remaining length of exactly
    // 268,435,455 is refused here, where the packet-size calculators allow it.
    if property_length
        .saturating_add(encoded)
        .saturating_add(length)
        >= REMAINING_LENGTH_INVALID
    {
        return Err(DisconnectError::BadParameter);
    }

    let remaining_length = length
        .saturating_add(encoded)
        .saturating_add(property_length);
    let packet_size = remaining_length
        .saturating_add(variable_length_encoded_size(remaining_length))
        .saturating_add(1);

    if packet_size > max_packet_size {
        return Err(DisconnectError::BadParameter);
    }

    Ok(DisconnectSize {
        remaining_length,
        packet_size,
    })
}

/// `MQTT_SerializeDisconnect`: lay the packet down.
///
/// Returns how many bytes were written. The caller supplies `remaining_length`,
/// normally from [`disconnect_packet_size`] — the C takes it as a parameter and
/// does not recompute it, so a caller who passes a different number gets a
/// packet that disagrees with itself.
///
/// # Errors
///
/// [`DisconnectError::BadParameter`] for a property length with no reason code
/// or a remaining length past MQTT's maximum. [`DisconnectError::NoMemory`] if
/// `destination` is smaller than the packet.
pub fn serialize_disconnect(
    destination: &mut [u8],
    reason_code: Option<u8>,
    properties: &[u8],
    remaining_length: u32,
) -> Result<usize, DisconnectError> {
    let property_length =
        u32::try_from(properties.len()).map_err(|_| DisconnectError::BadParameter)?;

    if property_length >= REMAINING_LENGTH_INVALID {
        return Err(DisconnectError::BadParameter);
    }

    if reason_code.is_none() && property_length != 0 {
        return Err(DisconnectError::BadParameter);
    }

    if remaining_length >= REMAINING_LENGTH_INVALID {
        return Err(DisconnectError::BadParameter);
    }

    let packet_size = remaining_length
        .saturating_add(variable_length_encoded_size(remaining_length))
        .saturating_add(1);

    // The C checks this because its next three steps are pointer writes and a
    // memcpy with no bound at all. Every write below goes through a slice, so
    // each one refuses on its own and this check cannot change the answer --
    // `the_buffer_check_is_the_slice_bounds_restated` pins that. Kept because
    // the C has it and because refusing up front beats refusing three
    // statements in.
    if (destination.len() as u64) < u64::from(packet_size) {
        return Err(DisconnectError::NoMemory);
    }

    let written =
        crate::writer::serialize_disconnect_fixed(destination, reason_code, remaining_length);

    if written == 0 {
        return Err(DisconnectError::NoMemory);
    }

    let mut at = written;
    let rest = destination.get_mut(at..).ok_or(DisconnectError::NoMemory)?;
    let encoded = crate::header::encode_variable_length(rest, property_length);

    if encoded == 0 {
        return Err(DisconnectError::NoMemory);
    }

    at = at.saturating_add(encoded);

    if property_length != 0 {
        let end = at.saturating_add(properties.len());
        let slot = destination
            .get_mut(at..end)
            .ok_or(DisconnectError::NoMemory)?;
        slot.copy_from_slice(properties);
        at = end;
    }

    Ok(at)
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

    fn read(body: &[u8]) -> Result<Disconnect<'_>, DisconnectError> {
        deserialize_disconnect(
            &PacketInfo {
                packet_type: 0xE0,
                remaining_length: u32::try_from(body.len()).unwrap(),
                remaining_data: body,
            },
            1024,
        )
    }

    /// The two directions' reason-code tables, against MQTT 5.0 §3.14.2.1.
    ///
    /// The outgoing one is exactly the specification's client column. The
    /// incoming one is the server column **minus `0x9F`** — a defect, pinned
    /// here as arithmetic so that a later "tidy-up" adding it would part the
    /// arms with nothing else failing.
    #[test]
    fn the_two_directions_have_different_tables_and_one_is_short() {
        const CLIENT: [u8; 14] = [
            0x00, 0x04, 0x80, 0x81, 0x82, 0x83, 0x90, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99,
        ];
        // §3.14.2.1's server column, in full. 0x9F is in it.
        const SERVER: [u8; 29] = [
            0x00, 0x80, 0x81, 0x82, 0x83, 0x87, 0x89, 0x8B, 0x8C, 0x8D, 0x8E, 0x8F, 0x90, 0x93,
            0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0xA0, 0xA1,
            0xA2,
        ];

        for code in 0..=255u8 {
            assert_eq!(
                reason_code_allowed(code, false),
                CLIENT.contains(&code),
                "outgoing {code:#04x} disagrees with MQTT 5.0 §3.14.2.1"
            );

            // The one exception, and the whole finding.
            let expected = SERVER.contains(&code) && code != 0x9F;
            assert_eq!(
                reason_code_allowed(code, true),
                expected,
                "incoming {code:#04x} is no longer what the C does"
            );
        }

        assert!(
            !reason_code_allowed(0x9F, true),
            "0x9F, 'connection rate exceeded', is now accepted from a server -- \
             the upstream defect may be fixed, and the draft in docs/upstream/ \
             needs revisiting"
        );
    }

    /// A bare DISCONNECT is three bytes, and is correct by coincidence.
    ///
    /// With no reason code the C still charges a byte for the encoded property
    /// length, so the packet is `E0 01 00`. A broker reads that third byte as
    /// the **reason code**, not a property length — and it happens to be `0x00`,
    /// "normal disconnection", which is what was meant.
    ///
    /// The two sides agree on the bytes for different reasons, which is the
    /// fragile kind of agreement. It holds only because a property length may
    /// be non-zero only when a reason code is present, so the byte is forced to
    /// zero. Pinned because a later simplification would break it silently.
    #[test]
    fn a_bare_disconnect_is_three_bytes_by_coincidence() {
        let size = disconnect_packet_size(None, 0, 1024).expect("a bare disconnect");
        assert_eq!(size.remaining_length, 1);
        assert_eq!(size.packet_size, 3);

        let mut out = [0xCCu8; 8];
        let written = serialize_disconnect(&mut out, None, &[], size.remaining_length).unwrap();

        assert_eq!(written, 3);
        assert_eq!(&out[..3], &[0xE0, 0x01, 0x00]);

        // And what the C's own reader makes of it: a reason code of zero, which
        // is what the writer meant, reached by reading a byte the writer thought
        // was a property length.
        let read_back = read(&out[2..3]).expect("our own bare disconnect");
        assert_eq!(read_back.reason_code, Some(0x00));
        assert!(read_back.properties.is_empty());
    }

    /// The size calculator and the writer agree, for every case that succeeds.
    ///
    /// The cross-slice shape the packet-size calculators introduced, applied
    /// within one module: `disconnect_packet_size` says how many bytes, and
    /// `serialize_disconnect` lays them down. Neither differential alone can
    /// catch the two disagreeing.
    #[test]
    fn the_size_and_the_writer_agree() {
        let properties: [&[u8]; 3] = [&[], &[0x1F, 0x00, 0x01, b'x'], &[0x11, 0, 0, 0x0E, 0x10]];

        for reason in [None, Some(0x00), Some(0x80), Some(0x99)] {
            for props in properties {
                if reason.is_none() && !props.is_empty() {
                    continue;
                }

                let length = u32::try_from(props.len()).unwrap();
                let Ok(size) = disconnect_packet_size(reason, length, 1024) else {
                    continue;
                };

                let mut out = [0xCCu8; 64];
                let written =
                    serialize_disconnect(&mut out, reason, props, size.remaining_length).unwrap();

                assert_eq!(
                    written as u32, size.packet_size,
                    "the calculator said {} bytes and the writer laid down {written}",
                    size.packet_size
                );
            }
        }
    }

    /// Why the up-front buffer check cannot change the answer.
    ///
    /// A poison that did not fire. The C needs the check because its next three
    /// steps are pointer writes and a `memcpy` with no bound; here the fixed
    /// header, the property length and the property copy each go through a
    /// slice, and each refuses on its own. So the answer is `NoMemory` either
    /// way, at every buffer size.
    ///
    /// Fifth appearance of the family, and the first on the WRITING side: the
    /// size calculators' in-loop check (wrapping arithmetic), the ack
    /// deserializers' property bound and the PUBLISH's three length floors
    /// (pointer arithmetic), the CONNACK's three-byte minimum (redundant in
    /// both arms), and now this.
    #[test]
    fn the_buffer_check_is_the_slice_bounds_restated() {
        let cases: [(Option<u8>, &[u8]); 4] = [
            (None, &[]),
            (Some(0x00), &[]),
            (Some(0x80), &[0x1F, 0x00, 0x01, b'x']),
            (Some(0x00), &[0x26, 0x00, 0x01, b'k', 0x00, 0x01, b'v']),
        ];

        let mut refused = 0usize;
        let mut accepted = 0usize;

        for (reason, props) in cases {
            let length = u32::try_from(props.len()).unwrap();
            let size = disconnect_packet_size(reason, length, 1024).unwrap();

            for buffer_size in 0..(size.packet_size as usize + 3) {
                let mut out = vec![0xCCu8; buffer_size];
                let result = serialize_disconnect(&mut out, reason, props, size.remaining_length);

                // The predicate the C's check tests, stated here directly.
                if buffer_size < size.packet_size as usize {
                    refused += 1;
                    assert_eq!(
                        result,
                        Err(DisconnectError::NoMemory),
                        "a {buffer_size}-byte buffer took a {}-byte packet",
                        size.packet_size
                    );
                } else {
                    accepted += 1;
                    assert_eq!(
                        result,
                        Ok(size.packet_size as usize),
                        "a {buffer_size}-byte buffer refused a {}-byte packet",
                        size.packet_size
                    );

                    // And nothing past the packet was touched.
                    assert!(
                        out[size.packet_size as usize..].iter().all(|&b| b == 0xCC),
                        "a byte past the reported packet size was written"
                    );
                }
            }
        }

        assert!(refused >= 10, "only {refused} buffers are too small");
        assert!(accepted >= 8, "only {accepted} buffers are big enough");
    }

    /// An empty incoming DISCONNECT is legal; an empty outgoing one is never
    /// produced.
    ///
    /// The asymmetry is real and worth naming: the C reads a remaining length
    /// of zero happily and never writes one.
    #[test]
    fn a_body_less_disconnect_is_read_but_never_written() {
        let empty = read(&[]).expect("an empty disconnect");
        assert_eq!(empty.reason_code, None);
        assert!(empty.properties.is_empty());

        // Nothing this module can be asked for produces a remaining length of
        // zero.
        for reason in [None, Some(0x00)] {
            let size = disconnect_packet_size(reason, 0, 1024).unwrap();
            assert!(size.remaining_length >= 1);
        }
    }

    /// The two property tables are different, and neither is a superset.
    #[test]
    fn each_direction_refuses_the_other_direction_s_property() {
        // A server reference coming IN is fine...
        let incoming = read(&[0x00, 0x06, 0x1C, 0x00, 0x03, b'a', b'.', b'b']);
        assert!(incoming.is_ok());

        // ...and going OUT it is not.
        assert_eq!(
            validate_outgoing_properties(3600, &[0x1C, 0x00, 0x03, b'a', b'.', b'b']),
            Err(DisconnectError::BadParameter)
        );

        // A session expiry going OUT is fine...
        assert!(validate_outgoing_properties(3600, &[0x11, 0x00, 0x00, 0x0E, 0x10]).is_ok());

        // ...and coming IN it is not.
        assert_eq!(
            read(&[0x00, 0x06, 0x11, 0x00, 0x00, 0x0E, 0x10]),
            Err(DisconnectError::BadResponse)
        );
    }
}
