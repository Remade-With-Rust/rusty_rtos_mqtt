//! Building a CONNECT — the packet that starts a session.
//!
//! This is the largest thing a client assembles, and the only one whose fixed
//! header has to **agree with its payload**: three bits of the flags byte say
//! whether a will, a user name and a password are present, and the payload must
//! then carry exactly those, in that order, each length-prefixed.
//!
//! [`writer::serialize_connect_fixed_header`](crate::writer::serialize_connect_fixed_header)
//! writes that flags byte and was proven on its own over an exhaustive sweep.
//! This module writes everything after it, and the two are proven **together**:
//! a flags byte that says "there is a will" and a payload that omits one is a
//! packet a broker will reject, and neither half alone can see it.
//!
//! # Absent and empty are different
//!
//! The C distinguishes a NULL `pUserName` from a non-NULL one of length zero:
//! the first clears a flag bit and writes nothing, the second sets the bit and
//! writes two zero bytes. [`ConnectInfo`](crate::writer::ConnectInfo) already
//! models that with `Option<&[u8]>`, and it is the reason it does.
//!
//! The *property* sections are different again: the C treats a NULL builder and
//! an empty one identically, both writing a single `0x00` length byte. So they
//! are plain slices here, and nothing is lost.
//!
//! # One of the C's refusals cannot be reached
//!
//! `MQTT_GetConnectPacketSize` refuses a `pClientIdentifier` whose NULL-ness
//! disagrees with `clientIdentifierLength`. A `&[u8]` carries its own length so
//! the two cannot disagree — the sixth member of the family
//! [`ack`](crate::ack) started listing.
//!
//! The *other* half of that check is reachable, and surprising: it reads
//! `*pClientIdentifier == '\0'`, so **a client identifier whose first byte is
//! NUL is refused**, whatever its length. A NUL anywhere else is accepted. See
//! [`connect_packet_size`].

use crate::header::{MAX_REMAINING_LENGTH, encode_variable_length, variable_length_encoded_size};
use crate::property::encode_string;

use crate::writer::{ConnectInfo, WillInfo, serialize_connect_fixed_header};

/// `MQTT_PACKET_CONNECT_HEADER_SIZE`: the variable header before the payload.
///
/// Two length bytes and four of `MQTT`, the protocol version, the flags byte
/// and two of keep alive.
pub const CONNECT_HEADER_SIZE: u32 = 10;

/// The largest a length-prefixed CONNECT field may be.
const MAX_FIELD: usize = u16::MAX as usize;

/// A Last Will and Testament, whole.
///
/// [`WillInfo`] is the part the flags byte needs; this adds the three fields
/// that go in the payload.
#[derive(Debug, Clone, Copy)]
pub struct Will<'a> {
    /// The QoS and retain flag, which live in the CONNECT's flags byte.
    pub info: WillInfo,
    /// Where the broker should publish it.
    pub topic_name: &'a [u8],
    /// What it should say. May be empty, which is a will with no message.
    pub payload: &'a [u8],
    /// The will's own property section, without its length prefix.
    pub properties: &'a [u8],
}

/// Everything a CONNECT carries.
#[derive(Debug, Clone, Copy)]
pub struct Connect<'a> {
    /// Clean session, keep alive, and the user name and password — the parts
    /// the flags byte needs.
    pub info: ConnectInfo<'a>,
    /// The client identifier. May be empty, which asks the broker to assign
    /// one and return it in the CONNACK.
    pub client_identifier: &'a [u8],
    /// The CONNECT's property section, without its length prefix.
    pub properties: &'a [u8],
    /// The Last Will and Testament, if there is one.
    pub will: Option<Will<'a>>,
}

/// The two numbers an outgoing CONNECT needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectSize {
    /// What goes in the fixed header.
    pub remaining_length: u32,
    /// The whole packet, header included.
    pub packet_size: u32,
}

/// Why a CONNECT could not be built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectError {
    /// `MQTTBadParameter`: a field too long for its 16-bit prefix, a property
    /// section past MQTT's maximum, a total past it, or a client identifier
    /// beginning with NUL.
    BadParameter,
    /// `MQTTNoMemory`: the caller's buffer is smaller than the packet.
    NoMemory,
}

/// `MQTT_GetConnectPacketSize`.
///
/// # The client identifier may not begin with NUL
///
/// The C's check is one expression meant to catch a length/pointer mismatch:
///
/// ```c
/// ( pConnectInfo->clientIdentifierLength == 0U ) !=
///     ( ( pConnectInfo->pClientIdentifier == NULL ) ||
///       ( *( pConnectInfo->pClientIdentifier ) == '\0' ) )
/// ```
///
/// The NULL half cannot happen here. The `'\0'` half can, and it means a
/// non-empty identifier whose **first** byte is zero is refused while one with
/// a zero anywhere else is accepted. MQTT 5.0 §1.5.4 forbids U+0000 anywhere in
/// a UTF-8 string, so refusing is defensible — but the C refuses only the first
/// byte, and by accident. Transcribed, and pinned by
/// `a_client_id_may_not_begin_with_nul`.
///
/// # Errors
///
/// [`ConnectError::BadParameter`] for a client identifier beginning with NUL,
/// any of the five length-prefixed fields past 65,535, either property section
/// past [`MAX_REMAINING_LENGTH`], or a remaining length past it.
pub fn connect_packet_size(connect: &Connect<'_>) -> Result<ConnectSize, ConnectError> {
    // The C's order, which is observable: a CONNECT can be wrong in several
    // ways at once and it reports the first.
    if let Some(&first) = connect.client_identifier.first() {
        if first == 0 {
            return Err(ConnectError::BadParameter);
        }
    }

    if connect.client_identifier.len() > MAX_FIELD {
        return Err(ConnectError::BadParameter);
    }

    if connect.info.username.is_some_and(|u| u.len() > MAX_FIELD) {
        return Err(ConnectError::BadParameter);
    }

    if connect.info.password.is_some_and(|p| p.len() > MAX_FIELD) {
        return Err(ConnectError::BadParameter);
    }

    if let Some(will) = connect.will.as_ref() {
        if will.payload.len() > MAX_FIELD || will.topic_name.len() > MAX_FIELD {
            return Err(ConnectError::BadParameter);
        }
    }

    let property_length = section_length(connect.properties)?;
    let will_property_length = match connect.will.as_ref() {
        Some(will) => section_length(will.properties)?,
        None => 0,
    };

    // Ten bytes of variable header, then every length-prefixed field that is
    // present. Saturating throughout: the C's comment argues at length that its
    // additions cannot overflow given the checks above, and saturating means
    // the argument does not have to be re-checked every time a field is added.
    let mut length = CONNECT_HEADER_SIZE
        .saturating_add(property_length)
        .saturating_add(variable_length_encoded_size(property_length))
        .saturating_add(field(connect.client_identifier.len()));

    if let Some(will) = connect.will.as_ref() {
        length = length
            .saturating_add(will_property_length)
            .saturating_add(variable_length_encoded_size(will_property_length))
            .saturating_add(field(will.topic_name.len()))
            .saturating_add(field(will.payload.len()));
    }

    if let Some(username) = connect.info.username {
        length = length.saturating_add(field(username.len()));
    }

    if let Some(password) = connect.info.password {
        length = length.saturating_add(field(password.len()));
    }

    // `>` and not `>=`: a remaining length of exactly 268,435,455 is allowed
    // here, where the DISCONNECT's calculator refuses it. The two are one line
    // apart in the same file and disagree; transcribed as each has it.
    //
    // The differential CANNOT reach this check, and the reason is the point.
    // In the C it is load-bearing against a property builder that lies: the
    // function reads `currentIndex` and never touches `pBuffer`, so a caller
    // can claim 268 million bytes while pointing at eight, and the driver did
    // exactly that before the cases were withdrawn. **A `&[u8]` cannot make
    // that claim** -- its length is its data -- so the entire class of input
    // that makes the check matter does not exist on this side. What is left is
    // a caller genuinely holding 268 MB of property bytes, which is a 64-bit
    // host's problem and not a microcontroller's.
    //
    // The check stays, and `the_remaining_length_limit_is_an_exact_boundary`
    // pins the operator, which is the part that differs from the DISCONNECT's.
    if !within_remaining_length(length) {
        return Err(ConnectError::BadParameter);
    }

    Ok(ConnectSize {
        remaining_length: length,
        packet_size: length
            .saturating_add(variable_length_encoded_size(length))
            .saturating_add(1),
    })
}

/// The CONNECT's own limit: `<=`, where the DISCONNECT's calculator uses `<`.
///
/// Named so the boundary can be asserted, since the differential cannot reach
/// it — see the note at the call site.
const fn within_remaining_length(length: u32) -> bool {
    length <= MAX_REMAINING_LENGTH
}

/// A property section's length, refused if it will not fit a variable integer.
fn section_length(properties: &[u8]) -> Result<u32, ConnectError> {
    let length = u32::try_from(properties.len()).map_err(|_| ConnectError::BadParameter)?;

    if length > MAX_REMAINING_LENGTH {
        return Err(ConnectError::BadParameter);
    }

    Ok(length)
}

/// A length-prefixed field costs its bytes plus two.
fn field(length: usize) -> u32 {
    u32::try_from(length).unwrap_or(u32::MAX).saturating_add(2)
}

/// `MQTT_SerializeConnect` and the `serializeConnectPacket` it calls.
///
/// Returns how many bytes were written. `remaining_length` comes from
/// [`connect_packet_size`] — the C does not recompute it and says so in a
/// comment ("part of the API contract to call `MQTT_GetConnectPacketSize()`
/// before this function"), so a caller who passes a different number gets a
/// packet that disagrees with itself. Reproduced, and
/// `the_size_and_the_serializer_agree` is what keeps *our* two in step.
///
/// # Errors
///
/// [`ConnectError::BadParameter`] for any of the five fields past 65,535.
/// [`ConnectError::NoMemory`] if `destination` is smaller than the packet.
pub fn serialize_connect(
    destination: &mut [u8],
    connect: &Connect<'_>,
    remaining_length: u32,
) -> Result<usize, ConnectError> {
    // The C re-checks the 16-bit limits here rather than trusting the size
    // call, and does NOT re-check the client identifier or the property
    // lengths. Transcribed: the asymmetry is the C's, not ours.
    if connect.client_identifier.len() > MAX_FIELD
        || connect.info.username.is_some_and(|u| u.len() > MAX_FIELD)
        || connect.info.password.is_some_and(|p| p.len() > MAX_FIELD)
        || connect
            .will
            .as_ref()
            .is_some_and(|w| w.topic_name.len() > MAX_FIELD)
    {
        return Err(ConnectError::BadParameter);
    }

    let packet_size = remaining_length
        .saturating_add(variable_length_encoded_size(remaining_length))
        .saturating_add(1);

    // As in `disconnect::serialize_disconnect`, this is the same predicate as
    // the slices below, because the C has pointer writes and a `memcpy` where
    // this has bounded writes. Sixth appearance of the family; poisoning it
    // changes no answer, and `the_buffer_check_is_the_slice_bounds_restated`
    // walks every buffer size to say so.
    if (destination.len() as u64) < u64::from(packet_size) {
        return Err(ConnectError::NoMemory);
    }

    let will_info = connect.will.as_ref().map(|will| will.info);
    let mut at = serialize_connect_fixed_header(
        destination,
        &connect.info,
        will_info.as_ref(),
        remaining_length,
    );

    if at == 0 {
        return Err(ConnectError::NoMemory);
    }

    at = put_section(destination, at, connect.properties)?;
    at = put_string(destination, at, connect.client_identifier)?;

    if let Some(will) = connect.will.as_ref() {
        // The will's property section comes BEFORE its topic, which is the one
        // ordering in this packet that is easy to get backwards -- both are
        // length-prefixed, so a swap still parses and produces a will published
        // to the wrong topic.
        at = put_section(destination, at, will.properties)?;
        at = put_string(destination, at, will.topic_name)?;
        at = put_string(destination, at, will.payload)?;
    }

    if let Some(username) = connect.info.username {
        at = put_string(destination, at, username)?;
    }

    if let Some(password) = connect.info.password {
        at = put_string(destination, at, password)?;
    }

    Ok(at)
}

/// A property section: its variable-byte length, then its bytes.
fn put_section(destination: &mut [u8], at: usize, section: &[u8]) -> Result<usize, ConnectError> {
    let length = section_length(section)?;
    let rest = destination.get_mut(at..).ok_or(ConnectError::NoMemory)?;
    let encoded = encode_variable_length(rest, length);

    if encoded == 0 {
        return Err(ConnectError::NoMemory);
    }

    let body_at = at.saturating_add(encoded);

    if section.is_empty() {
        return Ok(body_at);
    }

    let end = body_at.saturating_add(section.len());
    let slot = destination
        .get_mut(body_at..end)
        .ok_or(ConnectError::NoMemory)?;
    slot.copy_from_slice(section);
    Ok(end)
}

/// `encodeString`: a two-byte big-endian length, then the bytes.
fn put_string(destination: &mut [u8], at: usize, value: &[u8]) -> Result<usize, ConnectError> {
    let length = u16::try_from(value.len()).map_err(|_| ConnectError::BadParameter)?;
    let rest = destination.get_mut(at..).ok_or(ConnectError::NoMemory)?;
    let written = encode_string(rest, Some(value), length);

    // `encode_string` answers 0 for "it did not fit", and a zero-length string
    // still costs its two length bytes, so 0 always means no room.
    if written == 0 {
        return Err(ConnectError::NoMemory);
    }

    Ok(at.saturating_add(written))
}

/// The protocol name and version every CONNECT begins with, for callers that
/// want to check a packet by eye.
///
/// `0x00 0x04 'M' 'Q' 'T' 'T' 0x05`.
pub const PROTOCOL_PREAMBLE: [u8; 7] = [0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05];

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
    use crate::state::QoS;

    fn plain<'a>(client_identifier: &'a [u8]) -> Connect<'a> {
        Connect {
            info: ConnectInfo {
                clean_session: true,
                keep_alive_seconds: 60,
                username: None,
                password: None,
            },
            client_identifier,
            properties: &[],
            will: None,
        }
    }

    /// A client identifier may not BEGIN with NUL, and may contain one.
    ///
    /// The C's check is one expression meant to catch a length/pointer
    /// mismatch, and this is its other effect: it dereferences the pointer and
    /// compares the first byte with `'\0'`. MQTT 5.0 §1.5.4 forbids U+0000
    /// anywhere in a UTF-8 string, so the refusal is defensible — but it is
    /// only the first byte, and it is incidental.
    #[test]
    fn a_client_id_may_not_begin_with_nul() {
        assert_eq!(
            connect_packet_size(&plain(&[0x00, b'b', b'c'])),
            Err(ConnectError::BadParameter)
        );

        // A NUL anywhere else is fine, which is what makes it incidental
        // rather than a UTF-8 check.
        assert!(connect_packet_size(&plain(&[b'a', 0x00, b'c'])).is_ok());
        assert!(connect_packet_size(&plain(&[b'a', b'b', 0x00])).is_ok());

        // And an EMPTY identifier is legal: §3.1.3.1 has the server assign one.
        assert!(connect_packet_size(&plain(&[])).is_ok());
    }

    /// The remaining-length limit is an EXACT boundary, and `>` not `>=`.
    ///
    /// The differential cannot reach this check — see the note at the call
    /// site — so the operator is pinned here instead. It matters because
    /// [`disconnect::disconnect_packet_size`](crate::disconnect::disconnect_packet_size)
    /// uses `>=` for the same limit, one function away in the same C file. Two
    /// calculators in one library disagreeing about whether 268,435,455 is
    /// legal is the C's, not ours, and a tidy-up that made them agree would be
    /// wrong in one of the two.
    #[test]
    fn the_remaining_length_limit_is_an_exact_boundary() {
        // The arithmetic a CONNECT with a two-byte client id and no other
        // field comes to: ten of variable header, the encoded property length,
        // and two plus the identifier.
        let overhead = CONNECT_HEADER_SIZE + 1 + 2 + 2;
        assert_eq!(overhead, 15);

        // Exactly at the maximum is allowed, and one more is not. This is the
        // operator itself: `<=` and not `<`.
        assert!(within_remaining_length(MAX_REMAINING_LENGTH));
        assert!(!within_remaining_length(MAX_REMAINING_LENGTH + 1));
        assert!(within_remaining_length(MAX_REMAINING_LENGTH - 1));

        // ...and the DISCONNECT's calculator refuses a property length of the
        // same value, which is the disagreement worth remembering.
        assert_eq!(
            crate::disconnect::disconnect_packet_size(Some(0x00), MAX_REMAINING_LENGTH, u32::MAX),
            Err(crate::disconnect::DisconnectError::BadParameter),
            "the DISCONNECT calculator now accepts a property length the \
             CONNECT's would too -- the two used to disagree, and one of them \
             has changed"
        );
    }

    /// Why the up-front buffer check cannot change the answer.
    ///
    /// A poison that did not fire, for the fifth time in this package and the
    /// second on the writing side. Every write below it goes through a bounded
    /// slice, so each refuses on its own; the C needs the check because its
    /// next several steps are pointer writes and a `memcpy`.
    #[test]
    fn the_buffer_check_is_the_slice_bounds_restated() {
        let connect = Connect {
            info: ConnectInfo {
                clean_session: true,
                keep_alive_seconds: 60,
                username: Some(b"u"),
                password: Some(b"pw"),
            },
            client_identifier: b"id",
            properties: &[0x11, 0, 0, 0, 1],
            will: Some(Will {
                info: WillInfo {
                    qos: QoS::AtLeastOnce,
                    retain: true,
                },
                topic_name: b"w/t",
                payload: b"bye",
                properties: &[],
            }),
        };

        let size = connect_packet_size(&connect).unwrap();
        let (mut refused, mut accepted) = (0usize, 0usize);

        for buffer_size in 0..(size.packet_size as usize + 3) {
            let mut out = vec![0xCCu8; buffer_size];
            let result = serialize_connect(&mut out, &connect, size.remaining_length);

            if buffer_size < size.packet_size as usize {
                refused += 1;
                assert_eq!(
                    result,
                    Err(ConnectError::NoMemory),
                    "a {buffer_size}-byte buffer took a {}-byte packet",
                    size.packet_size
                );
            } else {
                accepted += 1;
                assert_eq!(result, Ok(size.packet_size as usize));
                assert!(
                    out[size.packet_size as usize..].iter().all(|&b| b == 0xCC),
                    "a byte past the reported packet size was written"
                );
            }
        }

        assert!(refused >= 20, "only {refused} buffers are too small");
        assert!(accepted >= 3, "only {accepted} buffers are big enough");
    }

    /// The size calculator and the serializer agree, over every shape.
    ///
    /// The C does not recompute the remaining length in the serializer and says
    /// so in a comment, which makes the two functions a contract rather than a
    /// computation. This is the test that keeps our end of it.
    #[test]
    fn the_size_and_the_serializer_agree() {
        let sections: [&[u8]; 3] = [&[], &[0x11, 0, 0, 0, 1], &[0x21, 0x00, 0x14]];
        let optional: [Option<&[u8]>; 3] = [None, Some(&[]), Some(b"user")];

        let mut shapes = 0usize;

        for username in optional {
            for password in optional {
                for properties in sections {
                    for will_properties in sections {
                        for will_qos in [None, Some(QoS::AtMostOnce), Some(QoS::ExactlyOnce)] {
                            let will = will_qos.map(|qos| Will {
                                info: WillInfo { qos, retain: true },
                                topic_name: b"w/t",
                                payload: b"bye",
                                properties: will_properties,
                            });

                            let connect = Connect {
                                info: ConnectInfo {
                                    clean_session: true,
                                    keep_alive_seconds: 60,
                                    username,
                                    password,
                                },
                                client_identifier: b"id",
                                properties,
                                will,
                            };

                            let size = connect_packet_size(&connect).expect("a small connect");
                            let mut out = vec![0xCCu8; size.packet_size as usize + 4];
                            let written =
                                serialize_connect(&mut out, &connect, size.remaining_length)
                                    .expect("room");

                            assert_eq!(
                                written as u32, size.packet_size,
                                "the calculator said {} bytes and the serializer laid down \
                                 {written}",
                                size.packet_size
                            );

                            // Nothing past the reported size was touched.
                            assert!(
                                out[written..].iter().all(|&b| b == 0xCC),
                                "a byte past the packet was written"
                            );

                            // And every packet begins the same way.
                            assert_eq!(&out[2..9], &PROTOCOL_PREAMBLE);

                            shapes += 1;
                        }
                    }
                }
            }
        }

        assert_eq!(shapes, 3 * 3 * 3 * 3 * 3, "the shape space shrank");
    }

    /// The flags byte and the payload have to agree, and only together.
    ///
    /// The writers' slice swept the flags byte exhaustively and proved every
    /// bit; it could not prove that the payload then carries what the bits
    /// claim. This walks the three optional fields and checks the packet's
    /// LENGTH against the bits actually set — a field written without its bit,
    /// or a bit set without its field, moves one and not the other.
    #[test]
    fn the_flags_byte_and_the_payload_agree() {
        for username in [None, Some(&b"u"[..])] {
            for password in [None, Some(&b"pw"[..])] {
                for will in [None, Some(QoS::AtLeastOnce)] {
                    let connect = Connect {
                        info: ConnectInfo {
                            clean_session: true,
                            keep_alive_seconds: 60,
                            username,
                            password,
                        },
                        client_identifier: b"id",
                        properties: &[],
                        will: will.map(|qos| Will {
                            info: WillInfo { qos, retain: false },
                            topic_name: b"w/t",
                            payload: b"bye",
                            properties: &[],
                        }),
                    };

                    let size = connect_packet_size(&connect).unwrap();
                    let mut out = vec![0xCCu8; 64];
                    serialize_connect(&mut out, &connect, size.remaining_length).unwrap();

                    // The flags byte is the tenth of the packet: type, one
                    // length byte, six of protocol name, one version.
                    let flags = out[9];

                    assert_eq!(
                        flags & 0x80 != 0,
                        username.is_some(),
                        "the user name bit disagrees with the payload"
                    );
                    assert_eq!(
                        flags & 0x40 != 0,
                        password.is_some(),
                        "the password bit disagrees with the payload"
                    );
                    assert_eq!(
                        flags & 0x04 != 0,
                        will.is_some(),
                        "the will bit disagrees with the payload"
                    );

                    // And the length moves by exactly what each field costs.
                    let expected = 10
                        + 1
                        + 4
                        + username.map_or(0, |u| u.len() + 2)
                        + password.map_or(0, |p| p.len() + 2)
                        + will.map_or(0, |_| 1 + 5 + 5);

                    assert_eq!(size.remaining_length as usize, expected);
                }
            }
        }
    }
}
