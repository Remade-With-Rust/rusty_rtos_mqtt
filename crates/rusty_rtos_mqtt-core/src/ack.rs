//! Reading an acknowledgement off the wire.
//!
//! [`writer`](crate::writer) and [`size`](crate::size) are the outgoing half of
//! the primitive layer. This is the first *incoming* one, and the difference
//! matters: every byte here was chosen by whoever is on the other end of the
//! socket. A broker that is compromised, impersonated or simply buggy sends
//! these, and `MQTT_DeserializeAck` is the function that decides what a client
//! believes about them.
//!
//! It handles seven packet types, in three families:
//!
//! * **PUBACK, PUBREC, PUBREL, PUBCOMP** — a packet id, optionally a reason
//!   code, optionally a property section;
//! * **SUBACK, UNSUBACK** — a packet id, a property section, then one reason
//!   code per topic filter;
//! * **PINGRESP** — nothing at all, which is the whole check.
//!
//! CONNACK is deliberately *not* here: the C refuses it with `MQTTBadParameter`
//! and points the caller at `MQTT_DeserializeConnAck`, so that refusal is
//! transcribed and the CONNACK path is a later slice.
//!
//! # The claimed length and the bytes that exist are two different things
//!
//! `MQTTPacketInfo_t` carries a `pRemainingData` pointer and a `remainingLength`
//! count, and every deserializer here indexes the first using the second. They
//! agree when the receive loop filled the buffer; an attacker who can make them
//! disagree gets an out-of-bounds read out of a stock client.
//!
//! [`PacketInfo`] keeps both, deliberately, exactly as the fixed header does:
//! `remaining_length` is what the packet **claimed** and `remaining_data` is
//! what **exists**. Every read goes through `get`, so a claim larger than the
//! slice produces a refusal rather than whatever is next in memory.
//!
//! # Five of the C's refusals cannot be reached from Rust
//!
//! `MQTT_DeserializeAck` returns `MQTTBadParameter` for a null
//! `pIncomingPacket`, `pConnectProperties`, `pPacketId`, `pRemainingData` or
//! `pReasonCode`. A `&T` cannot be null and a returned value has no
//! out-parameter, so none of those five is reachable here. They are listed
//! rather than silently dropped, and the differential's driver skips them
//! rather than inventing a line one arm cannot produce.

use crate::header::{packet, variable_length_encoded_size};
use crate::property::{PropertyError, PropertyReader, decode_variable_length};

/// `MQTT_PACKET_PINGRESP_REMAINING_LENGTH`: a PINGRESP carries no body.
pub const PINGRESP_REMAINING_LENGTH: u32 = 0;

/// `MQTT_PACKET_TYPE_PUBREL` is `0x62`, not the `0x60` nibble.
///
/// [`incoming_packet_valid`](crate::header::incoming_packet_valid) masks to the
/// nibble and checks the reserved bit separately, because it is sorting all 256
/// possible bytes. This module matches one exact byte, so `0x60` falls through
/// to the unknown-type arm and is refused — which is right, and is why the
/// constant is spelled out here rather than reused.
pub const PUBREL: u8 = 0x62;

/// `MQTT_REASON_STRING_ID`.
const REASON_STRING_ID: u8 = 0x1F;
/// `MQTT_USER_PROPERTY_ID`.
const USER_PROPERTY_ID: u8 = 0x26;

/// An incoming packet, as the receive loop hands it over.
///
/// The C's `MQTTPacketInfo_t`, minus the flags field the ack path never reads.
#[derive(Debug, Clone, Copy)]
pub struct PacketInfo<'a> {
    /// The fixed header's first byte, whole — `0x62` for a PUBREL, not `0x60`.
    pub packet_type: u8,
    /// What the packet **claimed** its body was. May exceed `remaining_data`.
    pub remaining_length: u32,
    /// The bytes that actually arrived.
    pub remaining_data: &'a [u8],
}

/// The two connection properties an ack is checked against.
///
/// The C passes the whole `MQTTConnectionProperties_t`; these are the only two
/// fields `MQTT_DeserializeAck` reads, and naming them keeps the CONNACK slice
/// from being a prerequisite for this one.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// `maxPacketSize`: what this client told the broker it would accept. Zero
    /// is refused, because a client that accepts nothing cannot have connected.
    pub max_packet_size: u32,
    /// `requestProblemInfo`: whether this client asked for reason strings and
    /// user properties. If it did not, a broker that sends them anyway is
    /// committing a protocol error and the packet is refused whole.
    pub request_problem_info: bool,
}

/// What an acknowledgement turned out to contain.
///
/// The three out-parameters the C fills, returned together — so a refusal
/// cannot leave a caller holding one of them updated and the others not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckInfo<'a> {
    /// `None` only for a PINGRESP, which has no packet id. The C expresses
    /// that as "`pPacketId` may be NULL for this one type".
    pub packet_id: Option<u16>,
    /// The reason codes: none, one, or one per topic filter. Borrowed out of
    /// the packet exactly as the C's `MQTTReasonCodeInfo_t` points into it.
    pub reason_codes: &'a [u8],
    /// The property section, without its length prefix. Empty when the packet
    /// carried none.
    pub properties: &'a [u8],
}

/// Why an acknowledgement was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckError {
    /// `MQTTBadParameter`: the call itself was wrong — a CONNACK routed here,
    /// or a zero maximum packet size.
    BadParameter,
    /// `MQTTBadResponse`: the **packet** was wrong. Everything an attacker can
    /// cause lands here.
    BadResponse,
}

impl From<PropertyError> for AckError {
    fn from(_: PropertyError) -> Self {
        // `PropertyError` has one variant, and the C's property helpers answer
        // `MQTTBadResponse` for all of it.
        Self::BadResponse
    }
}

/// `MQTT_DeserializeAck`: read any acknowledgement except a CONNACK.
///
/// # Errors
///
/// [`AckError::BadParameter`] for a CONNACK (which has its own function) or a
/// zero `max_packet_size`. [`AckError::BadResponse`] for everything the packet
/// itself can get wrong: a size past the maximum, an unknown packet type, a
/// zero packet id, a body too short for its type, a malformed property
/// section, an unknown property, or a reason code this library does not know.
pub fn deserialize_ack<'a>(
    packet: &PacketInfo<'a>,
    limits: &Limits,
) -> Result<AckInfo<'a>, AckError> {
    if packet.packet_type == packet::CONNACK {
        // The C says "please use MQTT_DeserializeConnAck" and refuses. A
        // CONNACK reaching this function is a caller bug, not a bad packet,
        // which is why it is BadParameter where an unknown type is BadResponse.
        return Err(AckError::BadParameter);
    }

    if limits.max_packet_size == 0 {
        return Err(AckError::BadParameter);
    }

    // The whole packet: the body, the bytes that encode its length, and the
    // type byte. Saturating, because both additions are on an attacker-chosen
    // length where the C's `uint32_t` would wrap.
    let packet_size = packet
        .remaining_length
        .saturating_add(variable_length_encoded_size(packet.remaining_length))
        .saturating_add(1);

    if packet_size > limits.max_packet_size {
        return Err(AckError::BadResponse);
    }

    match packet.packet_type {
        packet::PUBACK | packet::PUBREC | PUBREL | packet::PUBCOMP => {
            let ack = deserialize_pub_acks(packet, limits.request_problem_info)?;

            // The C validates the reason code only when the packet carried one,
            // and only AFTER the body parsed -- so a malformed property section
            // is reported instead of an unknown reason code even though the
            // reason code byte came first. The order is observable, so it is
            // transcribed rather than tidied.
            if packet.remaining_length > 2 {
                if let Some(&code) = ack.reason_codes.first() {
                    validate_ack_reason_code(code)?;
                }
            }

            Ok(ack)
        }

        packet::SUBACK | packet::UNSUBACK => deserialize_sub_unsub_ack(packet),

        packet::PINGRESP => {
            if packet.remaining_length == PINGRESP_REMAINING_LENGTH {
                Ok(AckInfo {
                    packet_id: None,
                    reason_codes: &[],
                    properties: &[],
                })
            } else {
                Err(AckError::BadResponse)
            }
        }

        _ => Err(AckError::BadResponse),
    }
}

/// `deserializePubAcks`: a packet id, then an optional reason code and
/// properties.
///
/// The optional parts are gated on the remaining length alone: `> 2` means a
/// reason code follows, `> 3` means properties follow it. A two-byte body is a
/// bare acknowledgement and is legal.
fn deserialize_pub_acks<'a>(
    packet: &PacketInfo<'a>,
    request_problem_info: bool,
) -> Result<AckInfo<'a>, AckError> {
    if packet.remaining_length < 2 {
        return Err(AckError::BadResponse);
    }

    let packet_id = read_packet_id(packet.remaining_data)?;

    // The reason code, if the packet is long enough to carry one. The C takes a
    // pointer here and only reads through it later; this takes the byte now,
    // which is where the claimed length meets what actually arrived.
    let reason_codes = if packet.remaining_length > 2 {
        packet
            .remaining_data
            .get(2..3)
            .ok_or(AckError::BadResponse)?
    } else {
        &[]
    };

    if packet.remaining_length <= 3 {
        return Ok(AckInfo {
            packet_id: Some(packet_id),
            reason_codes,
            properties: &[],
        });
    }

    if !request_problem_info {
        // A reason string or a user property, in an ack, from a client that
        // asked for neither. The C calls this a protocol error and refuses the
        // whole packet rather than skipping the section.
        return Err(AckError::BadResponse);
    }

    // Three bytes are spent: the packet id and the reason code.
    let body_length = packet.remaining_length.saturating_sub(3);
    let body = bounded(packet.remaining_data, 3, body_length)?;
    let properties = decode_pub_ack_properties(body)?;

    Ok(AckInfo {
        packet_id: Some(packet_id),
        reason_codes,
        properties,
    })
}

/// `decodePubAckProperties`: the property section is the REST of the packet.
///
/// The check here is an equality, not a bound: because the properties are last
/// in a publish acknowledgement, the bytes left must be exactly the encoded
/// length plus the length it encodes. A shorter or longer body is malformed.
/// The SUB/UNSUBACK version cannot make that check, because reason codes follow
/// — see [`deserialize_sub_unsub_ack_properties`].
fn decode_pub_ack_properties(body: &[u8]) -> Result<&[u8], AckError> {
    let property_length = decode_variable_length(body)?;
    let encoded = variable_length_encoded_size(property_length);

    let available = u32::try_from(body.len()).map_err(|_| AckError::BadResponse)?;

    if available != property_length.saturating_add(encoded) {
        return Err(AckError::BadResponse);
    }

    let properties = bounded(body, encoded as usize, property_length)?;
    walk_ack_properties(properties, property_length)?;
    Ok(properties)
}

/// `deserializeSubUnsubAck`: a packet id, a property section, then the reason
/// codes.
fn deserialize_sub_unsub_ack<'a>(packet: &PacketInfo<'a>) -> Result<AckInfo<'a>, AckError> {
    if packet.remaining_length < 4 {
        return Err(AckError::BadResponse);
    }

    let packet_id = read_packet_id(packet.remaining_data)?;

    // Everything after the packet id: the property section and then the reason
    // codes, which is why this decoder cannot demand an exact fit.
    let after_id = bounded(
        packet.remaining_data,
        2,
        packet.remaining_length.saturating_sub(2),
    )?;

    let (properties, property_bytes) = deserialize_sub_unsub_ack_properties(after_id)?;

    // The reason codes are whatever is left. The slice cannot fail, because the
    // bound inside the property decoder already refused a section that overran.
    let reason_codes = after_id
        .get(property_bytes..)
        .ok_or(AckError::BadResponse)?;

    read_suback_status(reason_codes)?;

    Ok(AckInfo {
        packet_id: Some(packet_id),
        reason_codes,
        properties,
    })
}

/// `deserializeSubUnsubAckProperties`: the property section, and how many bytes
/// of `after_id` it took.
///
/// Returns the properties themselves and the total they occupied, because the
/// caller needs the second number to find where the reason codes start.
fn deserialize_sub_unsub_ack_properties(after_id: &[u8]) -> Result<(&[u8], usize), AckError> {
    let property_length = decode_variable_length(after_id)?;
    let encoded = variable_length_encoded_size(property_length);

    // The bound the pub-ack version states as an equality. The C compares
    // against the WHOLE remaining length, so the two bytes of packet id that
    // `after_id` has already skipped are added back rather than dropped.
    //
    // This check cannot change our answer. `after_id` is exactly the bytes the
    // C would have indexed, so `claimed > available` is the SAME PREDICATE as
    // the `bounded` call below failing -- and the C needs the check precisely
    // because it has no `bounded` call, only pointer arithmetic. It is kept
    // because the C has it, and
    // `the_sub_ack_bound_is_the_slice_bound_restated` pins the equivalence, so
    // that a later edit to either one has to answer for parting them.
    let claimed = property_length.saturating_add(encoded).saturating_add(2);
    let available = u32::try_from(after_id.len())
        .map_err(|_| AckError::BadResponse)?
        .saturating_add(2);

    if claimed > available {
        return Err(AckError::BadResponse);
    }

    let properties = bounded(after_id, encoded as usize, property_length)?;
    walk_ack_properties(properties, property_length)?;

    Ok((
        properties,
        (encoded as usize).saturating_add(properties.len()),
    ))
}

/// The property walk both ack families share: a reason string, any number of
/// user properties, and nothing else.
///
/// An acknowledgement may carry exactly two kinds of property. Anything else is
/// `MQTTBadResponse` — the C does not skip unknown properties, and skipping
/// them would let a broker hide bytes inside a packet a client believes it has
/// fully parsed.
fn walk_ack_properties(properties: &[u8], property_length: u32) -> Result<(), AckError> {
    let mut reader = PropertyReader::new(properties, property_length);
    let mut reason_string_seen = false;

    while reader.remaining() > 0 {
        let id = reader.property_id()?;

        match id {
            REASON_STRING_ID => {
                let _ = reader.utf8(&mut reason_string_seen)?;
            }
            USER_PROPERTY_ID => {
                let _ = reader.user_property()?;
            }
            _ => return Err(AckError::BadResponse),
        }
    }

    Ok(())
}

/// `readSubackStatus`: every reason code must be one this library knows.
///
/// # The two acks share one table, and the specification does not
///
/// coreMQTT checks SUBACK and UNSUBACK reason codes against the same list, and
/// that list is the SUBACK one. It accepts `0x00`, `0x01` and `0x02` — granted
/// QoS values, which are meaningless in an UNSUBACK — and it does **not**
/// accept `0x11`, "No subscription existed", which MQTT 5.0 §3.11.3 lists as a
/// legal UNSUBACK reason code.
///
/// So a stock client refuses a specification-conformant UNSUBACK from a broker
/// reporting an unsubscribe for a filter that was not subscribed. The behaviour
/// is transcribed exactly, because this is the oracle; it is written up in
/// `docs/upstream/` for the owner to file.
fn read_suback_status(reason_codes: &[u8]) -> Result<(), AckError> {
    for &code in reason_codes {
        match code {
            // Granted QoS 0, 1 and 2. The C lists them as three cases; they
            // happen to be contiguous, and the range says the same thing.
            0x00..=0x02 => {}
            // Refusals, in the order the C lists them.
            0x80 | 0x83 | 0x87 | 0x8F | 0x91 | 0x97 | 0x9E | 0xA1 | 0xA2 => {}
            _ => return Err(AckError::BadResponse),
        }
    }

    Ok(())
}

/// `logAckResponse`: the reason codes a publish acknowledgement may carry.
///
/// The C's function exists to log, and returns `MQTTBadResponse` for anything
/// it has no message for — so the log table IS the validation table. Ten bytes
/// of 256 are accepted, and `0x92` ("packet identifier not found") is among
/// them for **all four** publish acks, though MQTT 5.0 allows it only in a
/// PUBREL and a PUBCOMP.
fn validate_ack_reason_code(code: u8) -> Result<(), AckError> {
    match code {
        0x00 | 0x10 | 0x80 | 0x83 | 0x87 | 0x90 | 0x91 | 0x92 | 0x97 | 0x99 => Ok(()),
        _ => Err(AckError::BadResponse),
    }
}

/// The first two bytes, big-endian, refusing zero.
///
/// A packet id of zero is reserved: an ack carrying it cannot be matched to
/// anything in flight, so the C refuses it rather than searching for a record
/// that cannot exist.
fn read_packet_id(remaining_data: &[u8]) -> Result<u16, AckError> {
    let bytes = remaining_data.get(..2).ok_or(AckError::BadResponse)?;
    let Ok(array) = <[u8; 2]>::try_from(bytes) else {
        return Err(AckError::BadResponse);
    };
    let id = u16::from_be_bytes(array);

    if id == 0 {
        return Err(AckError::BadResponse);
    }

    Ok(id)
}

/// `length` bytes starting at `from`, if the buffer really has them.
///
/// The C computes the same slice by pointer arithmetic on a length it was
/// handed. This is the one place the transcription is strictly safer rather
/// than equal: a claimed length larger than the bytes received is a refusal
/// here and an out-of-bounds read there.
fn bounded(buffer: &[u8], from: usize, length: u32) -> Result<&[u8], AckError> {
    let length = usize::try_from(length).map_err(|_| AckError::BadResponse)?;
    let end = from.checked_add(length).ok_or(AckError::BadResponse)?;
    buffer.get(from..end).ok_or(AckError::BadResponse)
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
    use crate::header::variable_length_encoded_size;

    fn limits() -> Limits {
        Limits {
            max_packet_size: 1024,
            request_problem_info: true,
        }
    }

    fn info<'a>(packet_type: u8, body: &'a [u8]) -> PacketInfo<'a> {
        PacketInfo {
            packet_type,
            remaining_length: u32::try_from(body.len()).unwrap(),
            remaining_data: body,
        }
    }

    /// A PUBREL is `0x62` here, and `0x60` is not a PUBREL.
    ///
    /// The header codec masks to the nibble and checks the reserved bit
    /// separately; this module matches the byte. Reusing the nibble constant
    /// would silently accept a PUBREL with its reserved bit clear, which is
    /// exactly the malformed packet the header codec exists to refuse.
    #[test]
    fn a_pubrel_needs_its_reserved_bit() {
        let body = [0x00, 0x2A];

        assert!(deserialize_ack(&info(PUBREL, &body), &limits()).is_ok());
        assert_eq!(
            deserialize_ack(&info(packet::PUBREL_NIBBLE, &body), &limits()),
            Err(AckError::BadResponse),
            "0x60 is a PUBREL with its reserved bit clear and must not be read as one"
        );
    }

    /// A claimed length larger than the bytes received is refused, not read.
    ///
    /// This is the guarantee the C cannot make: `pRemainingData` is a pointer
    /// and `remainingLength` is trusted, so the same input reads past the
    /// allocation there. The third instance in this package, after the fixed
    /// header's byte count and the property reader's budget.
    #[test]
    fn a_claim_larger_than_the_buffer_is_refused() {
        let body = [0x00, 0x2A, 0x00];

        for claimed in 4u32..64 {
            let packet = PacketInfo {
                packet_type: packet::PUBACK,
                remaining_length: claimed,
                remaining_data: &body,
            };

            assert_eq!(
                deserialize_ack(&packet, &limits()),
                Err(AckError::BadResponse),
                "a claim of {claimed} bytes over a {}-byte buffer was accepted",
                body.len()
            );
        }
    }

    /// The reason-code tables are what they are, and each is wrong once.
    ///
    /// Pinned as arithmetic rather than left to the sweep, because these are
    /// the two divergences from MQTT 5.0 this slice found, and a later tidy-up
    /// that "fixed" either of them would part the arms with nothing else
    /// failing.
    #[test]
    fn the_two_reason_code_tables_diverge_from_the_specification() {
        // An UNSUBACK may say "no subscription existed" (0x11). coreMQTT
        // refuses it, because it checks UNSUBACK codes against the SUBACK
        // table.
        assert_eq!(read_suback_status(&[0x11]), Err(AckError::BadResponse));

        // And that same table accepts granted-QoS bytes, which an UNSUBACK has
        // no business carrying.
        assert_eq!(read_suback_status(&[0x02]), Ok(()));

        // A PUBACK may not say "packet identifier not found" (0x92) -- that is
        // a PUBREL and PUBCOMP code -- but the shared log table accepts it.
        assert_eq!(validate_ack_reason_code(0x92), Ok(()));
    }

    /// A bare two-byte acknowledgement is legal and carries no reason code.
    #[test]
    fn a_two_byte_ack_is_a_success_with_nothing_in_it() {
        let ack = deserialize_ack(&info(packet::PUBACK, &[0x12, 0x34]), &limits()).unwrap();

        assert_eq!(ack.packet_id, Some(0x1234));
        assert!(ack.reason_codes.is_empty());
        assert!(ack.properties.is_empty());
    }

    /// Why the SUB/UNSUBACK property bound cannot change our answer.
    ///
    /// This started as a poison that did not fire: dropping the two bytes of
    /// packet id from the C's bound made the check looser by two, and the run
    /// still agreed with the C line for line.
    ///
    /// The reason is that the two arms are checking the same thing by
    /// different means. The C's bound exists because the next thing it does is
    /// pointer arithmetic with no bound at all; ours is followed by a slice,
    /// and the slice fails on exactly the inputs the bound rejects. The check
    /// is kept for fidelity, and this test is what makes the redundancy a
    /// recorded fact rather than an accident.
    ///
    /// Second member of a family the size calculators started: **a check that
    /// is load-bearing in the C can be subsumed in the transcription**, there
    /// because the arithmetic wraps, here because the C has no bounds check to
    /// begin with.
    ///
    /// Note what this test does NOT do: it does not fail when the check is
    /// loosened in the function above. It cannot, and it should not — the
    /// whole content of the finding is that loosening it changes no answer.
    /// What it fails on is the day that stops being true.
    #[test]
    fn the_sub_ack_bound_is_the_slice_bound_restated() {
        for length in 0usize..24 {
            let after_id = vec![0u8; length];

            for property_length in 0u32..32 {
                let encoded = variable_length_encoded_size(property_length);

                // What the C's check says.
                let claimed = property_length.saturating_add(encoded).saturating_add(2);
                let available = u32::try_from(after_id.len()).unwrap().saturating_add(2);
                let bound_refuses = claimed > available;

                // What taking the slice says.
                let slice_refuses = bounded(&after_id, encoded as usize, property_length).is_err();

                assert_eq!(
                    bound_refuses, slice_refuses,
                    "the two disagree at length {length}, property length                      {property_length} -- the C's bound is no longer a                      restatement of the slice, so removing it would change an                      answer"
                );
            }
        }
    }

    /// A PINGRESP is the absence of a body, and nothing else.
    #[test]
    fn a_pingresp_must_be_empty() {
        assert!(deserialize_ack(&info(packet::PINGRESP, &[]), &limits()).is_ok());
        assert_eq!(
            deserialize_ack(&info(packet::PINGRESP, &[0x00]), &limits()),
            Err(AckError::BadResponse)
        );
    }
}
