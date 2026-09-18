//! Reading an incoming PUBLISH — the packet that carries application data.
//!
//! The acknowledgements and the CONNACK are protocol bookkeeping a library
//! consumes. A PUBLISH is **handed to the application**, so its topic name, its
//! payload length and its property section are the numbers somebody else's code
//! will index with. A wrong length here is not a dropped packet; it is a buffer
//! overrun one layer up, in code that trusted this one.
//!
//! It is also the only packet whose type byte is partly *data*: the low nibble
//! carries DUP, QoS and RETAIN, so the first byte off the socket is four more
//! inputs rather than a constant. That is why
//! [`deserialize_publish`] matches the high nibble where [`ack`](crate::ack)
//! matches whole bytes — and why the flags are swept in the differential.
//!
//! # QoS decides the shape of the body
//!
//! A QoS 0 PUBLISH is topic length, topic, property length, properties,
//! payload. A QoS 1 or 2 PUBLISH has a **packet identifier** between the topic
//! and the properties. Everything downstream of that — where the properties
//! start, how long the payload is — moves by two bytes depending on two bits of
//! the first byte.
//!
//! So the payload length is a subtraction with four terms, and the checks that
//! keep it from going negative are the whole safety argument of this module.
//! The C does that subtraction on a `uint32_t`; ours saturates, and a test pins
//! that the two agree because the checks make the difference unreachable.
//!
//! # Where this disagrees with MQTT 5.0
//!
//! Two of the specification's protocol errors are **not** errors here, because
//! they are not errors in the oracle: a zero-length topic name with no Topic
//! Alias, and a Subscription Identifier of zero. Both are transcribed exactly
//! and written up in `docs/upstream/` for filing. See the notes on
//! [`PublishInfo::topic_name`] and the property walk.

use crate::ack::{AckError, PacketInfo, bounded};
use crate::header::{REMAINING_LENGTH_INVALID, packet, variable_length_encoded_size};
use crate::property::{PropertyError, PropertyReader, decode_variable_length};
use crate::state::QoS;

/// Bit positions in the PUBLISH flags nibble.
pub mod flag {
    /// `MQTT_PUBLISH_FLAG_RETAIN`.
    pub const RETAIN: u8 = 0;
    /// `MQTT_PUBLISH_FLAG_QOS1`.
    pub const QOS1: u8 = 1;
    /// `MQTT_PUBLISH_FLAG_QOS2`.
    pub const QOS2: u8 = 2;
    /// `MQTT_PUBLISH_FLAG_DUP`.
    pub const DUP: u8 = 3;
}

/// The property identifiers a PUBLISH may carry (MQTT 5.0 §3.3.2.3).
pub mod property {
    /// Payload Format Indicator: 0 or 1, and nothing else.
    pub const PAYLOAD_FORMAT: u8 = 0x01;
    /// Message Expiry Interval, four bytes.
    pub const MESSAGE_EXPIRY: u8 = 0x02;
    /// Content Type, a string.
    pub const CONTENT_TYPE: u8 = 0x03;
    /// Response Topic, a string.
    pub const RESPONSE_TOPIC: u8 = 0x08;
    /// Correlation Data. Binary data per the specification, read as a string —
    /// identical wire format, and nothing validates UTF-8 in either arm.
    pub const CORRELATION_DATA: u8 = 0x09;
    /// Subscription Identifier, a VARIABLE-length integer. The only property in
    /// the library that is one, and the only one a PUBLISH may repeat.
    pub const SUBSCRIPTION_ID: u8 = 0x0B;
    /// Topic Alias, two bytes. Non-zero, and at most the client's maximum.
    pub const TOPIC_ALIAS: u8 = 0x23;
    /// User Property, two strings. May repeat.
    pub const USER_PROPERTY: u8 = 0x26;
}

/// An incoming PUBLISH, read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishInfo<'a> {
    /// The delivery guarantee, from two bits of the type byte.
    pub qos: QoS,
    /// A re-delivery of an earlier attempt.
    ///
    /// MQTT 5.0 [MQTT-3.3.1-2] says a sender MUST clear this for QoS 0, and the
    /// C does not check that a broker did. Transcribed, and recorded.
    pub dup: bool,
    /// The broker is delivering this as a retained message.
    pub retain: bool,
    /// Present only at QoS 1 and 2; a QoS 0 PUBLISH carries none.
    pub packet_id: Option<u16>,
    /// The topic this was published to.
    ///
    /// **May be empty.** MQTT 5.0 §3.3.2.3.4 makes a zero-length topic name a
    /// protocol error unless a Topic Alias is present, and the C enforces no
    /// such link — it accepts an empty topic with or without one. Reproduced
    /// exactly, and written up for filing.
    pub topic_name: &'a [u8],
    /// The property section, without its length prefix.
    pub properties: &'a [u8],
    /// The application's bytes. Empty rather than absent when there are none;
    /// the C uses a NULL pointer for the same thing.
    pub payload: &'a [u8],
}

/// Why a PUBLISH was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishError {
    /// `MQTTBadParameter`: not a PUBLISH, or a remaining length past MQTT's
    /// own maximum.
    ///
    /// Two of the C's uses of this status are odd and are transcribed anyway:
    /// `deserializePublishProperties` answers `MQTTBadParameter` when the topic
    /// length does not fit the remaining length, which is something a **broker**
    /// chose, not the caller. A caller therefore cannot tell "you called me
    /// wrong" from "the packet was malformed" for those two shapes.
    BadParameter,
    /// `MQTTBadResponse`: the packet was wrong.
    BadResponse,
}

impl From<PropertyError> for PublishError {
    fn from(_: PropertyError) -> Self {
        Self::BadResponse
    }
}

impl From<AckError> for PublishError {
    fn from(error: AckError) -> Self {
        match error {
            AckError::BadParameter => Self::BadParameter,
            AckError::BadResponse => Self::BadResponse,
        }
    }
}

/// `MQTT_DeserializePublish`.
///
/// `topic_alias_max` is what this client announced in its CONNECT; a Topic
/// Alias above it is a protocol error, and a client that announced zero accepts
/// none at all.
///
/// # Errors
///
/// [`PublishError::BadParameter`] if the high nibble of `packet_type` is not
/// `0x30`, if the remaining length is at or past MQTT's maximum, or if the
/// topic length will not fit the remaining length. [`PublishError::BadResponse`]
/// for everything else a packet can get wrong: too large for
/// `max_packet_size`, QoS 3, a body too short for its QoS, a zero packet
/// identifier, a property section that overruns, an unknown or repeated
/// property, a Payload Format Indicator above 1, a zero Topic Alias, or one
/// above `topic_alias_max`.
pub fn deserialize_publish<'a>(
    packet: &PacketInfo<'a>,
    max_packet_size: u32,
    topic_alias_max: u16,
) -> Result<PublishInfo<'a>, PublishError> {
    // The high nibble only: the low four bits are DUP, QoS and RETAIN, so this
    // is the one packet type that cannot be matched as a whole byte.
    if (packet.packet_type & 0xF0) != packet::PUBLISH {
        return Err(PublishError::BadParameter);
    }

    if packet.remaining_length >= REMAINING_LENGTH_INVALID {
        return Err(PublishError::BadParameter);
    }

    let packet_size = packet
        .remaining_length
        .saturating_add(variable_length_encoded_size(packet.remaining_length))
        .saturating_add(1);

    if packet_size > max_packet_size {
        return Err(PublishError::BadResponse);
    }

    let (qos, dup, retain) = process_publish_flags(packet.packet_type & 0x0F)?;

    // `checkPublishRemainingLength` with a floor of 4: two bytes of topic
    // length, at least one of topic, at least one of property length. QoS 1 and
    // 2 add two for the packet identifier.
    check_remaining_length(packet.remaining_length, qos, 4)?;

    let length_bytes = packet
        .remaining_data
        .get(..2)
        .ok_or(PublishError::BadResponse)?;
    let Ok(array) = <[u8; 2]>::try_from(length_bytes) else {
        return Err(PublishError::BadResponse);
    };
    let topic_name_length = u16::from_be_bytes(array);

    // And again, now that the topic length is known: the variable header is the
    // two length bytes, the topic, and at least one byte of property length.
    check_remaining_length(
        packet.remaining_length,
        qos,
        u32::from(topic_name_length).saturating_add(3),
    )?;

    let topic_name = bounded(packet.remaining_data, 2, u32::from(topic_name_length))?;

    // The packet identifier sits between the topic and the properties, and
    // only at QoS 1 and 2. This is where the body's shape forks.
    let after_topic = usize::from(topic_name_length).saturating_add(2);

    let packet_id = if qos == QoS::AtMostOnce {
        None
    } else {
        let bytes = packet
            .remaining_data
            .get(after_topic..after_topic.saturating_add(2))
            .ok_or(PublishError::BadResponse)?;
        let Ok(array) = <[u8; 2]>::try_from(bytes) else {
            return Err(PublishError::BadResponse);
        };
        let id = u16::from_be_bytes(array);

        if id == 0 {
            return Err(PublishError::BadResponse);
        }

        Some(id)
    };

    let qos_bytes = if qos == QoS::AtMostOnce { 0usize } else { 2 };
    let after_id = after_topic.saturating_add(qos_bytes);

    let (properties, property_length) =
        read_publish_properties(packet, after_id, qos, topic_name_length, topic_alias_max)?;

    // The payload is what is left: the remaining length, less the topic and its
    // two length bytes, less the property section and the bytes that encode its
    // length, less the packet identifier when there is one.
    //
    // The C does this on a `uint32_t` and would WRAP if any term were too
    // large. Ours saturates -- and `the_payload_length_can_never_underflow`
    // pins that the two agree, because the checks above make the difference
    // unreachable rather than merely unlikely.
    let encoded = variable_length_encoded_size(property_length);
    let consumed = u32::from(topic_name_length)
        .saturating_add(2)
        .saturating_add(property_length)
        .saturating_add(encoded)
        .saturating_add(qos_bytes as u32);
    let payload_length = packet.remaining_length.saturating_sub(consumed);

    // `payload_at + payload_length` is `remaining_length` exactly, by the
    // arithmetic just above -- and the property section's own slice already
    // required that much of the buffer to exist. So this `?` CANNOT fire: it is
    // the same bound, checked later. Kept because every read in this module
    // goes through `bounded` and an exception would be the thing a reader
    // stopped trusting; `the_remaining_length_floors_are_the_slice_bounds_restated`
    // asserts the identity that makes it unreachable.
    let payload_at = after_id
        .saturating_add(encoded as usize)
        .saturating_add(property_length as usize);
    let payload = bounded(packet.remaining_data, payload_at, payload_length)?;

    Ok(PublishInfo {
        qos,
        dup,
        retain,
        packet_id,
        topic_name,
        properties,
        payload,
    })
}

/// `processPublishFlags`: QoS, RETAIN and DUP out of the low nibble.
///
/// Both QoS bits set is QoS 3, which does not exist. Every other combination is
/// legal, including DUP on a QoS 0 publish — which MQTT 5.0 [MQTT-3.3.1-2]
/// forbids a sender to do, and which the C accepts.
fn process_publish_flags(flags: u8) -> Result<(QoS, bool, bool), PublishError> {
    let qos = if flags & (1 << flag::QOS2) != 0 {
        if flags & (1 << flag::QOS1) != 0 {
            // QoS 3.
            return Err(PublishError::BadResponse);
        }
        QoS::ExactlyOnce
    } else if flags & (1 << flag::QOS1) != 0 {
        QoS::AtLeastOnce
    } else {
        QoS::AtMostOnce
    };

    Ok((
        qos,
        flags & (1 << flag::DUP) != 0,
        flags & (1 << flag::RETAIN) != 0,
    ))
}

/// `checkPublishRemainingLength`: the floor, plus two more at QoS 1 and 2.
///
/// Called three times with a growing floor — once before the topic length is
/// known, once after, and once more inside the property decoder with the
/// property section counted too. In the C the third is what keeps the payload
/// subtraction from going negative.
///
/// # All three are subsumed here, and that is one finding rather than three
///
/// Poisoning each of the three left the differential passing, and the reason is
/// the same each time: **the C needs them because it indexes with a length it
/// was handed, and this module reads through [`bounded`] instead.** Each floor
/// is the same predicate as the slice that follows it — the third, for
/// instance, fails exactly when `bounded(section, encoded, property_length)`
/// would.
///
/// They are kept because the C has them and a differential arm does not tidy
/// its oracle, and `the_remaining_length_floors_are_the_slice_bounds_restated`
/// records the equivalence. Fourth appearance of a family the size calculators
/// opened: a check load-bearing in the C can be redundant in the transcription,
/// and here three of them are redundant for one reason.
fn check_remaining_length(
    remaining_length: u32,
    qos: QoS,
    qos0_minimum: u32,
) -> Result<(), PublishError> {
    let minimum = if qos == QoS::AtMostOnce {
        qos0_minimum
    } else {
        qos0_minimum.saturating_add(2)
    };

    if remaining_length < minimum {
        return Err(PublishError::BadResponse);
    }

    Ok(())
}

/// `deserializePublishProperties`: the section, and how long it claimed to be.
fn read_publish_properties<'a>(
    packet: &PacketInfo<'a>,
    at: usize,
    qos: QoS,
    topic_name_length: u16,
    topic_alias_max: u16,
) -> Result<(&'a [u8], u32), PublishError> {
    // The C's own bound: what is left after the topic and the packet id. Note
    // it answers BadParameter rather than BadResponse for both of these, though
    // a broker chose the numbers -- see `PublishError::BadParameter`.
    let topic_cost = u32::from(topic_name_length).saturating_add(2);

    if packet.remaining_length < topic_cost {
        return Err(PublishError::BadParameter);
    }

    let mut left = packet.remaining_length.saturating_sub(topic_cost);
    let qos_cost = if qos == QoS::AtMostOnce { 0 } else { 2 };

    if left < qos_cost {
        return Err(PublishError::BadParameter);
    }

    left = left.saturating_sub(qos_cost);

    let section = bounded(packet.remaining_data, at, left)?;
    let property_length = decode_variable_length(section)?;
    let encoded = variable_length_encoded_size(property_length);

    // The third and last remaining-length check, with the property section
    // counted. Unlike the acknowledgements' it is a FLOOR, not an equality:
    // a PUBLISH's properties are followed by the payload, so anything left
    // over is data rather than a malformed packet.
    check_remaining_length(
        packet.remaining_length,
        qos,
        u32::from(topic_name_length)
            .saturating_add(2)
            .saturating_add(property_length)
            .saturating_add(encoded),
    )?;

    let properties = bounded(section, encoded as usize, property_length)?;
    walk_publish_properties(properties, property_length, topic_alias_max)?;

    Ok((properties, property_length))
}

/// The eight properties of MQTT 5.0 §3.3.2.3, in the C's order.
fn walk_publish_properties(
    properties: &[u8],
    property_length: u32,
    topic_alias_max: u16,
) -> Result<(), PublishError> {
    let mut reader = PropertyReader::new(properties, property_length);

    let mut payload_format = false;
    let mut topic_alias_seen = false;
    let mut response_topic = false;
    let mut correlation_data = false;
    let mut message_expiry = false;
    let mut content_type = false;
    let mut topic_alias_value = 0u16;

    while reader.remaining() > 0 {
        let id = reader.property_id()?;

        match id {
            property::PAYLOAD_FORMAT => {
                let value = reader.u8(&mut payload_format)?;

                // §3.3.2.3.2: 0 or 1, and nothing else.
                if value > 1 {
                    return Err(PublishError::BadResponse);
                }
            }

            property::TOPIC_ALIAS => {
                topic_alias_value = reader.u16(&mut topic_alias_seen)?;

                // §3.3.2.3.4: zero is never a valid alias.
                if topic_alias_value == 0 {
                    return Err(PublishError::BadResponse);
                }
            }

            property::RESPONSE_TOPIC => {
                let _ = reader.utf8(&mut response_topic)?;
            }

            property::CORRELATION_DATA => {
                let _ = reader.utf8(&mut correlation_data)?;
            }

            property::MESSAGE_EXPIRY => {
                let _ = reader.u32(&mut message_expiry)?;
            }

            property::CONTENT_TYPE => {
                let _ = reader.utf8(&mut content_type)?;
            }

            // The only variable-length-integer property in the library, and the
            // only one a PUBLISH may carry more than once -- a message matching
            // several subscriptions gets one identifier per match, so there is
            // no `seen` flag here and that is correct.
            //
            // MQTT 5.0 §3.3.2.3.8 also says a value of ZERO is a protocol
            // error, and the C does not check it. Transcribed as the C has it,
            // and written up for filing; `a_zero_subscription_id_is_accepted`
            // pins our side of the divergence.
            property::SUBSCRIPTION_ID => {
                let _ = reader.variable_length()?;
            }

            property::USER_PROPERTY => {
                let _ = reader.user_property()?;
            }

            _ => return Err(PublishError::BadResponse),
        }
    }

    // Checked AFTER the whole walk, as the C does -- so a packet with both an
    // over-large alias and a malformed later property reports the property.
    // The order is observable, so it is transcribed rather than tidied.
    if topic_alias_seen && topic_alias_max < topic_alias_value {
        return Err(PublishError::BadResponse);
    }

    Ok(())
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

    fn info(packet_type: u8, body: &[u8]) -> PacketInfo<'_> {
        PacketInfo {
            packet_type,
            remaining_length: u32::try_from(body.len()).unwrap(),
            remaining_data: body,
        }
    }

    fn read(packet_type: u8, body: &[u8]) -> Result<PublishInfo<'_>, PublishError> {
        deserialize_publish(&info(packet_type, body), 1024, 10)
    }

    /// The payload length is a four-term subtraction and can never go negative.
    ///
    /// The C does it on a `uint32_t`, so a wrap would hand the application a
    /// payload length near four billion pointing into a packet of a few bytes
    /// — the worst possible failure in the one module whose output another
    /// program indexes with. Three `checkPublishRemainingLength` calls are what
    /// make it unreachable; this walks a wide space and proves the saturating
    /// subtraction never had to save us.
    #[test]
    fn the_payload_length_can_never_underflow() {
        for qos_bits in [0x00u8, 0x02, 0x04] {
            for topic_len in 0..6usize {
                for prop_len in 0..6usize {
                    for extra in 0..6usize {
                        let qos0 = qos_bits == 0;
                        let mut body = Vec::new();
                        body.extend_from_slice(&(topic_len as u16).to_be_bytes());
                        body.extend(std::iter::repeat_n(b't', topic_len));

                        if !qos0 {
                            body.extend_from_slice(&42u16.to_be_bytes());
                        }

                        body.push(u8::try_from(prop_len).unwrap());
                        // A property section that is not valid on purpose: the
                        // point is the ARITHMETIC, and a refusal is a fine
                        // outcome so long as it is not a wrapped length.
                        body.extend(std::iter::repeat_n(0x00, prop_len));
                        body.extend(std::iter::repeat_n(b'p', extra));

                        if let Ok(publish) = read(0x30 | qos_bits, &body) {
                            assert!(
                                publish.payload.len() <= body.len(),
                                "a payload of {} bytes came out of a {}-byte body \
                                 (qos bits {qos_bits:#04x}, topic {topic_len}, \
                                 properties {prop_len}, extra {extra})",
                                publish.payload.len(),
                                body.len()
                            );
                        }
                    }
                }
            }
        }
    }

    /// Why the three remaining-length floors cannot change our answer.
    ///
    /// This started as three poisons that did not fire, and they are one
    /// finding: the C's floors exist because it indexes with a length it was
    /// handed, and every read here goes through `bounded` instead. Each floor
    /// is the same predicate as the slice it guards.
    ///
    /// The half that matters is stated directly below: **whenever a floor is
    /// not met, this function refuses** — over a space that includes claimed
    /// lengths LARGER than the buffer, which the differential deliberately
    /// cannot cover because the C reads past its own allocation there.
    #[test]
    fn the_remaining_length_floors_are_the_slice_bounds_restated() {
        // A valid property section at each of three lengths, because a section
        // of zero bytes would make almost every shape a refusal and the
        // acceptance half of this test would prove nothing.
        const SECTIONS: [&[u8]; 3] = [
            &[],
            &[0x01, 0x01],                   // payload format indicator
            &[0x02, 0x00, 0x00, 0x0E, 0x10], // message expiry interval
        ];

        let mut under_the_floor = 0usize;
        let mut accepted = 0usize;

        for qos_bits in [0x00u8, 0x02, 0x04] {
            let qos_cost: u32 = if qos_bits == 0 { 0 } else { 2 };

            for topic_len in 0..5u32 {
                for section in SECTIONS {
                    for payload_len in 0..4u32 {
                        let prop_len = u32::try_from(section.len()).unwrap();
                        let mut body = Vec::new();
                        body.extend_from_slice(&u16::try_from(topic_len).unwrap().to_be_bytes());
                        body.extend(core::iter::repeat_n(b't', topic_len as usize));

                        if qos_bits != 0 {
                            body.extend_from_slice(&42u16.to_be_bytes());
                        }

                        body.push(u8::try_from(prop_len).unwrap());
                        body.extend_from_slice(section);
                        body.extend(core::iter::repeat_n(b'p', payload_len as usize));

                        // Claims short of, equal to and well past the body.
                        for claimed in 0..(body.len() as u32 + 4) {
                            let packet = PacketInfo {
                                packet_type: 0x30 | qos_bits,
                                remaining_length: claimed,
                                remaining_data: &body,
                            };
                            let result = deserialize_publish(&packet, 1024, 10);

                            // The C's third and tightest floor.
                            let floor = topic_len + 2 + prop_len + 1 + qos_cost;

                            if claimed < floor {
                                under_the_floor += 1;
                                assert!(
                                    result.is_err(),
                                    "claimed {claimed} is under the floor {floor}                                      (qos bits {qos_bits:#04x}, topic {topic_len},                                      properties {prop_len}) and was accepted"
                                );
                            }

                            // And whatever the claim, an accepted packet's PARTS
                            // RECONSTRUCT IT. This is the assertion with teeth:
                            // "the payload is not too big" is satisfied by a
                            // payload silently emptied, and an identity is not.
                            if let Ok(publish) = result {
                                accepted += 1;

                                let encoded = variable_length_encoded_size(
                                    u32::try_from(publish.properties.len()).unwrap(),
                                );
                                let parts = 2
                                    + publish.topic_name.len()
                                    + if publish.packet_id.is_some() { 2 } else { 0 }
                                    + encoded as usize
                                    + publish.properties.len()
                                    + publish.payload.len();

                                assert_eq!(
                                    parts,
                                    claimed as usize,
                                    "the parts of an accepted PUBLISH do not add up                                      to its remaining length: topic {}, properties                                      {}, payload {}, claimed {claimed}",
                                    publish.topic_name.len(),
                                    publish.properties.len(),
                                    publish.payload.len()
                                );

                                assert!(
                                    claimed as usize <= body.len(),
                                    "a claim of {claimed} over a {}-byte body was                                      accepted -- the over-claim guarantee is gone",
                                    body.len()
                                );
                            }
                        }
                    }
                }
            }
        }

        // Neither half may be vacuous: a space that never goes under the floor
        // proves nothing about the floor, and one that never accepts anything
        // proves nothing about the identity.
        assert!(
            under_the_floor >= 100,
            "only {under_the_floor} shapes are under the floor"
        );
        assert!(accepted >= 100, "only {accepted} shapes are accepted");
    }

    /// QoS 3 does not exist, and both QoS bits set is how a packet claims it.
    #[test]
    fn qos_three_is_refused_and_every_other_nibble_is_not() {
        let body = [0x00, 0x01, b'a', 0x00, b'h', b'i'];

        for nibble in 0..16u8 {
            let qos3 = (nibble & 0b0110) == 0b0110;
            let result = read(0x30 | nibble, &body);

            if qos3 {
                assert_eq!(
                    result,
                    Err(PublishError::BadResponse),
                    "nibble {nibble:#x} claims QoS 3 and was accepted"
                );
            }
        }
    }

    /// DUP, RETAIN and QoS come out of the nibble independently.
    #[test]
    fn the_three_flags_are_read_separately() {
        let body = [0x00, 0x01, b'a', 0x00, b'h', b'i'];

        let plain = read(0x30, &body).unwrap();
        assert!(!plain.dup && !plain.retain && plain.qos == QoS::AtMostOnce);

        let retained = read(0x31, &body).unwrap();
        assert!(!retained.dup && retained.retain);

        let duplicate = read(0x38, &body).unwrap();
        assert!(duplicate.dup && !duplicate.retain);

        let both = read(0x39, &body).unwrap();
        assert!(both.dup && both.retain);
    }

    /// Two protocol errors MQTT 5.0 names that coreMQTT does not enforce.
    ///
    /// Both are transcribed because the C is the oracle, and both are written
    /// up in `docs/upstream/` for filing. Pinned here so that a later "tidy-up"
    /// that added either check would part the arms with nothing else failing —
    /// and so that the divergence is a recorded decision rather than an
    /// oversight a reader has to rediscover.
    #[test]
    fn two_protocol_errors_are_reproduced_rather_than_fixed() {
        // §3.3.2.3.4: a zero-length topic name is a protocol error unless a
        // Topic Alias is present. This one has none.
        let no_alias = read(0x30, &[0x00, 0x00, 0x00, b'p']).expect("an empty topic name");
        assert!(no_alias.topic_name.is_empty());
        assert_eq!(no_alias.payload, b"p");

        // §3.3.2.3.8: a Subscription Identifier of zero is a protocol error.
        let zero_id = read(0x30, &[0x00, 0x01, b'a', 0x02, 0x0B, 0x00])
            .expect("a zero subscription identifier");
        assert_eq!(zero_id.properties, &[0x0B, 0x00]);

        // And the zero is only reachable in its MINIMAL spelling: two bytes for
        // the same value is refused, by the non-minimal check rather than by
        // any zero check -- which is what shows there is no zero check at all.
        assert_eq!(
            read(0x30, &[0x00, 0x01, b'a', 0x03, 0x0B, 0x80, 0x00]),
            Err(PublishError::BadResponse)
        );
    }

    /// A topic alias is bounded by what the client announced, and zero is never
    /// legal.
    #[test]
    fn a_topic_alias_is_bounded_by_what_the_client_announced() {
        let body = |alias: u16| {
            let mut v = vec![0x00, 0x01, b'a', 0x03, 0x23];
            v.extend_from_slice(&alias.to_be_bytes());
            v
        };

        for alias in 1..=10u16 {
            assert!(read(0x30, &body(alias)).is_ok(), "alias {alias} refused");
        }

        assert_eq!(read(0x30, &body(11)), Err(PublishError::BadResponse));
        assert_eq!(read(0x30, &body(0)), Err(PublishError::BadResponse));

        // A client that announced no aliases accepts none at all.
        assert_eq!(
            deserialize_publish(&info(0x30, &body(1)), 1024, 0),
            Err(PublishError::BadResponse)
        );
    }

    /// The properties are a FLOOR here, not an exact fit — the payload follows.
    ///
    /// Every other property section in this crate must fill its packet exactly,
    /// because nothing follows it. A PUBLISH's does not, and a transcription
    /// that carried the equality across would refuse every publish that
    /// actually carried data.
    #[test]
    fn properties_are_followed_by_the_payload_not_by_the_end() {
        let publish = read(0x30, &[0x00, 0x01, b'a', 0x02, 0x01, 0x01, b'h', b'i']).unwrap();

        assert_eq!(publish.properties, &[0x01, 0x01]);
        assert_eq!(publish.payload, b"hi");
    }
}
