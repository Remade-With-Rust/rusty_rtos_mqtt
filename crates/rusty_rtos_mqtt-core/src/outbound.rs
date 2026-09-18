//! The rest of what a client sends: SUBSCRIBE, UNSUBSCRIBE, the publish
//! acknowledgements and PINGREQ.
//!
//! With [`connect`](crate::connect), [`outpublish`](crate::outpublish) and
//! [`disconnect`](crate::disconnect), these complete the outgoing wire codec —
//! every packet an MQTT client can put on a socket.
//!
//! # The subscription options byte
//!
//! SUBSCRIBE carries one options byte per topic filter, packing **five**
//! decisions into six bits: QoS in two, no-local, retain-as-published, and
//! retain handling in two more with three legal values. Same shape as the
//! CONNECT flags byte, same treatment — the differential sweeps all 36
//! combinations and digests the bytes.
//!
//! Getting a bit wrong here subscribes at the wrong QoS, or asks for retained
//! messages that never arrive, and the packet looks perfectly well-formed.
//!
//! # The library validates ack reason codes twice, and disagrees with itself
//!
//! [`ack_reason_code_allowed`] is the **writing** side: it checks per packet
//! type, nine values for a PUBACK and two for a PUBREL, which is exactly MQTT
//! 5.0 §3.4.2.1 and §3.6.2.1.
//!
//! The reading side — `logAckResponse`, transcribed in [`ack`](crate::ack) —
//! checks all four against **one shared table of ten**. So coreMQTT will not
//! *send* a PUBACK carrying `0x92` and will happily *accept* one, and the half
//! that is right is this one. Both are transcribed; the divergence is written
//! up in `docs/upstream/`.

use crate::header::{
    MAX_REMAINING_LENGTH, REMAINING_LENGTH_INVALID, encode_variable_length, packet,
    variable_length_encoded_size,
};
use crate::property::encode_string;
use crate::state::QoS;
use crate::writer::{PINGREQ, serialize_subscribe_header, serialize_unsubscribe_header};

/// Bit positions in a SUBSCRIBE options byte.
pub mod option {
    /// `MQTT_SUBSCRIBE_QOS1`.
    pub const QOS1: u8 = 0;
    /// `MQTT_SUBSCRIBE_QOS2`.
    pub const QOS2: u8 = 1;
    /// `MQTT_SUBSCRIBE_NO_LOCAL`.
    pub const NO_LOCAL: u8 = 2;
    /// `MQTT_SUBSCRIBE_RETAIN_AS_PUBLISHED`.
    pub const RETAIN_AS_PUBLISHED: u8 = 3;
    /// `MQTT_SUBSCRIBE_RETAIN_HANDLING1`.
    pub const RETAIN_HANDLING1: u8 = 4;
    /// `MQTT_SUBSCRIBE_RETAIN_HANDLING2`.
    pub const RETAIN_HANDLING2: u8 = 5;
}

/// `MQTT_PACKET_SIMPLE_ACK_REMAINING_LENGTH`: a bare acknowledgement's body.
pub const SIMPLE_ACK_REMAINING_LENGTH: u8 = 2;

/// `MQTT_PUBLISH_ACK_PACKET_SIZE`: the smallest publish acknowledgement.
pub const PUBLISH_ACK_PACKET_SIZE: usize = 4;

/// `MQTTRetainHandling_t`: when the broker should send retained messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RetainHandling {
    /// At the time of subscribing.
    #[default]
    OnSubscribe,
    /// At subscribe time, but only if the subscription did not already exist.
    OnSubscribeIfNew,
    /// Never.
    Never,
}

/// One entry of a SUBSCRIBE or UNSUBSCRIBE list.
///
/// The four option fields are ignored for an UNSUBSCRIBE, which carries topic
/// filters and nothing else — one struct serves both, as the C's does.
#[derive(Debug, Clone, Copy)]
pub struct Subscription<'a> {
    /// The filter. Must not be empty.
    pub topic_filter: &'a [u8],
    /// The maximum QoS the broker may deliver at.
    pub qos: QoS,
    /// Do not send this client its own publications back.
    pub no_local: bool,
    /// Keep the RETAIN flag as the publisher set it.
    pub retain_as_published: bool,
    /// When retained messages should arrive.
    pub retain_handling: RetainHandling,
}

/// Why an outgoing packet could not be built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboundError {
    /// `MQTTBadParameter`.
    BadParameter,
    /// `MQTTNoMemory`: the caller's buffer is too small.
    NoMemory,
}

/// `validateSubscriptionSerializeParams`, shared by both list packets.
fn validate_list(
    subscriptions: &[Subscription<'_>],
    packet_id: u16,
    remaining_length: u32,
    destination: &[u8],
) -> Result<usize, OutboundError> {
    if subscriptions.is_empty() {
        return Err(OutboundError::BadParameter);
    }

    if packet_id == 0 {
        return Err(OutboundError::BadParameter);
    }

    if remaining_length >= REMAINING_LENGTH_INVALID {
        return Err(OutboundError::BadParameter);
    }

    let packet_size = remaining_length
        .saturating_add(variable_length_encoded_size(remaining_length))
        .saturating_add(1) as usize;

    if destination.len() < packet_size {
        return Err(OutboundError::NoMemory);
    }

    // The filters are checked LAST, after the buffer -- so a call that is wrong
    // about both reports NoMemory. The order is observable and is the C's.
    for subscription in subscriptions {
        if subscription.topic_filter.is_empty() {
            return Err(OutboundError::BadParameter);
        }

        if subscription.topic_filter.len() > usize::from(u16::MAX) {
            return Err(OutboundError::BadParameter);
        }
    }

    Ok(packet_size)
}

/// The options byte for one subscription.
///
/// Five decisions in six bits. `RetainHandling::OnSubscribe` is the zero case
/// and sets neither of its two bits, which is why the C's `else` branch for it
/// is empty.
#[must_use]
pub fn subscription_options(subscription: &Subscription<'_>) -> u8 {
    let mut options = 0u8;

    match subscription.qos {
        QoS::AtMostOnce => {}
        QoS::AtLeastOnce => options |= 1 << option::QOS1,
        QoS::ExactlyOnce => options |= 1 << option::QOS2,
    }

    if subscription.no_local {
        options |= 1 << option::NO_LOCAL;
    }

    if subscription.retain_as_published {
        options |= 1 << option::RETAIN_AS_PUBLISHED;
    }

    match subscription.retain_handling {
        RetainHandling::OnSubscribe => {}
        RetainHandling::OnSubscribeIfNew => options |= 1 << option::RETAIN_HANDLING1,
        RetainHandling::Never => options |= 1 << option::RETAIN_HANDLING2,
    }

    options
}

/// `MQTT_SerializeSubscribe`.
///
/// `remaining_length` comes from
/// [`subscribe_packet_size`](crate::size::subscribe_packet_size). The C takes
/// it as a parameter and **never recomputes it**, so a caller who gets it wrong
/// gets a packet whose own length byte disagrees with its contents. Reproduced,
/// and the differential has a case named for it.
///
/// # Errors
///
/// [`OutboundError::BadParameter`] for an empty list, a zero packet identifier,
/// a remaining length at or past 268,435,456, a property section at or past it,
/// or any empty or over-long topic filter. [`OutboundError::NoMemory`] if
/// `destination` is smaller than the packet.
pub fn serialize_subscribe(
    destination: &mut [u8],
    subscriptions: &[Subscription<'_>],
    properties: &[u8],
    packet_id: u16,
    remaining_length: u32,
) -> Result<usize, OutboundError> {
    let property_length = section_length(properties)?;
    let packet_size = validate_list(subscriptions, packet_id, remaining_length, destination)?;

    let mut at = serialize_subscribe_header(destination, remaining_length, packet_id);

    if at == 0 {
        return Err(OutboundError::NoMemory);
    }

    at = put_section(destination, at, properties, property_length)?;

    for subscription in subscriptions {
        at = put_string(destination, at, subscription.topic_filter)?;

        let slot = destination.get_mut(at).ok_or(OutboundError::NoMemory)?;
        *slot = subscription_options(subscription);
        at = at.saturating_add(1);
    }

    let _ = at;
    Ok(packet_size)
}

/// `MQTT_SerializeUnsubscribe`: the same, without the options byte.
///
/// # Errors
///
/// As [`serialize_subscribe`].
pub fn serialize_unsubscribe(
    destination: &mut [u8],
    subscriptions: &[Subscription<'_>],
    properties: &[u8],
    packet_id: u16,
    remaining_length: u32,
) -> Result<usize, OutboundError> {
    let property_length = section_length(properties)?;
    let packet_size = validate_list(subscriptions, packet_id, remaining_length, destination)?;

    let mut at = serialize_unsubscribe_header(destination, remaining_length, packet_id);

    if at == 0 {
        return Err(OutboundError::NoMemory);
    }

    at = put_section(destination, at, properties, property_length)?;

    for subscription in subscriptions {
        at = put_string(destination, at, subscription.topic_filter)?;
    }

    let _ = at;
    Ok(packet_size)
}

fn section_length(properties: &[u8]) -> Result<u32, OutboundError> {
    let length = u32::try_from(properties.len()).map_err(|_| OutboundError::BadParameter)?;

    if length >= REMAINING_LENGTH_INVALID {
        return Err(OutboundError::BadParameter);
    }

    Ok(length)
}

fn put_section(
    destination: &mut [u8],
    at: usize,
    section: &[u8],
    length: u32,
) -> Result<usize, OutboundError> {
    let rest = destination.get_mut(at..).ok_or(OutboundError::NoMemory)?;
    let encoded = encode_variable_length(rest, length);

    if encoded == 0 {
        return Err(OutboundError::NoMemory);
    }

    let body_at = at.saturating_add(encoded);

    if section.is_empty() {
        return Ok(body_at);
    }

    let end = body_at.saturating_add(section.len());
    let slot = destination
        .get_mut(body_at..end)
        .ok_or(OutboundError::NoMemory)?;
    slot.copy_from_slice(section);
    Ok(end)
}

fn put_string(destination: &mut [u8], at: usize, value: &[u8]) -> Result<usize, OutboundError> {
    let length = u16::try_from(value.len()).map_err(|_| OutboundError::BadParameter)?;
    let rest = destination.get_mut(at..).ok_or(OutboundError::NoMemory)?;
    let written = encode_string(rest, Some(value), length);

    if written == 0 {
        return Err(OutboundError::NoMemory);
    }

    Ok(at.saturating_add(written))
}

/// `validateReasonCodeForAck`: may this acknowledgement carry this reason code?
///
/// **Per packet type**, which is what makes it right: MQTT 5.0 gives a PUBACK
/// and a PUBREC nine reason codes each (§3.4.2.1, §3.5.2.1) and a PUBREL and a
/// PUBCOMP two each (§3.6.2.1, §3.7.2.1).
///
/// The reading side checks all four against one shared table of ten, so
/// coreMQTT refuses to *send* a PUBACK carrying `0x92` and accepts one on the
/// way in. The two halves of the library disagree and this half is correct.
#[must_use]
pub fn ack_reason_code_allowed(packet_type: u8, reason_code: u8) -> bool {
    match packet_type {
        // §3.4.2.1 and §3.5.2.1: the same nine.
        packet::PUBACK | packet::PUBREC => matches!(
            reason_code,
            0x00 | 0x10 | 0x80 | 0x83 | 0x87 | 0x90 | 0x91 | 0x97 | 0x99
        ),
        // §3.6.2.1 and §3.7.2.1: success, or "packet identifier not found".
        crate::ack::PUBREL | packet::PUBCOMP => matches!(reason_code, 0x00 | 0x92),
        _ => false,
    }
}

/// `MQTT_SerializeAck`, `serializeAckBody` and `serializeAckWithProperties`.
///
/// Three shapes, which the C names in a comment: no reason code (four bytes), a
/// reason code and no properties (six), or a reason code with properties.
///
/// # A too-small buffer answers two different ways
///
/// `MQTT_SerializeAck` answers `MQTTNoMemory` for a buffer under four bytes and
/// `serializeAckBody` answers **`MQTTBadParameter`** for one that is four bytes
/// but cannot hold a reason code. Two statuses for one condition, a hundred
/// lines apart. Transcribed as each has it; the differential has a case for
/// each.
///
/// # Errors
///
/// [`OutboundError::BadParameter`] for a packet type that is not a publish
/// acknowledgement, a zero packet identifier, properties with no reason code, a
/// reason code the type may not carry, a property section at or past
/// 268,435,456, or a buffer too small for the reason code.
/// [`OutboundError::NoMemory`] for a buffer under four bytes.
pub fn serialize_ack(
    destination: &mut [u8],
    packet_type: u8,
    packet_id: u16,
    reason_code: Option<u8>,
    properties: &[u8],
) -> Result<usize, OutboundError> {
    if destination.len() < PUBLISH_ACK_PACKET_SIZE {
        return Err(OutboundError::NoMemory);
    }

    if packet_id == 0 {
        return Err(OutboundError::BadParameter);
    }

    if reason_code.is_none() && !properties.is_empty() {
        return Err(OutboundError::BadParameter);
    }

    if let Some(code) = reason_code {
        if !ack_reason_code_allowed(packet_type, code) {
            return Err(OutboundError::BadParameter);
        }
    } else if !matches!(
        packet_type,
        packet::PUBACK | packet::PUBREC | crate::ack::PUBREL | packet::PUBCOMP
    ) {
        // Without a reason code the type is only checked by the routing switch
        // at the bottom of `MQTT_SerializeAck`, so it is checked here instead.
        return Err(OutboundError::BadParameter);
    }

    let property_length = section_length(properties)?;

    *destination.first_mut().ok_or(OutboundError::NoMemory)? = packet_type;

    let Some(code) = reason_code else {
        // Four bytes: type, a remaining length of two, and the packet id.
        let slot: &mut [u8; 3] = destination
            .get_mut(1..4)
            .and_then(|s| <&mut [u8; 3]>::try_from(s).ok())
            .ok_or(OutboundError::NoMemory)?;
        *slot = [
            SIMPLE_ACK_REMAINING_LENGTH,
            packet_id.to_be_bytes()[0],
            packet_id.to_be_bytes()[1],
        ];
        return Ok(4);
    };

    if destination.len() < PUBLISH_ACK_PACKET_SIZE.saturating_add(2) {
        // The C's odd one out: BadParameter, not NoMemory.
        return Err(OutboundError::BadParameter);
    }

    if properties.is_empty() {
        // Six bytes: the four above, the reason code, and a property length of
        // zero -- which MQTT 5 requires even when there are no properties.
        let slot: &mut [u8; 5] = destination
            .get_mut(1..6)
            .and_then(|s| <&mut [u8; 5]>::try_from(s).ok())
            .ok_or(OutboundError::NoMemory)?;
        *slot = [
            SIMPLE_ACK_REMAINING_LENGTH.saturating_add(2),
            packet_id.to_be_bytes()[0],
            packet_id.to_be_bytes()[1],
            code,
            // MQTT 5 requires the property-length byte even when there are
            // none; dropping it is one of the poisons.
            0x00,
        ];
        return Ok(6);
    }

    let remaining_length = 3u32
        .saturating_add(variable_length_encoded_size(property_length))
        .saturating_add(property_length);

    if remaining_length > MAX_REMAINING_LENGTH {
        return Err(OutboundError::BadParameter);
    }

    let packet_size = remaining_length
        .saturating_add(variable_length_encoded_size(remaining_length))
        .saturating_add(1) as usize;

    if destination.len() < packet_size {
        // And again BadParameter rather than NoMemory, in the third shape.
        return Err(OutboundError::BadParameter);
    }

    let rest = destination.get_mut(1..).ok_or(OutboundError::NoMemory)?;
    let encoded = encode_variable_length(rest, remaining_length);

    if encoded == 0 {
        return Err(OutboundError::NoMemory);
    }

    let mut at = 1usize.saturating_add(encoded);
    let slot: &mut [u8; 3] = destination
        .get_mut(at..at.saturating_add(3))
        .and_then(|s| <&mut [u8; 3]>::try_from(s).ok())
        .ok_or(OutboundError::NoMemory)?;
    *slot = [packet_id.to_be_bytes()[0], packet_id.to_be_bytes()[1], code];
    at = at.saturating_add(3);

    at = put_section(destination, at, properties, property_length)?;
    let _ = at;

    Ok(packet_size)
}

/// `MQTT_SerializePingreq`: two bytes that never change.
///
/// # Errors
///
/// [`OutboundError::NoMemory`] if `destination` is under two bytes.
pub fn serialize_pingreq(destination: &mut [u8]) -> Result<usize, OutboundError> {
    let slot = destination
        .get_mut(..PINGREQ.len())
        .ok_or(OutboundError::NoMemory)?;
    slot.copy_from_slice(&PINGREQ);
    Ok(PINGREQ.len())
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

    fn filter(topic: &[u8]) -> Subscription<'_> {
        Subscription {
            topic_filter: topic,
            qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::OnSubscribe,
        }
    }

    /// The library validates ack reason codes twice and disagrees with itself.
    ///
    /// The **writing** side checks per packet type and matches MQTT 5.0
    /// exactly. The **reading** side checks all four against one shared table of
    /// ten. So coreMQTT will not send a PUBACK carrying `0x92` and will accept
    /// one — and it is the reading side that is wrong.
    ///
    /// Pinned from both directions, because this is the sharpest evidence for
    /// the upstream report: the library already contains the correct table,
    /// three thousand lines from the incorrect one.
    #[test]
    fn the_two_halves_disagree_about_ack_reason_codes() {
        // Writing: 0x92 is a PUBREL and PUBCOMP code only.
        assert!(!ack_reason_code_allowed(packet::PUBACK, 0x92));
        assert!(!ack_reason_code_allowed(packet::PUBREC, 0x92));
        assert!(ack_reason_code_allowed(crate::ack::PUBREL, 0x92));
        assert!(ack_reason_code_allowed(packet::PUBCOMP, 0x92));

        // Writing: 0x10 is a PUBACK and PUBREC code only.
        assert!(ack_reason_code_allowed(packet::PUBACK, 0x10));
        assert!(!ack_reason_code_allowed(crate::ack::PUBREL, 0x10));

        // Reading: one table of ten, for all four. Driven through the ack
        // deserializer, which is where that table lives.
        let read = |packet_type: u8, code: u8| {
            crate::ack::deserialize_ack(
                &crate::ack::PacketInfo {
                    packet_type,
                    remaining_length: 3,
                    remaining_data: &[0x00, 0x2A, code],
                },
                &crate::ack::Limits {
                    max_packet_size: 1024,
                    request_problem_info: true,
                },
            )
            .is_ok()
        };

        assert!(
            read(packet::PUBACK, 0x92),
            "the reading side now refuses 0x92 in a PUBACK, so the two halves \
             agree and the upstream report needs revisiting"
        );
        assert!(
            read(crate::ack::PUBREL, 0x10),
            "the reading side now refuses 0x10 in a PUBREL"
        );
    }

    /// Every combination of the subscription options byte, and its bits.
    #[test]
    fn the_options_byte_packs_five_decisions_into_six_bits() {
        let mut seen = std::collections::BTreeSet::new();

        for qos in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce] {
            for no_local in [false, true] {
                for retain_as_published in [false, true] {
                    for retain_handling in [
                        RetainHandling::OnSubscribe,
                        RetainHandling::OnSubscribeIfNew,
                        RetainHandling::Never,
                    ] {
                        let subscription = Subscription {
                            qos,
                            no_local,
                            retain_as_published,
                            retain_handling,
                            ..filter(b"f")
                        };
                        let byte = subscription_options(&subscription);

                        // Each decision owns its own bits and nothing else.
                        assert_eq!(byte & 0x03, qos as u8, "the QoS bits are wrong");
                        assert_eq!((byte >> option::NO_LOCAL) & 1, u8::from(no_local));
                        assert_eq!(
                            (byte >> option::RETAIN_AS_PUBLISHED) & 1,
                            u8::from(retain_as_published)
                        );
                        assert_eq!(byte & 0xC0, 0, "a reserved bit is set");

                        seen.insert(byte);
                    }
                }
            }
        }

        assert_eq!(seen.len(), 36, "two combinations produced the same byte");
    }

    /// A too-small buffer gets two different statuses, a hundred lines apart.
    #[test]
    fn a_small_ack_buffer_answers_two_ways() {
        // Under four bytes: NoMemory.
        let mut tiny = [0xCCu8; 3];
        assert_eq!(
            serialize_ack(&mut tiny, packet::PUBACK, 42, None, &[]),
            Err(OutboundError::NoMemory)
        );

        // Four bytes and no reason code: fine.
        let mut four = [0xCCu8; 4];
        assert_eq!(
            serialize_ack(&mut four, packet::PUBACK, 42, None, &[]),
            Ok(4)
        );

        // Four bytes WITH a reason code: BadParameter, not NoMemory.
        let mut four = [0xCCu8; 4];
        assert_eq!(
            serialize_ack(&mut four, packet::PUBACK, 42, Some(0x00), &[]),
            Err(OutboundError::BadParameter),
            "the C answers BadParameter here, not NoMemory -- if that has been \
             tidied, the two arms have parted"
        );
    }

    /// A PINGREQ is two bytes and has no inputs at all.
    #[test]
    fn a_pingreq_is_two_fixed_bytes() {
        let mut out = [0xCCu8; 4];
        assert_eq!(serialize_pingreq(&mut out), Ok(2));
        assert_eq!(&out[..2], &[0xC0, 0x00]);
        assert_eq!(out[2], 0xCC, "a byte past the packet was written");

        let mut one = [0xCCu8; 1];
        assert_eq!(serialize_pingreq(&mut one), Err(OutboundError::NoMemory));
    }

    /// The list validator checks the buffer BEFORE the filters.
    ///
    /// A call that is wrong about both reports `NoMemory`, not `BadParameter`.
    /// The order is observable and it is the C's.
    #[test]
    fn the_buffer_is_checked_before_the_filters() {
        let bad = [filter(&[])];
        let mut tiny = [0xCCu8; 2];

        assert_eq!(
            serialize_subscribe(&mut tiny, &bad, &[], 1, 9),
            Err(OutboundError::NoMemory),
            "the filter check now runs first"
        );

        // With room, the same list is a BadParameter.
        let mut roomy = [0xCCu8; 64];
        assert_eq!(
            serialize_subscribe(&mut roomy, &bad, &[], 1, 9),
            Err(OutboundError::BadParameter)
        );
    }
}
