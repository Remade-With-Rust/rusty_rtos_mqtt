//! Building an outgoing PUBLISH — three serializers, one packet.
//!
//! [`publish`](crate::publish) reads a PUBLISH a broker sent. This writes one,
//! and coreMQTT gives it **three** serializers rather than one, because a
//! payload can be large and a microcontroller would rather not copy it:
//!
//! * [`serialize_publish`] writes the whole packet, payload copied in;
//! * [`serialize_publish_header`] writes everything **but** the payload and
//!   reports how far it got, so the caller can send the payload straight from
//!   wherever it already lives;
//! * [`serialize_publish_header_without_topic`] writes less still — the type
//!   byte, the remaining length and the topic's two **length** bytes — so the
//!   topic name can be sent from its own buffer too.
//!
//! All three share one body, and what matters is that they **agree**: the short
//! one must be a prefix of the middle one, and the middle one a prefix of the
//! long one. The differential runs all three on every case and prints all three
//! results, so that relationship is in the trace rather than asserted on one
//! side only.
//!
//! # What each one refuses, and they are not the same
//!
//! The three validate differently, and the differences are the C's:
//!
//! | | topic required | packet id at QoS > 0 | DUP at QoS 0 | buffer |
//! |---|---|---|---|---|
//! | [`serialize_publish`] | yes | yes | refused | whole packet |
//! | [`serialize_publish_header`] | yes | yes | refused | header only |
//! | [`serialize_publish_header_without_topic`] | **no** | **no** | **allowed** | header only |
//!
//! The last one validates almost nothing, and it is the one a caller reaches
//! for when performance matters. That asymmetry is transcribed, not tidied.
//!
//! # One shape a `&[u8]` cannot express
//!
//! The C's payload is a pointer and a length, and the vectored-I/O path uses a
//! NULL pointer with a non-zero length: the header serializer never copies the
//! payload, so it does not need the bytes. The three functions then disagree
//! about it — the full one refuses, the header one accepts, the size one never
//! looks.
//!
//! A `&[u8]` cannot claim a length it does not have, so that state does not
//! exist here and all three take the slice. It costs a caller nothing: to send
//! the payload it must have the bytes somewhere. Seventh member of the family
//! [`ack`](crate::ack) started listing.

use crate::header::{
    MAX_REMAINING_LENGTH, REMAINING_LENGTH_INVALID, encode_variable_length, packet,
    variable_length_encoded_size,
};
use crate::property::encode_string;
use crate::publish::flag;
use crate::state::QoS;

/// Everything an outgoing PUBLISH carries.
#[derive(Debug, Clone, Copy)]
pub struct OutgoingPublish<'a> {
    /// The delivery guarantee.
    pub qos: QoS,
    /// A re-delivery. MQTT forbids it at QoS 0 and so do two of the three
    /// serializers.
    pub dup: bool,
    /// Ask the broker to retain this as the topic's last known good message.
    pub retain: bool,
    /// Where it is published. Must not be empty for two of the three.
    pub topic_name: &'a [u8],
    /// The application's bytes. May be empty.
    pub payload: &'a [u8],
    /// The property section, without its length prefix.
    pub properties: &'a [u8],
}

/// The two numbers an outgoing PUBLISH needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishSize {
    /// What goes in the fixed header.
    pub remaining_length: u32,
    /// The whole packet, header included.
    pub packet_size: u32,
}

/// Why an outgoing PUBLISH could not be built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutgoingError {
    /// `MQTTBadParameter`.
    BadParameter,
    /// `MQTTNoMemory`: the caller's buffer is smaller than what was asked for.
    NoMemory,
}

/// `MQTT_GetPublishPacketSize` and the `calculatePublishPacketSize` it calls.
///
/// # This calculator checks BEFORE adding, where the CONNECT's adds first
///
/// It computes how much of MQTT's 268,435,455 is left after the topic, the
/// packet identifier and the encoded property length, and then tests the
/// property section and the payload against **that remaining budget** — so no
/// addition can overflow in the first place. The CONNECT's calculator adds
/// everything and tests the total, and argues in a comment that its inputs are
/// bounded enough for the sum to fit.
///
/// Two strategies, one file, a few hundred lines apart. Transcribed as each has
/// it, because the order of the two checks is observable: a packet that is over
/// on both counts reports the property one.
///
/// # Errors
///
/// [`OutgoingError::BadParameter`] for a topic name past 65,535, a payload at
/// or past 268,435,456, a property section past 268,435,455, a property section
/// or payload that will not fit the remaining budget, or a packet larger than
/// `max_packet_size`.
pub fn publish_packet_size(
    publish: &OutgoingPublish<'_>,
    max_packet_size: u32,
) -> Result<PublishSize, OutgoingError> {
    let property_length =
        u32::try_from(publish.properties.len()).map_err(|_| OutgoingError::BadParameter)?;

    if property_length > MAX_REMAINING_LENGTH {
        return Err(OutgoingError::BadParameter);
    }

    if publish.topic_name.len() > usize::from(u16::MAX) {
        return Err(OutgoingError::BadParameter);
    }

    let payload_length =
        u32::try_from(publish.payload.len()).map_err(|_| OutgoingError::BadParameter)?;

    if payload_length >= REMAINING_LENGTH_INVALID {
        return Err(OutgoingError::BadParameter);
    }

    // The fixed part: the topic and its two length bytes, the packet identifier
    // at QoS 1 and 2, and the bytes that encode the property length. All
    // bounded above, so this cannot overflow.
    let mut packet_size = u32::try_from(publish.topic_name.len())
        .map_err(|_| OutgoingError::BadParameter)?
        .saturating_add(2);

    if publish.qos != QoS::AtMostOnce {
        packet_size = packet_size.saturating_add(2);
    }

    packet_size = packet_size.saturating_add(variable_length_encoded_size(property_length));

    // What is left of MQTT's maximum after that. The C subtracts on a
    // `uint32_t` and relies on `packet_size` being at most 65,543 here; that
    // holds, and saturating makes it hold whatever is added later.
    let mut budget = MAX_REMAINING_LENGTH.saturating_sub(packet_size);

    if property_length > budget {
        return Err(OutgoingError::BadParameter);
    }

    packet_size = packet_size.saturating_add(property_length);
    budget = budget.saturating_sub(property_length);

    if payload_length > budget {
        return Err(OutgoingError::BadParameter);
    }

    packet_size = packet_size.saturating_add(payload_length);

    let remaining_length = packet_size;
    let packet_size = remaining_length
        .saturating_add(variable_length_encoded_size(remaining_length))
        .saturating_add(1);

    if packet_size > max_packet_size {
        return Err(OutgoingError::BadParameter);
    }

    Ok(PublishSize {
        remaining_length,
        packet_size,
    })
}

/// The flags nibble: the type byte with QoS, RETAIN and DUP set.
fn publish_flags(publish: &OutgoingPublish<'_>) -> u8 {
    let mut flags = packet::PUBLISH;

    match publish.qos {
        QoS::AtMostOnce => {}
        QoS::AtLeastOnce => flags |= 1 << flag::QOS1,
        QoS::ExactlyOnce => flags |= 1 << flag::QOS2,
    }

    if publish.retain {
        flags |= 1 << flag::RETAIN;
    }

    if publish.dup {
        flags |= 1 << flag::DUP;
    }

    flags
}

/// `serializePublishCommon`, with the payload copied or not.
fn serialize_common(
    destination: &mut [u8],
    publish: &OutgoingPublish<'_>,
    packet_id: u16,
    remaining_length: u32,
    with_payload: bool,
) -> Result<usize, OutgoingError> {
    let mut at = 0usize;

    *destination.first_mut().ok_or(OutgoingError::NoMemory)? = publish_flags(publish);
    at = at.saturating_add(1);

    let rest = destination.get_mut(at..).ok_or(OutgoingError::NoMemory)?;
    let encoded = encode_variable_length(rest, remaining_length);

    if encoded == 0 {
        return Err(OutgoingError::NoMemory);
    }

    at = at.saturating_add(encoded);

    at = put_string(destination, at, publish.topic_name)?;

    if publish.qos != QoS::AtMostOnce {
        let slot = destination
            .get_mut(at..at.saturating_add(2))
            .ok_or(OutgoingError::NoMemory)?;
        slot.copy_from_slice(&packet_id.to_be_bytes());
        at = at.saturating_add(2);
    }

    at = put_section(destination, at, publish.properties)?;

    // The payload goes in only when asked. This is the whole reason there are
    // three entry points: the caller who is about to write the payload to a
    // socket from its own buffer does not want it copied here first.
    if with_payload && !publish.payload.is_empty() {
        let end = at.saturating_add(publish.payload.len());
        let slot = destination
            .get_mut(at..end)
            .ok_or(OutgoingError::NoMemory)?;
        slot.copy_from_slice(publish.payload);
        at = end;
    }

    Ok(at)
}

/// A two-byte big-endian length, then the bytes.
fn put_string(destination: &mut [u8], at: usize, value: &[u8]) -> Result<usize, OutgoingError> {
    let length = u16::try_from(value.len()).map_err(|_| OutgoingError::BadParameter)?;
    let rest = destination.get_mut(at..).ok_or(OutgoingError::NoMemory)?;
    let written = encode_string(rest, Some(value), length);

    if written == 0 {
        return Err(OutgoingError::NoMemory);
    }

    Ok(at.saturating_add(written))
}

/// A property section: its variable-byte length, then its bytes.
fn put_section(destination: &mut [u8], at: usize, section: &[u8]) -> Result<usize, OutgoingError> {
    let length = u32::try_from(section.len()).map_err(|_| OutgoingError::BadParameter)?;
    let rest = destination.get_mut(at..).ok_or(OutgoingError::NoMemory)?;
    let encoded = encode_variable_length(rest, length);

    if encoded == 0 {
        return Err(OutgoingError::NoMemory);
    }

    let body_at = at.saturating_add(encoded);

    if section.is_empty() {
        return Ok(body_at);
    }

    let end = body_at.saturating_add(section.len());
    let slot = destination
        .get_mut(body_at..end)
        .ok_or(OutgoingError::NoMemory)?;
    slot.copy_from_slice(section);
    Ok(end)
}

/// The checks the two strict serializers share.
fn validate_strict(
    publish: &OutgoingPublish<'_>,
    packet_id: u16,
    remaining_length: u32,
) -> Result<(), OutgoingError> {
    if publish.topic_name.is_empty() {
        return Err(OutgoingError::BadParameter);
    }

    if publish.topic_name.len() > usize::from(u16::MAX) {
        return Err(OutgoingError::BadParameter);
    }

    if publish.qos != QoS::AtMostOnce && packet_id == 0 {
        return Err(OutgoingError::BadParameter);
    }

    if publish.dup && publish.qos == QoS::AtMostOnce {
        return Err(OutgoingError::BadParameter);
    }

    if remaining_length >= REMAINING_LENGTH_INVALID {
        return Err(OutgoingError::BadParameter);
    }

    Ok(())
}

/// `MQTT_SerializePublish`: the whole packet, payload and all.
///
/// Returns how many bytes were written, which equals
/// [`PublishSize::packet_size`].
///
/// # Errors
///
/// [`OutgoingError::BadParameter`] for an empty or over-long topic name, a zero
/// packet identifier at QoS 1 or 2, DUP at QoS 0, a remaining length at or past
/// 268,435,456, or a property section past 268,435,455.
/// [`OutgoingError::NoMemory`] if `destination` is smaller than the packet.
pub fn serialize_publish(
    destination: &mut [u8],
    publish: &OutgoingPublish<'_>,
    packet_id: u16,
    remaining_length: u32,
) -> Result<usize, OutgoingError> {
    validate_strict(publish, packet_id, remaining_length)?;

    if publish.properties.len() > MAX_REMAINING_LENGTH as usize {
        return Err(OutgoingError::BadParameter);
    }

    let packet_size = remaining_length
        .saturating_add(variable_length_encoded_size(remaining_length))
        .saturating_add(1);

    if (destination.len() as u64) < u64::from(packet_size) {
        return Err(OutgoingError::NoMemory);
    }

    serialize_common(destination, publish, packet_id, remaining_length, true)
}

/// `MQTT_SerializePublishHeader`: everything except the payload.
///
/// Returns the header size — the whole packet less the payload — so a caller
/// can send that many bytes and then the payload straight from its own buffer.
///
/// # Errors
///
/// As [`serialize_publish`], plus [`OutgoingError::BadParameter`] if
/// `remaining_length` is smaller than the payload, which would make the header
/// size negative.
pub fn serialize_publish_header(
    destination: &mut [u8],
    publish: &OutgoingPublish<'_>,
    packet_id: u16,
    remaining_length: u32,
) -> Result<usize, OutgoingError> {
    validate_strict(publish, packet_id, remaining_length)?;

    let payload_length =
        u32::try_from(publish.payload.len()).map_err(|_| OutgoingError::BadParameter)?;

    if remaining_length < payload_length {
        return Err(OutgoingError::BadParameter);
    }

    let header_size = remaining_length
        .saturating_add(variable_length_encoded_size(remaining_length))
        .saturating_add(1)
        .saturating_sub(payload_length);

    if (destination.len() as u64) < u64::from(header_size) {
        return Err(OutgoingError::NoMemory);
    }

    let written = serialize_common(destination, publish, packet_id, remaining_length, false)?;

    // The C returns the SIZE IT COMPUTED from the remaining length it was
    // handed, not the bytes it wrote -- and the two differ in EITHER direction
    // once the caller stops passing what `publish_packet_size` returned:
    //
    //   remaining length 16 for an 11-byte packet -> reports 13, wrote 8;
    //   remaining length  8 for an 11-byte packet -> reports  5, wrote 8.
    //
    // This comment first said "a header size that overruns what was written"
    // and carried a `debug_assert!(written <= header_size)`. The
    // broken-contract cases refuted it in one run: the assumption was mine, not
    // the C's, and it was wrong in the second direction. The computed size is
    // what the C reports, so it is what this reports, and nothing is asserted
    // about the relationship.
    let _ = written;
    Ok(header_size as usize)
}

/// `MQTT_SerializePublishHeaderWithoutTopic`: the type byte, the remaining
/// length, and the topic's two LENGTH bytes.
///
/// Four or five bytes, and then the caller sends the topic name and the rest
/// itself. **It validates almost nothing** — no topic, no packet identifier, no
/// DUP rule — which is transcribed rather than tidied: it is the function a
/// caller reaches for when performance matters, and its looseness is the C's
/// choice.
///
/// # Errors
///
/// [`OutgoingError::BadParameter`] if `remaining_length` is at or past
/// 268,435,456. [`OutgoingError::NoMemory`] if `destination` will not hold the
/// four or five bytes.
pub fn serialize_publish_header_without_topic(
    destination: &mut [u8],
    publish: &OutgoingPublish<'_>,
    remaining_length: u32,
) -> Result<usize, OutgoingError> {
    if remaining_length >= REMAINING_LENGTH_INVALID {
        return Err(OutgoingError::BadParameter);
    }

    let header_length = 1usize
        .saturating_add(variable_length_encoded_size(remaining_length) as usize)
        .saturating_add(2);

    if destination.len() < header_length {
        return Err(OutgoingError::NoMemory);
    }

    *destination.first_mut().ok_or(OutgoingError::NoMemory)? = publish_flags(publish);

    let rest = destination.get_mut(1..).ok_or(OutgoingError::NoMemory)?;
    let encoded = encode_variable_length(rest, remaining_length);

    if encoded == 0 {
        return Err(OutgoingError::NoMemory);
    }

    let at = 1usize.saturating_add(encoded);
    let length = u16::try_from(publish.topic_name.len()).unwrap_or(u16::MAX);
    let slot = destination
        .get_mut(at..at.saturating_add(2))
        .ok_or(OutgoingError::NoMemory)?;
    slot.copy_from_slice(&length.to_be_bytes());

    Ok(header_length)
}

/// `MQTT_UpdateDuplicatePublishFlag`: patch the DUP bit of a serialized header.
///
/// This is how a resend happens without rebuilding the packet: the header byte
/// is already in a buffer, and one bit changes. It accepts only bytes whose
/// high nibble is `0x30`, which is sixteen of 256.
///
/// # Errors
///
/// [`OutgoingError::BadParameter`] if `header` is not a PUBLISH type byte.
pub fn update_duplicate_flag(header: &mut u8, set: bool) -> Result<(), OutgoingError> {
    if (*header & 0xF0) != packet::PUBLISH {
        return Err(OutgoingError::BadParameter);
    }

    if set {
        *header |= 1 << flag::DUP;
    } else {
        *header &= !(1 << flag::DUP);
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

    fn publish<'a>(qos: QoS, payload: &'a [u8]) -> OutgoingPublish<'a> {
        OutgoingPublish {
            qos,
            dup: false,
            retain: false,
            topic_name: b"a/b",
            payload,
            properties: &[],
        }
    }

    /// The three serializers must agree as PREFIXES.
    ///
    /// The short one writes the type byte, the remaining length and the topic's
    /// two length bytes; the middle one writes all of that and then the topic,
    /// the packet identifier and the properties; the long one adds the payload.
    /// If the three ever disagree, a caller using the vectored path sends a
    /// packet that is not the packet the size calculator described — and the
    /// per-function differentials would each still pass.
    #[test]
    fn the_three_serializers_are_prefixes_of_each_other() {
        let mut shapes = 0usize;

        for qos in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce] {
            for retain in [false, true] {
                for payload in [&b""[..], b"hello", b"a longer payload here"] {
                    for properties in [&[][..], &[0x01, 0x01]] {
                        let packet_id = if qos == QoS::AtMostOnce { 0 } else { 42 };
                        let out = OutgoingPublish {
                            retain,
                            properties,
                            ..publish(qos, payload)
                        };

                        let size = publish_packet_size(&out, 1024).unwrap();

                        let mut long = [0xCCu8; 128];
                        let n_long =
                            serialize_publish(&mut long, &out, packet_id, size.remaining_length)
                                .unwrap();
                        assert_eq!(n_long as u32, size.packet_size);

                        let mut middle = [0xCCu8; 128];
                        let n_middle = serialize_publish_header(
                            &mut middle,
                            &out,
                            packet_id,
                            size.remaining_length,
                        )
                        .unwrap();

                        let mut short = [0xCCu8; 128];
                        let n_short = serialize_publish_header_without_topic(
                            &mut short,
                            &out,
                            size.remaining_length,
                        )
                        .unwrap();

                        assert!(n_short <= n_middle && n_middle <= n_long);
                        assert_eq!(
                            &short[..n_short],
                            &middle[..n_short],
                            "the short header is not a prefix of the middle one"
                        );
                        assert_eq!(
                            &middle[..n_middle],
                            &long[..n_middle],
                            "the middle header is not a prefix of the whole packet"
                        );

                        // And the header size plus the payload is the packet.
                        assert_eq!(n_middle + payload.len(), n_long);

                        shapes += 1;
                    }
                }
            }
        }

        assert_eq!(shapes, 3 * 2 * 3 * 2);
    }

    /// The three validate differently, and that is the C's choice.
    #[test]
    fn the_loose_serializer_refuses_what_the_strict_ones_do_not() {
        // A zero packet id at QoS 1: the strict two refuse it.
        let out = publish(QoS::AtLeastOnce, b"hello");
        let size = publish_packet_size(&out, 1024).unwrap();
        let mut buffer = [0xCCu8; 64];

        assert_eq!(
            serialize_publish(&mut buffer, &out, 0, size.remaining_length),
            Err(OutgoingError::BadParameter)
        );
        assert_eq!(
            serialize_publish_header(&mut buffer, &out, 0, size.remaining_length),
            Err(OutgoingError::BadParameter)
        );
        // ...and the loose one never looks at the packet id at all.
        assert!(
            serialize_publish_header_without_topic(&mut buffer, &out, size.remaining_length)
                .is_ok()
        );

        // An empty topic: same split.
        let empty = OutgoingPublish {
            topic_name: &[],
            ..publish(QoS::AtMostOnce, b"hello")
        };
        let size = publish_packet_size(&empty, 1024).unwrap();

        assert_eq!(
            serialize_publish(&mut buffer, &empty, 0, size.remaining_length),
            Err(OutgoingError::BadParameter)
        );
        assert!(
            serialize_publish_header_without_topic(&mut buffer, &empty, size.remaining_length)
                .is_ok()
        );
    }

    /// An outgoing PUBLISH must have a topic; an incoming one need not.
    ///
    /// [`publish`](crate::publish) accepts a zero-length topic name with no
    /// Topic Alias — a divergence from MQTT 5.0 that the C has and this package
    /// reproduces. The WRITING side refuses the same packet. The two halves of
    /// one library disagreeing about whether a topic is optional is worth
    /// knowing, and is the C's.
    #[test]
    fn the_two_directions_disagree_about_an_empty_topic() {
        let empty = OutgoingPublish {
            topic_name: &[],
            ..publish(QoS::AtMostOnce, b"p")
        };
        let size = publish_packet_size(&empty, 1024).unwrap();
        let mut buffer = [0xCCu8; 64];

        assert_eq!(
            serialize_publish(&mut buffer, &empty, 0, size.remaining_length),
            Err(OutgoingError::BadParameter),
            "the writing side now accepts an empty topic"
        );

        // ...and the reading side takes it.
        let incoming = crate::publish::deserialize_publish(
            &crate::ack::PacketInfo {
                packet_type: 0x30,
                remaining_length: 4,
                remaining_data: &[0x00, 0x00, 0x00, b'p'],
            },
            1024,
            10,
        );
        assert!(
            incoming.is_ok(),
            "the reading side now refuses an empty topic too, so the two halves \
             agree and this test is recording something that is no longer true"
        );
    }

    /// The DUP patch takes sixteen bytes of 256, and sets the right bit.
    #[test]
    fn the_duplicate_flag_patch_takes_only_publish_headers() {
        let (mut accepted, mut refused) = (0usize, 0usize);

        for value in 0..=255u8 {
            let mut header = value;

            if update_duplicate_flag(&mut header, true).is_ok() {
                accepted += 1;
                assert_eq!(
                    header,
                    value | 0x08,
                    "the wrong bit was set on {value:#04x}"
                );
                assert_eq!(value & 0xF0, 0x30);
            } else {
                refused += 1;
                assert_eq!(header, value, "a refused header was modified");
            }
        }

        assert_eq!(accepted, 16);
        assert_eq!(refused, 240);

        // And clearing is the inverse.
        for value in 0x30..=0x3Fu8 {
            let mut header = value;
            update_duplicate_flag(&mut header, false).unwrap();
            assert_eq!(header, value & !0x08);
        }
    }
}
