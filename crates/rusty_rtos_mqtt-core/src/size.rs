//! How big an outgoing packet will be.
//!
//! [`writer`](crate::writer) lays down a fixed header for a remaining length
//! the caller has already worked out. This is where that number comes from —
//! and it is the only place in the library that does **arithmetic on sizes the
//! application does not entirely choose**. A subscription list is the
//! application's, but its topic filters often come from a configuration file,
//! a provisioning payload or a cloud-side policy, and MQTT 5's limit of
//! 268,435,455 is one a long enough list reaches.
//!
//! Every calculator answers two numbers: the **remaining length**, which goes
//! into the fixed header, and the **packet size**, which is the remaining
//! length plus the header that encodes it. The second is what gets checked
//! against the maximum the broker announced in its CONNACK.
//!
//! # A refusal here hands the caller nothing
//!
//! The C writes its out-parameters *before* its final check, so a call that
//! fails on the maximum-packet-size test has still updated them — a caller who
//! ignored the status would serialize with a length the library had just
//! refused. A `Result` cannot express that: there is nothing to hand back on
//! the error path, so there is nothing to misuse. The behaviour is recorded
//! rather than reproduced, and
//! `a_refusal_hands_the_caller_nothing` pins our side of it.

use crate::header::{MAX_REMAINING_LENGTH, REMAINING_LENGTH_INVALID, variable_length_encoded_size};

/// `MQTT_PACKET_PINGREQ_SIZE`: a PINGREQ is always two bytes.
pub const PINGREQ_PACKET_SIZE: u32 = 2;

/// The two numbers every size calculation produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PacketSize {
    /// What goes in the fixed header: everything after it.
    pub remaining_length: u32,
    /// The whole packet, header included. This is what the broker's maximum
    /// applies to.
    pub packet_size: u32,
}

/// Why a size could not be calculated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeError {
    /// `MQTTBadParameter`: an empty subscription list, a zero maximum packet
    /// size, a topic filter that will not fit a `u16`, or a total past MQTT 5's
    /// 268,435,455 limit.
    ///
    /// The C does not distinguish these either. All of them mean the packet
    /// cannot be built as asked.
    BadParameter,
}

/// Which of the two list packets is being sized.
///
/// They differ by exactly one byte per filter: SUBSCRIBE carries requested
/// options after each topic filter and UNSUBSCRIBE does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListPacket {
    /// SUBSCRIBE, which adds one options byte per filter.
    Subscribe,
    /// UNSUBSCRIBE, which does not.
    Unsubscribe,
}

/// `MQTT_GetAckPacketSize`: how big a publish acknowledgement will be.
///
/// The body is a two-byte packet id, a one-byte reason code, and the property
/// section — its own length, variable-byte encoded, then the properties.
///
/// # Errors
///
/// [`SizeError::BadParameter`] for a zero `max_packet_size`, a property length
/// past [`MAX_REMAINING_LENGTH`], a remaining length past it, or a packet that
/// will not fit `max_packet_size`.
pub fn ack_packet_size(
    max_packet_size: u32,
    property_length: usize,
) -> Result<PacketSize, SizeError> {
    if max_packet_size == 0 {
        return Err(SizeError::BadParameter);
    }

    // The C checks both that the value fits 32 bits and that it is within the
    // MQTT limit; on a 32-bit target the first is vacuous and on a 64-bit one
    // the second implies it, so one check covers both.
    let Ok(property_length) = u32::try_from(property_length) else {
        return Err(SizeError::BadParameter);
    };
    if property_length > MAX_REMAINING_LENGTH {
        return Err(SizeError::BadParameter);
    }

    // 2 bytes of packet id, 1 of reason code.
    let remaining_length = 3u32
        .saturating_add(variable_length_encoded_size(property_length))
        .saturating_add(property_length);

    if remaining_length > MAX_REMAINING_LENGTH {
        return Err(SizeError::BadParameter);
    }

    let packet_size = remaining_length
        .saturating_add(1)
        .saturating_add(variable_length_encoded_size(remaining_length));

    if packet_size > max_packet_size {
        return Err(SizeError::BadParameter);
    }

    Ok(PacketSize {
        remaining_length,
        packet_size,
    })
}

/// `calculateSubscriptionPacketSize`: how big a SUBSCRIBE or UNSUBSCRIBE will be.
///
/// Each topic filter costs its own length plus the two bytes that prefix it,
/// and a SUBSCRIBE adds one more for the requested options.
///
/// # Errors
///
/// [`SizeError::BadParameter`] for an empty list, a zero `max_packet_size`, a
/// topic filter longer than a `u16` can express, a total past
/// [`MAX_REMAINING_LENGTH`], or a packet that will not fit `max_packet_size`.
pub fn list_packet_size(
    kind: ListPacket,
    topic_filter_lengths: &[usize],
    property_length: u32,
    max_packet_size: u32,
) -> Result<PacketSize, SizeError> {
    if topic_filter_lengths.is_empty() || max_packet_size == 0 {
        return Err(SizeError::BadParameter);
    }

    if property_length > MAX_REMAINING_LENGTH {
        return Err(SizeError::BadParameter);
    }

    // 2 bytes of packet id, then the property section.
    let mut remaining_length = 2u32
        .saturating_add(property_length)
        .saturating_add(variable_length_encoded_size(property_length));

    for length in topic_filter_lengths {
        // A topic filter is prefixed with a 16-bit length, so it cannot be
        // longer than one.
        if u16::try_from(*length).is_err() {
            return Err(SizeError::BadParameter);
        }

        let length = u32::try_from(*length).unwrap_or(u32::MAX);
        remaining_length = remaining_length.saturating_add(length).saturating_add(2);

        // The C checks HERE, after the filter and before the options byte.
        //
        // In the C this check is LOAD-BEARING: its accumulator is a `uint32_t`
        // and its additions wrap, so a long enough list would wrap past zero
        // and come out under the limit. Stopping early is what prevents that.
        //
        // Here the additions SATURATE, so a list long enough to overflow ends
        // at `u32::MAX`, which the final check refuses anyway — this check
        // cannot change our answer. It is kept because the C has it and the
        // arithmetic it guards is one edit away from mattering;
        // `the_in_loop_check_is_subsumed_by_saturating_arithmetic` pins that.
        if remaining_length >= REMAINING_LENGTH_INVALID {
            return Err(SizeError::BadParameter);
        }

        if matches!(kind, ListPacket::Subscribe) {
            remaining_length = remaining_length.saturating_add(1);
        }
    }

    if remaining_length > MAX_REMAINING_LENGTH {
        return Err(SizeError::BadParameter);
    }

    let packet_size = remaining_length
        .saturating_add(1)
        .saturating_add(variable_length_encoded_size(remaining_length));

    if packet_size > max_packet_size {
        return Err(SizeError::BadParameter);
    }

    Ok(PacketSize {
        remaining_length,
        packet_size,
    })
}

/// `MQTT_GetSubscribePacketSize`.
///
/// # Errors
///
/// See [`list_packet_size`].
pub fn subscribe_packet_size(
    topic_filter_lengths: &[usize],
    property_length: u32,
    max_packet_size: u32,
) -> Result<PacketSize, SizeError> {
    list_packet_size(
        ListPacket::Subscribe,
        topic_filter_lengths,
        property_length,
        max_packet_size,
    )
}

/// `MQTT_GetUnsubscribePacketSize`.
///
/// # Errors
///
/// See [`list_packet_size`].
pub fn unsubscribe_packet_size(
    topic_filter_lengths: &[usize],
    property_length: u32,
    max_packet_size: u32,
) -> Result<PacketSize, SizeError> {
    list_packet_size(
        ListPacket::Unsubscribe,
        topic_filter_lengths,
        property_length,
        max_packet_size,
    )
}

#[cfg(test)]
// A test asserts and unwraps its own fixtures; the workspace's
// deny-by-default is written for library code, where either is a defect.
#[allow(
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic
)]
mod tests {
    use super::*;

    /// A refusal hands the caller nothing at all.
    ///
    /// The C writes its out-parameters before its last check, so a call that
    /// fails on the maximum-packet-size test has still updated them — and a
    /// caller who ignored the status would go on to serialize with a length the
    /// library had just refused.
    ///
    /// A `Result` has no error-path value to hand back, so the misuse has no
    /// Rust equivalent. This is the third shape of that category in this
    /// package, after the fixed header's over-large byte count and the property
    /// reader's over-claimed budget: **a differential bounds what the C
    /// answers, never what it leaves behind.**
    #[test]
    fn a_refusal_hands_the_caller_nothing() {
        // Sized at 13 bytes, offered a maximum of 12.
        let refused = subscribe_packet_size(&[5], 0, 12);
        assert_eq!(refused, Err(SizeError::BadParameter));

        // And the same list with room succeeds, so the refusal above is the
        // maximum and not the arithmetic.
        let allowed = subscribe_packet_size(&[5], 0, 13).expect("13 bytes fit in 13");
        assert_eq!(allowed.packet_size, 13);
        assert_eq!(allowed.remaining_length, 11);
    }

    /// SUBSCRIBE costs exactly one byte per filter more than UNSUBSCRIBE.
    #[test]
    fn subscribe_costs_one_byte_per_filter_more() {
        for filters in [&[5usize][..], &[5, 10], &[5, 10, 20], &[0, 0, 0, 0]] {
            let sub = subscribe_packet_size(filters, 0, 1_000_000).expect("a small list");
            let unsub = unsubscribe_packet_size(filters, 0, 1_000_000).expect("a small list");

            assert_eq!(
                sub.remaining_length - unsub.remaining_length,
                filters.len() as u32,
                "the options byte is not being counted once per filter"
            );
        }
    }

    /// Why the in-loop overflow check cannot change our answer.
    ///
    /// This started as a poison that did not fire. In the C the check is
    /// load-bearing: `packetSize += ...` on a `uint32_t` wraps, so a long
    /// enough subscription list would wrap past zero and pass the final limit
    /// check. Stopping the loop early is what prevents that.
    ///
    /// Our additions saturate instead, so an overflowing list ends at
    /// `u32::MAX` and the final check refuses it regardless. The check is kept
    /// for fidelity, and this test records why removing it would be safe HERE
    /// and unsafe THERE — a distinction that would otherwise be lost the next
    /// time someone tidies the function.
    #[test]
    fn the_in_loop_check_is_subsumed_by_saturating_arithmetic() {
        // A list long enough that the C's accumulator would wrap: each filter
        // costs 65,537 bytes and `u32::MAX / 65_537` is about 65,535.
        let filters = [65_535usize; 256];

        // Not enough on its own to overflow, but enough with a large property
        // section to pass the limit.
        let refused = subscribe_packet_size(&filters, 268_000_000, u32::MAX);
        assert_eq!(refused, Err(SizeError::BadParameter));

        // And the saturating behaviour itself: a remaining length driven to
        // `u32::MAX` is still refused, where a wrapping one might not be.
        let huge = subscribe_packet_size(&[65_535; 8], MAX_REMAINING_LENGTH, u32::MAX);
        assert_eq!(
            huge,
            Err(SizeError::BadParameter),
            "an over-large list was accepted -- the arithmetic may have wrapped"
        );
    }

    /// Why the early zero-maximum check cannot change the answer either.
    ///
    /// Also a poison that did not fire. The smallest packet any of these
    /// calculators can produce is four bytes, so a maximum of zero is refused
    /// by the final check whether or not it is refused up front. The early
    /// check is kept because the C has it and because refusing an obviously
    /// impossible request before doing the arithmetic is worth a line.
    #[test]
    fn a_zero_maximum_is_refused_either_way() {
        // The floor: one filter of length zero, no properties.
        let smallest = subscribe_packet_size(&[0], 0, u32::MAX).expect("the smallest list");
        assert!(
            smallest.packet_size >= 4,
            "a list packet can now be smaller than four bytes, so a zero \
             maximum may no longer be refused by the size check alone"
        );

        assert_eq!(
            subscribe_packet_size(&[0], 0, 0),
            Err(SizeError::BadParameter)
        );
        assert_eq!(ack_packet_size(0, 0), Err(SizeError::BadParameter));
    }

    /// A PINGREQ is two bytes and has no variables at all.
    #[test]
    fn a_pingreq_is_always_two_bytes() {
        assert_eq!(PINGREQ_PACKET_SIZE, 2);
    }
}
