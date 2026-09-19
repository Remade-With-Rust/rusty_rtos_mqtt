//! Does this topic name match that topic filter?
//!
//! The first slice of `core_mqtt.c`, and the part of it that needs no
//! connection. A client that subscribes to `sport/+` and then receives a
//! PUBLISH for `sport/tennis` has to decide which of its callbacks the message
//! belongs to, and this is that decision.
//!
//! # A divergence from MQTT 5.0, and a narrow one
//!
//! **A filter whose last level is `+` does not match a topic whose last level
//! is empty, once an earlier `+` has been used.**
//!
//! ```text
//! a/    against  a/+     matches
//! a/    against  +/+     does NOT match
//! /     against  +/+     does NOT match
//! a//   against  a/+/+   does NOT match
//! ```
//!
//! §4.7.1.3 makes `+` match exactly one level, and §4.7.3 makes an empty level
//! a legal one — the specification's own example has `sport/+` matching
//! `sport/`, which this does get right. The first `+` moves the filter index by
//! a different amount from the name index, and the end-of-name special case
//! that rescues a trailing `+` is written in terms of the filter index, so once
//! the two have diverged it no longer fires.
//!
//! A client subscribed to `+/+` silently never receives messages published to
//! `a/`. Transcribed as it stands, pinned by
//! [`tests`](self), and written up in `docs/upstream/`.
//!
//! # Why the differential prints a grid
//!
//! Every other sweep in this package walks one byte over its 256 values.
//! Matching takes two **strings**, so the sweep is the same idea one dimension
//! up: every string over `{a, /, +}` up to length three, 39 of them, as a
//! 39 × 39 matrix of which filter matches which topic. A grid is to a string
//! algorithm what a printed accepted set is to a table — the defect above is a
//! **column pattern** in it, not a line you have to already suspect.
//!
//! Then the same over `{a, b, /, +, #, $}` to length four: 2,414,916 pairs,
//! digested.

use crate::ack::{AckError, PacketInfo};
use crate::header::{REMAINING_LENGTH_INVALID, packet};
use crate::property::decode_variable_length;

/// Why a match could not be decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicError {
    /// `MQTTBadParameter`: an empty topic name or filter, or one longer than
    /// 65,535 bytes.
    BadParameter,
}

/// `MQTT_MatchTopic`: does `topic` match `filter`?
///
/// # Errors
///
/// [`TopicError::BadParameter`] if either side is empty or longer than 65,535
/// bytes. The C also refuses null pointers and a null out-parameter; a `&[u8]`
/// and a return value cannot be null.
pub fn matches(topic: &[u8], filter: &[u8]) -> Result<bool, TopicError> {
    if topic.is_empty() || filter.is_empty() {
        return Err(TopicError::BadParameter);
    }

    if u16::try_from(topic.len()).is_err() || u16::try_from(filter.len()).is_err() {
        return Err(TopicError::BadParameter);
    }

    // An exact match first, which is both the common case and the only way a
    // filter containing a wildcard character matches a topic containing the
    // same character literally.
    if topic.len() == filter.len() && topic == filter {
        return Ok(true);
    }

    // §4.7.2: a topic beginning with `$` is not matched by a filter beginning
    // with a wildcard, so `$SYS/broker` is invisible to `#`.
    let filter_starts_with_wildcard = matches!(filter.first().copied(), Some(b'+') | Some(b'#'));

    if topic.first().copied() == Some(b'$') && filter_starts_with_wildcard {
        return Ok(false);
    }

    Ok(match_filter(topic, filter))
}

/// `matchEndWildcardsSpecialCases`: the two shapes where a filter that has run
/// on past the end of the topic name still matches.
fn match_end_wildcards(filter: &[u8], at: usize) -> bool {
    let length = filter.len();
    let mut found = false;

    // `sport` against `sport/#`: the multi-level wildcard stands for the
    // parent level as well as its children.
    if length >= 3
        && at == length.wrapping_sub(3)
        && filter.get(at.wrapping_add(1)).copied() == Some(b'/')
        && filter.get(at.wrapping_add(2)).copied() == Some(b'#')
    {
        found = true;
    }

    // `sport/` against `sport/+` or `sport/#`.
    // Saturating as defence in depth, not because a test needs it. A
    // one-character filter is the only length that makes `length - 2`
    // saturate, and wrapping it instead leaves all 99 tests passing --
    // because that case cannot arrive here. `filter_index` is bounded by
    // `filter.len()`, so a one-byte filter pins it at zero; a zero
    // `filter_index` means no wildcard step has run, so `name_index` is zero
    // too; and `name_index == topic.len() - 1` then makes the topic one byte
    // as well, which `matches` has already answered with its exact-match
    // shortcut. The saturation holds only while that chain does, and it
    // costs one conditional move, so it stays.
    if at == length.saturating_sub(2) && filter.get(at).copied() == Some(b'/') {
        found = matches!(
            filter.get(at.wrapping_add(1)).copied(),
            Some(b'+') | Some(b'#')
        );
    }

    found
}

/// What [`match_wildcards`] decided, and whether the walk should stop.
struct Step {
    stop: bool,
    matched: bool,
    name_index: usize,
    filter_index: usize,
}

/// `matchWildcards`: the filter character under the cursor is not the topic's,
/// so it is a wildcard or it is a mismatch.
fn match_wildcards(topic: &[u8], filter: &[u8], name_index: usize, filter_index: usize) -> Step {
    let mut name = name_index;

    // A wildcard is only a wildcard at the start of a filter or after a `/`.
    // `a+/b` has a literal `+` in it.
    let valid_here =
        filter_index == 0 || filter.get(filter_index.wrapping_sub(1)).copied() == Some(b'/');

    let here = filter.get(filter_index).copied();

    if here == Some(b'+') && valid_here {
        // Walk the topic to the end of this level.
        let mut next_level_in_topic = false;

        while name < topic.len() {
            if topic.get(name).copied() == Some(b'/') {
                next_level_in_topic = true;
                break;
            }

            name = name.wrapping_add(1);
        }

        let next_level_in_filter = filter_index < filter.len().wrapping_sub(1)
            && filter.get(filter_index.wrapping_add(1)).copied() == Some(b'/');

        if next_level_in_topic && !next_level_in_filter {
            // The topic has more levels and the filter has run out.
            return Step {
                stop: true,
                matched: false,
                name_index: name,
                filter_index,
            };
        }

        if next_level_in_topic {
            // Both go on. The name index already sits on the separator, so
            // only the filter index needs moving -- and THIS is where the two
            // indices stop moving together, which is the defect in the module
            // note: `match_end_wildcards` is written in terms of the filter
            // index and no longer lines up.
            return Step {
                stop: false,
                matched: false,
                name_index: name,
                filter_index: filter_index.wrapping_add(1),
            };
        }

        // The loop ran off the end of the topic name, so step back to its last
        // character. The C spells this `nameIndex -= 1U` with a Coverity
        // annotation for the underflow it cannot reach: the loop body runs at
        // least once whenever `topicNameLength != 0`, which its caller asserts.
        return Step {
            stop: false,
            matched: false,
            name_index: name.saturating_sub(1),
            filter_index,
        };
    }

    // `#` takes everything that is left, and must be the filter's last
    // character.
    if here == Some(b'#') && filter_index == filter.len().wrapping_sub(1) && valid_here {
        return Step {
            stop: true,
            matched: true,
            name_index: name,
            filter_index,
        };
    }

    Step {
        stop: true,
        matched: false,
        name_index: name,
        filter_index,
    }
}

/// `matchTopicFilter`: walk the two strings together.
fn match_filter(topic: &[u8], filter: &[u8]) -> bool {
    let mut matched = false;
    let mut stop = false;
    let mut name_index = 0usize;
    let mut filter_index = 0usize;

    while name_index < topic.len() && filter_index < filter.len() {
        if topic.get(name_index) == filter.get(filter_index) {
            // The topic name has been consumed but the filter has not: the
            // filter may still end in a wildcard that covers it.
            if name_index == topic.len().wrapping_sub(1) {
                matched = match_end_wildcards(filter, filter_index);
            }
        } else {
            let step = match_wildcards(topic, filter, name_index, filter_index);
            stop = step.stop;
            matched = step.matched;
            name_index = step.name_index;
            filter_index = step.filter_index;
        }

        if matched || stop {
            break;
        }

        name_index = name_index.wrapping_add(1);
        filter_index = filter_index.wrapping_add(1);
    }

    if !matched {
        // Both ran out together, which is how `sport/+/player` matches
        // `sport/hockey/player`.
        matched = name_index == topic.len() && filter_index == filter.len();
    }

    matched
}

/// `MQTT_GetSubAckStatusCodes`: the reason codes in a SUBACK, without
/// deserializing it.
///
/// One code per topic filter the SUBSCRIBE carried, in the same order.
///
/// # Errors
///
/// [`AckError::BadParameter`] if the packet is not a SUBACK, is shorter than
/// four bytes, or claims a remaining length at or above
/// [`REMAINING_LENGTH_INVALID`]; [`AckError::BadResponse`] if its property
/// length is malformed.
pub fn suback_status_codes<'a>(packet: &PacketInfo<'a>) -> Result<&'a [u8], AckError> {
    status_codes(packet, packet::SUBACK)
}

/// `MQTT_GetUnsubAckStatusCodes`: the same, for an UNSUBACK.
///
/// # Errors
///
/// As [`suback_status_codes`], for an UNSUBACK.
pub fn unsuback_status_codes<'a>(packet: &PacketInfo<'a>) -> Result<&'a [u8], AckError> {
    status_codes(packet, packet::UNSUBACK)
}

fn status_codes<'a>(packet: &PacketInfo<'a>, expected: u8) -> Result<&'a [u8], AckError> {
    if packet.packet_type != expected {
        return Err(AckError::BadParameter);
    }

    // Two bytes of packet identifier, at least one byte of property length and
    // at least one reason code.
    if packet.remaining_length < 4 || packet.remaining_length >= REMAINING_LENGTH_INVALID {
        return Err(AckError::BadParameter);
    }

    let Some(after_id) = packet.remaining_data.get(2..) else {
        return Err(AckError::BadParameter);
    };

    // The C's `decodeSubackPropertyLength` answers with the property length
    // AND the bytes that encoded it folded together, which is why its caller
    // adds one number where two are needed.
    let value = decode_variable_length(after_id).map_err(|_| AckError::BadResponse)?;
    let width = crate::header::variable_length_encoded_size(value) as usize;
    let skip = width.saturating_add(value as usize);

    let Some(codes) = after_id.get(skip..) else {
        return Err(AckError::BadResponse);
    };

    Ok(codes)
}

/// `MQTT_Status_strerror`: the name of a status.
///
/// This crate has no single status enumeration — each module carries the
/// errors its own functions can produce, which is why a caller never has to
/// match on a variant that cannot happen there. So this takes the **C's**
/// enumerator value, and exists because it is a table the library defines and
/// a differential can compare.
///
/// One entry is missing in the C: `12`, `MQTTEndOfProperties`, which
/// `MQTT_GetNextPropertyType` returns at the end of every property walk, prints
/// as `Invalid MQTT Status code`. Transcribed, and in `docs/upstream/`.
#[must_use]
pub const fn status_name(code: u8) -> &'static str {
    match code {
        0 => "MQTTSuccess",
        1 => "MQTTBadParameter",
        2 => "MQTTNoMemory",
        3 => "MQTTSendFailed",
        4 => "MQTTRecvFailed",
        5 => "MQTTBadResponse",
        6 => "MQTTServerRefused",
        7 => "MQTTNoDataAvailable",
        8 => "MQTTIllegalState",
        9 => "MQTTStateCollision",
        10 => "MQTTKeepAliveTimeout",
        11 => "MQTTNeedMoreBytes",
        // 12 is MQTTEndOfProperties, and the C has no case for it.
        13 => "MQTTStatusConnected",
        14 => "MQTTStatusNotConnected",
        15 => "MQTTStatusDisconnectPending",
        16 => "MQTTPublishStoreFailed",
        17 => "MQTTPublishRetrieveFailed",
        _ => "Invalid MQTT Status code",
    }
}

/// `MQTT_GetPacketTypeString`: the name of a packet type byte.
///
/// **PUBLISH is matched on its nibble and everything else on the whole byte**,
/// and both halves of that are right. A PUBLISH's low nibble carries its QoS,
/// DUP and RETAIN flags, so all sixteen of `0x30`..`0x3F` are PUBLISHes; a
/// PUBREL's low nibble is reserved and must be `0x2`, so `0x60` is not a
/// PUBREL but a malformed packet. Three types are like that, and the table is
/// right about all three.
///
/// The asymmetry is easy to transcribe wrongly in the safe-looking direction —
/// matching everything on the whole byte — and the 256-value sweep is what
/// caught exactly that here.
#[must_use]
pub const fn packet_type_name(packet_type: u8) -> &'static str {
    // The flags live in the low nibble, so every one of 0x30..0x3F is one.
    if packet_type & 0xF0 == packet::PUBLISH {
        return "PUBLISH";
    }

    match packet_type {
        packet::CONNECT => "CONNECT",
        packet::CONNACK => "CONNACK",
        packet::PUBACK => "PUBACK",
        packet::PUBREC => "PUBREC",
        packet::PUBREL => "PUBREL",
        packet::PUBCOMP => "PUBCOMP",
        packet::SUBSCRIBE => "SUBSCRIBE",
        packet::SUBACK => "SUBACK",
        packet::UNSUBSCRIBE => "UNSUBSCRIBE",
        packet::UNSUBACK => "UNSUBACK",
        packet::PINGREQ => "PINGREQ",
        packet::PINGRESP => "PINGRESP",
        packet::DISCONNECT => "DISCONNECT",
        packet::AUTH => "AUTH",
        _ => "UNKNOWN",
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

    /// A trailing `+` stops matching an empty last level once an earlier `+`
    /// has been used.
    ///
    /// The specification's own example — `sport/+` against `sport/` — works,
    /// which is what makes this a defect rather than a design. A client
    /// subscribed to `+/+` never receives a message published to `a/`.
    /// Transcribed; drafted for upstream.
    #[test]
    fn a_trailing_plus_loses_the_empty_last_level_after_an_earlier_plus() {
        // The specification's example, and it works.
        assert_eq!(matches(b"sport/", b"sport/+"), Ok(true));
        assert_eq!(matches(b"a/", b"a/+"), Ok(true));

        // The same topic, one `+` earlier in the filter, and it stops.
        assert_eq!(
            matches(b"a/", b"+/+"),
            Ok(false),
            "the matcher now handles an empty last level after a wildcard"
        );
        assert_eq!(matches(b"/", b"+/+"), Ok(false));
        assert_eq!(matches(b"//", b"+/+/+"), Ok(false));
        assert_eq!(matches(b"a//", b"a/+/+"), Ok(false));

        // And it is specifically the EMPTY last level: a non-empty one is
        // fine, so the filter and the topic are not the problem.
        assert_eq!(matches(b"/a", b"+/+"), Ok(true));
        assert_eq!(matches(b"a/b", b"+/+"), Ok(true));
        assert_eq!(matches(b"/finance", b"+/+"), Ok(true));
    }

    /// The exact-match shortcut is an optimisation, not a rule.
    ///
    /// This started as a poison that did not fire: deleting the `strncmp` fast
    /// path changed no answer anywhere in the differential. It is worth
    /// knowing rather than guessing, because the shortcut is also the only
    /// reason a filter containing a literal `+` matches a topic containing one
    /// — or so it looks, and it is not so.
    ///
    /// Every string over `{a, /, +, #, $}` up to length four is its own topic
    /// name and its own filter, and the general walk matches all of them
    /// without help. So the fast path is what its name says.
    #[test]
    fn the_exact_match_shortcut_never_changes_an_answer() {
        let alphabet = *b"a/+#$";
        let mut checked = 0usize;

        for length in 1..=4usize {
            let total = alphabet.len().pow(length as u32);

            for i in 0..total {
                let mut value = i;
                let mut word = vec![0u8; length];

                for position in 0..length {
                    word[length - 1 - position] = alphabet[value % alphabet.len()];
                    value /= alphabet.len();
                }

                // What the public entry point says, shortcut and all.
                assert_eq!(
                    matches(&word, &word),
                    Ok(true),
                    "{:?} does not match itself",
                    String::from_utf8_lossy(&word)
                );

                // And what the walk says on its own, which is the same.
                assert!(
                    match_filter(&word, &word),
                    "{:?} needs the shortcut to match itself",
                    String::from_utf8_lossy(&word)
                );

                checked += 1;
            }
        }

        assert_eq!(checked, 5 + 25 + 125 + 625);
    }

    /// §4.7.2: a `$` topic is invisible to a filter that starts with a
    /// wildcard.
    #[test]
    fn a_dollar_topic_hides_from_a_leading_wildcard() {
        assert_eq!(matches(b"$SYS/broker", b"#"), Ok(false));
        assert_eq!(matches(b"$SYS/broker", b"+/broker"), Ok(false));

        // Named explicitly, it is visible.
        assert_eq!(matches(b"$SYS/broker", b"$SYS/#"), Ok(true));
        assert_eq!(matches(b"$SYS/broker", b"$SYS/+"), Ok(true));

        // And only at the START of the topic.
        assert_eq!(matches(b"a$b", b"#"), Ok(true));
    }

    /// A wildcard character is only a wildcard where a wildcard may be.
    ///
    /// `a+/b` contains a literal `+`, because a wildcard must start a filter or
    /// follow a `/`. The exact-match path is what lets such a filter match at
    /// all.
    #[test]
    fn a_wildcard_out_of_position_is_an_ordinary_character() {
        assert_eq!(matches(b"sport+", b"sport+"), Ok(true));
        assert_eq!(matches(b"sportx", b"sport+"), Ok(false));
        assert_eq!(matches(b"sport#", b"sport#"), Ok(true));
        assert_eq!(matches(b"sportx", b"sport#"), Ok(false));
        assert_eq!(matches(b"a/b", b"a+/b"), Ok(false));
        assert_eq!(matches(b"a/b", b"+a/b"), Ok(false));

        // `#` must also be LAST.
        assert_eq!(matches(b"a/b", b"#/b"), Ok(false));
        assert_eq!(matches(b"a/b", b"a/#/b"), Ok(false));
    }

    /// An empty topic or filter is refused, not answered.
    ///
    /// "No match" and "you asked a question I cannot answer" are different, and
    /// a caller that conflated them would treat a bad subscription as a topic
    /// that simply did not match.
    #[test]
    fn an_empty_side_is_refused_rather_than_unmatched() {
        assert_eq!(matches(b"", b"a"), Err(TopicError::BadParameter));
        assert_eq!(matches(b"a", b""), Err(TopicError::BadParameter));
        assert_eq!(matches(b"", b""), Err(TopicError::BadParameter));

        // And the 16-bit bound the C checks, which no plausible topic reaches.
        let huge = vec![b'a'; 65_536];
        assert_eq!(matches(&huge, b"a"), Err(TopicError::BadParameter));
        assert_eq!(matches(b"a", &huge), Err(TopicError::BadParameter));

        // One byte below it is fine, so the bound is the length and not the
        // size of the allocation.
        let big = vec![b'a'; 65_535];
        assert_eq!(matches(&big, b"#"), Ok(true));
    }

    /// The status table is missing the status a property walk ends with.
    #[test]
    fn the_status_table_has_a_hole_where_end_of_properties_should_be() {
        assert_eq!(status_name(11), "MQTTNeedMoreBytes");
        assert_eq!(
            status_name(12),
            "Invalid MQTT Status code",
            "the C has grown a name for MQTTEndOfProperties"
        );
        assert_eq!(status_name(13), "MQTTStatusConnected");
        assert_eq!(status_name(17), "MQTTPublishRetrieveFailed");
        assert_eq!(status_name(18), "Invalid MQTT Status code");
    }

    /// PUBLISH is named by its nibble; everything else by its whole byte.
    ///
    /// This one was transcribed wrongly first time — every type on the whole
    /// byte, which is the tidier-looking rule — and the 256-value digest is
    /// what caught it. The flags in a PUBLISH's low nibble are the reason, and
    /// a reserved bit in three other types is the reason for the other half.
    #[test]
    fn publish_is_named_by_its_nibble_and_the_rest_by_their_whole_byte() {
        for flags in 0..=0x0Fu8 {
            assert_eq!(
                packet_type_name(0x30 | flags),
                "PUBLISH",
                "a PUBLISH with flags {flags:#x} lost its name"
            );
        }

        assert_eq!(packet_type_name(0x60), "UNKNOWN");
        assert_eq!(packet_type_name(0x62), "PUBREL");
        assert_eq!(packet_type_name(0x80), "UNKNOWN");
        assert_eq!(packet_type_name(0x82), "SUBSCRIBE");
        assert_eq!(packet_type_name(0xA0), "UNKNOWN");
        assert_eq!(packet_type_name(0xA2), "UNSUBSCRIBE");

        // Which agrees with what the fixed header accepts: `0x60` is a PUBREL
        // with its reserved bit clear, and neither of them will have it.
        assert!(!crate::header::incoming_packet_valid(0x60));
        assert!(crate::header::incoming_packet_valid(0x62));
    }
}
