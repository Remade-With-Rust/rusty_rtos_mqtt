//! Which property may go in which outgoing packet.
//!
//! MQTT 5 lets almost every packet carry properties, and a **different set**
//! for each. coreMQTT enforces that with six hand-written validators, one per
//! packet kind, plus the DISCONNECT's — which lives in
//! [`disconnect`](crate::disconnect) because it needs the session expiry the
//! CONNECT asked for.
//!
//! Six tables is what this package has learned to sweep: each is a switch on
//! one byte, so each is swept over all 256 identifiers at each of six value
//! shapes and the accepted set is printed. Thirty-six sweeps, and together they
//! are the whole map.
//!
//! # The writing side is laxer than the reading side
//!
//! Sweeping them found three places where an outgoing packet is allowed
//! something the **same library** refuses on the way in, and the specification
//! forbids. All three are in the PUBLISH table:
//!
//! 1. **A Topic Alias of zero** passes [`validate_publish_properties`].
//!    [`publish`](crate::publish) refuses it coming in, and MQTT 5.0 §3.3.2.3.4
//!    says a sender must not send one.
//! 2. **A Payload Format Indicator above 1** passes it too, where
//!    [`validate_will_properties`] and the incoming deserializer both refuse
//!    anything but 0 or 1 (§3.3.2.3.2).
//! 3. **It deduplicates nothing but the Topic Alias.** Its `used` flag is
//!    declared *inside* the property loop, so the flag is false at every
//!    property and no repeat is ever seen — a repeated Payload Format
//!    Indicator, Content Type, Message Expiry, Response Topic or Correlation
//!    Data all pass. The Topic Alias escapes because its flag is declared
//!    *outside* the loop. The acknowledgement validator is the same loop one
//!    brace apart, and dedupes.
//!
//! All three are transcribed as they stand, pinned by unit tests, and written
//! up in `docs/upstream/`.
//!
//! # Two statuses, mixed by who refused rather than by what was wrong
//!
//! CONNECT and the will answer `MQTTBadResponse` for almost everything;
//! SUBSCRIBE, PUBLISH, the acks and UNSUBSCRIBE answer `MQTTBadParameter` for
//! things of the same kind. The split is not quite per table either: those four
//! still answer **`BadResponse`** whenever the refusal came from a *shared*
//! decoder — a truncated string, a malformed variable-byte integer — because
//! those helpers carry their own status and it is passed straight out. So each
//! of those tables answers both, by who refused. Transcribed, not tidied.
//!
//! # And one cross-property rule, checked after the walk
//!
//! [MQTT-3.1.2-32]: authentication data with no authentication method is a
//! protocol error. The C checks it **after** the property loop, so the sweep
//! sees `0x16` refused at every shape and only a paired case shows why — a
//! reminder that a sweep tells you what a table accepts **in isolation**, and a
//! cross-property rule needs cases.

use crate::header::REMAINING_LENGTH_INVALID;
use crate::outpublish::OutgoingPublish;
use crate::property::{PropertyError, PropertyReader};
use crate::state::QoS;

/// Why an outgoing property section was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidateError {
    /// `MQTTBadParameter`.
    BadParameter,
    /// `MQTTBadResponse`.
    BadResponse,
}

impl From<PropertyError> for ValidateError {
    /// Every shared decoder answers `MQTTBadResponse`, in all six tables.
    fn from(_: PropertyError) -> Self {
        Self::BadResponse
    }
}

/// What [`validate_connect_properties`] learned on the way through.
///
/// An accumulator the caller owns, the way `Read::read_to_end` takes one — and
/// for the same reason: the C writes through its two pointers **as it goes**,
/// so a refusal can still leave one of them written.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ConnectValidation {
    /// Whether the client asked for reason strings and user properties.
    ///
    /// Set to `false` at entry and raised only by a Request Problem
    /// Information of 1, so an absent property leaves it **false** — not
    /// "unknown", and not MQTT's own default of true.
    pub request_problem_info: bool,
    /// The maximum packet size the client announced, if it announced one.
    ///
    /// `None` is "the property was absent", which in the C is invisible: it
    /// leaves the caller's variable alone, and only a caller that pre-set it to
    /// something impossible can tell. An `Option` says it and cannot be missed.
    pub max_packet_size: Option<u32>,
}

/// The property identifiers these six tables accept between them.
pub mod id {
    /// Payload Format Indicator.
    pub const PAYLOAD_FORMAT: u8 = 0x01;
    /// Message Expiry Interval.
    pub const MESSAGE_EXPIRY: u8 = 0x02;
    /// Content Type.
    pub const CONTENT_TYPE: u8 = 0x03;
    /// Response Topic.
    pub const RESPONSE_TOPIC: u8 = 0x08;
    /// Correlation Data.
    pub const CORRELATION_DATA: u8 = 0x09;
    /// Subscription Identifier.
    pub const SUBSCRIPTION_ID: u8 = 0x0B;
    /// Session Expiry Interval.
    pub const SESSION_EXPIRY: u8 = 0x11;
    /// Authentication Method.
    pub const AUTH_METHOD: u8 = 0x15;
    /// Authentication Data.
    pub const AUTH_DATA: u8 = 0x16;
    /// Request Problem Information.
    pub const REQUEST_PROBLEM: u8 = 0x17;
    /// Will Delay Interval.
    pub const WILL_DELAY: u8 = 0x18;
    /// Request Response Information.
    pub const REQUEST_RESPONSE: u8 = 0x19;
    /// Reason String.
    pub const REASON_STRING: u8 = 0x1F;
    /// Receive Maximum.
    pub const RECEIVE_MAX: u8 = 0x21;
    /// Topic Alias Maximum.
    pub const TOPIC_ALIAS_MAX: u8 = 0x22;
    /// Topic Alias.
    pub const TOPIC_ALIAS: u8 = 0x23;
    /// User Property.
    pub const USER_PROPERTY: u8 = 0x26;
    /// Maximum Packet Size.
    pub const MAX_PACKET_SIZE: u8 = 0x27;
}

/// A reader over a property section the caller supplied.
///
/// The C's argument check, minus the null-pointer refusals a `&[u8]` cannot
/// reach — three more members of that family. What is left is the length bound,
/// and it answers `MQTTBadParameter` in **all six** tables, including the two
/// whose every other refusal is `MQTTBadResponse`.
fn reader(properties: &[u8]) -> Result<PropertyReader<'_>, ValidateError> {
    let length = u32::try_from(properties.len()).map_err(|_| ValidateError::BadParameter)?;

    if length >= REMAINING_LENGTH_INVALID {
        return Err(ValidateError::BadParameter);
    }

    Ok(PropertyReader::new(properties, length))
}

/// `checkOnce`: a property may appear once, and the bit says whether it has.
fn check_once(seen: &mut bool) -> Result<(), ValidateError> {
    if *seen {
        return Err(ValidateError::BadResponse);
    }

    *seen = true;
    Ok(())
}

/// `MQTT_ValidateConnectProperties`, and the `validateConnectProperty` it
/// drives.
///
/// `found` is an accumulator, not a return value, because the C writes through
/// its two pointers **during** the walk: a Maximum Packet Size followed by a
/// bad property leaves the size written and the status a failure. Taking it by
/// `&mut` reproduces that, where `Result<ConnectValidation, _>` would quietly
/// hide it — and the differential compares both fields on every case, refused
/// ones included.
///
/// `found.request_problem_info` is cleared once the length check passes, which
/// is where the C clears it: a section too long to validate leaves even that
/// alone.
///
/// # Errors
///
/// [`ValidateError::BadResponse`] for an unknown or repeated property, a zero
/// Receive Maximum or Maximum Packet Size, a Request Problem/Response
/// Information that is neither 0 nor 1, or a malformed value.
/// [`ValidateError::BadParameter`] for a section longer than
/// [`REMAINING_LENGTH_INVALID`], and for authentication data with no
/// authentication method — the one rule checked after the walk, and the one
/// with the other status.
pub fn validate_connect_properties(
    properties: &[u8],
    found: &mut ConnectValidation,
) -> Result<(), ValidateError> {
    let mut reader = reader(properties)?;

    // Cleared where the C clears it: AFTER the argument check, so a refusal
    // there leaves the caller's value alone and a refusal later does not.
    found.request_problem_info = false;

    let (mut session_expiry, mut receive_max, mut max_packet) = (false, false, false);
    let (mut topic_alias_max, mut request_response, mut request_problem) = (false, false, false);
    let (mut auth_method, mut auth_data) = (false, false);

    while reader.remaining() > 0 {
        // Fresh at every property, as the C's is: `validateConnectProperty`
        // declares it and is called once per property. Repeats are caught by
        // the bit mask, not by this.
        let mut used = false;

        match reader.property_id()? {
            id::SESSION_EXPIRY => {
                check_once(&mut session_expiry)?;
                let _ = reader.u32(&mut used)?;
            }

            id::RECEIVE_MAX => {
                check_once(&mut receive_max)?;

                if reader.u16(&mut used)? == 0 {
                    return Err(ValidateError::BadResponse);
                }
            }

            id::MAX_PACKET_SIZE => {
                check_once(&mut max_packet)?;
                let value = reader.u32(&mut used)?;

                if value == 0 {
                    return Err(ValidateError::BadResponse);
                }

                found.max_packet_size = Some(value);
            }

            id::TOPIC_ALIAS_MAX => {
                check_once(&mut topic_alias_max)?;
                let _ = reader.u16(&mut used)?;
            }

            id::REQUEST_RESPONSE => {
                check_once(&mut request_response)?;

                if reader.u8(&mut used)? > 1 {
                    return Err(ValidateError::BadResponse);
                }
            }

            id::REQUEST_PROBLEM => {
                check_once(&mut request_problem)?;
                let value = reader.u8(&mut used)?;

                if value > 1 {
                    return Err(ValidateError::BadResponse);
                }

                found.request_problem_info = value == 1;
            }

            id::AUTH_METHOD => {
                check_once(&mut auth_method)?;
                let _ = reader.utf8(&mut used)?;
            }

            id::AUTH_DATA => {
                check_once(&mut auth_data)?;
                // Binary data by the specification, read as a string: the same
                // wire shape, and neither arm checks that a string is UTF-8.
                let _ = reader.utf8(&mut used)?;
            }

            id::USER_PROPERTY => {
                let _ = reader.user_property()?;
            }

            _ => return Err(ValidateError::BadResponse),
        }
    }

    // [MQTT-3.1.2-32], checked AFTER the walk -- so the order the two appear in
    // does not matter, and a sweep over one property at a time sees `0x16`
    // refused at every shape without ever seeing why.
    if auth_data && !auth_method {
        return Err(ValidateError::BadParameter);
    }

    Ok(())
}

/// `MQTT_ValidateWillProperties`.
///
/// Seven properties, and it is the **strict** one: a Payload Format Indicator
/// must be 0 or 1, and every property is deduplicated. Worth comparing with
/// [`validate_publish_properties`], which carries five of the same seven and
/// checks neither.
///
/// # Errors
///
/// [`ValidateError::BadResponse`] for an unknown or repeated property, a
/// Payload Format Indicator above 1, or a malformed value.
/// [`ValidateError::BadParameter`] only for a section longer than
/// [`REMAINING_LENGTH_INVALID`].
pub fn validate_will_properties(properties: &[u8]) -> Result<(), ValidateError> {
    let mut reader = reader(properties)?;

    let (mut will_delay, mut payload_format, mut message_expiry) = (false, false, false);
    let (mut content_type, mut response_topic, mut correlation_data) = (false, false, false);

    while reader.remaining() > 0 {
        let mut used = false;

        match reader.property_id()? {
            id::WILL_DELAY => {
                check_once(&mut will_delay)?;
                let _ = reader.u32(&mut used)?;
            }

            id::PAYLOAD_FORMAT => {
                check_once(&mut payload_format)?;

                if reader.u8(&mut used)? > 1 {
                    return Err(ValidateError::BadResponse);
                }
            }

            id::MESSAGE_EXPIRY => {
                check_once(&mut message_expiry)?;
                let _ = reader.u32(&mut used)?;
            }

            id::CONTENT_TYPE => {
                check_once(&mut content_type)?;
                let _ = reader.utf8(&mut used)?;
            }

            id::RESPONSE_TOPIC => {
                check_once(&mut response_topic)?;
                let _ = reader.utf8(&mut used)?;
            }

            id::CORRELATION_DATA => {
                check_once(&mut correlation_data)?;
                let _ = reader.utf8(&mut used)?;
            }

            id::USER_PROPERTY => {
                let _ = reader.user_property()?;
            }

            _ => return Err(ValidateError::BadResponse),
        }
    }

    Ok(())
}

/// `MQTT_ValidateSubscribeProperties`.
///
/// Two properties, and the Subscription Identifier is conditional: a broker
/// that cleared `isSubscriptionIdAvailable` in its CONNACK may not be sent one.
/// It also refuses a **zero** identifier, which the incoming PUBLISH
/// deserializer does not — see the module note.
///
/// The three checks run in the C's order, which is **after** the value has been
/// decoded: an identifier that is both malformed and unwelcome answers for the
/// malformation, with the other status.
///
/// # Errors
///
/// [`ValidateError::BadParameter`] for an unknown property, a repeated
/// subscription identifier, one the server does not support, or a zero one.
/// [`ValidateError::BadResponse`] for a malformed identifier or user property.
pub fn validate_subscribe_properties(
    subscription_id_available: bool,
    properties: &[u8],
) -> Result<(), ValidateError> {
    let mut reader = reader(properties)?;
    let mut subscription_id = false;

    while reader.remaining() > 0 {
        match reader.property_id()? {
            id::SUBSCRIPTION_ID => {
                if subscription_id {
                    return Err(ValidateError::BadParameter);
                }

                // Decoded BEFORE the two policy checks, and its refusal is the
                // shared decoder's `MQTTBadResponse` rather than this table's
                // `MQTTBadParameter`.
                let value = reader.variable_length()?;

                if !subscription_id_available {
                    return Err(ValidateError::BadParameter);
                }

                // §3.8.2.1.2: zero is a protocol error. The C checks it here
                // and does NOT check it when reading a PUBLISH.
                if value == 0 {
                    return Err(ValidateError::BadParameter);
                }

                subscription_id = true;
            }

            id::USER_PROPERTY => {
                let _ = reader.user_property()?;
            }

            _ => return Err(ValidateError::BadParameter),
        }
    }

    Ok(())
}

/// `MQTT_ValidatePublishProperties`.
///
/// Seven properties, and the **laxest** table in the library. It bounds the
/// Topic Alias by what the server announced and refuses a repeated one — and it
/// does not bound the Payload Format Indicator to 0 or 1, does not refuse a
/// Topic Alias of **zero**, and does not refuse a repeat of anything else. All
/// three are divergences from MQTT 5.0 that the reading side gets right; see
/// the module note.
///
/// `topic_alias` is an accumulator for the same reason as
/// [`validate_connect_properties`]'s, and here the difference is visible in the
/// trace: **the C writes the alias before it checks the bound**, so an alias
/// over the maximum is refused with the offending value left behind.
///
/// # Errors
///
/// [`ValidateError::BadParameter`] for an unknown property, a Topic Alias above
/// `server_topic_alias_max`, or a Subscription Identifier — which
/// [MQTT-3.3.4-6] forbids a client to send. [`ValidateError::BadResponse`] for
/// a repeated Topic Alias or a malformed value.
pub fn validate_publish_properties(
    server_topic_alias_max: u16,
    properties: &[u8],
    topic_alias: &mut Option<u16>,
) -> Result<(), ValidateError> {
    let mut reader = reader(properties)?;

    // The ONE flag the C declares outside its loop, and so the one property
    // this table deduplicates.
    let mut alias_used = false;

    while reader.remaining() > 0 {
        // ...whereas this one the C declares INSIDE the loop, which makes it
        // false at every property and every check below it dead. Written the
        // same way deliberately: it is the defect, and a tidier transcription
        // would hide it.
        let mut used = false;

        match reader.property_id()? {
            id::PAYLOAD_FORMAT => {
                let _ = reader.u8(&mut used)?;
            }

            id::MESSAGE_EXPIRY => {
                let _ = reader.u32(&mut used)?;
            }

            id::CONTENT_TYPE | id::RESPONSE_TOPIC | id::CORRELATION_DATA => {
                let _ = reader.utf8(&mut used)?;
            }

            id::TOPIC_ALIAS => {
                let value = reader.u16(&mut alias_used)?;
                *topic_alias = Some(value);

                // The upper bound is checked and the lower one is not: a zero
                // alias passes here and is refused on the way in. The bound is
                // `max < alias`, so the maximum itself is legal.
                if server_topic_alias_max < value {
                    return Err(ValidateError::BadParameter);
                }
            }

            id::USER_PROPERTY => {
                let _ = reader.user_property()?;
            }

            _ => return Err(ValidateError::BadParameter),
        }
    }

    Ok(())
}

/// `MQTT_ValidatePublishAckProperties`: a reason string and user properties.
///
/// The same loop as [`validate_publish_properties`] with its flag declared one
/// brace higher, so this one **does** refuse a repeated reason string.
///
/// # Errors
///
/// [`ValidateError::BadParameter`] for any other property.
/// [`ValidateError::BadResponse`] for a repeated reason string or a malformed
/// value.
pub fn validate_publish_ack_properties(properties: &[u8]) -> Result<(), ValidateError> {
    let mut reader = reader(properties)?;

    // Outside the loop, so it survives from one property to the next.
    let mut reason_string = false;

    while reader.remaining() > 0 {
        match reader.property_id()? {
            id::REASON_STRING => {
                let _ = reader.utf8(&mut reason_string)?;
            }

            id::USER_PROPERTY => {
                let _ = reader.user_property()?;
            }

            _ => return Err(ValidateError::BadParameter),
        }
    }

    Ok(())
}

/// `MQTT_ValidateUnsubscribeProperties`: user properties, and nothing else.
///
/// The narrowest table in the library — one identifier of 256.
///
/// # Errors
///
/// [`ValidateError::BadParameter`] for any other property.
/// [`ValidateError::BadResponse`] for a malformed user property.
pub fn validate_unsubscribe_properties(properties: &[u8]) -> Result<(), ValidateError> {
    let mut reader = reader(properties)?;

    while reader.remaining() > 0 {
        match reader.property_id()? {
            id::USER_PROPERTY => {
                let _ = reader.user_property()?;
            }

            _ => return Err(ValidateError::BadParameter),
        }
    }

    Ok(())
}

/// `MQTT_ValidatePublishParams`: the six things an outgoing PUBLISH is checked
/// against before it is sized or serialized.
///
/// Not a property table — the packet's own fields, against the limits the
/// broker announced in its CONNACK.
///
/// # It enforces the rule the reading side does not
///
/// A zero-length topic name with no Topic Alias is refused here, which is
/// [MQTT-3.3.2-8]; [`publish`](crate::publish) accepts exactly that coming in.
/// The same shape as the three divergences in the module note, pointing the
/// other way, and evidence that the rule is known to the library.
///
/// # And one it gets wrong
///
/// The QoS check is `qos != 0 && max_qos == 0`, so it refuses QoS 1 and 2 when
/// the broker said Maximum QoS **0** and lets **QoS 2 through when the broker
/// said Maximum QoS 1**. [MQTT-3.2.2-11] forbids sending above the announced
/// maximum, and a broker that means it answers `0x9B` and drops the session.
/// Transcribed, pinned by
/// [`a_qos_above_the_maximum_is_only_refused_at_zero`](self), and drafted for
/// upstream.
///
/// # Errors
///
/// [`ValidateError::BadParameter`], which is the only status this function has:
/// retain when the broker does not support it, a QoS above zero when the broker
/// supports none, an empty topic name with no alias, a topic name longer than
/// 65,535 bytes, or a maximum packet size of zero.
pub fn validate_publish_params(
    publish: &OutgoingPublish<'_>,
    retain_available: u8,
    max_qos: u8,
    topic_alias: u16,
    max_packet_size: u32,
) -> Result<(), ValidateError> {
    if publish.retain && retain_available == 0 {
        return Err(ValidateError::BadParameter);
    }

    // The defect: `max_qos == 0`, not `qos > max_qos`.
    if publish.qos != QoS::AtMostOnce && max_qos == 0 {
        return Err(ValidateError::BadParameter);
    }

    // [MQTT-3.3.2-8]. The alias is the caller's, not the property section's,
    // because a section is not parsed here.
    if topic_alias == 0 && publish.topic_name.is_empty() {
        return Err(ValidateError::BadParameter);
    }

    // The C follows it with `pTopicName == NULL && topicNameLength != 0`, which
    // a `&[u8]` cannot be: two numbers that must agree, carried as one.
    if u16::try_from(publish.topic_name.len()).is_err() {
        return Err(ValidateError::BadParameter);
    }

    if max_packet_size == 0 {
        return Err(ValidateError::BadParameter);
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

    /// Read `section` back as the property part of an incoming QoS 0 PUBLISH.
    ///
    /// The point of the comparison: the same library, the same property
    /// identifiers, the opposite direction.
    fn the_reader_accepts(section: &[u8]) -> bool {
        let mut body = vec![0x00, 0x01, b'a', u8::try_from(section.len()).unwrap()];
        body.extend_from_slice(section);

        let packet = crate::ack::PacketInfo {
            packet_type: 0x30,
            remaining_length: u32::try_from(body.len()).unwrap(),
            remaining_data: &body,
        };

        crate::publish::deserialize_publish(&packet, 1024, 10).is_ok()
    }

    fn publish(section: &[u8]) -> Result<Option<u16>, ValidateError> {
        let mut alias = None;
        validate_publish_properties(10, section, &mut alias)?;
        Ok(alias)
    }

    /// Three places the writing side is laxer than the library's own reader.
    ///
    /// Findings, not fixtures, and written up in `docs/upstream/`. Pinned from
    /// **both** directions, because "the library already gets this right
    /// somewhere else" is what makes an upstream report hard to dismiss — the
    /// same shape as the acknowledgement reason codes.
    #[test]
    fn the_publish_validator_is_laxer_than_the_publish_reader() {
        // 1. A Topic Alias of zero. §3.3.2.3.4 forbids sending one.
        assert_eq!(publish(&[0x23, 0x00, 0x00]), Ok(Some(0)));
        assert!(
            !the_reader_accepts(&[0x23, 0x00, 0x00]),
            "the reader now accepts a zero Topic Alias too"
        );

        // 2. A Payload Format Indicator above 1. §3.3.2.3.2.
        assert_eq!(publish(&[0x01, 0x02]), Ok(None));
        assert!(
            !the_reader_accepts(&[0x01, 0x02]),
            "the reader now accepts a Payload Format above 1 too"
        );

        // 3. A repeat of anything but the Topic Alias.
        assert_eq!(publish(&[0x01, 0x01, 0x01, 0x00]), Ok(None));
        assert_eq!(
            publish(&[0x03, 0x00, 0x01, b't', 0x03, 0x00, 0x01, b't']),
            Ok(None)
        );
        assert_eq!(
            publish(&[0x23, 0x00, 0x05, 0x23, 0x00, 0x06]),
            Err(ValidateError::BadResponse),
            "the Topic Alias is the one property this table deduplicates"
        );

        // And the will table, strict about exactly what the publish one is lax
        // about, with five of the same seven identifiers.
        assert_eq!(
            validate_will_properties(&[0x01, 0x02]),
            Err(ValidateError::BadResponse)
        );
        assert_eq!(
            validate_will_properties(&[0x01, 0x01, 0x01, 0x00]),
            Err(ValidateError::BadResponse)
        );
        assert_eq!(
            validate_publish_ack_properties(&[0x1F, 0x00, 0x01, b'x', 0x1F, 0x00, 0x01, b'x']),
            Err(ValidateError::BadResponse)
        );
    }

    /// The subscription identifier: refused at zero going OUT, accepted coming
    /// IN.
    ///
    /// The mirror of a finding already drafted from the other side, and the
    /// evidence that makes it a duplication bug rather than an omission — the
    /// library knows the rule.
    #[test]
    fn a_zero_subscription_id_is_refused_going_out_and_accepted_coming_in() {
        assert_eq!(
            validate_subscribe_properties(true, &[0x0B, 0x00]),
            Err(ValidateError::BadParameter)
        );
        assert!(
            the_reader_accepts(&[0x0B, 0x00]),
            "the reader now refuses a zero subscription identifier too"
        );
    }

    /// Authentication data needs a method, in either order.
    ///
    /// [MQTT-3.1.2-32], checked AFTER the walk — so the order does not matter,
    /// and a sweep that sends one property at a time sees `0x16` refused at
    /// every shape without ever seeing why. **A sweep says what a table accepts
    /// in isolation; a cross-property rule needs a case.**
    #[test]
    fn authentication_data_needs_a_method_in_either_order() {
        let mut found = ConnectValidation::default();

        assert_eq!(
            validate_connect_properties(&[0x16, 0x00, 0x01, b'd'], &mut found),
            Err(ValidateError::BadParameter)
        );

        assert!(
            validate_connect_properties(
                &[0x15, 0x00, 0x01, b'x', 0x16, 0x00, 0x01, b'd'],
                &mut found
            )
            .is_ok()
        );
        assert!(
            validate_connect_properties(
                &[0x16, 0x00, 0x01, b'd', 0x15, 0x00, 0x01, b'x'],
                &mut found
            )
            .is_ok()
        );
    }

    /// Absent is not zero, absent is not the default, and a refusal can still
    /// leave a value behind.
    #[test]
    fn what_the_connect_validator_leaves_behind() {
        let mut found = ConnectValidation {
            request_problem_info: true,
            max_packet_size: Some(7),
        };

        // Entry clears the flag; an absent Maximum Packet Size leaves the
        // caller's value exactly as it was, which is what the C's untouched
        // out-parameter means.
        assert!(validate_connect_properties(&[], &mut found).is_ok());
        assert!(!found.request_problem_info);
        assert_eq!(found.max_packet_size, Some(7));

        let mut found = ConnectValidation::default();
        assert!(validate_connect_properties(&[0x17, 0x01], &mut found).is_ok());
        assert!(found.request_problem_info, "0x17 0x01 means true");
        assert_eq!(found.max_packet_size, None);

        // A size, then a property that fails: the size stays written. This is
        // why both of these are accumulators rather than a returned value, and
        // why the differential compares them on refused cases too.
        let mut found = ConnectValidation::default();
        assert_eq!(
            validate_connect_properties(&[0x27, 0x00, 0x01, 0x00, 0x00, 0xFE], &mut found),
            Err(ValidateError::BadResponse)
        );
        assert_eq!(found.max_packet_size, Some(65_536));
    }

    /// The Topic Alias is written before its bound is checked.
    ///
    /// The same shape one packet over, and the one the C trace shows directly:
    /// `publish-topic-alias-over` refuses with `alias=11` left behind.
    #[test]
    fn the_topic_alias_is_written_before_it_is_checked() {
        let mut alias = None;

        assert_eq!(
            validate_publish_properties(10, &[0x23, 0x00, 0x0B], &mut alias),
            Err(ValidateError::BadParameter)
        );
        assert_eq!(alias, Some(11), "the refused value is left behind");

        // `max < alias`, so the maximum itself is legal.
        let mut alias = None;
        assert!(validate_publish_properties(10, &[0x23, 0x00, 0x0A], &mut alias).is_ok());
        assert_eq!(alias, Some(10));
    }

    /// Each table's two statuses, and which side of the line they fall.
    ///
    /// SUBSCRIBE decides `BadParameter` itself and passes `BadResponse` out of
    /// the shared decoders. Nothing but a malformed value distinguishes them,
    /// so a transcription that mapped every subscribe failure to one status
    /// would pass every other case in the trace.
    #[test]
    fn a_tables_own_refusals_and_its_decoders_are_different_statuses() {
        assert_eq!(
            validate_subscribe_properties(true, &[0x0B, 0xFF]),
            Err(ValidateError::BadResponse),
            "a malformed identifier is the DECODER's refusal"
        );
        assert_eq!(
            validate_subscribe_properties(false, &[0x0B, 0x07]),
            Err(ValidateError::BadParameter),
            "an unwelcome identifier is the TABLE's refusal"
        );
        assert_eq!(
            validate_publish_ack_properties(&[0x1F, 0x00, 0x09, b'x']),
            Err(ValidateError::BadResponse)
        );
        assert_eq!(
            validate_publish_ack_properties(&[0x11, 0x00, 0x00, 0x0E, 0x10]),
            Err(ValidateError::BadParameter)
        );
    }

    /// A QoS above the broker's maximum is refused only when the maximum is
    /// zero.
    ///
    /// The C's check is `qos != 0 && maxQos == 0`. So QoS 2 to a broker that
    /// announced Maximum QoS 1 is a protocol error the library builds, sends
    /// and is disconnected for. Drafted for upstream; transcribed here.
    #[test]
    fn a_qos_above_the_maximum_is_only_refused_at_zero() {
        let publish = |qos| OutgoingPublish {
            qos,
            dup: false,
            retain: false,
            topic_name: b"a",
            payload: b"",
            properties: b"",
        };

        // What it does catch.
        for qos in [QoS::AtLeastOnce, QoS::ExactlyOnce] {
            assert_eq!(
                validate_publish_params(&publish(qos), 1, 0, 0, 1024),
                Err(ValidateError::BadParameter)
            );
        }

        // And what it does not: QoS 2 where the broker allows at most 1.
        assert_eq!(
            validate_publish_params(&publish(QoS::ExactlyOnce), 1, 1, 0, 1024),
            Ok(()),
            "the library now refuses a QoS above the announced maximum"
        );
    }

    /// The outgoing side enforces [MQTT-3.3.2-8]; the incoming side does not.
    ///
    /// A zero-length topic name needs a Topic Alias. The reader accepts the
    /// same packet, and the application is handed a message with no topic and
    /// no alias to resolve one — which is the first defect in
    /// `coremqtt-publish-protocol-errors.md`, seen from the side that gets it
    /// right.
    #[test]
    fn an_empty_topic_needs_an_alias_going_out_and_not_coming_in() {
        let empty = OutgoingPublish {
            qos: QoS::AtMostOnce,
            dup: false,
            retain: false,
            topic_name: b"",
            payload: b"",
            properties: b"",
        };

        assert_eq!(
            validate_publish_params(&empty, 1, 2, 0, 1024),
            Err(ValidateError::BadParameter)
        );
        assert_eq!(
            validate_publish_params(&empty, 1, 2, 1, 1024),
            Ok(()),
            "an alias makes the empty topic name legal"
        );

        // The same packet read back: a zero-length topic name, no alias, and
        // one byte of payload -- the shape from the upstream draft.
        let mut body = vec![0x00, 0x00, 0x00, 0x70];
        let packet = crate::ack::PacketInfo {
            packet_type: 0x30,
            remaining_length: u32::try_from(body.len()).unwrap(),
            remaining_data: &mut body,
        };
        assert!(
            crate::publish::deserialize_publish(&packet, 1024, 10).is_ok(),
            "the reader now refuses an empty topic name with no alias"
        );
    }

    /// UNSUBSCRIBE takes one property of 256, the narrowest table here.
    #[test]
    fn unsubscribe_takes_one_property_and_no_other() {
        assert!(validate_unsubscribe_properties(&[]).is_ok());
        assert!(
            validate_unsubscribe_properties(&[0x26, 0x00, 0x01, b'k', 0x00, 0x01, b'v']).is_ok()
        );

        // A reason string is legal in an acknowledgement and not here.
        assert_eq!(
            validate_unsubscribe_properties(&[0x1F, 0x00, 0x01, b'x']),
            Err(ValidateError::BadParameter)
        );
        assert!(validate_publish_ack_properties(&[0x1F, 0x00, 0x01, b'x']).is_ok());
    }
}
