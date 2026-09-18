//! Reading the CONNACK — the one packet [`ack`](crate::ack) refuses.
//!
//! `MQTT_DeserializeAck` answers `MQTTBadParameter` for a CONNACK and points
//! the caller here, and the split is not arbitrary. A CONNACK is the **first**
//! thing a broker sends and the only packet that sets connection-wide state:
//! the maximum packet size the server will accept, how many messages it will
//! take in flight, whether retain and wildcards and shared subscriptions work
//! at all, and what keep-alive the client must now use.
//!
//! Every later size check and every later subscription is decided by numbers
//! that arrive in this packet. Getting it wrong is not one malformed message —
//! it is the whole session running under limits somebody else chose.
//!
//! # Two tables, both swept, and this time both were right
//!
//! The reason code is one byte with 22 legal values, and the property
//! identifier is one byte with 17. The [`ack`](crate::ack) slice swept the same
//! shape and found three divergences from MQTT 5.0; these two are swept the
//! same way and are **exactly** the specification's §3.2.2.2 and §3.2.2.3. A
//! sweep that confirms conformance is a result, and it is recorded as one.
//!
//! # A refused connection still parses
//!
//! The C returns `MQTTServerRefused` — a third status — when the reason code
//! is non-zero, and it goes on to parse the property section anyway. That is
//! deliberate and useful: the reason string that says *why* the broker refused
//! lives in those properties.
//!
//! So a refusal is [`Ok`] here, carrying the reason code and everything the
//! packet said. [`ConnAck::refused`] is the C's `MQTTServerRefused`. Making it
//! an `Err` would have thrown away the explanation, which is the one thing a
//! refused client actually needs.

use crate::ack::{AckError, PacketInfo, bounded};
use crate::header::{packet, variable_length_encoded_size};
use crate::property::{PropertyError, PropertyReader, decode_variable_length};

/// `MQTT_PACKET_CONNACK_MINIMUM_SIZE`: flags, reason code, property length.
pub const CONNACK_MINIMUM_SIZE: u32 = 3;

/// `MQTT_PACKET_CONNACK_SESSION_PRESENT_MASK`: the lowest bit of the flags.
pub const SESSION_PRESENT_MASK: u8 = 0x01;

/// The property identifiers a CONNACK may carry (MQTT 5.0 §3.2.2.3).
pub mod property {
    /// `MQTT_SESSION_EXPIRY_ID`.
    pub const SESSION_EXPIRY: u8 = 0x11;
    /// `MQTT_ASSIGNED_CLIENT_ID`.
    pub const ASSIGNED_CLIENT_ID: u8 = 0x12;
    /// `MQTT_SERVER_KEEP_ALIVE_ID`.
    pub const SERVER_KEEP_ALIVE: u8 = 0x13;
    /// `MQTT_AUTH_METHOD_ID`.
    pub const AUTH_METHOD: u8 = 0x15;
    /// `MQTT_AUTH_DATA_ID`. Binary data, not a string — see the note on the
    /// walk below.
    pub const AUTH_DATA: u8 = 0x16;
    /// `MQTT_RESPONSE_INFO_ID`.
    pub const RESPONSE_INFO: u8 = 0x1A;
    /// `MQTT_SERVER_REF_ID`.
    pub const SERVER_REFERENCE: u8 = 0x1C;
    /// `MQTT_REASON_STRING_ID`.
    pub const REASON_STRING: u8 = 0x1F;
    /// `MQTT_RECEIVE_MAX_ID`.
    pub const RECEIVE_MAX: u8 = 0x21;
    /// `MQTT_TOPIC_ALIAS_MAX_ID`.
    pub const TOPIC_ALIAS_MAX: u8 = 0x22;
    /// `MQTT_MAX_QOS_ID`.
    pub const MAX_QOS: u8 = 0x24;
    /// `MQTT_RETAIN_AVAILABLE_ID`.
    pub const RETAIN_AVAILABLE: u8 = 0x25;
    /// `MQTT_USER_PROPERTY_ID`.
    pub const USER_PROPERTY: u8 = 0x26;
    /// `MQTT_MAX_PACKET_SIZE_ID`.
    pub const MAX_PACKET_SIZE: u8 = 0x27;
    /// `MQTT_WILDCARD_ID`.
    pub const WILDCARD_AVAILABLE: u8 = 0x28;
    /// `MQTT_SUB_AVAILABLE_ID`.
    pub const SUBSCRIPTION_ID_AVAILABLE: u8 = 0x29;
    /// `MQTT_SHARED_SUB_ID`.
    pub const SHARED_SUB_AVAILABLE: u8 = 0x2A;
}

/// Bit positions in [`ConnAck::fields_present`], the C's `fieldSet`.
///
/// The C's numbering, kept exactly: these values cross the API in a `u32`, so
/// renumbering them would be a silent format change for anyone reading the
/// bitmap rather than the fields.
pub mod field {
    /// Subscription Identifier.
    pub const SUBSCRIPTION_ID: u32 = 1;
    /// Session Expiry Interval.
    pub const SESSION_EXPIRY_INTERVAL: u32 = 2;
    /// Receive Maximum.
    pub const RECEIVE_MAXIMUM: u32 = 3;
    /// Maximum Packet Size.
    pub const MAX_PACKET_SIZE: u32 = 4;
    /// Topic Alias Maximum.
    pub const TOPIC_ALIAS_MAX: u32 = 5;
    /// Request Response Information.
    pub const REQUEST_RESPONSE_INFO: u32 = 6;
    /// Request Problem Information.
    pub const REQUEST_PROBLEM_INFO: u32 = 7;
    /// Authentication Method.
    pub const AUTHENTICATION_METHOD: u32 = 9;
    /// Authentication Data.
    pub const AUTHENTICATION_DATA: u32 = 10;
    /// Payload Format Indicator.
    pub const PAYLOAD_FORMAT_INDICATOR: u32 = 11;
    /// Message Expiry Interval.
    pub const MESSAGE_EXPIRY_INTERVAL: u32 = 12;
    /// Topic Alias.
    pub const TOPIC_ALIAS: u32 = 13;
    /// Response Topic.
    pub const RESPONSE_TOPIC: u32 = 14;
    /// Correlation Data.
    pub const CORRELATION_DATA: u32 = 15;
    /// Content Type.
    pub const CONTENT_TYPE: u32 = 16;
    /// Reason String.
    pub const REASON_STRING: u32 = 17;
    /// Will Delay Interval.
    pub const WILL_DELAY: u32 = 18;
    /// Assigned Client Identifier.
    pub const ASSIGNED_CLIENT_ID: u32 = 19;
    /// Server Keep Alive.
    pub const SERVER_KEEP_ALIVE: u32 = 20;
    /// Response Information.
    pub const RESPONSE_INFORMATION: u32 = 21;
    /// Server Reference.
    pub const SERVER_REFERENCE: u32 = 22;
    /// Maximum QoS.
    pub const MAX_QOS: u32 = 23;
    /// Retain Available.
    pub const RETAIN_AVAILABLE: u32 = 24;
    /// Wildcard Subscription Available.
    pub const WILDCARD_SUBSCRIPTION_AVAILABLE: u32 = 25;
    /// Subscription Identifiers Available.
    pub const SUBSCRIPTION_ID_AVAILABLE: u32 = 26;
    /// Shared Subscription Available.
    pub const SHARED_SUBSCRIPTION_AVAILABLE: u32 = 27;
    /// User Property.
    pub const USER_PROP: u32 = 28;
}

/// What the client told the broker, and which the CONNACK is checked against.
///
/// The C keeps these in the same `MQTTConnectionProperties_t` it writes the
/// server's answers into. They are split here because they are **inputs**: a
/// caller who cannot tell which fields it is supposed to fill in is one
/// refactor away from feeding the broker's numbers back as its own.
#[derive(Debug, Clone, Copy)]
pub struct ClientSettings {
    /// `maxPacketSize`: what this client will accept. Zero is refused.
    pub max_packet_size: u32,
    /// `requestResponseInfo`: whether the client asked for Response
    /// Information. If it did not, a broker that sends it anyway has committed
    /// a protocol error and the packet is refused.
    ///
    /// Note that Reason String and User Property are **not** gated this way,
    /// and that is correct: MQTT 5.0 [MQTT-3.1.2-29] lets a server return them
    /// on a CONNACK whatever Request Problem Information said. Only the other
    /// packet types gate on it — see [`ack::Limits`](crate::ack::Limits).
    pub request_response_info: bool,
}

/// What the broker said, with zero standing for each absent property.
///
/// Check [`ConnAck::fields_present`] to tell "the server said 0" from "the
/// server said nothing"; the C makes the same distinction with its `fieldSet`,
/// and the distinction is real — a Maximum QoS of 0 and an absent Maximum QoS
/// mean opposite things.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ServerSettings {
    /// Session Expiry Interval, in seconds.
    pub session_expiry: u32,
    /// Receive Maximum: how many QoS 1 and 2 messages the server will take in
    /// flight. Never zero — the C refuses a CONNACK that says so.
    pub receive_max: u16,
    /// Maximum QoS the server supports, 0 or 1. Absent means 2.
    pub max_qos: u8,
    /// Whether retained messages work. Absent means yes.
    pub retain_available: u8,
    /// The largest packet the server will accept. Never zero.
    pub max_packet_size: u32,
    /// The highest topic alias the server will accept.
    pub topic_alias_max: u16,
    /// Whether wildcard subscriptions work. Absent means yes.
    pub wildcard_available: u8,
    /// Whether subscription identifiers work. Absent means yes.
    pub subscription_id_available: u8,
    /// Whether shared subscriptions work. Absent means yes.
    pub shared_available: u8,
    /// The keep-alive the server is imposing, which overrides the client's.
    pub keep_alive: u16,
}

/// A CONNACK, read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnAck<'a> {
    /// Whether the broker resumed a previous session. MQTT 5.0 requires this
    /// to be false whenever the reason code is non-zero, and the C enforces it.
    pub session_present: bool,
    /// §3.2.2.2. Zero is success; anything else is a refusal with a reason.
    pub reason_code: u8,
    /// Every limit the rest of the session runs under.
    pub server: ServerSettings,
    /// Which properties the packet actually carried, by [`field`] bit.
    pub fields_present: u32,
    /// The property section, without its length prefix.
    pub properties: &'a [u8],
}

impl ConnAck<'_> {
    /// The C's `MQTTServerRefused`: the packet parsed, and the broker said no.
    ///
    /// The properties are still filled in, because the Reason String that says
    /// *why* lives in them and is the one thing a refused client needs.
    #[must_use]
    pub const fn refused(&self) -> bool {
        self.reason_code != 0
    }
}

/// Why a CONNACK was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnAckError {
    /// `MQTTBadParameter`: not a CONNACK, or a zero maximum packet size.
    BadParameter,
    /// `MQTTBadResponse`: the packet was wrong. Everything a broker can cause.
    BadResponse,
}

impl From<PropertyError> for ConnAckError {
    fn from(_: PropertyError) -> Self {
        Self::BadResponse
    }
}

impl From<AckError> for ConnAckError {
    fn from(error: AckError) -> Self {
        match error {
            AckError::BadParameter => Self::BadParameter,
            AckError::BadResponse => Self::BadResponse,
        }
    }
}

/// `MQTT_DeserializeConnAck`.
///
/// # Errors
///
/// [`ConnAckError::BadParameter`] if the packet is not a CONNACK, or
/// `max_packet_size` is zero. [`ConnAckError::BadResponse`] for everything the
/// packet itself can get wrong: too large, a body under three bytes, a reserved
/// flag bit set, session-present with a non-zero reason code, an unknown reason
/// code, a property length that does not fill the body exactly, an unknown or
/// repeated property, a zero Receive Maximum or Maximum Packet Size, a boolean
/// property that is neither 0 nor 1, or Response Information the client never
/// asked for.
pub fn deserialize_connack<'a>(
    packet: &PacketInfo<'a>,
    client: &ClientSettings,
) -> Result<ConnAck<'a>, ConnAckError> {
    if packet.packet_type != packet::CONNACK {
        return Err(ConnAckError::BadParameter);
    }

    if client.max_packet_size == 0 {
        return Err(ConnAckError::BadParameter);
    }

    let packet_size = packet
        .remaining_length
        .saturating_add(variable_length_encoded_size(packet.remaining_length))
        .saturating_add(1);

    if packet_size > client.max_packet_size {
        return Err(ConnAckError::BadResponse);
    }

    // `validateConnackParams`, in its order -- which is observable, because a
    // CONNACK can be wrong in more than one way at once and the C reports the
    // first one it reaches.
    //
    // The THIRD of those bytes is not load-bearing, in either arm: a two-byte
    // body leaves the property-length decoder no byte to read, and it refuses
    // on its own -- here and in the C, whose `decodeVariableLength` answers
    // `MQTTBadResponse` for a zero-length buffer. Only the first two bytes are
    // needed to make the indexing safe. The check is kept because it states the
    // packet's shape in one place instead of leaving a reader to derive it, and
    // `the_three_byte_minimum_is_stated_not_load_bearing` pins that.
    if packet.remaining_length < CONNACK_MINIMUM_SIZE {
        return Err(ConnAckError::BadResponse);
    }

    let flags = *packet
        .remaining_data
        .first()
        .ok_or(ConnAckError::BadResponse)?;
    let reason_code = *packet
        .remaining_data
        .get(1)
        .ok_or(ConnAckError::BadResponse)?;

    // Every bit but the lowest is reserved and must be zero.
    if (flags | SESSION_PRESENT_MASK) != SESSION_PRESENT_MASK {
        return Err(ConnAckError::BadResponse);
    }

    let session_present = (flags & SESSION_PRESENT_MASK) == SESSION_PRESENT_MASK;

    // MQTT 5.0 [MQTT-3.2.2-4]: a non-zero reason code MUST come with session
    // present clear. A broker that sets both is claiming to have resumed a
    // session it just refused to open.
    if session_present && reason_code != 0 {
        return Err(ConnAckError::BadResponse);
    }

    if !connack_reason_code_valid(reason_code) {
        return Err(ConnAckError::BadResponse);
    }

    // Everything after the flags and the reason code: the property section,
    // and nothing else.
    let after_header = bounded(
        packet.remaining_data,
        2,
        packet.remaining_length.saturating_sub(2),
    )?;

    let property_length = decode_variable_length(after_header)?;
    let encoded = variable_length_encoded_size(property_length);

    // An EQUALITY, like the publish acknowledgements' and unlike the list
    // acks': nothing follows a CONNACK's properties, so the body must be the
    // flags, the reason code, the encoded length, and exactly that many bytes.
    if packet.remaining_length != property_length.saturating_add(encoded).saturating_add(2) {
        return Err(ConnAckError::BadResponse);
    }

    let properties = bounded(after_header, encoded as usize, property_length)?;

    let mut connack = ConnAck {
        session_present,
        reason_code,
        server: ServerSettings::default(),
        fields_present: 0,
        properties,
    };

    walk_connack_properties(&mut connack, property_length, client)?;

    Ok(connack)
}

/// `isValidConnackReasonCode`: the 22 values of MQTT 5.0 §3.2.2.2.
///
/// Swept over all 256 and compared with the specification: this table is
/// exactly right, which after the [`ack`](crate::ack) slice found three
/// divergences in the same shape is worth having checked rather than assumed.
fn connack_reason_code_valid(reason_code: u8) -> bool {
    matches!(
        reason_code,
        0x00 | 0x80
            | 0x81
            | 0x82
            | 0x83
            | 0x84
            | 0x85
            | 0x86
            | 0x87
            | 0x88
            | 0x89
            | 0x8A
            | 0x8C
            | 0x90
            | 0x95
            | 0x97
            | 0x99
            | 0x9A
            | 0x9B
            | 0x9C
            | 0x9D
            | 0x9F
    )
}

/// Which properties have been seen, so a repeat is a protocol error.
///
/// The C's `ConnackSeenFlags_t`. Every MQTT 5 property may appear at most once
/// except User Property, which is why that one has no flag here.
#[derive(Default)]
struct Seen {
    session_expiry: bool,
    receive_max: bool,
    max_qos: bool,
    retain: bool,
    max_packet: bool,
    client_id: bool,
    topic_alias: bool,
    reason_string: bool,
    wildcard: bool,
    sub_id: bool,
    shared_sub: bool,
    keep_alive: bool,
    response_info: bool,
    server_ref: bool,
    auth_method: bool,
    auth_data: bool,
}

/// `deserializeConnackProperties` and `deserializeConnackProperty`, fused.
///
/// # Authentication Data is binary, and is read as a string anyway
///
/// MQTT 5.0 makes Authentication Data a Binary Data field and Authentication
/// Method a UTF-8 String, and the C reads both with `decodeUtf8`. That is not a
/// defect: the two have the identical wire format — a two-byte big-endian
/// length then that many bytes — and `decodeUtf8` never validated UTF-8 in the
/// first place. Recorded because the name suggests a check that is not there,
/// in either arm.
fn walk_connack_properties(
    connack: &mut ConnAck<'_>,
    property_length: u32,
    client: &ClientSettings,
) -> Result<(), ConnAckError> {
    let mut reader = PropertyReader::new(connack.properties, property_length);
    let mut seen = Seen::default();

    while reader.remaining() > 0 {
        let id = reader.property_id()?;

        // Each arm reads, then validates, then records the bit -- in that
        // order, because the C only sets the bit once the value is known good.
        let bit = match id {
            property::SESSION_EXPIRY => {
                connack.server.session_expiry = reader.u32(&mut seen.session_expiry)?;
                field::SESSION_EXPIRY_INTERVAL
            }

            property::RECEIVE_MAX => {
                connack.server.receive_max = reader.u16(&mut seen.receive_max)?;

                // MQTT 5.0 §3.2.2.3.2: a Receive Maximum of zero is a protocol
                // error. A server that said it would take zero messages in
                // flight has refused the connection without saying so.
                if connack.server.receive_max == 0 {
                    return Err(ConnAckError::BadResponse);
                }

                field::RECEIVE_MAXIMUM
            }

            property::MAX_QOS => {
                connack.server.max_qos = boolean(&mut reader, &mut seen.max_qos)?;
                field::MAX_QOS
            }

            property::RETAIN_AVAILABLE => {
                connack.server.retain_available = boolean(&mut reader, &mut seen.retain)?;
                field::RETAIN_AVAILABLE
            }

            property::MAX_PACKET_SIZE => {
                connack.server.max_packet_size = reader.u32(&mut seen.max_packet)?;

                // §3.2.2.3.5: zero is a protocol error, for the same reason.
                if connack.server.max_packet_size == 0 {
                    return Err(ConnAckError::BadResponse);
                }

                field::MAX_PACKET_SIZE
            }

            property::ASSIGNED_CLIENT_ID => {
                let _ = reader.utf8(&mut seen.client_id)?;
                field::ASSIGNED_CLIENT_ID
            }

            property::TOPIC_ALIAS_MAX => {
                connack.server.topic_alias_max = reader.u16(&mut seen.topic_alias)?;
                field::TOPIC_ALIAS_MAX
            }

            property::REASON_STRING => {
                let _ = reader.utf8(&mut seen.reason_string)?;
                field::REASON_STRING
            }

            property::USER_PROPERTY => {
                let _ = reader.user_property()?;
                field::USER_PROP
            }

            property::WILDCARD_AVAILABLE => {
                connack.server.wildcard_available = boolean(&mut reader, &mut seen.wildcard)?;
                field::WILDCARD_SUBSCRIPTION_AVAILABLE
            }

            property::SUBSCRIPTION_ID_AVAILABLE => {
                connack.server.subscription_id_available = boolean(&mut reader, &mut seen.sub_id)?;
                field::SUBSCRIPTION_ID_AVAILABLE
            }

            property::SHARED_SUB_AVAILABLE => {
                connack.server.shared_available = boolean(&mut reader, &mut seen.shared_sub)?;
                field::SHARED_SUBSCRIPTION_AVAILABLE
            }

            property::SERVER_KEEP_ALIVE => {
                connack.server.keep_alive = reader.u16(&mut seen.keep_alive)?;
                field::SERVER_KEEP_ALIVE
            }

            property::RESPONSE_INFO => {
                let _ = reader.utf8(&mut seen.response_info)?;

                // §3.1.2.11.6 [MQTT-3.1.2-25]: if the client did not ask for
                // Response Information the server MUST NOT send it. Note the
                // order -- the string is READ first and the gate applied
                // after, exactly as the C does it, so the cursor has already
                // moved when this fails.
                if !client.request_response_info {
                    return Err(ConnAckError::BadResponse);
                }

                field::RESPONSE_INFORMATION
            }

            property::SERVER_REFERENCE => {
                let _ = reader.utf8(&mut seen.server_ref)?;
                field::SERVER_REFERENCE
            }

            property::AUTH_METHOD => {
                let _ = reader.utf8(&mut seen.auth_method)?;
                field::AUTHENTICATION_METHOD
            }

            property::AUTH_DATA => {
                let _ = reader.utf8(&mut seen.auth_data)?;
                field::AUTHENTICATION_DATA
            }

            // An unknown property is a malformed packet, not something to
            // skip. Skipping would let a broker hide bytes inside a packet the
            // client believes it has read whole.
            _ => return Err(ConnAckError::BadResponse),
        };

        connack.fields_present |= 1_u32 << bit;
    }

    Ok(())
}

/// `decodeConnackBoolProp`: a one-byte property that must be 0 or 1.
///
/// Five of the CONNACK's properties are flags the server sets, and the C
/// refuses any other value rather than treating non-zero as true. That matters
/// for Maximum QoS in particular, where the legal values are 0 and 1 and QoS 2
/// is expressed by leaving the property out entirely.
fn boolean(reader: &mut PropertyReader<'_>, seen: &mut bool) -> Result<u8, ConnAckError> {
    let value = reader.u8(seen)?;

    if value > 1 {
        return Err(ConnAckError::BadResponse);
    }

    Ok(value)
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

    fn client() -> ClientSettings {
        ClientSettings {
            max_packet_size: 1024,
            request_response_info: true,
        }
    }

    fn info(body: &[u8]) -> PacketInfo<'_> {
        PacketInfo {
            packet_type: packet::CONNACK,
            remaining_length: u32::try_from(body.len()).unwrap(),
            remaining_data: body,
        }
    }

    /// The smallest legal CONNACK is three bytes and says almost nothing.
    #[test]
    fn the_minimum_connack_is_a_success_with_no_properties() {
        let connack = deserialize_connack(&info(&[0x00, 0x00, 0x00]), &client()).unwrap();

        assert!(!connack.session_present);
        assert_eq!(connack.reason_code, 0);
        assert!(!connack.refused());
        assert_eq!(connack.fields_present, 0);
        assert!(connack.properties.is_empty());
        assert_eq!(connack.server, ServerSettings::default());
    }

    /// A refusal parses, and the reason string survives it.
    ///
    /// The C returns `MQTTServerRefused` and goes on to read the properties,
    /// because the string that explains the refusal is in them. Making a
    /// refusal an `Err` here would have discarded exactly that.
    #[test]
    fn a_refused_connection_still_carries_its_reason_string() {
        // flags 0, reason 0x87 (not authorized), property length 7,
        // reason string "nope".
        let body = [0x00, 0x87, 0x07, 0x1F, 0x00, 0x04, b'n', b'o', b'p', b'e'];
        let connack = deserialize_connack(&info(&body), &client()).unwrap();

        assert!(connack.refused());
        assert_eq!(connack.reason_code, 0x87);
        assert_eq!(connack.fields_present, 1 << field::REASON_STRING);
        assert_eq!(connack.properties, &body[3..]);
    }

    /// Session present with a non-zero reason code is a contradiction.
    ///
    /// MQTT 5.0 [MQTT-3.2.2-4]. A broker that sets both is claiming to have
    /// resumed a session it just refused to open, and a client that believed it
    /// would skip re-subscribing on a connection it does not have.
    #[test]
    fn a_resumed_session_cannot_come_with_a_refusal() {
        assert_eq!(
            deserialize_connack(&info(&[0x01, 0x87, 0x00]), &client()),
            Err(ConnAckError::BadResponse)
        );

        // Either half alone is fine.
        assert!(deserialize_connack(&info(&[0x01, 0x00, 0x00]), &client()).is_ok());
        assert!(deserialize_connack(&info(&[0x00, 0x87, 0x00]), &client()).is_ok());
    }

    /// Every bit of the flags byte but the lowest is reserved.
    #[test]
    fn the_reserved_flag_bits_must_all_be_clear() {
        for bit in 1..8u8 {
            let body = [1_u8 << bit, 0x00, 0x00];
            assert_eq!(
                deserialize_connack(&info(&body), &client()),
                Err(ConnAckError::BadResponse),
                "reserved flag bit {bit} was accepted"
            );
        }
    }

    /// The two limits a server may not set to zero.
    ///
    /// Both are protocol errors in MQTT 5.0 §3.2.2.3, and both would otherwise
    /// be a silent refusal: a Receive Maximum of zero means no message may ever
    /// be in flight, a Maximum Packet Size of zero means no packet may be sent.
    #[test]
    fn a_zero_limit_is_a_protocol_error_not_a_limit() {
        // Receive Maximum = 0.
        assert_eq!(
            deserialize_connack(&info(&[0x00, 0x00, 0x03, 0x21, 0x00, 0x00]), &client()),
            Err(ConnAckError::BadResponse)
        );
        // ...and 1 is fine.
        let ok = deserialize_connack(&info(&[0x00, 0x00, 0x03, 0x21, 0x00, 0x01]), &client())
            .expect("a receive maximum of one");
        assert_eq!(ok.server.receive_max, 1);

        // Maximum Packet Size = 0.
        assert_eq!(
            deserialize_connack(
                &info(&[0x00, 0x00, 0x05, 0x27, 0x00, 0x00, 0x00, 0x00]),
                &client()
            ),
            Err(ConnAckError::BadResponse)
        );
    }

    /// Maximum QoS is 0 or 1, and QoS 2 is the ABSENCE of the property.
    ///
    /// Worth its own test because "the server supports QoS 2" and "the server
    /// said nothing" are the same state, and a transcription that defaulted
    /// `max_qos` to 2 would look more helpful and would disagree with the C.
    #[test]
    fn maximum_qos_two_is_spelled_by_saying_nothing() {
        let absent = deserialize_connack(&info(&[0x00, 0x00, 0x00]), &client()).unwrap();
        assert_eq!(absent.server.max_qos, 0);
        assert_eq!(absent.fields_present & (1 << field::MAX_QOS), 0);

        let one = deserialize_connack(&info(&[0x00, 0x00, 0x02, 0x24, 0x01]), &client()).unwrap();
        assert_eq!(one.server.max_qos, 1);
        assert_ne!(one.fields_present & (1 << field::MAX_QOS), 0);

        // A server cannot say "2" in the property.
        assert_eq!(
            deserialize_connack(&info(&[0x00, 0x00, 0x02, 0x24, 0x02]), &client()),
            Err(ConnAckError::BadResponse)
        );
    }

    /// Why the third of the three minimum bytes changes no answer.
    ///
    /// This started as a poison that did not fire: lowering
    /// [`CONNACK_MINIMUM_SIZE`] to two left the run agreeing with the C line
    /// for line. Two bytes is what the flags and the reason code need; the
    /// third is what the property-length decoder needs, and that decoder
    /// refuses a zero-length buffer by itself — in **both** arms, since the C's
    /// `decodeVariableLength` answers `MQTTBadResponse` when it has no byte to
    /// read.
    ///
    /// Third member of a family this package keeps meeting, and the first of
    /// its shape. The size calculators' in-loop check is load-bearing in the C
    /// (its arithmetic wraps) and subsumed here (ours saturates); the ack
    /// deserializers' property bound is load-bearing in the C (pointer
    /// arithmetic) and subsumed here (a slice). **This one is redundant in the
    /// C too.** It is kept because it states the packet's shape in one place,
    /// which is worth a line.
    #[test]
    fn the_three_byte_minimum_is_stated_not_load_bearing() {
        // Nothing under three bytes can parse, whatever the constant says.
        for length in 0..CONNACK_MINIMUM_SIZE as usize {
            let body = vec![0x00_u8; length];
            assert_eq!(
                deserialize_connack(&info(&body), &client()),
                Err(ConnAckError::BadResponse),
                "a {length}-byte CONNACK was accepted"
            );
        }

        // And the reason the third byte is not needed: the property length is
        // decoded from a slice that a two-byte body leaves empty.
        assert!(
            decode_variable_length(&[]).is_err(),
            "an empty buffer now decodes to a property length, so the three-byte              minimum has become load-bearing and this test is the only thing              that was watching it"
        );

        // Exactly three bytes IS the floor, and it parses.
        assert!(deserialize_connack(&info(&[0x00, 0x00, 0x00]), &client()).is_ok());
    }

    /// The reason-code table is exactly MQTT 5.0 §3.2.2.2 — 22 of 256.
    ///
    /// Pinned as arithmetic as well as swept, because after the
    /// [`ack`](crate::ack) slice found three tables that were *not* the
    /// specification's, "this one is" deserves to be an assertion rather than a
    /// claim in a comment.
    #[test]
    fn the_reason_code_table_is_the_specification() {
        const SPEC: [u8; 22] = [
            0x00, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8C, 0x90,
            0x95, 0x97, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9F,
        ];

        for code in 0..=255u8 {
            assert_eq!(
                connack_reason_code_valid(code),
                SPEC.contains(&code),
                "reason code {code:#04x} is treated differently from MQTT 5.0 §3.2.2.2"
            );
        }
    }
}
