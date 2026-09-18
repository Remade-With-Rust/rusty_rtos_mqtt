//! Every limit a session runs under, and where each one came from.
//!
//! MQTT 5 negotiates: the client states its limits in the CONNECT properties,
//! the broker states its own in the CONNACK, and everything afterwards is
//! bounded by both. coreMQTT keeps all of it in one struct, and this is that
//! struct with its two halves named.
//!
//! # A third copy of the CONNECT property table
//!
//! [`update_with_connect_props`] walks the property section of a CONNECT that
//! is about to be sent, and it is the **third** walk of that table in the
//! library: [`validate_connect_properties`](crate::validate) checks it, this
//! fills the context from it, and `core_mqtt_prop_serializer.c` wrote it. All
//! three accept the same nine identifiers. Two of the three agree about
//! nothing else:
//!
//! | on the same section | the validator | this |
//! |---|---|---|
//! | a Receive Maximum of 0 | refuses | **stores it** |
//! | a Maximum Packet Size of 0 | refuses | **stores it** |
//! | Request Problem Information of 2 | refuses | accepts |
//! | authentication data with no method | refuses | accepts |
//! | a repeated Request Problem, Response, Auth Method or Auth Data | refuses | accepts |
//! | an unknown identifier | `MQTTBadResponse` | `MQTTBadParameter` |
//!
//! Inside `MQTT_Connect` this is harmless — the validator runs first, on the
//! same bytes. It stops being harmless because `updateContextWithConnectProps`
//! is **declared in the public header with a worked example**, so an
//! application may call it alone, and a context holding
//! `maxPacketSize == 0` fails every packet-size calculator in the library. It
//! is drafted for upstream.
//!
//! The differential prints both tables' accepted sets, so the comparison above
//! is a diff of two trace files rather than a claim.

use crate::header::REMAINING_LENGTH_INVALID;
use crate::property::PropertyReader;
use crate::validate::id;

/// MQTT's largest possible packet: the greatest remaining length, plus the
/// five bytes of fixed header that can precede it.
///
/// `MQTT_MAX_PACKET_SIZE`. Note it is **larger** than
/// [`REMAINING_LENGTH_INVALID`], which is the first illegal *remaining* length
/// — the two bound different things and differ by four.
pub const MAX_PACKET_SIZE: u32 = 268_435_460;

/// Why a connection context could not be filled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextError {
    /// `MQTTBadParameter`: an identifier that may not appear in a CONNECT.
    BadParameter,
    /// `MQTTBadResponse`: a repeated or malformed property.
    ///
    /// Which of the nine repeat freely is not a rule — it is which flags the C
    /// happens to declare outside its loop. See [`update_with_connect_props`].
    BadResponse,
}

/// What this client asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientLimits {
    /// How many QoS 1 and 2 messages this client will accept in flight.
    pub receive_max: u16,
    /// The largest packet this client will accept.
    pub max_packet_size: u32,
    /// The highest topic alias this client will accept from the broker.
    pub topic_alias_max: u16,
    /// Whether this client asked for Response Information in the CONNACK.
    pub request_response_info: bool,
    /// Whether this client asked for reason strings and user properties.
    ///
    /// **Defaults to `true`**, which is MQTT's own default and the opposite of
    /// what [`validate_connect_properties`](crate::validate) leaves behind when
    /// the property is absent. The two are not in conflict: that one reports
    /// what the SECTION said, and this is what the session runs under until a
    /// section says otherwise.
    pub request_problem_info: bool,
}

/// What the broker granted.
///
/// Every field starts at the value MQTT 5.0 gives for an **absent** property,
/// which is why none of them is zero: an absent Maximum QoS means 2, an absent
/// Retain Available means yes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerLimits {
    /// How many QoS 1 and 2 messages the broker will take in flight.
    pub receive_max: u16,
    /// The highest QoS the broker supports.
    pub max_qos: u8,
    /// Whether retained messages work.
    pub retain_available: u8,
    /// The largest packet the broker will accept.
    pub max_packet_size: u32,
    /// The highest topic alias the broker will accept.
    pub topic_alias_max: u16,
    /// Whether wildcard subscriptions work.
    pub wildcard_available: u8,
    /// Whether subscription identifiers work.
    pub subscription_id_available: u8,
    /// Whether shared subscriptions work.
    pub shared_available: u8,
    /// The keep-alive the broker is imposing, which overrides the client's.
    pub keep_alive: u16,
}

/// `MQTTConnectionProperties_t`: both halves, and the one field they share.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionProperties {
    /// How long the broker should keep this session after the connection ends.
    ///
    /// **One field, two owners.** The client asks for it in the CONNECT and
    /// the broker may overwrite it in the CONNACK, which is why the C keeps a
    /// single `sessionExpiry` rather than a pair. Reproduced, with the comment
    /// the C leaves beside it.
    pub session_expiry: u32,
    /// What this client asked for.
    pub client: ClientLimits,
    /// What the broker granted.
    pub server: ServerLimits,
}

impl ConnectionProperties {
    /// `MQTT_InitConnect`: the state a session starts in.
    ///
    /// Not zeroes. Every value here is either MQTT's default for an absent
    /// property or the widest thing the protocol allows, so a context that has
    /// negotiated nothing still describes a legal session.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            session_expiry: 0,
            client: ClientLimits {
                receive_max: u16::MAX,
                max_packet_size: MAX_PACKET_SIZE,
                topic_alias_max: 0,
                request_response_info: false,
                request_problem_info: true,
            },
            server: ServerLimits {
                receive_max: u16::MAX,
                max_qos: 2,
                retain_available: 1,
                max_packet_size: MAX_PACKET_SIZE,
                topic_alias_max: 0,
                wildcard_available: 1,
                subscription_id_available: 1,
                shared_available: 1,
                keep_alive: u16::MAX,
            },
        }
    }
}

impl Default for ConnectionProperties {
    /// [`ConnectionProperties::new`] — **not** all zeroes.
    fn default() -> Self {
        Self::new()
    }
}

/// `updateContextWithConnectProps`: fill the client's half from a CONNECT's
/// property section.
///
/// Only four properties reach the context — Session Expiry, Receive Maximum,
/// Maximum Packet Size and Topic Alias Maximum. The other five are decoded to
/// step the cursor past them and discarded, one of them with a `TODO` in the C
/// asking whether it should be kept.
///
/// **This does not validate.** It stores whatever it decodes, including a
/// Receive Maximum or Maximum Packet Size of zero, which
/// [`validate_connect_properties`](crate::validate) refuses two calls earlier.
/// See the module note; it is transcribed as it stands.
///
/// # Errors
///
/// [`ContextError::BadParameter`] for an identifier that may not appear in a
/// CONNECT. [`ContextError::BadResponse`] for a malformed value, or a repeat of
/// one of the **four** properties the C happens to guard.
pub fn update_with_connect_props(
    properties: &[u8],
    context: &mut ConnectionProperties,
) -> Result<(), ContextError> {
    let length = u32::try_from(properties.len()).map_err(|_| ContextError::BadParameter)?;

    // The C asserts this rather than checking it, because its caller has
    // already refused a longer section. An assert that a caller must uphold is
    // a caller's job in C and a type's job here: this is a refusal.
    if length >= REMAINING_LENGTH_INVALID {
        return Err(ContextError::BadParameter);
    }

    let mut reader = PropertyReader::new(properties, length);

    // These four are declared OUTSIDE the C's loop, so these four are
    // deduplicated. The fifth flag is declared inside it, which is why Request
    // Problem Information, Request Response Information, the authentication
    // method and the authentication data may all repeat. It is the same
    // one-brace defect as the outgoing PUBLISH property validator's.
    let (mut session_expiry, mut receive_max) = (false, false);
    let (mut max_packet, mut topic_alias_max) = (false, false);

    while reader.remaining() > 0 {
        let mut used = false;

        match reader
            .property_id()
            .map_err(|_| ContextError::BadResponse)?
        {
            id::SESSION_EXPIRY => {
                context.session_expiry = reader
                    .u32(&mut session_expiry)
                    .map_err(|_| ContextError::BadResponse)?;
            }

            id::RECEIVE_MAX => {
                context.client.receive_max = reader
                    .u16(&mut receive_max)
                    .map_err(|_| ContextError::BadResponse)?;
            }

            id::MAX_PACKET_SIZE => {
                context.client.max_packet_size = reader
                    .u32(&mut max_packet)
                    .map_err(|_| ContextError::BadResponse)?;
            }

            id::TOPIC_ALIAS_MAX => {
                context.client.topic_alias_max = reader
                    .u16(&mut topic_alias_max)
                    .map_err(|_| ContextError::BadResponse)?;
            }

            // Decoded to step past, and dropped. The C's own comment here is
            // `/* TODO: should this go in the context? */`, and the answer for
            // Request Problem Information is surely yes -- it decides whether
            // a broker may send reason strings, which `ack::Limits` needs.
            id::REQUEST_PROBLEM | id::REQUEST_RESPONSE => {
                let _ = reader
                    .u8(&mut used)
                    .map_err(|_| ContextError::BadResponse)?;
            }

            id::AUTH_DATA | id::AUTH_METHOD => {
                let _ = reader
                    .utf8(&mut used)
                    .map_err(|_| ContextError::BadResponse)?;
            }

            id::USER_PROPERTY => {
                let _ = reader
                    .user_property()
                    .map_err(|_| ContextError::BadResponse)?;
            }

            _ => return Err(ContextError::BadParameter),
        }
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
    use crate::validate::{ConnectValidation, ValidateError, validate_connect_properties};

    /// The two walks of one table, on the same bytes, disagreeing.
    ///
    /// Four values the validator calls protocol errors are stored by the
    /// context filler, two of them into fields that then break every
    /// packet-size calculator in the library. Inside `MQTT_Connect` the
    /// validator runs first; the helper is public, documented and callable
    /// alone. Drafted for upstream.
    #[test]
    fn the_context_filler_stores_what_the_validator_refuses() {
        for (name, section) in [
            ("a zero Receive Maximum", &[0x21, 0x00, 0x00][..]),
            (
                "a zero Maximum Packet Size",
                &[0x27, 0x00, 0x00, 0x00, 0x00][..],
            ),
            ("Request Problem Information of 2", &[0x17, 0x02][..]),
            (
                "authentication data with no method",
                &[0x16, 0x00, 0x01, b'd'][..],
            ),
        ] {
            let mut found = ConnectValidation::default();
            assert!(
                validate_connect_properties(section, &mut found).is_err(),
                "the validator now accepts {name}"
            );

            let mut context = ConnectionProperties::new();
            assert_eq!(
                update_with_connect_props(section, &mut context),
                Ok(()),
                "the context filler now refuses {name} too"
            );
        }

        // And the two that are stored, which is what makes it more than a
        // difference of opinion.
        let mut context = ConnectionProperties::new();
        update_with_connect_props(&[0x21, 0x00, 0x00], &mut context).unwrap();
        assert_eq!(context.client.receive_max, 0);

        let mut context = ConnectionProperties::new();
        update_with_connect_props(&[0x27, 0x00, 0x00, 0x00, 0x00], &mut context).unwrap();
        assert_eq!(
            context.client.max_packet_size, 0,
            "a zero here refuses every packet this client could send"
        );
    }

    /// Four of the nine properties are deduplicated and five are not.
    ///
    /// Not a rule — which flags the C declares outside its loop. The four with
    /// a flag of their own are exactly the four that reach the context.
    #[test]
    fn four_of_the_nine_are_deduplicated_and_five_are_not() {
        let mut context = ConnectionProperties::new();

        for repeated in [
            &[0x11, 0, 0, 0x0E, 0x10, 0x11, 0, 0, 0x0E, 0x10][..],
            &[0x21, 0x00, 0x14, 0x21, 0x00, 0x15][..],
        ] {
            assert_eq!(
                update_with_connect_props(repeated, &mut context),
                Err(ContextError::BadResponse)
            );
        }

        for repeated in [
            &[0x17, 0x00, 0x17, 0x01][..],
            &[0x19, 0x00, 0x19, 0x01][..],
            &[0x15, 0x00, 0x01, b'x', 0x15, 0x00, 0x01, b'y'][..],
        ] {
            assert_eq!(
                update_with_connect_props(repeated, &mut context),
                Ok(()),
                "this one is guarded now"
            );
        }
    }

    /// The defaults are MQTT's, not zero.
    ///
    /// A context that has negotiated nothing still describes a legal session —
    /// which is the whole point of `MQTT_InitConnect`, and the reason
    /// `Default` delegates to it rather than deriving.
    #[test]
    fn a_fresh_context_is_a_legal_session_not_an_empty_one() {
        let fresh = ConnectionProperties::new();

        assert_eq!(fresh, ConnectionProperties::default());
        assert_eq!(fresh.client.receive_max, 65_535);
        assert_eq!(fresh.client.max_packet_size, MAX_PACKET_SIZE);
        assert!(fresh.client.request_problem_info, "MQTT's default is true");
        assert_eq!(fresh.server.max_qos, 2, "an absent Maximum QoS means 2");
        assert_eq!(fresh.server.retain_available, 1);
        assert_eq!(fresh.server.keep_alive, 65_535);
        assert_eq!(fresh.session_expiry, 0);

        // The two limits bound different things, and the packet size is the
        // LARGER: the greatest remaining length plus the fixed header that can
        // precede it.
        assert_eq!(MAX_PACKET_SIZE, REMAINING_LENGTH_INVALID + 4);
    }

    /// The two tables answer a foreign identifier with different statuses.
    #[test]
    fn a_foreign_identifier_gets_a_different_status_from_each_walk() {
        let will_delay = &[0x18, 0x00, 0x00, 0x00, 0x0A];

        let mut context = ConnectionProperties::new();
        assert_eq!(
            update_with_connect_props(will_delay, &mut context),
            Err(ContextError::BadParameter)
        );

        let mut found = ConnectValidation::default();
        assert_eq!(
            validate_connect_properties(will_delay, &mut found),
            Err(ValidateError::BadResponse)
        );
    }
}
