//! The client context, and what it refuses before it sends anything.
//!
//! The last of `core_mqtt.c` that needs no transport: the constructor, the
//! packet-identifier allocator, and the validators an outgoing SUBSCRIBE,
//! UNSUBSCRIBE or PUBLISH goes through. `MQTT_Subscribe` validates **before**
//! it looks at the connection status and long before it touches the transport,
//! so every one of these is reachable from a context that has never connected.
//!
//! # Two defects, and both are about a list
//!
//! **Only the last subscription in a list decides whether the list is valid.**
//! `validateSubscribeUnsubscribeParams` ends with
//!
//! ```c
//!     for( iterator = 0U; iterator < subscriptionCount; iterator++ )
//!     {
//!         status = validateTopicFilter( pContext, pSubscriptionList, iterator, subscriptionType );
//!     }
//! ```
//!
//! — which **assigns** rather than accumulates and never breaks, so a list
//! whose last entry is good passes however bad the earlier ones are. A
//! zero-length topic filter, a QoS of 3, a wildcard the broker forbade, a
//! malformed shared subscription: all accepted, as long as something valid
//! follows. The loop three lines above it, over the same list, *does* break.
//!
//! **And a topic filter is searched past its length.**
//! `checkWildcardSubscriptions` reaches for the filter with `strchr`, which
//! stops at a NUL — but `MQTTSubscribeInfo_t` carries a pointer *and* a length,
//! and nothing requires the pointer to be NUL-terminated. A filter of `"abc"`
//! with length 3, sitting in a buffer that reads `"abc#"`, is refused as
//! containing a wildcard. With no NUL at all it reads off the end.
//!
//! Both are transcribed, pinned by [`tests`](self), and written up in
//! `docs/upstream/`. The second one this crate **cannot** reproduce: a `&[u8]`
//! has no bytes past its length, so the Rust arm answers `Ok` where the C
//! refuses, and that trace line is a bounded exception like
//! [`builder`](crate::builder)'s.

use crate::context::ConnectionProperties;
use crate::state::QoS;
use crate::validate::{ValidateError, validate_subscribe_properties};

/// Why the client refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientError {
    /// `MQTTBadParameter`.
    BadParameter,
    /// `MQTTStatusNotConnected`: the packet was well formed and there is no
    /// connection to send it on.
    NotConnected,
    /// `MQTTStatusDisconnectPending`: the transport has failed and the
    /// connection is on its way down.
    DisconnectPending,
}

/// `MQTTConnectionStatus_t`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConnectionStatus {
    /// Not connected, which is where a fresh context starts.
    #[default]
    NotConnected,
    /// Connected.
    Connected,
    /// The transport failed and the connection must be closed.
    DisconnectPending,
}

/// Whether a list is being subscribed or unsubscribed.
///
/// `MQTTSubscriptionType_t`. It matters because an UNSUBSCRIBE carries no QoS,
/// no retain handling and no shared-subscription rules — only the filter itself
/// is checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionType {
    /// SUBSCRIBE: every option is checked.
    Subscribe,
    /// UNSUBSCRIBE: only the filter is.
    Unsubscribe,
}

/// How a broker should treat retained messages when a subscription is made.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RetainHandling {
    /// Send them at subscribe time.
    #[default]
    OnSubscribe,
    /// Send them only if the subscription is new.
    OnSubscribeIfNew,
    /// Never send them.
    Never,
}

/// One entry in a SUBSCRIBE or UNSUBSCRIBE list.
#[derive(Debug, Clone, Copy)]
pub struct Subscription<'a> {
    /// The filter. May not be empty, and may not exceed 65,535 bytes.
    pub topic_filter: &'a [u8],
    /// The delivery guarantee asked for.
    pub qos: QoS,
    /// Do not send me my own messages.
    pub no_local: bool,
    /// Keep the RETAIN flag as the publisher set it.
    pub retain_as_published: bool,
    /// What to do about retained messages.
    pub retain_handling: RetainHandling,
}

/// `MQTTContext_t`, as far as this slice needs it.
#[derive(Debug)]
pub struct MqttContext<'a> {
    /// Every limit the session runs under.
    pub properties: ConnectionProperties,
    /// Whether there is a connection.
    pub connect_status: ConnectionStatus,
    network: &'a mut [u8],
    next_packet_id: u16,
    outgoing_records: usize,
    incoming_records: usize,
    ack_properties: usize,
}

impl<'a> MqttContext<'a> {
    /// `MQTT_Init`.
    ///
    /// The C takes a transport, a clock and a callback and refuses a null for
    /// each; those are supplied at the call sites that need them here, so five
    /// of its six refusals have nowhere to come from and this cannot fail. The
    /// sixth — `MQTT_InitConnect` — cannot fail either.
    ///
    /// A fresh context is **not** zeroed: every connection limit starts at what
    /// MQTT 5.0 says an absent property means. See
    /// [`ConnectionProperties::new`].
    #[must_use]
    pub fn new(network: &'a mut [u8]) -> Self {
        Self {
            properties: ConnectionProperties::new(),
            connect_status: ConnectionStatus::NotConnected,
            network,
            // Zero is not a valid packet identifier, so the first one is 1.
            next_packet_id: 1,
            outgoing_records: 0,
            incoming_records: 0,
            ack_properties: 0,
        }
    }

    /// How big the network buffer is.
    #[must_use]
    pub const fn network_size(&self) -> usize {
        self.network.len()
    }

    /// `MQTT_InitStatefulQoS`: turn on QoS 1 and QoS 2.
    ///
    /// Until this is called, a QoS above 0 is refused — there is nowhere to
    /// keep the handshake. The C's four refusals are all a pointer disagreeing
    /// with a count, or being called before `MQTT_Init`; slices and a method on
    /// an existing context make all four unrepresentable.
    pub fn enable_qos(&mut self, outgoing: usize, incoming: usize, ack_properties: usize) {
        self.outgoing_records = outgoing;
        self.incoming_records = incoming;
        self.ack_properties = ack_properties;
    }

    /// How many outgoing QoS records there is room for.
    #[must_use]
    pub const fn outgoing_records(&self) -> usize {
        self.outgoing_records
    }

    /// How many incoming QoS records there is room for.
    #[must_use]
    pub const fn incoming_records(&self) -> usize {
        self.incoming_records
    }

    /// How much room an acknowledgement's properties have.
    #[must_use]
    pub const fn ack_properties(&self) -> usize {
        self.ack_properties
    }

    /// `MQTT_CheckConnectStatus`.
    #[must_use]
    pub const fn status(&self) -> ConnectionStatus {
        self.connect_status
    }

    /// `MQTT_GetPacketId`: the next identifier, and step on.
    ///
    /// Wraps to **1**, not to 0, because zero is not a legal packet
    /// identifier. The C's `nextPacketId` starts at 1 and the wrap is the only
    /// place that rule is written down.
    pub const fn next_packet_id(&mut self) -> u16 {
        let id = self.next_packet_id;

        self.next_packet_id = if self.next_packet_id == u16::MAX {
            1
        } else {
            self.next_packet_id.wrapping_add(1)
        };

        id
    }

    /// The connection-status refusal every send shares.
    const fn connected(&self) -> Result<(), ClientError> {
        match self.connect_status {
            ConnectionStatus::Connected => Ok(()),
            ConnectionStatus::NotConnected => Err(ClientError::NotConnected),
            ConnectionStatus::DisconnectPending => Err(ClientError::DisconnectPending),
        }
    }

    /// `validateSubscribeUnsubscribeParams`, and everything it drives.
    ///
    /// **Reproduces the last-one-wins defect**: the answer is the *last*
    /// entry's, not the list's. See the module note.
    ///
    /// # Errors
    ///
    /// [`ClientError::BadParameter`] for an empty list, a zero packet
    /// identifier, a QoS above 0 with no records to keep it in, or a last entry
    /// that is malformed.
    pub fn validate_subscriptions(
        &self,
        list: &[Subscription<'_>],
        packet_id: u16,
        which: SubscriptionType,
    ) -> Result<(), ClientError> {
        if list.is_empty() || packet_id == 0 {
            return Err(ClientError::BadParameter);
        }

        // This loop DOES break, and the one below does not.
        if self.incoming_records == 0 {
            for entry in list {
                if entry.qos != QoS::AtMostOnce {
                    return Err(ClientError::BadParameter);
                }
            }
        }

        // Transcribed: `status` is assigned, not accumulated, and nothing
        // breaks out. Every entry but the last is validated and then forgotten.
        let mut status = Ok(());

        for entry in list {
            status = self.validate_one(entry, which);
        }

        status
    }

    /// `validateTopicFilter` for one entry.
    fn validate_one(
        &self,
        entry: &Subscription<'_>,
        which: SubscriptionType,
    ) -> Result<(), ClientError> {
        if entry.topic_filter.is_empty() {
            return Err(ClientError::BadParameter);
        }

        if u16::try_from(entry.topic_filter.len()).is_err() {
            return Err(ClientError::BadParameter);
        }

        if which == SubscriptionType::Unsubscribe {
            // An UNSUBSCRIBE carries no options, so nothing below applies.
            return Ok(());
        }

        // A QoS above 2 has no encoding. `QoS` has three variants, so this is
        // the C's check made unreachable rather than transcribed -- and the
        // driver's `qos-three` case is answered by the type instead.
        if self.wildcards_forbidden(entry.topic_filter) {
            return Err(ClientError::BadParameter);
        }

        // Retain handling is likewise an enumeration here, so `> 2` cannot
        // happen.
        self.validate_shared(entry)
    }

    /// `checkWildcardSubscriptions`.
    ///
    /// **This is the one that differs.** The C uses `strchr`, which runs to a
    /// NUL and knows nothing of `topicFilterLength`; a slice has no bytes past
    /// its length. So a filter whose wildcard lies *outside* its length is
    /// refused there and accepted here, and the trace line that shows it is a
    /// bounded exception.
    fn wildcards_forbidden(&self, filter: &[u8]) -> bool {
        self.properties.server.wildcard_available == 0
            && (filter.contains(&b'#') || filter.contains(&b'+'))
    }

    /// `validateSharedSubscriptions`: `$share/{name}/{filter}`.
    fn validate_shared(&self, entry: &Subscription<'_>) -> Result<(), ClientError> {
        const PREFIX: &[u8] = b"$share/";

        let filter = entry.topic_filter;

        // The C tests `length > 7` and then compares seven bytes, so a filter
        // of exactly `"$share/"` is not a shared subscription at all.
        if filter.len() <= PREFIX.len() || !filter.starts_with(PREFIX) {
            return Ok(());
        }

        let Some(rest) = filter.get(PREFIX.len()..) else {
            return Ok(());
        };

        // The share name runs to the next `/`.
        let Some(end) = rest.iter().position(|byte| *byte == b'/') else {
            return Err(ClientError::BadParameter);
        };

        // An empty share name, which is `$share//...`.
        if end == 0 {
            return Err(ClientError::BadParameter);
        }

        if entry.no_local {
            return Err(ClientError::BadParameter);
        }

        if self.properties.server.shared_available == 0 {
            return Err(ClientError::BadParameter);
        }

        // The separator is the filter's last byte, so nothing follows the
        // share name.
        if PREFIX.len().saturating_add(end) == filter.len().saturating_sub(1) {
            return Err(ClientError::BadParameter);
        }

        // A wildcard in the share name itself.
        let Some(name) = rest.get(..end) else {
            return Ok(());
        };

        if name.contains(&b'#') || name.contains(&b'+') {
            return Err(ClientError::BadParameter);
        }

        Ok(())
    }

    /// `MQTT_Subscribe`, as far as the wire: validate, check the properties,
    /// then discover there is no connection.
    ///
    /// # Errors
    ///
    /// [`ClientError::BadParameter`] from the validators,
    /// [`ClientError::NotConnected`] or [`ClientError::DisconnectPending`]
    /// from the connection.
    pub fn subscribe(
        &self,
        list: &[Subscription<'_>],
        packet_id: u16,
        properties: Option<&[u8]>,
    ) -> Result<(), ClientError> {
        self.validate_subscriptions(list, packet_id, SubscriptionType::Subscribe)?;

        if let Some(section) = properties {
            let available = self.properties.server.subscription_id_available != 0;

            if validate_subscribe_properties(available, section) != Ok(()) {
                return Err(ClientError::BadParameter);
            }
        }

        self.connected()
    }

    /// `MQTT_Unsubscribe`.
    ///
    /// # Errors
    ///
    /// As [`subscribe`](Self::subscribe), with only the filter validated.
    pub fn unsubscribe(
        &self,
        list: &[Subscription<'_>],
        packet_id: u16,
    ) -> Result<(), ClientError> {
        self.validate_subscriptions(list, packet_id, SubscriptionType::Unsubscribe)?;
        self.connected()
    }

    /// `validatePublishParams`.
    ///
    /// # Errors
    ///
    /// [`ClientError::BadParameter`] for a QoS above 0 with no packet
    /// identifier or no records, a topic name over 65,535 bytes, or a payload
    /// at or above [`REMAINING_LENGTH_INVALID`](crate::header).
    pub fn validate_publish(
        &self,
        qos: QoS,
        packet_id: u16,
        topic_length: usize,
        payload_length: usize,
    ) -> Result<(), ClientError> {
        if qos != QoS::AtMostOnce && packet_id == 0 {
            return Err(ClientError::BadParameter);
        }

        // The C's `payloadLength > 0 && pPayload == NULL` is the null-pointer
        // family: a `&[u8]` carries both.

        if u16::try_from(topic_length).is_err() {
            return Err(ClientError::BadParameter);
        }

        if u32::try_from(payload_length)
            .is_ok_and(|length| length >= crate::header::REMAINING_LENGTH_INVALID)
            || u32::try_from(payload_length).is_err()
        {
            return Err(ClientError::BadParameter);
        }

        if self.outgoing_records == 0 && qos != QoS::AtMostOnce {
            return Err(ClientError::BadParameter);
        }

        Ok(())
    }

    /// `MQTT_Publish`, as far as the wire.
    ///
    /// # Errors
    ///
    /// As [`validate_publish`](Self::validate_publish), then the connection.
    pub fn publish(
        &self,
        qos: QoS,
        packet_id: u16,
        topic: &[u8],
        payload: &[u8],
    ) -> Result<(), ClientError> {
        self.validate_publish(qos, packet_id, topic.len(), payload.len())?;

        // `MQTT_ValidatePublishParams` in the serializer refuses an empty topic
        // with no alias, and `MQTT_Publish` reaches it through
        // `MQTT_GetPublishPacketSize`.
        if topic.is_empty() {
            return Err(ClientError::BadParameter);
        }

        self.connected()
    }
}

impl From<ValidateError> for ClientError {
    fn from(_: ValidateError) -> Self {
        Self::BadParameter
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

    fn context(buffer: &mut [u8]) -> MqttContext<'_> {
        let mut client = MqttContext::new(buffer);
        client.enable_qos(4, 4, 0);
        client
    }

    fn plain(filter: &[u8]) -> Subscription<'_> {
        Subscription {
            topic_filter: filter,
            qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::OnSubscribe,
        }
    }

    /// Only the LAST subscription in a list decides whether the list is valid.
    ///
    /// `validateSubscribeUnsubscribeParams` assigns its status in a loop that
    /// never breaks, so an invalid filter is accepted whenever something valid
    /// follows it. The loop three lines above, over the same list, does break —
    /// which is what makes this a slip rather than a decision. Drafted for
    /// upstream; transcribed here.
    #[test]
    fn only_the_last_subscription_decides() {
        let mut buffer = [0u8; 64];
        let client = context(&mut buffer);

        let good = plain(b"a/b");
        let bad = plain(b"");

        // Alone, the empty filter is refused.
        assert_eq!(
            client.validate_subscriptions(&[bad], 1, SubscriptionType::Subscribe),
            Err(ClientError::BadParameter)
        );

        // Last, it is still refused.
        assert_eq!(
            client.validate_subscriptions(&[good, bad], 1, SubscriptionType::Subscribe),
            Err(ClientError::BadParameter)
        );

        // FIRST, it is accepted -- because the good entry that follows
        // overwrites the verdict.
        assert_eq!(
            client.validate_subscriptions(&[bad, good], 1, SubscriptionType::Subscribe),
            Ok(()),
            "the list validator now reports the whole list"
        );

        // And it is not special to the empty filter: a shared subscription with
        // no share name behaves the same way.
        let malformed = plain(b"$share//a/b");
        assert_eq!(
            client.validate_subscriptions(&[malformed], 1, SubscriptionType::Subscribe),
            Err(ClientError::BadParameter)
        );
        assert_eq!(
            client.validate_subscriptions(&[malformed, good], 1, SubscriptionType::Subscribe),
            Ok(())
        );
    }

    /// The QoS loop, by contrast, does break — and so reports the whole list.
    ///
    /// Two loops over one list, three lines apart, and only one of them is
    /// right. Pinning both is what turns "this looks wrong" into "these
    /// disagree".
    #[test]
    fn the_qos_loop_reports_the_whole_list_and_the_other_does_not() {
        let mut buffer = [0u8; 64];
        let mut client = MqttContext::new(&mut buffer);
        client.enable_qos(4, 0, 0);

        let mut qos1 = plain(b"a/b");
        qos1.qos = QoS::AtLeastOnce;
        let good = plain(b"a/b");

        // First OR last, a QoS 1 entry with no incoming records is refused.
        for list in [[qos1, good], [good, qos1]] {
            assert_eq!(
                client.validate_subscriptions(&list, 1, SubscriptionType::Subscribe),
                Err(ClientError::BadParameter),
                "the QoS loop stopped reporting the whole list"
            );
        }
    }

    /// An UNSUBSCRIBE checks the filter and nothing else.
    #[test]
    fn an_unsubscribe_checks_only_the_filter() {
        let mut buffer = [0u8; 64];
        let client = context(&mut buffer);

        // A shared subscription with no share name is malformed for a
        // SUBSCRIBE and unremarkable for an UNSUBSCRIBE.
        let malformed = plain(b"$share//a/b");

        assert_eq!(
            client.validate_subscriptions(&[malformed], 1, SubscriptionType::Subscribe),
            Err(ClientError::BadParameter)
        );
        assert_eq!(
            client.validate_subscriptions(&[malformed], 1, SubscriptionType::Unsubscribe),
            Ok(())
        );

        // The filter itself is still checked.
        assert_eq!(
            client.validate_subscriptions(&[plain(b"")], 1, SubscriptionType::Unsubscribe),
            Err(ClientError::BadParameter)
        );
    }

    /// A packet identifier is never zero, even at the wrap.
    #[test]
    fn a_packet_identifier_wraps_to_one_and_never_to_zero() {
        let mut buffer = [0u8; 16];
        let mut client = MqttContext::new(&mut buffer);

        assert_eq!(client.next_packet_id(), 1);
        assert_eq!(client.next_packet_id(), 2);

        // Walk the whole space and check zero never comes out.
        let mut seen_zero = false;
        for _ in 0..70_000u32 {
            if client.next_packet_id() == 0 {
                seen_zero = true;
            }
        }

        assert!(!seen_zero, "zero is not a legal packet identifier");
    }

    /// The share-name rules, each refusal on its own.
    #[test]
    fn a_shared_subscription_needs_a_name_and_a_filter_after_it() {
        let mut buffer = [0u8; 64];
        let client = context(&mut buffer);

        let check = |filter: &[u8]| {
            client.validate_subscriptions(&[plain(filter)], 1, SubscriptionType::Subscribe)
        };

        assert_eq!(check(b"$share/g/a/b"), Ok(()));

        assert_eq!(check(b"$share//a/b"), Err(ClientError::BadParameter));
        assert_eq!(check(b"$share/g/"), Err(ClientError::BadParameter));
        assert_eq!(check(b"$share/g#/a/b"), Err(ClientError::BadParameter));
        assert_eq!(check(b"$share/g"), Err(ClientError::BadParameter));

        // Exactly the prefix is NOT a shared subscription: the C tests
        // `length > 7` before comparing seven bytes.
        assert_eq!(check(b"$share/"), Ok(()));

        // And no-local is forbidden on one.
        let mut local = plain(b"$share/g/a/b");
        local.no_local = true;
        assert_eq!(
            client.validate_subscriptions(&[local], 1, SubscriptionType::Subscribe),
            Err(ClientError::BadParameter)
        );
    }

    /// A filter is searched within its length, and the C searches past it.
    ///
    /// `checkWildcardSubscriptions` uses `strchr`. A `&[u8]` has no bytes past
    /// its length, so this arm cannot reproduce the refusal — the one place in
    /// the slice where the two differ, and the trace line is a bounded
    /// exception.
    #[test]
    fn a_wildcard_past_the_filters_length_is_not_in_the_filter() {
        let mut buffer = [0u8; 64];
        let mut client = context(&mut buffer);
        client.properties.server.wildcard_available = 0;

        // `abc#`, of which the filter is only `abc`.
        let whole = b"abc#";
        let filter = &whole[..3];

        assert_eq!(
            client.validate_subscriptions(&[plain(filter)], 1, SubscriptionType::Subscribe),
            Ok(()),
            "a wildcard outside the filter is not in the filter"
        );

        // And a wildcard INSIDE it is refused, so the check still works.
        assert_eq!(
            client.validate_subscriptions(&[plain(b"a/#")], 1, SubscriptionType::Subscribe),
            Err(ClientError::BadParameter)
        );
    }
}
