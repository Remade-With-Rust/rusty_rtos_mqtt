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
use crate::reader::{Sent, Transport};
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
    /// `MQTTSendFailed`: the whole packet did not reach the transport.
    ///
    /// A partial send is this too. Half an MQTT packet on a stream is not
    /// something a broker can recover from, so there is no "sent some of it"
    /// answer to give a caller.
    SendFailed,
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
    /// When the last byte went out, by the caller's clock.
    last_packet_tx_time: u32,
    /// When the outstanding PINGREQ was sent.
    ping_req_send_time: u32,
    /// Whether a PINGRESP is owed.
    waiting_for_ping_resp: bool,
    /// How far into the network buffer the receive path has got.
    index: usize,
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
            last_packet_tx_time: 0,
            ping_req_send_time: 0,
            waiting_for_ping_resp: false,
            index: 0,
        }
    }

    /// When the last byte went out, by the caller's clock.
    #[must_use]
    pub const fn last_packet_tx_time(&self) -> u32 {
        self.last_packet_tx_time
    }

    /// When the outstanding PINGREQ went out.
    #[must_use]
    pub const fn ping_req_send_time(&self) -> u32 {
        self.ping_req_send_time
    }

    /// Whether the broker owes a PINGRESP.
    #[must_use]
    pub const fn waiting_for_ping_resp(&self) -> bool {
        self.waiting_for_ping_resp
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

/// How long a send may take before the library gives up.
///
/// `MQTT_SEND_TIMEOUT_MS`, and the default is 20 seconds. coreMQTT's config
/// header is explicit that **if the clock is a no-op this must be zero**: with
/// a frozen clock and a transport that never accepts a byte, the sender loops
/// for ever by construction. That is a precondition, not a defect, and it is
/// why no case in `oracle/send.trace` has a frozen clock.
pub const SEND_TIMEOUT_MS: u32 = 20_000;

/// A source of milliseconds.
///
/// `MQTTGetCurrentTimeFunc_t`. It need not be an absolute time and it need not
/// start anywhere in particular; only differences are used, and those are taken
/// with wrapping subtraction so they are right across the 32-bit wrap.
pub trait Clock {
    /// Milliseconds, from any origin.
    fn now_ms(&mut self) -> u32;
}

/// How much of a buffer went out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendOutcome {
    /// This many bytes were accepted. Fewer than asked means the send timed
    /// out part-way, which every caller treats as a failure.
    Sent(usize),
    /// The transport failed.
    Failed,
}

/// `calculateElapsedTime`: `later - start`, wrapping.
///
/// One line in the C, and the only reason it is right across the 32-bit wrap is
/// that both sides are unsigned. A signed subtraction, or a `later > start`
/// guard "for safety", would make a client that has been up for 49.7 days stop
/// timing out. `oracle/send.trace` drives the clock over the wrap for that
/// reason, and the three `elapsed` lines are identical.
#[must_use]
pub const fn elapsed_ms(later: u32, start: u32) -> u32 {
    later.wrapping_sub(start)
}

impl<'a> MqttContext<'a> {
    /// `sendBuffer`: push one buffer, however many calls that takes.
    ///
    /// The loop is the interesting part. A transport may take everything, some
    /// of it, or none of it, and each answer puts the sender round again with a
    /// different offset — so what it OFFERS on each call is as much of the
    /// behaviour as how many bytes arrive, which is why the differential logs
    /// every call.
    fn send_buffer<T: Transport, C: Clock>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        buffer: &[u8],
    ) -> SendOutcome {
        let total = buffer.len();
        let start = clock.now_ms();
        let mut sent = 0usize;

        while sent < total {
            let Some(remaining) = buffer.get(sent..) else {
                return SendOutcome::Failed;
            };

            match transport.send(remaining) {
                Sent::Bytes(count) => {
                    // A transport that took more than it was offered is a bug
                    // in the transport; the C asserts, and this clamps, because
                    // a library that must not panic cannot assert.
                    sent = sent.saturating_add(count.min(remaining.len()));
                    self.last_packet_tx_time = clock.now_ms();
                }

                Sent::Nothing => {}

                Sent::Failed => {
                    // The C sets the status here and then leaves the loop on
                    // its `>= 0` condition -- but not before running the
                    // timeout check below, which reads the clock once more.
                    if self.connect_status == ConnectionStatus::Connected {
                        self.connect_status = ConnectionStatus::DisconnectPending;
                    }

                    let _ = clock.now_ms();
                    return SendOutcome::Failed;
                }
            }

            if elapsed_ms(clock.now_ms(), start) >= SEND_TIMEOUT_MS {
                break;
            }
        }

        SendOutcome::Sent(sent)
    }

    /// `sendMessageVector`: push an array of buffers.
    ///
    /// The same loop with one more thing to get wrong: when a transport takes
    /// part of a vector, the next offer must start inside it. The C advances a
    /// whole-vector iterator first and then adjusts `iov_base` and `iov_len`
    /// for the partial one; `stops-mid-vector` in the trace is the case that
    /// separates a sender that does from one that re-offers the whole vector.
    ///
    /// `writev` is not used: this arm takes the per-vector `send` path, which
    /// is the one coreMQTT falls back to and the one the trace drives.
    fn send_vectors<T: Transport, C: Clock>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        vectors: &mut [&[u8]],
    ) -> SendOutcome {
        let total: usize = vectors.iter().map(|part| part.len()).sum();
        let start = clock.now_ms();
        let mut sent = 0usize;
        let mut at = 0usize;

        while sent < total {
            let Some(current) = vectors.get(at).copied() else {
                break;
            };

            let mut taken = match transport.send(current) {
                Sent::Bytes(count) => {
                    let count = count.min(current.len());
                    sent = sent.saturating_add(count);
                    self.last_packet_tx_time = clock.now_ms();
                    count
                }

                Sent::Nothing => 0,

                Sent::Failed => {
                    if self.connect_status == ConnectionStatus::Connected {
                        self.connect_status = ConnectionStatus::DisconnectPending;
                    }

                    let _ = clock.now_ms();
                    return SendOutcome::Failed;
                }
            };

            // Step over every vector this call finished.
            while at < vectors.len() {
                let Some(part) = vectors.get(at) else { break };

                if taken < part.len() {
                    break;
                }

                taken = taken.saturating_sub(part.len());
                at = at.saturating_add(1);
            }

            // And advance inside the one it did not.
            if taken > 0 {
                if let Some(part) = vectors.get_mut(at) {
                    if let Some(rest) = part.get(taken..) {
                        *part = rest;
                    }
                }
            }

            if elapsed_ms(clock.now_ms(), start) >= SEND_TIMEOUT_MS {
                break;
            }
        }

        SendOutcome::Sent(sent)
    }

    /// `MQTT_Ping`: a PINGREQ, which is two fixed bytes.
    ///
    /// The one packet coreMQTT sends with `sendBuffer` rather than the vector
    /// sender, "as the Ping packet does not have numerous fields which need to
    /// be copied from the user provided buffers".
    ///
    /// # Errors
    ///
    /// [`ClientError::NotConnected`] or [`ClientError::DisconnectPending`] if
    /// there is no connection, and [`ClientError::SendFailed`] if the whole
    /// packet did not go.
    pub fn ping<T: Transport, C: Clock>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
    ) -> Result<(), ClientError> {
        self.connected()?;

        let mut packet = [0u8; 2];
        let written =
            crate::outbound::serialize_pingreq(&mut packet).map_err(|_| ClientError::SendFailed)?;

        let Some(bytes) = packet.get(..written) else {
            return Err(ClientError::SendFailed);
        };

        match self.send_buffer(transport, clock, bytes) {
            SendOutcome::Sent(count) if count == written => {
                self.ping_req_send_time = self.last_packet_tx_time;
                self.waiting_for_ping_resp = true;
                Ok(())
            }
            _ => Err(ClientError::SendFailed),
        }
    }

    /// `MQTT_Disconnect` and the `sendDisconnectWithoutCopy` it drives.
    ///
    /// **The connection is marked closed before the packet is sent**, and the
    /// network buffer is zeroed, so a send that fails still leaves a context
    /// that believes it is disconnected. That is the C's order and it is the
    /// right one — there is nothing useful to do with a connection whose
    /// DISCONNECT would not go.
    ///
    /// Note which state it refuses: only `NotConnected`. A context whose
    /// transport has already failed — `DisconnectPending` — is allowed to try
    /// to send a DISCONNECT, which is the one thing it might still manage.
    ///
    /// # Errors
    ///
    /// [`ClientError::NotConnected`] if there was never a connection,
    /// [`ClientError::BadParameter`] if properties were supplied without a
    /// reason code or the section is malformed, and [`ClientError::SendFailed`]
    /// if the whole packet did not go.
    pub fn disconnect<T: Transport, C: Clock>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        reason_code: Option<u8>,
        properties: &[u8],
    ) -> Result<(), ClientError> {
        if reason_code.is_none() && !properties.is_empty() {
            return Err(ClientError::BadParameter);
        }

        if !properties.is_empty() {
            crate::disconnect::validate_outgoing_properties(
                self.properties.session_expiry,
                properties,
            )
            .map_err(|_| ClientError::BadParameter)?;
        }

        let size = crate::disconnect::disconnect_packet_size(
            reason_code,
            u32::try_from(properties.len()).map_err(|_| ClientError::BadParameter)?,
            self.properties.server.max_packet_size,
        )
        .map_err(|_| ClientError::BadParameter)?;

        if self.connect_status == ConnectionStatus::NotConnected {
            return Err(ClientError::NotConnected);
        }

        // Before the send, as the C does.
        self.connect_status = ConnectionStatus::NotConnected;
        self.index = 0;
        self.network.fill(0);

        let mut fixed = [0u8; 6];
        // Returns 0 when the destination is too small, which six bytes never
        // is: one type byte, at most four of remaining length, one reason code.
        let header = crate::writer::serialize_disconnect_fixed(
            &mut fixed,
            reason_code,
            size.remaining_length,
        );

        if header == 0 {
            return Err(ClientError::SendFailed);
        }

        let mut length = [0u8; 4];
        let length_written = crate::header::encode_variable_length(
            &mut length,
            u32::try_from(properties.len()).unwrap_or(0),
        );

        let (head, _) = fixed.split_at(header);
        let (prefix, _) = length.split_at(length_written);

        let total = header
            .saturating_add(length_written)
            .saturating_add(properties.len());

        let mut vectors: [&[u8]; 3] = [head, prefix, properties];
        let count = if properties.is_empty() { 2 } else { 3 };

        let Some(parts) = vectors.get_mut(..count) else {
            return Err(ClientError::SendFailed);
        };

        match self.send_vectors(transport, clock, parts) {
            SendOutcome::Sent(sent) if sent == total => Ok(()),
            _ => Err(ClientError::SendFailed),
        }
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

    /// The elapsed-time arithmetic must wrap, and a wrong one does not answer
    /// wrongly — it never answers.
    ///
    /// This is pinned here rather than by the differential because of how it
    /// fails. Replacing `wrapping_sub` with `saturating_sub` makes a clock that
    /// has passed 0xFFFFFFFF report zero elapsed for ever, so `send_buffer`
    /// against a transport that never accepts a byte **does not return**. The
    /// poison is caught in the strongest possible sense and the harness cannot
    /// see it, because a hang is not a failing assertion.
    ///
    /// A client that has been up for 49.7 days crosses this. The C is right by
    /// construction — `later - start` on two `uint32_t` — and the only way to
    /// break it is to add a guard that looks like care.
    #[test]
    fn the_elapsed_time_wraps_and_a_guarded_subtraction_would_never_time_out() {
        // Ordinary.
        assert_eq!(elapsed_ms(10_001, 0), 10_001);
        assert_eq!(elapsed_ms(20_000, 0), 20_000);

        // Across the wrap: the clock read 10,000 before it and 1 after.
        assert_eq!(elapsed_ms(1, 0xFFFF_FFFF), 2);
        assert_eq!(elapsed_ms(10_000, 0xFFFF_D8F0), 20_000);

        // And the value that decides: at the wrap, an elapsed time at or over
        // the timeout must still be at or over it.
        assert!(elapsed_ms(10_000, 0xFFFF_D8F0) >= SEND_TIMEOUT_MS);

        // A saturating subtraction gives zero for every one of these, which is
        // why it would never time out.
        assert_eq!(1u32.saturating_sub(0xFFFF_FFFF), 0);
        assert_eq!(10_000u32.saturating_sub(0xFFFF_D8F0), 0);
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
