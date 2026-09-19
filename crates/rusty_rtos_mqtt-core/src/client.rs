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
use crate::outbound::subscription_options;
use crate::outpublish::OutgoingPublish;
use crate::publish::PublishInfo;
use crate::reader::{Sent, Transport};
use crate::state::{PublishRecords, QoS, Record, StateError};
use crate::validate::{
    ValidateError, validate_publish_params, validate_publish_properties,
    validate_subscribe_properties, validate_unsubscribe_properties,
};

/// `MQTTRetainHandling_t`.
///
/// One type, two users: the serializer builds the options byte from it and the
/// client validates it. There was briefly a second copy here with the same five
/// fields, which is the twenty-second shape of the guard pointed at a struct —
/// *two types that cannot be told apart are one type with two names*.
pub use crate::outbound::{RetainHandling, Subscription};

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
    /// `MQTTPublishStoreFailed`: the application refused to keep the copy that
    /// a QoS 1 or 2 PUBLISH needs in order to be retransmitted.
    ///
    /// **Nothing was sent.** The copy is taken before the packet goes out, so a
    /// refusal here is the one send failure that leaves the wire untouched.
    PublishStoreFailed,
    /// What the publish-state machine said. `MQTTStateCollision` is the one to
    /// expect: that packet identifier is already in flight.
    State(StateError),
    /// `MQTTRecvFailed`: the transport failed, or delivered less of a packet
    /// than its own header said was coming.
    RecvFailed,
    /// `MQTTBadResponse`: what arrived was not what the protocol allows there.
    BadResponse,
    /// `MQTTServerRefused`: the CONNACK parsed and said no.
    ServerRefused,
    /// `MQTTStatusConnected`: there is already a connection.
    ///
    /// Not [`NotConnected`](Self::NotConnected) pointed the other way:
    /// `MQTT_Connect` is the one call that wants the context DISCONNECTED, so
    /// it is the one call with its own answer for finding it connected.
    StatusConnected,
    /// `MQTTPublishRetrieveFailed`: a resumed session needed a packet back out
    /// of the retransmit store and did not get it.
    PublishRetrieveFailed,
    /// `MQTTNoDataAvailable`: nothing had arrived yet.
    ///
    /// The receive loop **resets this to success** before returning, because
    /// "no data available is not an error"; it is visible only inside.
    NoDataAvailable,
    /// `MQTTNeedMoreBytes`: part of a packet is in the buffer and the rest is
    /// not. The bytes stay put and the next call continues them.
    NeedMoreBytes,
    /// `MQTTEventCallbackFailed`: the application answered `false`.
    ///
    /// The packet has already been consumed when this is raised, and the C's
    /// comment beside every one of its call sites is a TODO asking whether it
    /// should be re-delivered.
    EventCallbackFailed,
    /// `MQTTKeepAliveTimeout`: a PINGRESP did not arrive in time.
    KeepAliveTimeout,
}

impl From<crate::header::HeaderError> for ClientError {
    fn from(error: crate::header::HeaderError) -> Self {
        match error {
            crate::header::HeaderError::NoDataAvailable => Self::NoDataAvailable,
            crate::header::HeaderError::NeedMoreBytes => Self::NeedMoreBytes,
            crate::header::HeaderError::BadResponse => Self::BadResponse,
        }
    }
}

impl From<crate::publish::PublishError> for ClientError {
    fn from(error: crate::publish::PublishError) -> Self {
        match error {
            crate::publish::PublishError::BadParameter => Self::BadParameter,
            crate::publish::PublishError::BadResponse => Self::BadResponse,
        }
    }
}

impl From<crate::ack::AckError> for ClientError {
    fn from(error: crate::ack::AckError) -> Self {
        match error {
            crate::ack::AckError::BadParameter => Self::BadParameter,
            crate::ack::AckError::BadResponse => Self::BadResponse,
        }
    }
}

impl From<crate::disconnect::DisconnectError> for ClientError {
    fn from(error: crate::disconnect::DisconnectError) -> Self {
        match error {
            crate::disconnect::DisconnectError::BadParameter => Self::BadParameter,
            crate::disconnect::DisconnectError::BadResponse => Self::BadResponse,
            crate::disconnect::DisconnectError::NoMemory => Self::SendFailed,
        }
    }
}

impl From<crate::reader::ReadError> for ClientError {
    fn from(error: crate::reader::ReadError) -> Self {
        match error {
            crate::reader::ReadError::NoDataAvailable => Self::NoDataAvailable,
            crate::reader::ReadError::RecvFailed => Self::RecvFailed,
            crate::reader::ReadError::BadResponse => Self::BadResponse,
        }
    }
}

impl From<crate::connack::ConnAckError> for ClientError {
    fn from(error: crate::connack::ConnAckError) -> Self {
        match error {
            crate::connack::ConnAckError::BadParameter => Self::BadParameter,
            crate::connack::ConnAckError::BadResponse => Self::BadResponse,
        }
    }
}

impl From<crate::connect::ConnectError> for ClientError {
    fn from(_: crate::connect::ConnectError) -> Self {
        Self::BadParameter
    }
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

/// `MQTTContext_t`, as far as this slice needs it.
#[derive(Debug)]
pub struct MqttContext<'a> {
    /// Every limit the session runs under.
    pub properties: ConnectionProperties,
    /// Whether there is a connection.
    pub connect_status: ConnectionStatus,
    pub(crate) network: &'a mut [u8],
    next_packet_id: u16,
    pub(crate) records: Option<PublishRecords<'a>>,
    /// `ackPropsBuffer`: where the application writes properties for the
    /// acknowledgement the library is about to send.
    pub(crate) ack_properties: &'a mut [u8],
    /// How much of it is in use. `MQTTPropBuilder_t.currentIndex`.
    pub(crate) ack_used: usize,
    /// When the last byte went out, by the caller's clock.
    pub(crate) last_packet_tx_time: u32,
    /// When the last byte came in.
    pub(crate) last_packet_rx_time: u32,
    /// When the outstanding PINGREQ was sent.
    pub(crate) ping_req_send_time: u32,
    /// Whether a PINGRESP is owed.
    pub(crate) waiting_for_ping_resp: bool,
    /// The keep alive the session is running under.
    ///
    /// Set from the CONNECT and then **overwritten by the CONNACK**: MQTT 5
    /// lets a broker impose its own, so the client's is a request.
    pub(crate) keep_alive_seconds: u16,
    /// Whether anything has gone out since the last keep-alive check.
    pub(crate) control_packet_sent: bool,
    /// How far into the network buffer the receive path has got.
    pub(crate) index: usize,
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
            records: None,
            ack_properties: &mut [],
            ack_used: 0,
            last_packet_tx_time: 0,
            last_packet_rx_time: 0,
            ping_req_send_time: 0,
            waiting_for_ping_resp: false,
            keep_alive_seconds: 0,
            control_packet_sent: false,
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

    /// When the last byte came in, by the caller's clock.
    #[must_use]
    pub const fn last_packet_rx_time(&self) -> u32 {
        self.last_packet_rx_time
    }

    /// The keep alive the session is running under, in seconds.
    #[must_use]
    pub const fn keep_alive_seconds(&self) -> u16 {
        self.keep_alive_seconds
    }

    /// Whether anything has gone out since the last keep-alive check.
    #[must_use]
    pub const fn control_packet_sent(&self) -> bool {
        self.control_packet_sent
    }

    /// How far into the network buffer the receive path has got.
    #[must_use]
    pub const fn index(&self) -> usize {
        self.index
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
    ///
    /// The C keeps a pointer and a count for each array; here it is the arrays
    /// themselves, so a context that has been given records **is** a context
    /// that can do QoS 1 — the two cannot drift apart.
    pub fn enable_qos(
        &mut self,
        outgoing: &'a mut [Record],
        incoming: &'a mut [Record],
        ack_properties: &'a mut [u8],
    ) {
        self.records = Some(PublishRecords::new(outgoing, incoming));
        self.ack_properties = ack_properties;
        self.ack_used = 0;
    }

    /// How many outgoing QoS records there is room for.
    #[must_use]
    pub fn outgoing_records(&self) -> usize {
        self.records
            .as_ref()
            .map_or(0, |records| records.outgoing().len())
    }

    /// How many incoming QoS records there is room for.
    #[must_use]
    pub fn incoming_records(&self) -> usize {
        self.records
            .as_ref()
            .map_or(0, |records| records.incoming().len())
    }

    /// The outgoing records, for a caller that wants to see what is in flight.
    ///
    /// The C exposes the array it was handed, so there is nothing hidden here
    /// that was not already the application's.
    #[must_use]
    pub fn outgoing(&self) -> &[Record] {
        self.records
            .as_ref()
            .map_or(&[], |records| records.outgoing())
    }

    /// The incoming records.
    #[must_use]
    pub fn incoming(&self) -> &[Record] {
        self.records
            .as_ref()
            .map_or(&[], |records| records.incoming())
    }

    /// The two record arrays, to write to.
    ///
    /// Both at once, because a caller restoring a session has entries for each
    /// and cannot borrow them one at a time.
    pub fn records_mut(&mut self) -> (&mut [Record], &mut [Record]) {
        match self.records.as_mut() {
            Some(records) => records.both_mut(),
            None => (&mut [], &mut []),
        }
    }

    /// Set the keep alive the session runs under, in seconds.
    ///
    /// Everything below is a field the C's application writes DIRECTLY, its
    /// context being an open struct — a client restoring a session across a
    /// reboot has to put them back, and the C's own tests set every one of
    /// them. They are here for the same reason and with the same caveat: the
    /// library will overwrite them as the connection runs.
    pub const fn set_keep_alive_seconds(&mut self, seconds: u16) {
        self.keep_alive_seconds = seconds;
    }

    /// Set whether a PINGRESP is owed.
    pub const fn set_waiting_for_ping_resp(&mut self, waiting: bool) {
        self.waiting_for_ping_resp = waiting;
    }

    /// Set when the last byte went out.
    pub const fn set_last_packet_tx_time(&mut self, now: u32) {
        self.last_packet_tx_time = now;
    }

    /// Set when the last byte came in.
    pub const fn set_last_packet_rx_time(&mut self, now: u32) {
        self.last_packet_rx_time = now;
    }

    /// Set when the outstanding PINGREQ went out.
    pub const fn set_ping_req_send_time(&mut self, now: u32) {
        self.ping_req_send_time = now;
    }

    /// The outgoing records, to write to.
    ///
    /// The C's array belongs to the APPLICATION — `MQTT_InitStatefulQoS` is
    /// handed a pointer and the application keeps one too — so a caller that
    /// restores a session from its own storage writes into it directly. There
    /// is nothing hidden here that the C hides.
    pub fn outgoing_mut(&mut self) -> &mut [Record] {
        match self.records.as_mut() {
            Some(records) => records.outgoing_mut(),
            None => &mut [],
        }
    }

    /// `MQTT_CancelCallback`: forget an outgoing message before it is answered.
    ///
    /// # Errors
    ///
    /// [`ClientError::BadParameter`] if QoS was never enabled, and
    /// [`ClientError::State`] if there is no such record.
    pub fn cancel_callback(&mut self, packet_id: u16) -> Result<(), ClientError> {
        let Some(records) = self.records.as_mut() else {
            return Err(ClientError::BadParameter);
        };

        records.remove(packet_id).map_err(ClientError::State)
    }

    /// How much room an acknowledgement's properties have.
    #[must_use]
    pub const fn ack_properties(&self) -> usize {
        self.ack_properties.len()
    }

    /// How much of that room the application has used.
    #[must_use]
    pub const fn ack_used(&self) -> usize {
        self.ack_used
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
    pub(crate) const fn connected(&self) -> Result<(), ClientError> {
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
        if self.incoming_records() == 0 {
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

        if self.outgoing_records() == 0 && qos != QoS::AtMostOnce {
            return Err(ClientError::BadParameter);
        }

        Ok(())
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
    pub(crate) fn send_buffer<T: Transport, C: Clock>(
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
    pub(crate) fn send_vectors<T: Transport, C: Clock>(
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
            let Some(outstanding) = vectors.get(at..) else {
                break;
            };

            if outstanding.is_empty() {
                break;
            }

            let mut taken = match transport.writev(outstanding) {
                Sent::Bytes(count) => {
                    // The C asserts that a transport never takes more than the
                    // whole remaining packet; a library that must not panic
                    // clamps instead.
                    let count = count.min(total.saturating_sub(sent));
                    sent = sent.saturating_add(count);
                    self.last_packet_tx_time = clock.now_ms();
                    count
                }

                Sent::Nothing => 0,

                Sent::Failed => {
                    if self.connect_status == ConnectionStatus::Connected {
                        self.connect_status = ConnectionStatus::DisconnectPending;
                    }

                    // And it does NOT read the clock again: this sender's
                    // timeout check is guarded by `bytesSentOrError >= 0`,
                    // where `sendBuffer`'s is not. `vec 8 fails-at-once` reads
                    // the clock once and `ping 9 fails-at-once` reads it twice.
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

            // **The two senders do not check the timeout the same way**, and
            // they are thirty lines apart in one file. `sendBuffer` reads the
            // clock on every turn and compares with `>=`; this one reads it
            // ONLY when it is about to go round again, and compares with `>`.
            //
            // So a step that lands the elapsed time exactly on
            // `MQTT_SEND_TIMEOUT_MS` stops the buffer sender and does not stop
            // this one, and a completed vector send reads the clock one time
            // fewer than a completed buffer send. Both differences are in the
            // trace; neither is anything but a transcription.
            if sent < total && elapsed_ms(clock.now_ms(), start) > SEND_TIMEOUT_MS {
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

// ---- the outgoing packets -----------------------------------------------

/// What arrived, as the application sees it.
///
/// The C splits this across `MQTTPacketInfo_t` and `MQTTDeserializedInfo_t` and
/// hands both to the callback; here they are one thing, because no caller ever
/// gets one without the other.
#[derive(Debug, Clone, Copy)]
pub struct Event<'a> {
    /// The packet type byte, flags and all. A PUBLISH is `0x30..=0x3F`.
    pub packet_type: u8,
    /// The packet identifier, or 0 where the packet carries none.
    pub packet_id: u16,
    /// Present only for a PUBLISH.
    pub publish: Option<PublishInfo<'a>>,
    /// The reason codes: none, one, or one per topic filter for a SUBACK.
    pub reason_codes: &'a [u8],
    /// The property section the packet carried, without its length prefix.
    pub properties: &'a [u8],
}

/// Where the application may put a reason code and properties for the
/// acknowledgement the library is about to send back.
///
/// The C passes two pointers that are **NULL** when no reply is wanted — for a
/// QoS 0 PUBLISH, for a PUBACK or PUBCOMP, for a PINGRESP — and the application
/// is expected to check. Here "no reply is wanted" is
/// [`accepted`](Self::accepted) answering `false`, and a reply written anyway
/// is simply dropped rather than being a null dereference.
#[derive(Debug)]
pub struct AckReply<'a> {
    accepted: bool,
    reason_code: Option<u8>,
    buffer: &'a mut [u8],
    used: usize,
}

impl<'a> AckReply<'a> {
    /// The library's own constructor. Applications receive one; they do not
    /// build one.
    pub(crate) fn new(accepted: bool, buffer: &'a mut [u8], used: usize) -> Self {
        Self {
            accepted,
            reason_code: None,
            buffer,
            used,
        }
    }

    /// How much of the buffer is in use after the application had its turn.
    pub(crate) const fn used(&self) -> usize {
        self.used
    }

    /// Whether this event wants a reply at all.
    #[must_use]
    pub const fn accepted(&self) -> bool {
        self.accepted
    }

    /// Set the reason code the acknowledgement will carry.
    pub const fn set_reason_code(&mut self, reason_code: u8) {
        if self.accepted {
            self.reason_code = Some(reason_code);
        }
    }

    /// The reason code the application set, if it set one.
    #[must_use]
    pub const fn reason_code(&self) -> Option<u8> {
        self.reason_code
    }

    /// Build properties for the acknowledgement.
    ///
    /// The closure is not run at all when the context was given no buffer, or
    /// when this event wants no reply — which is the C's "the pointer it handed
    /// you was NULL", turned into something a caller cannot dereference.
    ///
    /// How much was written is taken from the builder afterwards, so a caller
    /// cannot forget to hand it back.
    pub fn with_properties<F>(&mut self, build: F)
    where
        F: FnOnce(&mut crate::builder::PropertyBuilder<'_>),
    {
        if !self.accepted || self.buffer.is_empty() {
            return;
        }

        // RESUMED, not started: the C's builder lives in the context and its
        // index survives between acknowledgements, so an adder appends to
        // whatever is already there. That is what makes emptying it after a
        // send load-bearing.
        let Ok(mut builder) = crate::builder::PropertyBuilder::resume(self.buffer, self.used)
        else {
            return;
        };

        build(&mut builder);
        self.used = builder.len();
    }
}

/// `MQTTEventCallback_t`: the application, called once per packet.
///
/// Answering `false` is `MQTTEventCallbackFailed`, and the C's comment beside
/// every one of its call sites is a TODO asking whether that should stop the
/// library processing further packets. It does not; the status is returned and
/// the loop has already consumed the packet.
pub trait EventHandler {
    /// One packet arrived. Answer `false` to report a failure.
    fn on_event(&mut self, event: &Event<'_>, reply: &mut AckReply<'_>) -> bool;
}

/// An [`EventHandler`] that accepts everything and replies to nothing.
#[derive(Debug, Clone, Copy, Default)]
pub struct IgnoreEvents;

impl EventHandler for IgnoreEvents {
    fn on_event(&mut self, _event: &Event<'_>, _reply: &mut AckReply<'_>) -> bool {
        true
    }
}

/// `MQTT_SUB_UNSUB_MAX_VECTORS`: how many vectors one gather may hold.
///
/// A SUBSCRIBE spends **three** vectors on a topic — its length, the filter,
/// the options byte — and an UNSUBSCRIBE spends **two**, and the C's guard is
/// `ioVectorLength <= MAX - per_topic`. With the default of four that gives an
/// asymmetry worth stating, because it is entirely invisible in the bytes:
///
/// | | first gather | after it |
/// |---|---|---|
/// | SUBSCRIBE | header + property length, and **no filter** | one filter |
/// | UNSUBSCRIBE | header + property length + **one filter** | two filters |
///
/// The count is never reset before the filter loop, only after each send, so
/// the header's own two or three vectors are what the first filter has to fit
/// around. One MQTT packet comes out either way: the GATHERS are split, not the
/// stream, and only the call log tells them apart.
pub const MAX_VECTORS: usize = 4;

/// `CORE_MQTT_SUBSCRIBE_PER_TOPIC_VECTOR_LENGTH`.
const SUBSCRIBE_PER_TOPIC: usize = 3;

/// `CORE_MQTT_UNSUBSCRIBE_PER_TOPIC_VECTOR_LENGTH`.
const UNSUBSCRIBE_PER_TOPIC: usize = 2;

/// How many vectors a PUBLISH can need: header, topic, packet id, property
/// length, properties, payload.
const PUBLISH_MAX_VECTORS: usize = 6;

// The precondition the C does not state and cannot check.
//
// Its guard is `ioVectorLength <= MQTT_SUB_UNSUB_MAX_VECTORS - PER_TOPIC` in
// **unsigned** arithmetic, so a configuration where the maximum is below the
// per-topic cost makes the subtraction wrap to `SIZE_MAX`, the guard always
// true, and the loop write past the end of `pIoVector`. Two is enough to do it,
// and `MQTT_SUB_UNSUB_MAX_VECTORS` is a documented user-settable macro with no
// stated minimum. See `docs/upstream/`.
//
// Here it is a build error.
const _: () = assert!(MAX_VECTORS >= SUBSCRIBE_PER_TOPIC);
const _: () = assert!(MAX_VECTORS >= UNSUBSCRIBE_PER_TOPIC);

/// Somewhere to keep a QoS 1 or 2 PUBLISH until it is acknowledged.
///
/// `MQTTStorePacketForRetransmit`. The C hands the application an opaque
/// `MQTTVec_t` and two functions to measure and flatten it; here the parts
/// **are** the argument, and [`vector_bytes`] and [`serialize_vector`] are
/// those two functions with nothing left to hide.
///
/// **The copy this is given has the DUP flag raised** and the packet that then
/// goes on the wire has it clear. See [`MqttContext::publish`].
pub trait Store {
    /// Keep this packet. Answering `false` fails the publish, and nothing is
    /// sent.
    fn store(&mut self, packet_id: u32, parts: &[&[u8]]) -> bool;

    /// Give back a packet kept earlier, flattened.
    ///
    /// `MQTTRetrievePacketForRetransmit`, which hands back a pointer and a
    /// length; here it is the slice those two were. `None` is the C's `false`.
    fn retrieve(&mut self, packet_id: u32) -> Option<&[u8]>;

    /// Forget a packet. `MQTTClearPacketForRetransmit`.
    fn clear(&mut self, packet_id: u32);
}

/// `SET_INCOMING_PUB_FLAG`: an INCOMING publish's key in the same store.
///
/// A packet identifier is sixteen bits and the store is keyed on
/// thirty-two, so bit 16 separates the two directions — an outgoing PUBLISH
/// waiting for its PUBACK and an incoming one waiting to be PUBRELed can share
/// a number without sharing a slot.
#[must_use]
pub const fn incoming_key(packet_id: u16) -> u32 {
    (packet_id as u32) | (1 << 16)
}

/// A [`Store`] that keeps nothing, for a client that does not retransmit.
///
/// `MQTT_InitRetransmits` is optional in the C and its absence is a null
/// pointer in the context. Here it is an `Option<&mut S>` argument, and this is
/// the type to name when passing `None` — the whole of that function's body is
/// four null checks and three assignments, so the parameter is the remake.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoStore;

impl Store for NoStore {
    fn store(&mut self, _packet_id: u32, _parts: &[&[u8]]) -> bool {
        false
    }

    fn retrieve(&mut self, _packet_id: u32) -> Option<&[u8]> {
        None
    }

    fn clear(&mut self, _packet_id: u32) {}
}

/// `MQTT_GetBytesInMQTTVec`: how long the flattened packet would be.
///
/// # Errors
///
/// [`ClientError::BadParameter`] if the total would overflow, which is the C's
/// only refusal here.
pub fn vector_bytes(parts: &[&[u8]]) -> Result<usize, ClientError> {
    let mut total = 0usize;

    for part in parts {
        total = total
            .checked_add(part.len())
            .ok_or(ClientError::BadParameter)?;
    }

    Ok(total)
}

/// `MQTT_SerializeMQTTVec`: flatten the packet into one buffer.
///
/// Returns how many bytes were written, or **0** if `destination` will not hold
/// them — which the C cannot say, because it is a `void` that asserts the
/// caller sized the buffer with [`vector_bytes`] first. A library that must not
/// panic has to answer something instead, and zero is the answer no correct
/// caller ever sees.
#[must_use]
pub fn serialize_vector(destination: &mut [u8], parts: &[&[u8]]) -> usize {
    let Ok(needed) = vector_bytes(parts) else {
        return 0;
    };

    if destination.len() < needed {
        return 0;
    }

    let mut at = 0usize;

    for part in parts {
        let end = at.saturating_add(part.len());

        let Some(slot) = destination.get_mut(at..end) else {
            return 0;
        };

        slot.copy_from_slice(part);
        at = end;
    }

    at
}

impl<'a> MqttContext<'a> {
    /// `MQTT_Subscribe`, and the `sendSubscribeWithoutCopy` it drives.
    ///
    /// The filters are **not copied**: they stay in the caller's buffers and
    /// the transport gathers them.
    ///
    /// # Errors
    ///
    /// [`ClientError::BadParameter`] from the validators or the size
    /// calculator, [`ClientError::NotConnected`] or
    /// [`ClientError::DisconnectPending`] from the connection, and
    /// [`ClientError::SendFailed`] if a gather did not go out whole.
    pub fn subscribe<T: Transport, C: Clock>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        list: &[Subscription<'_>],
        packet_id: u16,
        properties: &[u8],
    ) -> Result<(), ClientError> {
        self.send_list(transport, clock, list, packet_id, properties, true)
    }

    /// `MQTT_Unsubscribe`, and the `sendUnsubscribeWithoutCopy` it drives.
    ///
    /// # Errors
    ///
    /// As [`subscribe`](Self::subscribe), with only the filters validated and
    /// no options byte on the wire.
    pub fn unsubscribe<T: Transport, C: Clock>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        list: &[Subscription<'_>],
        packet_id: u16,
        properties: &[u8],
    ) -> Result<(), ClientError> {
        self.send_list(transport, clock, list, packet_id, properties, false)
    }

    /// The body both of them are.
    ///
    /// The C has two functions of 130 lines each that differ in a packet type,
    /// a property validator, a per-topic vector count and one `if`. Writing
    /// them out twice here would be the guard's twenty-second shape — *two arms
    /// that never disagree are one arm driven twice* — so `subscribing` is that
    /// difference, named.
    fn send_list<T: Transport, C: Clock>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        list: &[Subscription<'_>],
        packet_id: u16,
        properties: &[u8],
        subscribing: bool,
    ) -> Result<(), ClientError> {
        let which = if subscribing {
            SubscriptionType::Subscribe
        } else {
            SubscriptionType::Unsubscribe
        };

        self.validate_subscriptions(list, packet_id, which)?;

        if !properties.is_empty() {
            if subscribing {
                let available = self.properties.server.subscription_id_available != 0;

                validate_subscribe_properties(available, properties)?;
            } else {
                validate_unsubscribe_properties(properties)?;
            }
        }

        let property_length =
            u32::try_from(properties.len()).map_err(|_| ClientError::BadParameter)?;
        let lengths = list.iter().map(|entry| entry.topic_filter.len());
        let max = self.properties.server.max_packet_size;

        let size = if subscribing {
            crate::size::subscribe_packet_size(lengths, property_length, max)
        } else {
            crate::size::unsubscribe_packet_size(lengths, property_length, max)
        }
        .map_err(|_| ClientError::BadParameter)?;

        self.connected()?;

        // One type byte, at most four of remaining length, two of packet id.
        let mut fixed = [0u8; 7];
        let header = if subscribing {
            crate::writer::serialize_subscribe_header(&mut fixed, size.remaining_length, packet_id)
        } else {
            crate::writer::serialize_unsubscribe_header(
                &mut fixed,
                size.remaining_length,
                packet_id,
            )
        };

        if header == 0 {
            return Err(ClientError::SendFailed);
        }

        let mut length_field = [0u8; 4];
        let length_written =
            crate::header::encode_variable_length(&mut length_field, property_length);

        let Some(head) = fixed.get(..header) else {
            return Err(ClientError::SendFailed);
        };
        let Some(prefix) = length_field.get(..length_written) else {
            return Err(ClientError::SendFailed);
        };

        // The header, the encoded property length, and the section when there
        // is one. These are CARRIED INTO the first gather rather than sent on
        // their own -- see [`MAX_VECTORS`].
        let carried: [&[u8]; 3] = [head, prefix, properties];
        let carried_count = if properties.is_empty() { 2 } else { 3 };
        let carried_bytes =
            header
                .saturating_add(length_written)
                .saturating_add(if properties.is_empty() {
                    0
                } else {
                    properties.len()
                });

        let per_topic = if subscribing {
            SUBSCRIBE_PER_TOPIC
        } else {
            UNSUBSCRIBE_PER_TOPIC
        };
        let room = MAX_VECTORS.saturating_sub(per_topic);

        let mut at = 0usize;
        let mut first = true;

        while first || at < list.len() {
            // The two little buffers the vectors point INTO live inside the
            // loop, so the gather that borrows them cannot outlive them. The
            // C's are function-scoped and it rewinds its iterator instead --
            // which is the same thing said in a language that will not check
            // it.
            let mut fields = [[0u8; 2]; MAX_VECTORS];
            let mut options = [0u8; MAX_VECTORS];

            let mut used = if first { carried_count } else { 0 };
            let mut taken = 0usize;

            while used <= room && at.saturating_add(taken) < list.len() {
                let Some(entry) = list.get(at.saturating_add(taken)) else {
                    break;
                };

                // The one refusal inside the loop that a slice can still reach.
                if u16::try_from(entry.topic_filter.len()).is_err() {
                    return Err(ClientError::BadParameter);
                }

                let filter_length = u16::try_from(entry.topic_filter.len()).unwrap_or(u16::MAX);

                let Some(field) = fields.get_mut(taken) else {
                    break;
                };

                *field = filter_length.to_be_bytes();

                // `addEncodedStringToVector` adds the length field always, and
                // the string only when it has bytes.
                used = used.saturating_add(1);

                if !entry.topic_filter.is_empty() {
                    used = used.saturating_add(1);
                }

                if subscribing {
                    if let Some(byte) = options.get_mut(taken) {
                        *byte = subscription_options(entry);
                    }

                    used = used.saturating_add(1);
                }

                taken = taken.saturating_add(1);
            }

            let mut parts: [&[u8]; MAX_VECTORS] = [&[]; MAX_VECTORS];
            let mut count = 0usize;
            let mut total = 0usize;

            // How many vectors this gather WANTED, which is `count` unless the
            // array was too small. The C has no equivalent: it writes past
            // `pIoVector[MQTT_SUB_UNSUB_MAX_VECTORS]` and carries on. Getting
            // `per_topic` one too small is all it takes -- see the module note
            // and `docs/upstream/`.
            let mut wanted = 0usize;

            if first {
                for part in carried.iter().take(carried_count) {
                    wanted = wanted.saturating_add(1);

                    if let Some(slot) = parts.get_mut(count) {
                        *slot = part;
                        count = count.saturating_add(1);
                    }
                }

                total = carried_bytes;
            }

            for index in 0..taken {
                let Some(entry) = list.get(at.saturating_add(index)) else {
                    break;
                };
                let Some(field) = fields.get(index) else {
                    break;
                };

                wanted = wanted.saturating_add(1);

                if let Some(slot) = parts.get_mut(count) {
                    *slot = field.as_slice();
                    count = count.saturating_add(1);
                }

                total = total.saturating_add(2);

                if !entry.topic_filter.is_empty() {
                    wanted = wanted.saturating_add(1);

                    if let Some(slot) = parts.get_mut(count) {
                        *slot = entry.topic_filter;
                        count = count.saturating_add(1);
                    }

                    total = total.saturating_add(entry.topic_filter.len());
                }

                if subscribing {
                    wanted = wanted.saturating_add(1);

                    if let Some(byte) = options.get(index..index.saturating_add(1)) {
                        if let Some(slot) = parts.get_mut(count) {
                            *slot = byte;
                            count = count.saturating_add(1);
                        }
                    }

                    total = total.saturating_add(1);
                }
            }

            if wanted != count {
                return Err(ClientError::SendFailed);
            }

            let Some(gather) = parts.get_mut(..count) else {
                return Err(ClientError::SendFailed);
            };

            match self.send_vectors(transport, clock, gather) {
                SendOutcome::Sent(sent) if sent == total => {}
                _ => return Err(ClientError::SendFailed),
            }

            at = at.saturating_add(taken);
            first = false;
        }

        Ok(())
    }

    /// `MQTT_Publish`, and the `sendPublishWithoutCopy` it drives.
    ///
    /// The topic name and the payload are **not copied**.
    ///
    /// # The stored copy and the sent copy differ by one bit
    ///
    /// A QoS 1 or 2 PUBLISH is handed to `store` **before** it is sent, and the
    /// header it is handed has the DUP flag raised — because what comes back
    /// out of a retransmit store is a list of `const` pointers that cannot be
    /// patched afterwards. The C raises the bit in the buffer, calls the store,
    /// and lowers it again; the packet on the wire has DUP clear. So the two
    /// copies of the same packet are one byte apart, on purpose, and
    /// `qos1-stored` in the trace is the case that shows it.
    ///
    /// If `publish.dup` was already set nothing is changed and both copies
    /// carry it. And if the store **refuses**, the C never lowers the bit —
    /// unobservable, because nothing is then sent.
    ///
    /// # Errors
    ///
    /// [`ClientError::BadParameter`] from the validators,
    /// [`ClientError::NotConnected`], [`ClientError::State`] if that packet
    /// identifier is already in flight, [`ClientError::PublishStoreFailed`] if
    /// `store` refused the copy, and [`ClientError::SendFailed`].
    pub fn publish<T: Transport, C: Clock, S: Store>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        store: Option<&mut S>,
        publish: &OutgoingPublish<'_>,
        packet_id: u16,
    ) -> Result<(), ClientError> {
        self.validate_publish(
            publish.qos,
            packet_id,
            publish.topic_name.len(),
            publish.payload.len(),
        )?;

        let mut topic_alias = None;

        if !publish.properties.is_empty() {
            validate_publish_properties(
                self.properties.server.topic_alias_max,
                publish.properties,
                &mut topic_alias,
            )?;
        }

        validate_publish_params(
            publish,
            self.properties.server.retain_available,
            self.properties.server.max_qos,
            topic_alias.unwrap_or(0),
            self.properties.server.max_packet_size,
        )?;

        let size =
            crate::outpublish::publish_packet_size(publish, self.properties.server.max_packet_size)
                .map_err(|_| ClientError::BadParameter)?;

        // One type byte, at most four of remaining length, two of topic length.
        // The topic LENGTH is in the fixed header and the topic is not: the C
        // moved it there to save a vector and a `send`, "since publish is one
        // of the most common operations".
        let mut header = [0u8; 7];
        let header_size = crate::outpublish::serialize_publish_header_without_topic(
            &mut header,
            publish,
            size.remaining_length,
        )
        .map_err(|_| ClientError::BadParameter)?;

        self.connected()?;

        if publish.qos != QoS::AtMostOnce {
            if let Some(records) = self.records.as_mut() {
                match records.reserve(packet_id, publish.qos) {
                    Ok(()) => {}

                    // A collision on a packet the caller says is a re-delivery
                    // is not an error: the record it collided with is the one
                    // being re-sent.
                    Err(StateError::StateCollision) if publish.dup => {}

                    Err(error) => return Err(ClientError::State(error)),
                }
            }
        }

        let id_bytes = packet_id.to_be_bytes();
        let property_length =
            u32::try_from(publish.properties.len()).map_err(|_| ClientError::BadParameter)?;
        let mut length_field = [0u8; 4];
        let length_written =
            crate::header::encode_variable_length(&mut length_field, property_length);

        // Build the store's header before anything borrows the wire one.
        let mut dup_header = header;

        if !publish.dup {
            if let Some(byte) = dup_header.first_mut() {
                crate::outpublish::update_duplicate_flag(byte, true)
                    .map_err(|_| ClientError::BadParameter)?;
            }
        }

        let Some(head) = header.get(..header_size) else {
            return Err(ClientError::SendFailed);
        };
        let Some(dup_head) = dup_header.get(..header_size) else {
            return Err(ClientError::SendFailed);
        };
        let Some(prefix) = length_field.get(..length_written) else {
            return Err(ClientError::SendFailed);
        };

        let mut parts: [&[u8]; PUBLISH_MAX_VECTORS] = [&[]; PUBLISH_MAX_VECTORS];
        let mut count = 0usize;
        let mut total = 0usize;

        for part in [head, publish.topic_name] {
            if let Some(slot) = parts.get_mut(count) {
                *slot = part;
                count = count.saturating_add(1);
                total = total.saturating_add(part.len());
            }
        }

        if publish.qos != QoS::AtMostOnce {
            if let Some(slot) = parts.get_mut(count) {
                *slot = id_bytes.as_slice();
                count = count.saturating_add(1);
                total = total.saturating_add(2);
            }
        }

        if let Some(slot) = parts.get_mut(count) {
            *slot = prefix;
            count = count.saturating_add(1);
            total = total.saturating_add(length_written);
        }

        if !publish.properties.is_empty() {
            if let Some(slot) = parts.get_mut(count) {
                *slot = publish.properties;
                count = count.saturating_add(1);
                total = total.saturating_add(publish.properties.len());
            }
        }

        // A PUBLISH is allowed to carry no payload, and then there is no vector
        // for it -- which is why `qos0-no-payload` is three calls and `qos0` is
        // four.
        if !publish.payload.is_empty() {
            if let Some(slot) = parts.get_mut(count) {
                *slot = publish.payload;
                count = count.saturating_add(1);
                total = total.saturating_add(publish.payload.len());
            }
        }

        if publish.qos != QoS::AtMostOnce {
            if let Some(keeper) = store {
                let mut copy = parts;

                if let Some(slot) = copy.first_mut() {
                    *slot = dup_head;
                }

                let Some(view) = copy.get(..count) else {
                    return Err(ClientError::SendFailed);
                };

                if !keeper.store(u32::from(packet_id), view) {
                    return Err(ClientError::PublishStoreFailed);
                }
            }
        }

        let Some(gather) = parts.get_mut(..count) else {
            return Err(ClientError::SendFailed);
        };

        match self.send_vectors(transport, clock, gather) {
            SendOutcome::Sent(sent) if sent == total => {}
            _ => return Err(ClientError::SendFailed),
        }

        if publish.qos != QoS::AtMostOnce {
            if let Some(records) = self.records.as_mut() {
                // The C logs a failure here and returns it, "However PUBLISH
                // packet was sent to the broker".
                let _ = records
                    .update_publish(packet_id, crate::state::Operation::Send, publish.qos)
                    .map_err(ClientError::State)?;
            }
        }

        Ok(())
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

    fn context<'a>(
        buffer: &'a mut [u8],
        outgoing: &'a mut [Record],
        incoming: &'a mut [Record],
    ) -> MqttContext<'a> {
        let mut client = MqttContext::new(buffer);
        client.enable_qos(outgoing, incoming, &mut []);
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

    /// A zero-length vector is never offered to the transport.
    ///
    /// Three poisons across two slices could not be made to fail, and this is
    /// why all three: the C's `addEncodedStringToVector` leaves out the body of
    /// an empty string, and a transcription that put it in anyway would be
    /// **unobservable** through a per-vector transport. The advance is what
    /// hides it — after any successful send the sender steps over every vector
    /// the count finished, and `taken < part.len()` is false for a part of
    /// length zero, so an empty vector is stepped over in the same pass rather
    /// than offered on the next.
    ///
    /// So it is not a workload gap. It is a property of the SENDER, it belongs
    /// here rather than in a trace, and the one instrument that can see it is a
    /// gathered `writev`, which is handed the vector COUNT — which is why
    /// `pub qos0-writev-no-payload` in the outgoing trace is `v3` and not `v4`.
    #[test]
    fn a_zero_length_vector_is_never_offered() {
        struct Watch(Vec<usize>);

        impl Transport for Watch {
            fn recv(&mut self, _into: &mut [u8]) -> crate::reader::Recv {
                crate::reader::Recv::Failed
            }

            fn send(&mut self, bytes: &[u8]) -> Sent {
                self.0.push(bytes.len());
                Sent::Bytes(bytes.len())
            }
        }

        struct Still;

        impl Clock for Still {
            fn now_ms(&mut self) -> u32 {
                0
            }
        }

        let mut buffer = [0u8; 16];
        let mut client = MqttContext::new(&mut buffer);
        let mut transport = Watch(Vec::new());
        let mut clock = Still;

        // An empty vector in the middle, and one at the end.
        let mut vectors: [&[u8]; 4] = [b"ab", b"", b"cde", b""];

        assert_eq!(
            client.send_vectors(&mut transport, &mut clock, &mut vectors),
            SendOutcome::Sent(5)
        );

        assert_eq!(
            transport.0,
            vec![2, 3],
            "an empty vector reached the transport"
        );
    }

    /// Two poisons this slice could not make fail, and the CONSTANT that is
    /// why.
    ///
    /// A gather takes filters while `used <= MAX_VECTORS - per_topic`, so what
    /// that subtraction comes to decides everything. At the shipped four:
    ///
    /// | | room | one filter costs | filters per gather |
    /// |---|---|---|---|
    /// | SUBSCRIBE | 1 | 2 or 3 | one, always |
    /// | UNSUBSCRIBE | 2 | 2 | two, or one after the header |
    ///
    /// A SUBSCRIBE's room is **one** and a filter costs **at least two**, so
    /// the first filter always ends the gather — whether or not the options
    /// byte is counted. That is why "the vector count does not charge for the
    /// options byte" changes no line of the trace: at this maximum it cannot.
    /// It is not a property of the code, it is a property of the number, and
    /// the number is what this test pins.
    ///
    /// The same arithmetic is why "an over-full gather is refused" never fires:
    /// `room + per_topic == MAX_VECTORS` exactly, so a correct loop cannot ask
    /// for more room than there is. The refusal is there for a maximum that is
    /// not four, and the `const` assertions above are what stop the one value
    /// that would make it unreachable AND wrong.
    #[test]
    fn the_gather_geometry_is_decided_by_a_constant() {
        let subscribe_room = MAX_VECTORS - SUBSCRIBE_PER_TOPIC;
        let unsubscribe_room = MAX_VECTORS - UNSUBSCRIBE_PER_TOPIC;

        assert_eq!(subscribe_room, 1);
        assert_eq!(unsubscribe_room, 2);

        // The cheapest a filter can be is its length field plus its bytes.
        const CHEAPEST_FILTER: usize = 2;

        assert!(
            CHEAPEST_FILTER > subscribe_room,
            "a SUBSCRIBE could take two filters in a gather, and the options \
             byte would then be load-bearing"
        );

        // And a correct loop can never want more vectors than there are.
        assert_eq!(subscribe_room + SUBSCRIBE_PER_TOPIC, MAX_VECTORS);
        assert_eq!(unsubscribe_room + UNSUBSCRIBE_PER_TOPIC, MAX_VECTORS);
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
        let mut outgoing = [Record::default(); 4];
        let mut incoming = [Record::default(); 4];
        let client = context(&mut buffer, &mut outgoing, &mut incoming);

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
        let mut outgoing = [Record::default(); 4];
        let mut incoming: [Record; 0] = [];
        let mut client = MqttContext::new(&mut buffer);
        client.enable_qos(&mut outgoing, &mut incoming, &mut []);

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
        let mut outgoing = [Record::default(); 4];
        let mut incoming = [Record::default(); 4];
        let client = context(&mut buffer, &mut outgoing, &mut incoming);

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
        let mut outgoing = [Record::default(); 4];
        let mut incoming = [Record::default(); 4];
        let client = context(&mut buffer, &mut outgoing, &mut incoming);

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
        let mut outgoing = [Record::default(); 4];
        let mut incoming = [Record::default(); 4];
        let mut client = context(&mut buffer, &mut outgoing, &mut incoming);
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
