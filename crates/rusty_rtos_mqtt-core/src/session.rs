//! Opening a connection: the CONNECT, the CONNACK, and what a resumed session
//! owes the broker.
//!
//! [`client`](crate::client) builds and sends the packets a connected client
//! sends. This is the part that makes it connected — and it is the first place
//! in the library where a function both **sends and receives**, so it is the
//! first that can deadlock, time out, or answer a packet that never arrived.
//!
//! # Two receive paths, and only one of them has a timeout
//!
//! `MQTT_Connect` reads its CONNACK in two stages. The header comes through
//! [`read_header`](crate::reader::read_header), which asks the transport for
//! one byte at a time and is retried by a loop that gives up either on a
//! **clock** or on a **retry count** — and which of the two depends on whether
//! the caller passed a non-zero `timeout_ms`:
//!
//! ```text
//! timeout_ms > 0    give up when the clock says so
//! timeout_ms == 0   give up after MAX_CONNACK_RECEIVE_RETRY_COUNT tries
//! ```
//!
//! The body then comes through [`recv_exact`](MqttContext::recv_exact), which
//! has a timeout of its own — `RECV_POLLING_TIMEOUT_MS`, ten milliseconds —
//! that resets **every time a byte arrives**. So a broker dribbling one byte
//! per nine milliseconds is never timed out, however long the packet takes.
//! That is the C's behaviour and the trace has a case for it.
//!
//! # The session is decided before the connection is
//!
//! A CONNACK carries Session Present, and what the client does next depends on
//! it: a fresh session clears every record and zeroes the network buffer, a
//! resumed one **re-sends** everything still in flight. The C does the first
//! inside its state lock and the second outside it, after
//! `connectStatus` has already been set to connected — so a retransmission
//! failure leaves a context that believes it is connected and is then forced to
//! `DisconnectPending` by the error path at the bottom. Reproduced.

use crate::ack::PacketInfo;
use crate::client::{
    ClientError, Clock, ConnectionStatus, MqttContext, SendOutcome, Store, elapsed_ms, incoming_key,
};
use crate::connack::{ClientSettings, ConnAck, field};
use crate::connect::Connect;
use crate::context::ServerLimits;
use crate::reader::{Recv, Transport};
use crate::state::{AckType, Cursor, Operation, PublishState};

/// `MQTT_RECV_POLLING_TIMEOUT_MS`: how long a body read waits on a silent
/// transport before giving up.
///
/// Ten milliseconds, and **it resets on every byte** — so it bounds the gap
/// between bytes, not the packet.
pub const RECV_POLLING_TIMEOUT_MS: u32 = 10;

/// `MQTT_MAX_CONNACK_RECEIVE_RETRY_COUNT`: how many times to ask for a CONNACK
/// header when there is no clock-based timeout.
///
/// Five. A value of zero would try **once**, because the count is checked after
/// the attempt.
pub const MAX_CONNACK_RECEIVE_RETRY_COUNT: u16 = 5;

/// How a body read ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOutcome {
    /// This many bytes are in the network buffer.
    Bytes(usize),
    /// The transport failed. The C returns its negative count here and every
    /// caller compares it against the count it wanted, so the number it carries
    /// is never used for anything but "not what I asked for".
    Failed,
}

impl<'a> MqttContext<'a> {
    /// `recvExact`: fill the front of the network buffer with `wanted` bytes.
    ///
    /// # The timeout resets on every byte
    ///
    /// `lastDataRecvTimeMs` is set before the loop and **again on every read
    /// that returns bytes**, so `RECV_POLLING_TIMEOUT_MS` bounds the silence
    /// between bytes rather than the whole read. A transport that delivers one
    /// byte every nine milliseconds is never timed out.
    ///
    /// And note which answer is checked first: a failure sets
    /// `DisconnectPending` and leaves, a zero starts the clock, and only a
    /// positive count advances. A transport that answers zero for ever times
    /// out; one that answers zero and then bytes does not.
    pub fn recv_exact<T: Transport, C: Clock>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        wanted: usize,
    ) -> ReadOutcome {
        let mut remaining = wanted;
        let mut total = 0usize;
        let mut last_data = clock.now_ms();

        while remaining > 0 {
            let end = wanted;
            let Some(into) = self.network.get_mut(total..end) else {
                return ReadOutcome::Failed;
            };

            match transport.recv(into) {
                Recv::Failed => {
                    if self.connect_status == ConnectionStatus::Connected {
                        self.connect_status = ConnectionStatus::DisconnectPending;
                    }

                    return ReadOutcome::Failed;
                }

                Recv::Bytes(0) | Recv::Nothing => {
                    // The C asks the clock again HERE, not at the top of the
                    // loop, so a read that delivers bytes costs one clock call
                    // and a read that does not costs two.
                    if elapsed_ms(clock.now_ms(), last_data) >= RECV_POLLING_TIMEOUT_MS {
                        return ReadOutcome::Bytes(total);
                    }
                }

                Recv::Bytes(count) => {
                    // A transport that delivered more than it was asked for is
                    // a bug in the transport; the C asserts and this clamps.
                    let count = count.min(remaining);

                    last_data = clock.now_ms();
                    remaining = remaining.saturating_sub(count);
                    total = total.saturating_add(count);
                }
            }
        }

        ReadOutcome::Bytes(total)
    }

    /// `receiveConnackPacket`: the body of a CONNACK, into the network buffer.
    fn receive_connack_packet<T: Transport, C: Clock>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        remaining_length: u32,
    ) -> Result<(), ClientError> {
        let wanted = usize::try_from(remaining_length).map_err(|_| ClientError::RecvFailed)?;

        // "MQTT spec doesn't allow 'dropping' packets", so a packet larger than
        // the buffer is the end of the connection rather than a skip.
        if wanted > self.network.len() {
            return Err(ClientError::RecvFailed);
        }

        match self.recv_exact(transport, clock, wanted) {
            ReadOutcome::Bytes(got) if got == wanted => Ok(()),
            _ => Err(ClientError::RecvFailed),
        }
    }

    /// `receiveConnack`: wait for the CONNACK, read it, and check it.
    ///
    /// # Errors
    ///
    /// [`ClientError::RecvFailed`] if the packet did not arrive,
    /// [`ClientError::BadResponse`] if what arrived was not a CONNACK or was
    /// malformed, and [`ClientError::ServerRefused`] if it was a refusal.
    fn receive_connack<T: Transport, C: Clock>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        timeout_ms: u32,
        clean_session: bool,
        session_present: &mut bool,
    ) -> Result<(), ClientError> {
        let entry = clock.now_ms();
        let mut loop_count = 0u16;
        let mut header;

        loop {
            header = crate::reader::read_header(transport);

            let give_up = if timeout_ms > 0 {
                elapsed_ms(clock.now_ms(), entry) >= timeout_ms
            } else {
                // Checked BEFORE the increment, so a maximum of zero tries
                // once rather than never.
                let done = loop_count >= MAX_CONNACK_RECEIVE_RETRY_COUNT;
                loop_count = loop_count.saturating_add(1);
                done
            };

            match header {
                Err(crate::reader::ReadError::NoDataAvailable) if !give_up => {}
                _ => break,
            }
        }

        let packet = header.map_err(ClientError::from)?;

        if packet.packet_type != crate::header::packet::CONNACK {
            return Err(ClientError::BadResponse);
        }

        self.receive_connack_packet(transport, clock, packet.remaining_length)?;

        let wanted =
            usize::try_from(packet.remaining_length).map_err(|_| ClientError::RecvFailed)?;
        let Some(body) = self.network.get(..wanted) else {
            return Err(ClientError::RecvFailed);
        };

        let info = PacketInfo {
            packet_type: packet.packet_type,
            remaining_length: packet.remaining_length,
            remaining_data: body,
        };

        let settings = ClientSettings {
            max_packet_size: self.properties.client.max_packet_size,
            request_response_info: self.properties.client.request_response_info,
        };

        let connack: ConnAck<'_> =
            crate::connack::deserialize_connack(&info, &settings).map_err(ClientError::from)?;

        // Written HERE, before either refusal below can happen.
        *session_present = connack.session_present;

        apply_connack(&mut self.properties, &connack);

        // A clean session that comes back resumed is a broker disagreeing with
        // the client about what was asked for, and the C treats that as the
        // packet being wrong rather than as a refusal.
        if clean_session && connack.session_present {
            return Err(ClientError::BadResponse);
        }

        if connack.refused() {
            return Err(ClientError::ServerRefused);
        }

        Ok(())
    }

    /// `sendConnectWithoutCopy`: the CONNECT, gathered from the caller's
    /// buffers.
    ///
    /// Up to fifteen vectors: the fixed header, the property length and
    /// section, the client id, the will's properties, topic and payload, and
    /// the username and password — every one of them a length field and a body,
    /// which is why the count is odd rather than round.
    fn send_connect<T: Transport, C: Clock>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        connect: &Connect<'_>,
        remaining_length: u32,
    ) -> Result<(), ClientError> {
        let mut fixed = [0u8; 15];
        let will_info = connect.will.map(|will| will.info);
        let header = crate::writer::serialize_connect_fixed_header(
            &mut fixed,
            &connect.info,
            will_info.as_ref(),
            remaining_length,
        );

        if header == 0 {
            return Err(ClientError::SendFailed);
        }

        // `sendConnectWithoutCopy` writes the CONNECT's keep alive into the
        // SERVER half of the properties, with the comment "may be overwritten
        // by CONNACK" — and `MQTT_Connect` then copies that field into
        // `keepAliveIntervalSec` whether or not the CONNACK said anything. So
        // the client's request reaches the session through the server's slot,
        // and a CONNACK with no Server Keep Alive leaves the client's value
        // standing. `zero-keep-alive` and `server-limits` are the two ends of
        // that in the trace.
        self.properties.server.keep_alive = connect.info.keep_alive_seconds;

        let property_length =
            u32::try_from(connect.properties.len()).map_err(|_| ClientError::BadParameter)?;
        let mut length_field = [0u8; 4];
        let length_written =
            crate::header::encode_variable_length(&mut length_field, property_length);

        let will_property_length = connect.will.map_or(0u32, |will| {
            u32::try_from(will.properties.len()).unwrap_or(0)
        });
        let mut will_length_field = [0u8; 4];
        let will_length_written =
            crate::header::encode_variable_length(&mut will_length_field, will_property_length);

        let client_id_length = u16::try_from(connect.client_identifier.len())
            .map_err(|_| ClientError::BadParameter)?;
        let client_id_field = client_id_length.to_be_bytes();

        let will_topic_field = connect.will.map_or([0u8; 2], |will| {
            u16::try_from(will.topic_name.len())
                .unwrap_or(u16::MAX)
                .to_be_bytes()
        });
        let will_payload_field = connect.will.map_or([0u8; 2], |will| {
            u16::try_from(will.payload.len())
                .unwrap_or(u16::MAX)
                .to_be_bytes()
        });
        let username_field = connect.info.username.map_or([0u8; 2], |value| {
            u16::try_from(value.len()).unwrap_or(u16::MAX).to_be_bytes()
        });
        let password_field = connect.info.password.map_or([0u8; 2], |value| {
            u16::try_from(value.len()).unwrap_or(u16::MAX).to_be_bytes()
        });

        let Some(head) = fixed.get(..header) else {
            return Err(ClientError::SendFailed);
        };
        let Some(prefix) = length_field.get(..length_written) else {
            return Err(ClientError::SendFailed);
        };
        let Some(will_prefix) = will_length_field.get(..will_length_written) else {
            return Err(ClientError::SendFailed);
        };

        let mut parts: [&[u8]; 15] = [&[]; 15];
        let mut count = 0usize;
        let mut total = 0usize;

        // The header, the property length, and the section when there is one.
        for part in [head, prefix] {
            if let Some(slot) = parts.get_mut(count) {
                *slot = part;
                count = count.saturating_add(1);
                total = total.saturating_add(part.len());
            }
        }

        if !connect.properties.is_empty() {
            if let Some(slot) = parts.get_mut(count) {
                *slot = connect.properties;
                count = count.saturating_add(1);
                total = total.saturating_add(connect.properties.len());
            }

            crate::context::update_with_connect_props(connect.properties, &mut self.properties)
                .map_err(|_| ClientError::BadParameter)?;
        }

        // Then the client identifier, which is always present even when empty.
        count = push_string(
            &mut parts,
            count,
            &mut total,
            &client_id_field,
            connect.client_identifier,
        )?;

        if let Some(will) = connect.will.as_ref() {
            if let Some(slot) = parts.get_mut(count) {
                *slot = will_prefix;
                count = count.saturating_add(1);
                total = total.saturating_add(will_prefix.len());
            }

            if !will.properties.is_empty() {
                if let Some(slot) = parts.get_mut(count) {
                    *slot = will.properties;
                    count = count.saturating_add(1);
                    total = total.saturating_add(will.properties.len());
                }
            }

            count = push_string(
                &mut parts,
                count,
                &mut total,
                &will_topic_field,
                will.topic_name,
            )?;
            count = push_string(
                &mut parts,
                count,
                &mut total,
                &will_payload_field,
                will.payload,
            )?;
        }

        if let Some(username) = connect.info.username {
            count = push_string(&mut parts, count, &mut total, &username_field, username)?;
        }

        if let Some(password) = connect.info.password {
            count = push_string(&mut parts, count, &mut total, &password_field, password)?;
        }

        let Some(gather) = parts.get_mut(..count) else {
            return Err(ClientError::SendFailed);
        };

        match self.send_vectors(transport, clock, gather) {
            SendOutcome::Sent(sent) if sent == total => Ok(()),
            _ => Err(ClientError::SendFailed),
        }
    }

    /// `handleCleanSession`: forget everything, because the broker has.
    ///
    /// # The second loop can never find anything
    ///
    /// The C clears the stored PUBLISHes, **zeroes the outgoing array**, and
    /// then asks `MQTT_PubrelToResend` what PUBRELs to clear — and
    /// `MQTT_PubrelToResend` reads that same outgoing array. So the answer is
    /// always "none", and every PUBREL the application stored under
    /// [`incoming_key`] stays stored for ever: a leak of one entry per QoS 2
    /// publish that was in flight when the session was replaced.
    ///
    /// The resumed path proves the record is findable — it re-sends exactly
    /// that PUBREL — so this is an ordering slip, not a missing record.
    /// Transcribed, with `clean-pubrel-only-with-store` in the trace standing
    /// over it: a store is wired up, a PUBREL is in flight, and `clear=0`.
    /// Drafted for upstream.
    fn handle_clean_session<S: Store>(&mut self, mut store: Option<&mut S>) {
        self.index = 0;
        self.network.fill(0);

        let Some(records) = self.records.as_mut() else {
            return;
        };

        if let Some(keeper) = store.as_mut() {
            let mut cursor = Cursor::new();

            while let Some(packet_id) = records.publish_to_resend(&mut cursor) {
                keeper.clear(u32::from(packet_id));
            }
        }

        // Here, and this is the line that makes the loop below useless.
        records.clear_outgoing();

        if let Some(keeper) = store.as_mut() {
            let mut cursor = Cursor::new();

            while let Some(packet_id) = records.pubrel_to_resend(&mut cursor) {
                keeper.clear(incoming_key(packet_id));
            }
        }

        records.clear_incoming();
    }

    /// `handleUncleanSessionResumption`: send again what the broker still
    /// expects.
    ///
    /// Two passes, and **only the first happens without a store**. Without one
    /// the C sends "default" acknowledgements for the outstanding PUBRELs and
    /// says so in a warning; the publishes cannot be re-sent at all, because
    /// their bytes were never kept.
    fn handle_unclean_session<T: Transport, C: Clock, S: Store>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        store: Option<&mut S>,
    ) -> Result<(), ClientError> {
        let mut store = store;
        let mut cursor = Cursor::new();

        loop {
            let next = match self.records.as_ref() {
                Some(records) => records.pubrel_to_resend(&mut cursor),
                None => None,
            };

            let Some(packet_id) = next else {
                break;
            };

            if let Some(keeper) = store.as_mut() {
                let Some(packet) = keeper.retrieve(incoming_key(packet_id)) else {
                    return Err(ClientError::PublishRetrieveFailed);
                };

                if packet.len() > crate::context::MAX_PACKET_SIZE as usize {
                    return Err(ClientError::BadParameter);
                }

                let wanted = packet.len();

                // The store's bytes go straight to the transport. They belong
                // to `keeper` and the sender belongs to `self`, so nothing has
                // to be copied to keep the borrow checker happy -- which is
                // why the C's `pMqttPacket` out-parameter is a slice here.
                match self.send_buffer(transport, clock, packet) {
                    SendOutcome::Sent(sent) if sent == wanted => {}
                    _ => return Err(ClientError::SendFailed),
                }
            } else {
                // "No Application provided retransmission storage callbacks,
                // sending 'default' PUBRECs."
                self.send_publish_acks(transport, clock, packet_id, PublishState::PubRelSend)?;
            }
        }

        // The second pass only happens WITH a store: a publish that was never
        // kept cannot be sent again, and the C does not pretend otherwise.
        let Some(keeper) = store.take() else {
            return Ok(());
        };

        let mut cursor = Cursor::new();

        loop {
            let next = match self.records.as_ref() {
                Some(records) => records.publish_to_resend(&mut cursor),
                None => None,
            };

            let Some(packet_id) = next else {
                break;
            };

            let Some(packet) = keeper.retrieve(u32::from(packet_id)) else {
                return Err(ClientError::PublishRetrieveFailed);
            };

            if packet.len() > crate::context::MAX_PACKET_SIZE as usize {
                return Err(ClientError::BadParameter);
            }

            let wanted = packet.len();

            match self.send_buffer(transport, clock, packet) {
                SendOutcome::Sent(sent) if sent == wanted => {}
                _ => return Err(ClientError::SendFailed),
            }
        }

        Ok(())
    }

    /// `getAckTypeToSend`: the packet TYPE BYTE a state owes, or zero.
    ///
    /// Four of the eleven states owe an acknowledgement; the rest are either
    /// waiting for one or finished, and the C's `default` is an empty branch
    /// rather than a refusal — so "owes nothing" comes back as a zero byte,
    /// which is not a packet type.
    ///
    /// It would be shorter here to answer an [`AckType`] and skip
    /// [`ack_from_packet_type`] entirely. The C does not: it converts to a byte
    /// and straight back, because the byte is what goes on the wire and the
    /// type is what the state machine wants. Both are kept, because a
    /// transcription that fuses them is no longer a transcription of either.
    #[must_use]
    pub const fn ack_type_to_send(state: PublishState) -> u8 {
        match state {
            PublishState::PubAckSend => crate::header::packet::PUBACK,
            PublishState::PubRecSend => crate::header::packet::PUBREC,
            PublishState::PubRelSend => crate::header::packet::PUBREL,
            PublishState::PubCompSend => crate::header::packet::PUBCOMP,
            _ => 0,
        }
    }

    /// `sendPublishAcks`: one four-byte acknowledgement, and the state change
    /// it earns.
    ///
    /// A state that owes nothing is **success with no packet** — the C's
    /// `packetTypeByte != 0U` wraps the whole body, so a caller that asks for
    /// an acknowledgement to a state that has none gets `MQTTSuccess` and an
    /// untouched transport.
    ///
    /// # Errors
    ///
    /// The connection's refusals, [`ClientError::SendFailed`], or
    /// [`ClientError::State`] if the state machine would not make the move.
    pub fn send_publish_acks<T: Transport, C: Clock>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        packet_id: u16,
        state: PublishState,
    ) -> Result<(), ClientError> {
        let packet_type = Self::ack_type_to_send(state);

        if packet_type == 0 {
            return Ok(());
        }

        let Some(ack) = ack_from_packet_type(packet_type) else {
            return Ok(());
        };

        let mut packet = [0u8; crate::outbound::PUBLISH_ACK_PACKET_SIZE];
        let written =
            crate::outbound::serialize_ack(&mut packet, packet_type, packet_id, None, &[])
                .map_err(|_| ClientError::BadParameter)?;

        self.connected()?;

        let Some(bytes) = packet.get(..written) else {
            return Err(ClientError::SendFailed);
        };

        match self.send_buffer(transport, clock, bytes) {
            SendOutcome::Sent(sent) if sent == written => {}
            _ => return Err(ClientError::SendFailed),
        }

        self.control_packet_sent = true;

        let Some(records) = self.records.as_mut() else {
            return Ok(());
        };

        let _ = records
            .update_ack(packet_id, ack, Operation::Send)
            .map_err(ClientError::State)?;

        Ok(())
    }

    /// `MQTT_Connect`: open the session.
    ///
    /// # Errors
    ///
    /// [`ClientError::BadParameter`] from the validators or the size
    /// calculator, [`ClientError::StatusConnected`] or
    /// [`ClientError::DisconnectPending`] if there is already a connection,
    /// [`ClientError::SendFailed`], [`ClientError::RecvFailed`],
    /// [`ClientError::BadResponse`] or [`ClientError::ServerRefused`].
    /// # `session_present` is written even when this refuses
    ///
    /// The C's is an out-parameter that `MQTT_DeserializeConnAck` fills in
    /// **before** the clean-session check can reject the packet, so a caller
    /// that asked for a clean session and got a resumed one is handed
    /// `MQTTBadResponse` *and* a `true` flag. A `Result<bool, _>` would lose
    /// that, so the flag stays a parameter — which is the honest shape, because
    /// the C's really does survive the failure. `clean-but-resumed` in the
    /// trace is the case that says so.
    pub fn connect<T: Transport, C: Clock, S: Store>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        store: Option<&mut S>,
        connect: &Connect<'_>,
        timeout_ms: u32,
        session_present: &mut bool,
    ) -> Result<(), ClientError> {
        if let Some(will) = connect.will.as_ref() {
            if !will.properties.is_empty() {
                crate::validate::validate_will_properties(will.properties)?;
            }
        }

        // A CONNECT with no properties of its own does NOT go out with an empty
        // property section: coreMQTT builds one containing a single Maximum
        // Packet Size, set to the size of the network buffer, on the grounds
        // that "otherwise the server can send a bigger packet which cannot be
        // processed by the coreMQTT library". Five bytes the application never
        // asked for, and they are on the wire.
        let mut invented = [0u8; 5];
        let network_size = u32::try_from(self.network.len()).unwrap_or(u32::MAX);

        if let Some(slot) = invented.first_mut() {
            *slot = crate::validate::id::MAX_PACKET_SIZE;
        }

        if let Some(slot) = invented.get_mut(1..5) {
            slot.copy_from_slice(&network_size.to_be_bytes());
        }

        if connect.properties.is_empty() {
            self.properties.client.max_packet_size = network_size;
        } else {
            let mut found = crate::validate::ConnectValidation::default();

            crate::validate::validate_connect_properties(connect.properties, &mut found)?;

            self.properties.client.request_problem_info = found.request_problem_info;

            // The buffer has to be able to hold what the client told the broker
            // it could take.
            if let Some(max) = found.max_packet_size {
                if max as usize > self.network.len() {
                    return Err(ClientError::BadParameter);
                }
            }
        }

        let outgoing = if connect.properties.is_empty() {
            Connect {
                properties: invented.as_slice(),
                ..*connect
            }
        } else {
            *connect
        };
        let connect = &outgoing;

        let size = crate::connect::connect_packet_size(connect).map_err(ClientError::from)?;

        match self.connect_status {
            ConnectionStatus::NotConnected => {}
            ConnectionStatus::Connected => return Err(ClientError::StatusConnected),
            ConnectionStatus::DisconnectPending => return Err(ClientError::DisconnectPending),
        }

        let outcome = self.connect_inner(
            transport,
            clock,
            store,
            connect,
            timeout_ms,
            size,
            session_present,
        );

        if outcome.is_err() && self.connect_status == ConnectionStatus::Connected {
            // "we need to retry the re-transmits which can only be done using
            // the connect API and that can only be done once we are
            // disconnected, hence we ask the user to call disconnect here"
            self.connect_status = ConnectionStatus::DisconnectPending;
        }

        outcome
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the C's MQTT_Connect takes seven and this adds the store;                   splitting them would hide the order the refusals happen in"
    )]
    fn connect_inner<T: Transport, C: Clock, S: Store>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        mut store: Option<&mut S>,
        connect: &Connect<'_>,
        timeout_ms: u32,
        size: crate::connect::ConnectSize,
        session_present: &mut bool,
    ) -> Result<(), ClientError> {
        self.send_connect(transport, clock, connect, size.remaining_length)?;

        self.receive_connack(
            transport,
            clock,
            timeout_ms,
            connect.info.clean_session,
            session_present,
        )?;

        let session_present = *session_present;

        // MQTT 5's Receive Maximum caps BOTH record arrays, from opposite
        // ends: the client's own announcement caps the INCOMING ones and the
        // broker's caps the OUTGOING. Neither can raise the other's.
        if let Some(records) = self.records.as_mut() {
            records.cap(
                usize::from(self.properties.server.receive_max),
                usize::from(self.properties.client.receive_max),
            );
        }

        if !session_present {
            self.handle_clean_session(store.as_deref_mut());
        }

        self.connect_status = ConnectionStatus::Connected;
        self.keep_alive_seconds = self.properties.server.keep_alive;
        self.waiting_for_ping_resp = false;
        self.ping_req_send_time = 0;

        if session_present {
            self.handle_unclean_session(transport, clock, store)?;
        }

        Ok(())
    }
}

/// A length field and its body, as one vector or two.
///
/// `addEncodedStringToVector` again — the length always, the bytes only when
/// there are any. The CONNECT's version is wrapped in
/// `addStringToVectorChecked`, whose extra job is the packet-size check that
/// [`connect_packet_size`](crate::connect::connect_packet_size) has already
/// done for the whole packet.
fn push_string<'b>(
    parts: &mut [&'b [u8]; 15],
    mut count: usize,
    total: &mut usize,
    field: &'b [u8; 2],
    body: &'b [u8],
) -> Result<usize, ClientError> {
    // `addStringToVectorChecked`'s whole contribution over
    // `addEncodedStringToVector`: the running total, plus this string's length
    // field and body, must still fit MQTT's maximum packet. It cannot fire at
    // any size this differential can reach — the network buffer is smaller than
    // the limit by four orders of magnitude — so it is transcribed rather than
    // proven, and no poison on it can fail.
    let after = total.saturating_add(2).saturating_add(body.len());

    if after > crate::context::MAX_PACKET_SIZE as usize {
        return Err(ClientError::BadParameter);
    }

    if let Some(slot) = parts.get_mut(count) {
        *slot = field.as_slice();
        count = count.saturating_add(1);
        *total = total.saturating_add(2);
    }

    if !body.is_empty() {
        if let Some(slot) = parts.get_mut(count) {
            *slot = body;
            count = count.saturating_add(1);
            *total = total.saturating_add(body.len());
        }
    }

    Ok(count)
}

/// Put a CONNACK's properties into the session, leaving the absent ones alone.
///
/// # Two structs with the same nine fields that are NOT the same type
///
/// `ServerSettings` — what the packet said — and
/// [`ServerLimits`](crate::context::ServerLimits) — what the session runs under
/// — have field for field the same shape, and merging them looks like the
/// twenty-second shape of the guard. It is not, and trying it is how this was
/// found: **their zero means different things.** An absent Maximum QoS is 2 in
/// a session and 0 in a packet, so assigning one to the other would silently
/// cap every publish at QoS 0 whenever a broker left the property out.
///
/// The C does not have this problem and does not have this function, because
/// `MQTT_DeserializeConnAck` writes STRAIGHT INTO the context that
/// `MQTT_InitConnect` already filled with the defaults — an absent property is
/// a field it never touches. This is that, written out.
fn apply_connack(properties: &mut crate::context::ConnectionProperties, connack: &ConnAck<'_>) {
    let present = |bit: u32| (connack.fields_present & (1_u32 << bit)) != 0;
    let server: &mut ServerLimits = &mut properties.server;

    if present(field::SESSION_EXPIRY_INTERVAL) {
        properties.session_expiry = connack.server.session_expiry;
    }

    if present(field::RECEIVE_MAXIMUM) {
        server.receive_max = connack.server.receive_max;
    }

    if present(field::MAX_QOS) {
        server.max_qos = connack.server.max_qos;
    }

    if present(field::RETAIN_AVAILABLE) {
        server.retain_available = connack.server.retain_available;
    }

    if present(field::MAX_PACKET_SIZE) {
        server.max_packet_size = connack.server.max_packet_size;
    }

    if present(field::TOPIC_ALIAS_MAX) {
        server.topic_alias_max = connack.server.topic_alias_max;
    }

    if present(field::WILDCARD_SUBSCRIPTION_AVAILABLE) {
        server.wildcard_available = connack.server.wildcard_available;
    }

    if present(field::SUBSCRIPTION_ID_AVAILABLE) {
        server.subscription_id_available = connack.server.subscription_id_available;
    }

    if present(field::SHARED_SUBSCRIPTION_AVAILABLE) {
        server.shared_available = connack.server.shared_available;
    }

    if present(field::SERVER_KEEP_ALIVE) {
        server.keep_alive = connack.server.keep_alive;
    }
}

/// `getAckFromPacketType`: the type byte back to the state machine's name for
/// it.
///
/// The C asserts on anything else, having just produced the byte itself two
/// lines earlier; here the impossible case is a `None` the caller returns
/// success for, which is what an assertion in a library that must not panic
/// comes to.
#[must_use]
pub const fn ack_from_packet_type(packet_type: u8) -> Option<AckType> {
    match packet_type {
        crate::header::packet::PUBACK => Some(AckType::PubAck),
        crate::header::packet::PUBREC => Some(AckType::PubRec),
        crate::header::packet::PUBREL => Some(AckType::PubRel),
        crate::header::packet::PUBCOMP => Some(AckType::PubComp),
        _ => None,
    }
}

const _: () = assert!(RECV_POLLING_TIMEOUT_MS > 0);
