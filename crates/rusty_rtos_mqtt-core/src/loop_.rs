//! The receive loop: one pass over whatever the transport has, and the
//! acknowledgements that come out of it.
//!
//! [`session`](crate::session) gets a connection open. This is what a connected
//! client does for the rest of its life — and it is the only part of the
//! library that **reassembles**: a read can deliver half a packet, two packets,
//! or a packet and a half, and the buffer has to carry the remainder into the
//! next call.
//!
//! # The index is the whole design
//!
//! `receiveSingleIteration` reads into `buffer[index..]`, adds what arrived to
//! `index`, and then asks whether a whole packet is there yet. When one is, it
//! is handled and then **the rest is moved to the front** and `index` drops by
//! the packet's length:
//!
//! ```text
//! index == 0 and nothing read   NoDataAvailable, reset to success at the end
//! index  > 0 and not enough     NeedMoreBytes, and the bytes stay put
//! index  > 0 and enough         handle it, slide the tail down, go round again
//! ```
//!
//! The loop's condition is `index > 0 && status == Success`, so **two packets
//! in one read are both handled in one call**, and the second one's failure is
//! what the caller sees.
//!
//! # Keep alive is checked only when nothing arrived
//!
//! The keep-alive check hangs off `recvBytes == 0`, not off the clock — so a
//! busy connection never pings however long it has been sending, and an idle
//! one is checked on every pass. Its own status is then **discarded on
//! success**: the `NoDataAvailable` or `NeedMoreBytes` the iteration had is put
//! back, because sending a PINGREQ is not news.
//!
//! And it runs on the SECOND and later turns of the same call, because the C
//! sets `recvBytes = 0` on any turn that did have data. So a call that handles
//! two buffered packets checks keep alive once, between them.

use crate::ack::{AckInfo, Limits, PacketInfo};
use crate::client::{
    AckReply, ClientError, Clock, ConnectionStatus, Event, EventHandler, MqttContext, SendOutcome,
    Store, elapsed_ms, incoming_key,
};
use crate::header::packet;
use crate::reader::{Recv, Transport};
use crate::state::{AckType, Operation, PublishState, QoS};

/// `PACKET_TX_TIMEOUT_MS`: the longest a connection may go without sending.
pub const PACKET_TX_TIMEOUT_MS: u32 = 30_000;

/// `PACKET_RX_TIMEOUT_MS`: the longest it may go without receiving.
pub const PACKET_RX_TIMEOUT_MS: u32 = 30_000;

/// `MQTT_PINGRESP_TIMEOUT_MS`: how long a PINGRESP may take.
pub const PINGRESP_TIMEOUT_MS: u32 = 5_000;

/// What an acknowledgement the library is about to send should carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Reply {
    reason_code: Option<u8>,
    property_length: usize,
}

impl<'a> MqttContext<'a> {
    /// `MQTT_ProcessLoop`: one pass, with keep alive managed.
    ///
    /// # Errors
    ///
    /// Everything the handlers can answer. [`ClientError::NoDataAvailable`] is
    /// **not** one of them: the C resets it to success at the bottom, because
    /// "no data available is not an error".
    pub fn process_loop<T: Transport, C: Clock, S: Store, H: EventHandler>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        store: Option<&mut S>,
        handler: &mut H,
    ) -> Result<(), ClientError> {
        self.control_packet_sent = false;
        self.receive_single_iteration(transport, clock, store, handler, true)
    }

    /// `MQTT_ReceiveLoop`: one pass, with keep alive left to the caller.
    ///
    /// The difference is not only the PINGREQ. A PINGRESP that arrives here is
    /// handed to the **application** instead of being swallowed, because a
    /// caller managing its own keep alive is the one that needs to see it.
    ///
    /// # Errors
    ///
    /// As [`process_loop`](Self::process_loop).
    pub fn receive_loop<T: Transport, C: Clock, S: Store, H: EventHandler>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        store: Option<&mut S>,
        handler: &mut H,
    ) -> Result<(), ClientError> {
        self.receive_single_iteration(transport, clock, store, handler, false)
    }

    /// `receiveSingleIteration`.
    fn receive_single_iteration<T: Transport, C: Clock, S: Store, H: EventHandler>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        mut store: Option<&mut S>,
        handler: &mut H,
        manage_keep_alive: bool,
    ) -> Result<(), ClientError> {
        // ONE read, before the loop. Everything after this works out of the
        // buffer, which is why two packets in one read are both handled.
        let mut received = match self.network.get_mut(self.index..) {
            Some(into) => transport.recv(into),
            None => Recv::Failed,
        };

        loop {
            let mut status: Result<(), ClientError> = Ok(());
            let mut total = 0usize;

            match received {
                Recv::Failed => {
                    if self.connect_status == ConnectionStatus::Connected {
                        self.connect_status = ConnectionStatus::DisconnectPending;
                    }

                    status = Err(ClientError::RecvFailed);
                }

                Recv::Nothing | Recv::Bytes(0) if self.index == 0 => {
                    status = Err(ClientError::NoDataAvailable);
                }

                _ => {
                    if let Recv::Bytes(count) = received {
                        self.index = self.index.saturating_add(count);
                    }

                    match self.measure() {
                        Ok(length) => total = length,
                        Err(error) => status = Err(error),
                    }
                }
            }

            // The keep-alive check hangs off "nothing arrived", not off the
            // clock. Note the `else`: any turn that DID have data sets the
            // count to zero, so the next turn of the same call checks it.
            if matches!(received, Recv::Bytes(0) | Recv::Nothing) {
                if manage_keep_alive
                    && matches!(
                        status,
                        Ok(())
                            | Err(ClientError::NoDataAvailable)
                            | Err(ClientError::NeedMoreBytes)
                    )
                {
                    let kept = status;

                    status = match self.handle_keep_alive(transport, clock) {
                        Ok(()) => kept,
                        Err(error) => Err(error),
                    };
                }
            } else {
                received = Recv::Bytes(0);
            }

            // The two size questions, which the C asks AFTER the keep alive —
            // so a packet too large for the buffer still gets a PINGREQ out
            // before the connection is given up on.
            if status.is_ok() {
                if total > self.network.len() {
                    status = Err(ClientError::RecvFailed);
                } else if total > self.index {
                    status = Err(ClientError::NeedMoreBytes);
                }
            }

            if status.is_ok() {
                status = self.handle_packet(
                    transport,
                    clock,
                    store.as_deref_mut(),
                    handler,
                    total,
                    manage_keep_alive,
                );

                if status.is_ok() {
                    self.consume(total);
                    self.last_packet_rx_time = clock.now_ms();
                }
            }

            if let Err(error) = status {
                return finish(Err(error));
            }

            if self.index == 0 {
                break;
            }
        }

        finish(Ok(()))
    }

    /// `MQTT_ProcessIncomingPacketTypeAndLength`: how long the packet at the
    /// front of the buffer claims to be.
    fn measure(&self) -> Result<usize, ClientError> {
        let header =
            crate::header::process_incoming_packet_type_and_length(self.network, self.index)
                .map_err(ClientError::from)?;

        Ok((header.remaining_length as usize).saturating_add(header.header_length))
    }

    /// Route one whole packet.
    fn handle_packet<T: Transport, C: Clock, S: Store, H: EventHandler>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        store: Option<&mut S>,
        handler: &mut H,
        total: usize,
        manage_keep_alive: bool,
    ) -> Result<(), ClientError> {
        let packet_type = match self.network.first() {
            Some(byte) => *byte,
            None => return Err(ClientError::RecvFailed),
        };

        // "PUBLISH packets allow flags in the lower four bits. For other packet
        // types, they are reserved."
        if packet_type & 0xF0 == packet::PUBLISH {
            return self.handle_incoming_publish(transport, clock, store, handler, total);
        }

        if packet_type == packet::DISCONNECT {
            return self.handle_incoming_disconnect(transport, clock, handler, total);
        }

        self.handle_incoming_ack(transport, clock, store, handler, total, manage_keep_alive)
    }

    /// Drop `total` bytes off the front and slide the rest down.
    fn consume(&mut self, total: usize) {
        self.index = self.index.saturating_sub(total);
        self.network.copy_within(total.., 0);
    }

    /// `handleKeepAlive`.
    ///
    /// Three separate reasons to send a PINGREQ, and they are checked in an
    /// order that matters: an outstanding PINGRESP is a **timeout** rather than
    /// a reason to send another, a silent transmitter pings on the keep-alive
    /// interval, and a silent receiver pings after thirty seconds — but only if
    /// something has ever been received, because `timeElapsed != 0` treats a
    /// zero elapsed time as "not yet".
    fn handle_keep_alive<T: Transport, C: Clock>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
    ) -> Result<(), ClientError> {
        let now = clock.now_ms();
        let mut tx_timeout = 1000u32.saturating_mul(u32::from(self.keep_alive_seconds));

        if PACKET_TX_TIMEOUT_MS < tx_timeout {
            tx_timeout = PACKET_TX_TIMEOUT_MS;
        }

        if self.waiting_for_ping_resp {
            // Strictly greater, where the two below are `>=`.
            if elapsed_ms(now, self.ping_req_send_time) > PINGRESP_TIMEOUT_MS {
                return Err(ClientError::KeepAliveTimeout);
            }

            return Ok(());
        }

        if tx_timeout != 0 && elapsed_ms(now, self.last_packet_tx_time) >= tx_timeout {
            return self.ping(transport, clock);
        }

        let since_rx = elapsed_ms(now, self.last_packet_rx_time);

        if since_rx != 0 && since_rx >= PACKET_RX_TIMEOUT_MS {
            return self.ping(transport, clock);
        }

        Ok(())
    }
}

/// The C resets `MQTTNoDataAvailable` to success at the bottom, "so the return
/// code will indicate success".
fn finish(outcome: Result<(), ClientError>) -> Result<(), ClientError> {
    match outcome {
        Err(ClientError::NoDataAvailable) => Ok(()),
        other => other,
    }
}

/// `getAckFromPacketType`, re-exported where the loop needs it.
use crate::session::ack_from_packet_type;

impl<'a> MqttContext<'a> {
    /// `handleIncomingPublish`.
    fn handle_incoming_publish<T: Transport, C: Clock, S: Store, H: EventHandler>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        store: Option<&mut S>,
        handler: &mut H,
        total: usize,
    ) -> Result<(), ClientError> {
        // Everything the rest of the function needs, taken out of the buffer
        // before anything borrows it mutably.
        let (accepted, reason_code, qos, packet_id, ack_state, duplicate) = {
            let Some(view) = packet_view(self.network, self.index, total) else {
                return Err(ClientError::RecvFailed);
            };

            let publish = crate::publish::deserialize_publish(
                &view,
                self.properties.client.max_packet_size,
                self.properties.client.topic_alias_max,
            )
            .map_err(ClientError::from)?;

            let qos = publish.qos;
            let packet_id = publish.packet_id.unwrap_or(0);

            // "Dropping the incoming publish. Please call MQTT_InitStatefulQoS
            // to enable use of QoS1 and QoS2 publishes."
            if self.records.is_none() && qos != QoS::AtMostOnce {
                return Err(ClientError::RecvFailed);
            }

            let mut duplicate = false;
            let mut ack_state = PublishState::Null;

            if let Some(records) = self.records.as_mut() {
                match records.update_publish(packet_id, Operation::Receive, qos) {
                    Ok(state) => ack_state = state,

                    // A duplicate is not an error: the broker is re-sending
                    // because our acknowledgement did not arrive, and the state
                    // the ACK needs is computed rather than looked up.
                    Err(crate::state::StateError::StateCollision) => {
                        duplicate = true;
                        ack_state = crate::state::calculate_state_publish(Operation::Receive, qos);
                    }

                    Err(error) => return Err(ClientError::State(error)),
                }
            }

            let event = Event {
                packet_type: view.packet_type,
                packet_id,
                publish: Some(publish),
                reason_codes: &[],
                properties: publish.properties,
            };

            // "Even a duplicate QoS1 packet must be forwarded to the
            // application [MQTT-4.3.2-5]." A duplicate QoS 2 is not.
            let tell = !duplicate || qos == QoS::AtLeastOnce;
            let wants_reply = qos != QoS::AtMostOnce;

            let (accepted, reason_code) = if tell {
                dispatch(
                    handler,
                    &event,
                    wants_reply,
                    self.ack_properties,
                    &mut self.ack_used,
                )
            } else {
                (true, None)
            };

            (accepted, reason_code, qos, packet_id, ack_state, duplicate)
        };

        let _ = duplicate;

        if !accepted {
            return Err(ClientError::EventCallbackFailed);
        }

        if qos == QoS::AtMostOnce {
            return Ok(());
        }

        if self.ack_used >= crate::header::REMAINING_LENGTH_INVALID as usize {
            return Err(ClientError::SendFailed);
        }

        self.send_ack_for(
            transport,
            clock,
            store,
            packet_id,
            ack_state,
            Reply {
                reason_code,
                property_length: self.ack_used,
            },
        )
    }

    /// `handlePublishAcks`.
    fn handle_publish_acks<T: Transport, C: Clock, S: Store, H: EventHandler>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        mut store: Option<&mut S>,
        handler: &mut H,
        total: usize,
    ) -> Result<(), ClientError> {
        let (accepted, reason_code, packet_id, ack, ack_state) = {
            let Some(view) = packet_view(self.network, self.index, total) else {
                return Err(ClientError::RecvFailed);
            };

            let Some(ack) = ack_from_packet_type(view.packet_type) else {
                return Err(ClientError::BadResponse);
            };

            let limits = Limits {
                max_packet_size: self.properties.client.max_packet_size,
                request_problem_info: self.properties.client.request_problem_info,
            };

            let info: AckInfo<'_> =
                crate::ack::deserialize_ack(&view, &limits).map_err(ClientError::from)?;
            let packet_id = info.packet_id.unwrap_or(0);

            let mut ack_state = PublishState::Null;

            if let Some(records) = self.records.as_mut() {
                ack_state = records
                    .update_ack(packet_id, ack, Operation::Receive)
                    .map_err(ClientError::State)?;
            }

            // What the acknowledgement releases from the store, and under which
            // key. A PUBCOMP and a PUBREL clear the INCOMING-flagged one; a
            // PUBACK and a PUBREC clear the plain one.
            if let Some(keeper) = store.as_deref_mut() {
                match ack {
                    AckType::PubAck | AckType::PubRec => keeper.clear(u32::from(packet_id)),
                    AckType::PubComp | AckType::PubRel => keeper.clear(incoming_key(packet_id)),
                }
            }

            let event = Event {
                packet_type: view.packet_type,
                packet_id,
                publish: None,
                reason_codes: info.reason_codes,
                properties: info.properties,
            };

            // A PUBACK and a PUBCOMP end their sequences, so there is nothing
            // to reply to and the C hands the application two NULLs.
            let wants_reply = !matches!(ack, AckType::PubAck | AckType::PubComp);

            let (accepted, reason_code) = dispatch(
                handler,
                &event,
                wants_reply,
                self.ack_properties,
                &mut self.ack_used,
            );

            (accepted, reason_code, packet_id, ack, ack_state)
        };

        if !accepted {
            return Err(ClientError::EventCallbackFailed);
        }

        if self.ack_used >= crate::header::REMAINING_LENGTH_INVALID as usize {
            return Err(ClientError::SendFailed);
        }

        if matches!(ack, AckType::PubAck | AckType::PubComp) {
            return Ok(());
        }

        self.send_ack_for(
            transport,
            clock,
            store,
            packet_id,
            ack_state,
            Reply {
                reason_code,
                property_length: self.ack_used,
            },
        )
    }

    /// `handleIncomingAck`.
    fn handle_incoming_ack<T: Transport, C: Clock, S: Store, H: EventHandler>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        store: Option<&mut S>,
        handler: &mut H,
        total: usize,
        manage_keep_alive: bool,
    ) -> Result<(), ClientError> {
        let packet_type = match self.network.first() {
            Some(byte) => *byte,
            None => return Err(ClientError::RecvFailed),
        };

        match packet_type {
            packet::PUBACK | packet::PUBREC | packet::PUBREL | packet::PUBCOMP => {
                self.handle_publish_acks(transport, clock, store, handler, total)
            }

            packet::PINGRESP => {
                let (accepted, packet_id, packet_type) = {
                    let Some(view) = packet_view(self.network, self.index, total) else {
                        return Err(ClientError::RecvFailed);
                    };

                    let limits = Limits {
                        max_packet_size: self.properties.client.max_packet_size,
                        request_problem_info: self.properties.client.request_problem_info,
                    };

                    let info =
                        crate::ack::deserialize_ack(&view, &limits).map_err(ClientError::from)?;

                    // The one packet the two loops treat DIFFERENTLY: with keep
                    // alive managed the library swallows it, and without, the
                    // application is told.
                    if manage_keep_alive {
                        self.waiting_for_ping_resp = false;
                        return Ok(());
                    }

                    let packet_id = info.packet_id.unwrap_or(0);
                    let event = Event {
                        packet_type: view.packet_type,
                        packet_id,
                        publish: None,
                        reason_codes: &[],
                        properties: &[],
                    };

                    let (accepted, _) = dispatch(
                        handler,
                        &event,
                        false,
                        self.ack_properties,
                        &mut self.ack_used,
                    );

                    (accepted, packet_id, view.packet_type)
                };

                let _ = (packet_id, packet_type);

                if accepted {
                    Ok(())
                } else {
                    Err(ClientError::EventCallbackFailed)
                }
            }

            packet::SUBACK | packet::UNSUBACK => self.handle_sub_unsub_ack(handler, total),

            _ => Err(ClientError::BadResponse),
        }
    }

    /// `handleSubUnsubAck`.
    fn handle_sub_unsub_ack<H: EventHandler>(
        &mut self,
        handler: &mut H,
        total: usize,
    ) -> Result<(), ClientError> {
        let accepted = {
            let Some(view) = packet_view(self.network, self.index, total) else {
                return Err(ClientError::RecvFailed);
            };

            let limits = Limits {
                max_packet_size: self.properties.client.max_packet_size,
                request_problem_info: self.properties.client.request_problem_info,
            };

            let info = crate::ack::deserialize_ack(&view, &limits).map_err(ClientError::from)?;

            let event = Event {
                packet_type: view.packet_type,
                packet_id: info.packet_id.unwrap_or(0),
                publish: None,
                reason_codes: info.reason_codes,
                properties: info.properties,
            };

            let (accepted, _) = dispatch(
                handler,
                &event,
                false,
                self.ack_properties,
                &mut self.ack_used,
            );

            accepted
        };

        if accepted {
            Ok(())
        } else {
            Err(ClientError::EventCallbackFailed)
        }
    }

    /// `handleIncomingDisconnect`, and what `receiveSingleIteration` does
    /// around it.
    ///
    /// **The connection is dropped whatever happens.** A malformed DISCONNECT
    /// gets one sent back, a well-formed one does not, the callback's refusal
    /// gets neither — and all three end with `connectStatus = NotConnected`,
    /// outside every branch.
    fn handle_incoming_disconnect<T: Transport, C: Clock, H: EventHandler>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        handler: &mut H,
        total: usize,
    ) -> Result<(), ClientError> {
        let outcome = {
            let Some(view) = packet_view(self.network, self.index, total) else {
                return Err(ClientError::RecvFailed);
            };

            match crate::disconnect::deserialize_disconnect(
                &view,
                self.properties.client.max_packet_size,
            ) {
                Err(error) => Err(ClientError::from(error)),

                Ok(incoming) => {
                    let reason_byte = match incoming.reason_code {
                        Some(code) => [code],
                        None => [0u8; 1],
                    };
                    let reason_byte = if incoming.reason_code.is_some() {
                        &reason_byte[..]
                    } else {
                        &reason_byte[..0]
                    };

                    let event = Event {
                        packet_type: view.packet_type,
                        packet_id: 0,
                        publish: None,
                        // The C hands the callback a `MQTTReasonCodeInfo_t`
                        // pointing at one byte, or at nothing when the packet
                        // had no body at all.
                        reason_codes: reason_byte,
                        properties: incoming.properties,
                    };

                    let (accepted, _) = dispatch(
                        handler,
                        &event,
                        false,
                        self.ack_properties,
                        &mut self.ack_used,
                    );

                    if accepted {
                        Ok(())
                    } else {
                        Err(ClientError::EventCallbackFailed)
                    }
                }
            }
        };

        let status = match outcome {
            Ok(()) => Ok(()),

            Err(ClientError::EventCallbackFailed) => Err(ClientError::EventCallbackFailed),

            // "Incoming packet is malformed at this stage" — so the client
            // answers with a DISCONNECT of its own carrying reason code 0x81,
            // and reports NotConnected if that went out.
            Err(_) => {
                match self.disconnect(
                    transport,
                    clock,
                    Some(crate::disconnect::reason::MALFORMED_PACKET),
                    &[],
                ) {
                    Ok(()) => Err(ClientError::NotConnected),
                    Err(error) => Err(error),
                }
            }
        };

        self.connect_status = ConnectionStatus::NotConnected;

        status
    }

    /// The two ack senders, which differ by whether the application said
    /// anything.
    ///
    /// `sendPublishAcksWithoutProperty` and `sendPublishAcksWithProperty` are
    /// separate functions in the C and nearly the same one; the branch between
    /// them is exactly this: **did the application add a property or set a
    /// reason code**.
    fn send_ack_for<T: Transport, C: Clock, S: Store>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        store: Option<&mut S>,
        packet_id: u16,
        state: PublishState,
        reply: Reply,
    ) -> Result<(), ClientError> {
        if reply.property_length == 0 && reply.reason_code.is_none() {
            self.send_publish_acks_without_property(transport, clock, store, packet_id, state)
        } else {
            self.send_publish_acks_with_property(transport, clock, store, packet_id, state, reply)
        }
    }

    /// `sendPublishAcksWithoutProperty`: four bytes, and the store.
    fn send_publish_acks_without_property<T: Transport, C: Clock, S: Store>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        store: Option<&mut S>,
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

        // The check `sendPublishAcks` does not make: four bytes against the
        // broker's maximum packet size.
        if crate::outbound::PUBLISH_ACK_PACKET_SIZE as u32 > self.properties.server.max_packet_size
        {
            return Err(ClientError::BadParameter);
        }

        self.connected()?;

        let Some(bytes) = packet.get(..written) else {
            return Err(ClientError::SendFailed);
        };

        // A PUBREC and a PUBREL are kept, because the broker will ask again. A
        // PUBACK and a PUBCOMP end their sequences and are not.
        if packet_type != packet::PUBACK && packet_type != packet::PUBCOMP {
            if let Some(keeper) = store {
                if !keeper.store(incoming_key(packet_id), &[bytes]) {
                    return Err(ClientError::PublishStoreFailed);
                }
            }
        }

        match self.send_buffer(transport, clock, bytes) {
            SendOutcome::Sent(sent) if sent == written => {}
            _ => return Err(ClientError::SendFailed),
        }

        self.control_packet_sent = true;

        if let Some(records) = self.records.as_mut() {
            let _ = records
                .update_ack(packet_id, ack, Operation::Send)
                .map_err(ClientError::State)?;
        }

        Ok(())
    }

    /// `sendPublishAcksWithProperty` and the `buildAndSendAckWithProps` it
    /// drives.
    ///
    /// # An acknowledgement with properties and no reason code is refused
    ///
    /// The C's reason code is an enumeration with a sentinel,
    /// `MQTT_INVALID_REASON_CODE = 0xFF`, meaning "the application did not set
    /// one" — and `validatePublishAckReasonCode`'s switch has no case for it,
    /// so it falls to the default and answers `MQTTBadParameter`. An
    /// application that adds a User Property or a Reason String to a PUBACK
    /// **and does not also set a reason code** therefore gets no
    /// acknowledgement sent at all, and the handshake is left where it stood.
    ///
    /// `qos1-with-property` in the trace is exactly that: one property added,
    /// no reason code, `BadParameter`, nothing on the wire. Reproduced, and
    /// drafted for upstream.
    fn send_publish_acks_with_property<T: Transport, C: Clock, S: Store>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        store: Option<&mut S>,
        packet_id: u16,
        state: PublishState,
        reply: Reply,
    ) -> Result<(), ClientError> {
        let packet_type = Self::ack_type_to_send(state);

        if packet_type != 0 {
            if reply.property_length > 0 {
                let Some(properties) = self.ack_properties.get(..reply.property_length) else {
                    return Err(ClientError::BadParameter);
                };

                crate::validate::validate_publish_ack_properties(properties)?;
            }

            validate_publish_ack_reason_code(reply.reason_code, packet_type)?;

            crate::size::ack_packet_size(
                self.properties.server.max_packet_size,
                reply.property_length,
            )
            .map_err(|_| ClientError::BadParameter)?;
        }

        self.connected()?;

        if packet_type == 0 {
            return Ok(());
        }

        let Some(ack) = ack_from_packet_type(packet_type) else {
            return Ok(());
        };

        let size = crate::size::ack_packet_size(
            self.properties.server.max_packet_size,
            reply.property_length,
        )
        .map_err(|_| ClientError::BadParameter)?;

        self.build_and_send_ack_with_props(
            transport,
            clock,
            store,
            packet_type,
            packet_id,
            reply.reason_code.unwrap_or(0),
            size.remaining_length,
            reply.property_length,
        )?;

        self.control_packet_sent = true;

        if let Some(records) = self.records.as_mut() {
            let _ = records
                .update_ack(packet_id, ack, Operation::Send)
                .map_err(ClientError::State)?;
        }

        Ok(())
    }

    /// `buildAndSendAckWithProps`: the header, the property length, and the
    /// section — and **the buffer is emptied whether or not the send works**.
    #[allow(
        clippy::too_many_arguments,
        reason = "the C's takes six plus the context; splitting them would hide \
                  the order the refusals happen in"
    )]
    fn build_and_send_ack_with_props<T: Transport, C: Clock, S: Store>(
        &mut self,
        transport: &mut T,
        clock: &mut C,
        store: Option<&mut S>,
        packet_type: u8,
        packet_id: u16,
        reason_code: u8,
        remaining_length: u32,
        property_length: usize,
    ) -> Result<(), ClientError> {
        let mut header = [0u8; 8];
        let written = crate::writer::serialize_ack_fixed(
            &mut header,
            packet_type,
            packet_id,
            remaining_length,
            reason_code,
        );

        if written == 0 {
            return Err(ClientError::SendFailed);
        }

        let mut length_field = [0u8; 4];
        let length_written = crate::header::encode_variable_length(
            &mut length_field,
            u32::try_from(property_length).unwrap_or(0),
        );

        let Some(head) = header.get(..written) else {
            return Err(ClientError::SendFailed);
        };
        let Some(prefix) = length_field.get(..length_written) else {
            return Err(ClientError::SendFailed);
        };
        let count = if property_length == 0 { 2 } else { 3 };
        let total = written
            .saturating_add(length_written)
            .saturating_add(property_length);

        if property_length > 0 {
            // Emptied HERE, before the send -- so a send that fails has still
            // consumed whatever the application wrote, and the next
            // acknowledgement goes out bare.
            self.ack_used = 0;
        }

        if total > crate::context::MAX_PACKET_SIZE as usize {
            return Err(ClientError::BadParameter);
        }

        // The property section comes out of the context's own buffer, and the
        // sender wants the context. Taking the buffer out for the duration is
        // what a C pointer does for free; here it has to be said, and putting
        // it back is the one line that must not be forgotten.
        let ack_buffer = core::mem::take(&mut self.ack_properties);

        let outcome = (|context: &mut Self| -> Result<(), ClientError> {
            let Some(properties) = ack_buffer.get(..property_length) else {
                return Err(ClientError::SendFailed);
            };

            let mut parts: [&[u8]; 3] = [head, prefix, properties];

            let Some(gather) = parts.get(..count) else {
                return Err(ClientError::SendFailed);
            };

            if packet_type != packet::PUBACK && packet_type != packet::PUBCOMP {
                if let Some(keeper) = store {
                    if !keeper.store(incoming_key(packet_id), gather) {
                        return Err(ClientError::PublishStoreFailed);
                    }
                }
            }

            let Some(gather) = parts.get_mut(..count) else {
                return Err(ClientError::SendFailed);
            };

            match context.send_vectors(transport, clock, gather) {
                SendOutcome::Sent(sent) if sent == total => Ok(()),
                _ => Err(ClientError::SendFailed),
            }
        })(self);

        self.ack_properties = ack_buffer;

        outcome
    }
}

/// `validatePublishAckReasonCode`: which reason codes each acknowledgement may
/// carry.
///
/// Success is legal on all four. The eight failure codes are for the FIRST half
/// of a handshake — a PUBACK or a PUBREC — and `PacketIdentifierNotFound` is
/// for the second, a PUBREL or a PUBCOMP. Anything else is refused.
///
/// # Errors
///
/// [`ClientError::BadParameter`] for a code the type may not carry.
pub const fn validate_publish_ack_reason_code(
    reason_code: Option<u8>,
    packet_type: u8,
) -> Result<(), ClientError> {
    let Some(reason_code) = reason_code else {
        // `MQTT_INVALID_REASON_CODE` is 0xFF, and the switch below has no case
        // for it, so it falls to the default and is refused. See the note on
        // [`send_publish_acks_with_property`].
        return Err(ClientError::BadParameter);
    };

    match reason_code {
        0x00 => Ok(()),

        0x10 | 0x80 | 0x83 | 0x87 | 0x90 | 0x91 | 0x97 | 0x99 => {
            if packet_type == packet::PUBACK || packet_type == packet::PUBREC {
                Ok(())
            } else {
                Err(ClientError::BadParameter)
            }
        }

        0x92 => {
            if packet_type == packet::PUBREL || packet_type == packet::PUBCOMP {
                Ok(())
            } else {
                Err(ClientError::BadParameter)
            }
        }

        _ => Err(ClientError::BadParameter),
    }
}

/// The packet at the front of the buffer, as the deserializers want it.
fn packet_view(network: &[u8], index: usize, total: usize) -> Option<PacketInfo<'_>> {
    let header = crate::header::process_incoming_packet_type_and_length(network, index).ok()?;
    let body = network.get(header.header_length..total)?;

    Some(PacketInfo {
        packet_type: header.packet_type,
        remaining_length: header.remaining_length,
        remaining_data: body,
    })
}

/// Hand the application an event and collect whatever reply it makes.
///
/// A free function rather than a method, because the event borrows the network
/// buffer and the reply borrows the acknowledgement buffer — two different
/// fields of the same context, which only a function taking them separately can
/// hold at once.
fn dispatch<H: EventHandler>(
    handler: &mut H,
    event: &Event<'_>,
    wants_reply: bool,
    ack_properties: &mut [u8],
    ack_used: &mut usize,
) -> (bool, Option<u8>) {
    let mut reply = AckReply::new(wants_reply, ack_properties, *ack_used);
    let accepted = handler.on_event(event, &mut reply);

    *ack_used = reply.used();

    (accepted, reply.reason_code())
}

#[cfg(test)]
// A test asserts its own fixtures; the workspace's deny-by-default is written
// for library code.
#[allow(clippy::panic, clippy::arithmetic_side_effects)]
mod tests {
    use super::*;

    /// Three poisons this slice could not make fail, and the ONE reason all
    /// three are dead.
    ///
    /// | the poison | why it changes nothing |
    /// |---|---|
    /// | a QoS 0 publish is acknowledged | its state owes nothing |
    /// | a PUBACK gets an acknowledgement of its own | its state owes nothing |
    /// | a zero elapsed receive time counts as a timeout | zero is under the limit |
    ///
    /// The first two are the same fact twice: `receiveSingleIteration` guards
    /// both paths with an explicit `qos > QoS0` or `ackType != PUBACK`, and
    /// `getAckTypeToSend` would have answered **zero** for those states
    /// anyway — so the guard is belt-and-braces and removing it is invisible.
    /// The third is arithmetic: the C writes `timeElapsed != 0 && timeElapsed
    /// >= PACKET_RX_TIMEOUT_MS`, and the second half already excludes zero.
    ///
    /// None of the three is a workload gap, and no case can be added that
    /// would make them fire. They are pinned here instead, in terms of the
    /// states and the constant rather than in terms of a trace.
    #[test]
    fn three_poisons_are_dead_because_the_state_owes_nothing() {
        // The state a QoS 0 publish leaves, and the state a PUBACK leaves.
        for state in [
            PublishState::Null,
            PublishState::PublishDone,
            PublishState::PubAckPending,
            PublishState::PubRecPending,
            PublishState::PubCompPending,
            PublishState::PubRelPending,
        ] {
            assert_eq!(
                MqttContext::ack_type_to_send(state),
                0,
                "{state:?} must owe no acknowledgement"
            );
        }

        // And the four that do.
        for (state, expected) in [
            (PublishState::PubAckSend, packet::PUBACK),
            (PublishState::PubRecSend, packet::PUBREC),
            (PublishState::PubRelSend, packet::PUBREL),
            (PublishState::PubCompSend, packet::PUBCOMP),
        ] {
            assert_eq!(MqttContext::ack_type_to_send(state), expected);
        }

        // The third: zero is already below the limit, so the `!= 0` half of
        // the C's guard can never change the answer.
        const { assert!(PACKET_RX_TIMEOUT_MS > 0) };
    }

    /// The reason-code table is exactly MQTT 5.0 §3.4.2.1 split by direction.
    #[test]
    fn a_reason_code_belongs_to_one_half_of_the_handshake() {
        // Success everywhere.
        for packet_type in [
            packet::PUBACK,
            packet::PUBREC,
            packet::PUBREL,
            packet::PUBCOMP,
        ] {
            assert_eq!(
                validate_publish_ack_reason_code(Some(0), packet_type),
                Ok(())
            );
        }

        // The eight failures are for the first half only.
        for code in [0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99] {
            assert_eq!(
                validate_publish_ack_reason_code(Some(code), packet::PUBACK),
                Ok(())
            );
            assert_eq!(
                validate_publish_ack_reason_code(Some(code), packet::PUBREC),
                Ok(())
            );
            assert_eq!(
                validate_publish_ack_reason_code(Some(code), packet::PUBREL),
                Err(ClientError::BadParameter)
            );
        }

        // And `PacketIdentifierNotFound` for the second.
        assert_eq!(
            validate_publish_ack_reason_code(Some(0x92), packet::PUBREL),
            Ok(())
        );
        assert_eq!(
            validate_publish_ack_reason_code(Some(0x92), packet::PUBACK),
            Err(ClientError::BadParameter)
        );

        // An absent one is the C's 0xFF sentinel, which its switch has no case
        // for — so a property added without a reason code costs the whole
        // acknowledgement.
        assert_eq!(
            validate_publish_ack_reason_code(None, packet::PUBACK),
            Err(ClientError::BadParameter)
        );
    }
}
