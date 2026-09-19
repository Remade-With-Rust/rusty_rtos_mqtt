//! `core_mqtt_state.c` remade: the QoS publish state machine.
//!
//! MQTT's delivery guarantees are a state machine per in-flight message, and
//! this is it. QoS 1 is a two-step handshake (PUBLISH, PUBACK) and QoS 2 is a
//! four-step one (PUBLISH, PUBREC, PUBREL, PUBCOMP), in both directions, and
//! **all of it has to survive the connection dropping in the middle**. When a
//! session resumes, whatever was in flight is resent from these records.
//!
//! There are no bytes here and no transport. The C module includes nothing but
//! its own header, which is why it is the first slice of a 21,000-line library
//! to be remade: it is self-contained, it is where the hard correctness lives,
//! and it can be diffed exactly.
//!
//! # The order of the records is load-bearing
//!
//! MQTT 5.0 requires message ordering, so these arrays are not a free list.
//! A new record is **appended** after the last occupied slot, never dropped
//! into the first hole; when the array is full of holes it is **compacted**,
//! preserving relative order; and when a PUBREC arrives for an outgoing QoS 2
//! publish the record is deliberately **deleted and re-added at the end**, so
//! that the PUBRELs resend in the order the publishes went out.
//!
//! A transcription that produced every correct status while ordering the array
//! differently would pass a status-only test and resend a session's backlog out
//! of order. The differential compares both arrays after every operation.

/// `MQTT_PACKET_ID_INVALID`: zero is never a packet id.
pub const PACKET_ID_INVALID: u16 = 0;

/// `MQTTQoS_t`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QoS {
    /// At most once: fire and forget, and no record is kept.
    #[default]
    AtMostOnce,
    /// At least once: PUBLISH, PUBACK.
    AtLeastOnce,
    /// Exactly once: PUBLISH, PUBREC, PUBREL, PUBCOMP.
    ExactlyOnce,
}

/// `MQTTPublishState_t`: where one in-flight message has got to.
///
/// The `*Send` states mean "this side owes the other a packet"; the `*Pending`
/// states mean "this side is waiting for one".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PublishState {
    /// No record.
    #[default]
    Null,
    /// An outgoing PUBLISH has been reserved and not yet sent.
    PublishSend,
    /// A PUBACK is owed for a received QoS 1 PUBLISH.
    PubAckSend,
    /// A PUBREC is owed for a received QoS 2 PUBLISH.
    PubRecSend,
    /// A PUBREL is owed, having received a PUBREC.
    PubRelSend,
    /// A PUBCOMP is owed, having received a PUBREL.
    PubCompSend,
    /// Waiting for a PUBACK to an outgoing QoS 1 PUBLISH.
    PubAckPending,
    /// Waiting for a PUBREC to an outgoing QoS 2 PUBLISH.
    PubRecPending,
    /// Waiting for a PUBREL to an incoming QoS 2 PUBLISH.
    PubRelPending,
    /// Waiting for a PUBCOMP to an outgoing PUBREL.
    PubCompPending,
    /// The handshake finished; the record is about to go.
    PublishDone,
}

/// `MQTTPubAckType_t`: the four acknowledgement packets.
///
/// The C takes this as an `enum` it must bounds-check, because C enums accept
/// any integer. Here the type has four values and no fifth, so
/// `MQTTBadParameter` for an out-of-range packet type has no equivalent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckType {
    /// Acknowledges a QoS 1 PUBLISH.
    PubAck,
    /// Acknowledges receipt of a QoS 2 PUBLISH.
    PubRec,
    /// Releases a QoS 2 PUBLISH.
    PubRel,
    /// Completes a QoS 2 handshake.
    PubComp,
}

/// `MQTTStateOperation_t`: which way the packet is going.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// We are sending it.
    Send,
    /// We received it.
    Receive,
}

/// `MQTTPubAckInfo_t`: one in-flight message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Record {
    /// The packet id, or [`PACKET_ID_INVALID`] for an empty slot.
    pub packet_id: u16,
    /// The quality of service the PUBLISH was sent at.
    pub qos: QoS,
    /// Where the handshake has got to.
    pub state: PublishState,
}

impl Record {
    /// Is this slot free?
    const fn is_empty(&self) -> bool {
        self.packet_id == PACKET_ID_INVALID
    }
}

/// `MQTTStateCursor_t`: a position in the outgoing records, for resending.
///
/// Start one with [`Cursor::new`] and pass the same one to successive calls;
/// each returns the next match and advances past it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Cursor(usize);

impl Cursor {
    /// `MQTT_STATE_CURSOR_INITIALIZER`.
    #[must_use]
    pub const fn new() -> Self {
        Self(0)
    }

    /// How far through the records this cursor has got.
    #[must_use]
    pub const fn position(&self) -> usize {
        self.0
    }
}

/// Why a state operation was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateError {
    /// `MQTTBadParameter`: a zero packet id, a QoS that does not match the
    /// record, or no such record to remove.
    BadParameter,
    /// `MQTTNoMemory`: the record array is full.
    NoMemory,
    /// `MQTTStateCollision`: that packet id is already in flight.
    StateCollision,
    /// `MQTTIllegalState`: the transition this packet asks for is not one the
    /// current state allows.
    IllegalState,
    /// `MQTTBadResponse`: an acknowledgement arrived for a message there is no
    /// record of. Usually a duplicate, sometimes an attack.
    BadResponse,
}

// ---- the transition tables ----------------------------------------------

/// `validateTransitionPublish`.
///
/// The `*Pending` self-transitions are the resend cases: when a session is
/// reestablished an outgoing publish is sent again and its state does not move.
const fn validate_transition_publish(
    current: PublishState,
    new: PublishState,
    op: Operation,
    qos: QoS,
) -> bool {
    match current {
        // From nothing, only an incoming publish creates a record.
        PublishState::Null => matches!(
            (op, new),
            (Operation::Receive, PublishState::PubAckSend)
                | (Operation::Receive, PublishState::PubRecSend)
        ),
        // Every outgoing publish starts here, from the reserve.
        PublishState::PublishSend => match qos {
            QoS::AtLeastOnce => matches!(new, PublishState::PubAckPending),
            QoS::ExactlyOnce => matches!(new, PublishState::PubRecPending),
            // QoS 0 is handled before this is ever called.
            QoS::AtMostOnce => false,
        },
        // Resending an outgoing publish after a session is reestablished.
        PublishState::PubAckPending => matches!(new, PublishState::PubAckPending),
        PublishState::PubRecPending => matches!(new, PublishState::PubRecPending),
        _ => false,
    }
}

/// `validateTransitionAck`.
///
/// Four of these allow a transition to the **same** state, and each is a
/// reconnect story: a duplicate PUBLISH whose PUBREC never arrived, a duplicate
/// PUBREL whose PUBCOMP never arrived, a PUBREL being resent. They are not
/// slack in the table; removing any one of them breaks a real recovery path.
const fn validate_transition_ack(current: PublishState, new: PublishState) -> bool {
    match current {
        // Incoming QoS 1, and outgoing QoS 1.
        PublishState::PubAckSend | PublishState::PubAckPending => {
            matches!(new, PublishState::PublishDone)
        }
        // Incoming QoS 2: we sent a PUBREC, now we wait for the PUBREL.
        PublishState::PubRecSend => matches!(new, PublishState::PubRelPending),
        // ... and the broker may resend the PUBLISH if our PUBREC was lost.
        PublishState::PubRelPending => {
            matches!(new, PublishState::PubCompSend | PublishState::PubRelPending)
        }
        // ... and may resend the PUBREL if our PUBCOMP was lost.
        PublishState::PubCompSend => {
            matches!(new, PublishState::PublishDone | PublishState::PubCompSend)
        }
        // Outgoing QoS 2.
        PublishState::PubRecPending => matches!(new, PublishState::PubRelSend),
        PublishState::PubRelSend => matches!(new, PublishState::PubCompPending),
        // ... and we may resend the PUBREL if the PUBCOMP was lost.
        PublishState::PubCompPending => matches!(
            new,
            PublishState::PublishDone | PublishState::PubCompPending
        ),
        _ => false,
    }
}

/// `isPublishOutgoing`: which array holds the publish this ack belongs to.
const fn is_publish_outgoing(ack: AckType, op: Operation) -> bool {
    match ack {
        // We receive these for publishes WE sent.
        AckType::PubAck | AckType::PubRec | AckType::PubComp => {
            matches!(op, Operation::Receive)
        }
        // We send a PUBREL for a publish we sent; we receive one for a publish
        // the broker sent us.
        AckType::PubRel => matches!(op, Operation::Send),
    }
}

/// `MQTT_CalculateStatePublish`: where a PUBLISH puts the record.
#[must_use]
pub const fn calculate_state_publish(op: Operation, qos: QoS) -> PublishState {
    match qos {
        QoS::AtMostOnce => PublishState::PublishDone,
        QoS::AtLeastOnce => match op {
            Operation::Send => PublishState::PubAckPending,
            Operation::Receive => PublishState::PubAckSend,
        },
        QoS::ExactlyOnce => match op {
            Operation::Send => PublishState::PubRecPending,
            Operation::Receive => PublishState::PubRecSend,
        },
    }
}

/// `MQTT_CalculateStateAck`: where an acknowledgement puts the record.
///
/// Returns [`PublishState::Null`] when the acknowledgement and the QoS disagree
/// — a PUBACK for a QoS 2 message, say — which the caller then rejects as an
/// illegal transition. That is the C's own sanity check, kept rather than
/// turned into a separate error.
#[must_use]
pub const fn calculate_state_ack(ack: AckType, op: Operation, qos: QoS) -> PublishState {
    // There are more QoS 2 cases than QoS 1, so start from that.
    let mut qos_valid = matches!(qos, QoS::ExactlyOnce);

    let state = match ack {
        AckType::PubAck => {
            qos_valid = matches!(qos, QoS::AtLeastOnce);
            PublishState::PublishDone
        }
        // Incoming publish: we send the PUBREC and wait for a PUBREL.
        // Outgoing publish: we receive the PUBREC and owe a PUBREL.
        AckType::PubRec => match op {
            Operation::Send => PublishState::PubRelPending,
            Operation::Receive => PublishState::PubRelSend,
        },
        AckType::PubRel => match op {
            Operation::Send => PublishState::PubCompPending,
            Operation::Receive => PublishState::PubCompSend,
        },
        AckType::PubComp => PublishState::PublishDone,
    };

    if qos_valid { state } else { PublishState::Null }
}

// ---- the record array ----------------------------------------------------

/// `compactRecords`: slide the occupied slots down, keeping their order.
fn compact(records: &mut [Record]) {
    let mut empty: Option<usize> = None;

    for index in 0..records.len() {
        let Some(record) = records.get(index).copied() else {
            break;
        };

        if record.is_empty() {
            if empty.is_none() {
                empty = Some(index);
            }
        } else if let Some(at) = empty {
            if let Some(slot) = records.get_mut(at) {
                *slot = record;
            }
            if let Some(slot) = records.get_mut(index) {
                *slot = Record::default();
            }
            empty = Some(at.saturating_add(1));
        }
    }
}

/// `addRecord`: append after the last occupied slot, compacting if need be.
///
/// The search runs from the END backwards and only accepts a free slot while
/// no occupied one has been seen yet — so a hole in the middle is never used.
/// That is what keeps the array in send order.
fn add_record(
    records: &mut [Record],
    packet_id: u16,
    qos: QoS,
    state: PublishState,
) -> Result<(), StateError> {
    let count = records.len();

    // Compaction is needed only when the last slot is taken, because that is
    // the only way an append can have nowhere to go.
    if records.last().is_some_and(|r| !r.is_empty()) {
        compact(records);
    }

    let mut available = count;
    let mut valid_entry_found = false;

    for index in (0..count).rev() {
        let Some(record) = records.get(index) else {
            continue;
        };

        if record.is_empty() {
            if !valid_entry_found {
                available = index;
            }
        } else {
            valid_entry_found = true;

            if record.packet_id == packet_id {
                return Err(StateError::StateCollision);
            }
        }
    }

    let Some(slot) = records.get_mut(available) else {
        return Err(StateError::NoMemory);
    };

    *slot = Record {
        packet_id,
        qos,
        state,
    };
    Ok(())
}

/// `findInRecord`.
fn find_in_record(records: &[Record], packet_id: u16) -> Option<(usize, QoS, PublishState)> {
    records
        .iter()
        .position(|r| r.packet_id == packet_id)
        .and_then(|index| {
            records
                .get(index)
                .map(|record| (index, record.qos, record.state))
        })
}

/// `updateRecord`.
fn update_record(records: &mut [Record], index: usize, new: PublishState, delete: bool) {
    if let Some(slot) = records.get_mut(index) {
        if delete {
            *slot = Record::default();
        } else {
            slot.state = new;
        }
    }
}

/// `stateSelect`: the next outgoing record in any of `wanted`, advancing past it.
fn state_select(records: &[Record], wanted: &[PublishState], cursor: &mut Cursor) -> u16 {
    while cursor.0 < records.len() {
        let found = records
            .get(cursor.0)
            .filter(|r| wanted.contains(&r.state))
            .map(|r| r.packet_id);

        cursor.0 = cursor.0.saturating_add(1);

        if let Some(packet_id) = found {
            return packet_id;
        }
    }

    PACKET_ID_INVALID
}

/// The two arrays of in-flight messages that make up an MQTT session's state.
///
/// Borrowed rather than owned, as the C's are: the application sizes them, and
/// on a device they are usually two `static` arrays. The sizes are independent
/// — a device that subscribes heavily and publishes rarely wants a big incoming
/// array and a small outgoing one.
#[derive(Debug)]
pub struct PublishRecords<'a> {
    outgoing: &'a mut [Record],
    incoming: &'a mut [Record],
}

impl<'a> PublishRecords<'a> {
    /// Take the two arrays. They are cleared, so a resumed session starts from
    /// whatever the caller puts in them afterwards rather than from stale data.
    #[must_use]
    pub fn new(outgoing: &'a mut [Record], incoming: &'a mut [Record]) -> Self {
        outgoing.fill(Record::default());
        incoming.fill(Record::default());
        Self { outgoing, incoming }
    }

    /// The outgoing records, in send order.
    #[must_use]
    pub fn outgoing(&self) -> &[Record] {
        self.outgoing
    }

    /// The incoming records, in arrival order.
    #[must_use]
    pub fn incoming(&self) -> &[Record] {
        self.incoming
    }

    /// `MQTT_ReserveState`: claim a packet id before sending a PUBLISH.
    ///
    /// QoS 0 keeps no record and always succeeds.
    ///
    /// # Errors
    ///
    /// [`StateError::BadParameter`] for a zero packet id,
    /// [`StateError::StateCollision`] if that id is already in flight, and
    /// [`StateError::NoMemory`] if the outgoing array is full.
    pub fn reserve(&mut self, packet_id: u16, qos: QoS) -> Result<(), StateError> {
        if matches!(qos, QoS::AtMostOnce) {
            return Ok(());
        }

        if packet_id == PACKET_ID_INVALID {
            return Err(StateError::BadParameter);
        }

        add_record(self.outgoing, packet_id, qos, PublishState::PublishSend)
    }

    /// `MQTT_UpdateStatePublish`: a PUBLISH is being sent or has arrived.
    ///
    /// For a send the record must already exist from [`reserve`](Self::reserve)
    /// and its QoS must match. For a receive the record is created here.
    ///
    /// # Errors
    ///
    /// See [`StateError`].
    pub fn update_publish(
        &mut self,
        packet_id: u16,
        op: Operation,
        qos: QoS,
    ) -> Result<PublishState, StateError> {
        if matches!(qos, QoS::AtMostOnce) {
            return Ok(PublishState::PublishDone);
        }

        if packet_id == PACKET_ID_INVALID {
            return Err(StateError::BadParameter);
        }

        let mut record_index = 0usize;
        let mut current = PublishState::Null;

        if matches!(op, Operation::Send) {
            // The QoS must match what was reserved; a mismatch means the caller
            // has confused two messages.
            let Some((index, found_qos, state)) = find_in_record(self.outgoing, packet_id) else {
                return Err(StateError::BadParameter);
            };

            if found_qos != qos {
                return Err(StateError::BadParameter);
            }

            record_index = index;
            current = state;
        }

        let new = calculate_state_publish(op, qos);

        if !validate_transition_publish(current, new, op, qos) {
            return Err(StateError::IllegalState);
        }

        match op {
            // `add_record` is what detects a duplicate incoming publish id.
            Operation::Receive => add_record(self.incoming, packet_id, qos, new)?,
            Operation::Send => {
                // A resend leaves the state where it was.
                if current != new {
                    update_record(self.outgoing, record_index, new, false);
                }
            }
        }

        Ok(new)
    }

    /// `MQTT_UpdateStateAck`: an acknowledgement is being sent or has arrived.
    ///
    /// # The PUBREC move
    ///
    /// Receiving a PUBREC for an outgoing QoS 2 publish deletes the record and
    /// adds it back **at the end**. That looks wasteful and is deliberate: the
    /// PUBRELs it now owes must resend in the order the publishes went out, and
    /// appending is what puts them there.
    ///
    /// # Errors
    ///
    /// [`StateError::BadResponse`] when there is no record for that packet id —
    /// a duplicate acknowledgement, or a forged one. See also [`StateError`].
    pub fn update_ack(
        &mut self,
        packet_id: u16,
        ack: AckType,
        op: Operation,
    ) -> Result<PublishState, StateError> {
        if packet_id == PACKET_ID_INVALID {
            return Err(StateError::BadParameter);
        }

        let records = if is_publish_outgoing(ack, op) {
            &mut *self.outgoing
        } else {
            &mut *self.incoming
        };

        let Some((index, qos, current)) = find_in_record(records, packet_id) else {
            return Err(StateError::BadResponse);
        };

        let new = calculate_state_ack(ack, op, qos);

        if !validate_transition_ack(current, new) {
            return Err(StateError::IllegalState);
        }

        // A resend can land on the state it is already in, and then there is
        // nothing to do.
        if current != new {
            let should_delete = matches!(new, PublishState::PublishDone | PublishState::PubRelSend);
            update_record(records, index, new, should_delete);

            if matches!(new, PublishState::PubRelSend) {
                add_record(
                    records,
                    packet_id,
                    QoS::ExactlyOnce,
                    PublishState::PubRelSend,
                )?;
            }
        }

        Ok(new)
    }

    /// `MQTT_RemoveStateRecord`: drop an outgoing record.
    ///
    /// # Errors
    ///
    /// [`StateError::BadParameter`] if there is no such record, or it is not a
    /// QoS 1 or 2 one.
    ///
    /// A zero packet id is also [`StateError::BadParameter`] here. The C
    /// `assert`s on it instead, inside `findInRecord`, so on a build with
    /// assertions live it aborts. A library that must not panic cannot do that,
    /// and answering the error the caller already handles is the nearest
    /// behaviour; no differential scenario reaches it, because the C would
    /// abort rather than answer.
    pub fn remove(&mut self, packet_id: u16) -> Result<(), StateError> {
        if packet_id == PACKET_ID_INVALID {
            return Err(StateError::BadParameter);
        }

        let Some((index, qos, current)) = find_in_record(self.outgoing, packet_id) else {
            return Err(StateError::BadParameter);
        };

        if matches!(current, PublishState::Null) {
            return Err(StateError::BadParameter);
        }

        if matches!(qos, QoS::AtMostOnce) {
            return Err(StateError::BadParameter);
        }

        update_record(self.outgoing, index, PublishState::Null, true);
        Ok(())
    }

    /// `MQTT_PublishToResend`: the next outgoing PUBLISH a resumed session owes.
    ///
    /// Call repeatedly with the same [`Cursor`] until it answers `None`.
    #[must_use]
    pub fn publish_to_resend(&self, cursor: &mut Cursor) -> Option<u16> {
        const WANTED: [PublishState; 3] = [
            PublishState::PublishSend,
            PublishState::PubAckPending,
            PublishState::PubRecPending,
        ];

        match state_select(self.outgoing, &WANTED, cursor) {
            PACKET_ID_INVALID => None,
            packet_id => Some(packet_id),
        }
    }

    /// Empty both arrays.
    ///
    /// `handleCleanSession` memsets them, which is this: a fresh session has
    /// nothing in flight by definition, and anything left over belongs to a
    /// session the broker has already forgotten.
    pub fn clear_outgoing(&mut self) {
        self.outgoing.fill(Record::default());
    }

    /// The outgoing records, to write to. See
    /// [`MqttContext::outgoing_mut`](crate::client::MqttContext::outgoing_mut).
    pub fn outgoing_mut(&mut self) -> &mut [Record] {
        self.outgoing
    }

    /// Empty the incoming array.
    pub fn clear_incoming(&mut self) {
        self.incoming.fill(Record::default());
    }

    /// Shrink the arrays to what MQTT 5's Receive Maximum allows.
    ///
    /// The C keeps a pointer and a count and lowers the COUNT, leaving the rest
    /// of the array allocated and unreachable. Here the array IS the count, so
    /// capping re-slices it. Neither side can raise the other's: a cap only
    /// ever shrinks.
    pub fn cap(&mut self, outgoing: usize, incoming: usize) {
        if outgoing < self.outgoing.len() {
            let slice = core::mem::take(&mut self.outgoing);
            let (head, _) = slice.split_at_mut(outgoing);
            self.outgoing = head;
        }

        if incoming < self.incoming.len() {
            let slice = core::mem::take(&mut self.incoming);
            let (head, _) = slice.split_at_mut(incoming);
            self.incoming = head;
        }
    }

    /// `MQTT_PubrelToResend`: the next PUBREL a resumed session owes.
    ///
    /// The C also hands back a state, which is always `MQTTPubRelSend` — there
    /// is nothing else it could be — so this returns the packet id alone.
    #[must_use]
    pub fn pubrel_to_resend(&self, cursor: &mut Cursor) -> Option<u16> {
        const WANTED: [PublishState; 2] = [PublishState::PubCompPending, PublishState::PubRelSend];

        match state_select(self.outgoing, &WANTED, cursor) {
            PACKET_ID_INVALID => None,
            packet_id => Some(packet_id),
        }
    }
}

#[cfg(test)]
// A test sweeps its own probe values; the workspace's arithmetic policy is
// written for library code, where an unchecked operation is a defect.
#[allow(clippy::arithmetic_side_effects)]
mod tests {
    use super::*;

    const STATES: [PublishState; 11] = [
        PublishState::Null,
        PublishState::PublishSend,
        PublishState::PubAckSend,
        PublishState::PubRecSend,
        PublishState::PubRelSend,
        PublishState::PubCompSend,
        PublishState::PubAckPending,
        PublishState::PubRecPending,
        PublishState::PubRelPending,
        PublishState::PubCompPending,
        PublishState::PublishDone,
    ];

    const ACKS: [AckType; 4] = [
        AckType::PubAck,
        AckType::PubRec,
        AckType::PubRel,
        AckType::PubComp,
    ];

    const OPS: [Operation; 2] = [Operation::Send, Operation::Receive];
    const QOSES: [QoS; 3] = [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce];

    /// Why the `current != new` guard in [`PublishRecords::update_ack`] is an
    /// optimisation rather than behaviour.
    ///
    /// This started as a poison that did not fire: removing the guard, so that
    /// a transition onto the state a record is already in still rewrites it,
    /// changes nothing anywhere in the 413-line differential.
    ///
    /// The reason is a property of the transition table. The record is deleted
    /// only when the new state is `PublishDone` or `PubRelSend`, and **neither
    /// is reachable as a LEGAL self-transition** — so the rewrite is always a
    /// no-op on a record that is staying put.
    ///
    /// The legality is the part that matters, and it is what the first version
    /// of this test missed: `calculate_state_ack` will happily compute
    /// `PublishDone` for a record already in `PublishDone`, but
    /// `validate_transition_ack` refuses it, so `update_ack` never gets there.
    /// Lose either half and the guard becomes load-bearing silently.
    #[test]
    fn a_legal_self_transition_never_lands_on_a_deleting_state() {
        let mut legal_self_transitions = 0usize;

        for ack in ACKS {
            for op in OPS {
                for qos in QOSES {
                    let new = calculate_state_ack(ack, op, qos);

                    for current in STATES {
                        if current != new {
                            continue;
                        }

                        // Only transitions the table actually allows can be
                        // reached by `update_ack`.
                        if !validate_transition_ack(current, new) {
                            continue;
                        }

                        legal_self_transitions += 1;

                        assert!(
                            !matches!(new, PublishState::PublishDone | PublishState::PubRelSend),
                            "a legal self-transition onto {new:?} would DELETE the \
                             record, so the `current != new` guard is load-bearing \
                             after all and this test is the wrong shape"
                        );
                    }
                }
            }
        }

        assert!(
            legal_self_transitions > 0,
            "no legal self-transition was reachable at all — the sweep is \
             testing nothing, and the reconnect paths it stands for are untested"
        );
    }

    /// The self-transitions are exactly the reconnect recovery paths.
    ///
    /// Each one is a story: a duplicate PUBLISH whose PUBREC was lost, a
    /// duplicate PUBREL whose PUBCOMP was lost, a PUBREL being resent. Naming
    /// them here means removing one is a failing test rather than a silent
    /// narrowing of what a session can recover from.
    #[test]
    fn every_reconnect_recovery_path_is_present() {
        for state in [
            PublishState::PubRelPending,
            PublishState::PubCompSend,
            PublishState::PubCompPending,
        ] {
            assert!(
                validate_transition_ack(state, state),
                "{state:?} can no longer transition to itself — a reconnect \
                 recovery path has been removed"
            );
        }

        // And an outgoing publish can be resent without its state moving.
        for (state, qos) in [
            (PublishState::PubAckPending, QoS::AtLeastOnce),
            (PublishState::PubRecPending, QoS::ExactlyOnce),
        ] {
            assert!(
                validate_transition_publish(state, state, Operation::Send, qos),
                "{state:?} can no longer be resent"
            );
        }
    }
}
