//! K7's no-panic and invariant gate for the MQTT publish state machine.
//!
//! This module takes no bytes from the network, so it looks safer than the
//! parsers. It is not: **the packet ids it is driven with come from the
//! broker**. Every PUBACK, PUBREC, PUBREL and PUBCOMP carries an id chosen by
//! whoever is on the other end of the socket, and each one indexes a record
//! array. An id that is unexpected, repeated, or simply never sent by us is an
//! ordinary thing for an attacker to produce and a normal thing for a confused
//! broker to produce.
//!
//! So the gate drives random operation sequences and asserts two things: that
//! nothing panics, and that the arrays stay **well formed** afterwards. The
//! second matters more than it looks — a duplicate packet id in the records
//! would make two messages share one handshake.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use rusty_rtos_mqtt_core::{AckType, Cursor, Operation, PublishRecords, PublishState, QoS, Record};

struct Lcg(u32);

impl Lcg {
    const fn new(seed: u32) -> Self {
        Self(seed)
    }
    fn next(&mut self) -> u32 {
        self.0 = self.0.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        self.0
    }
    fn below(&mut self, n: u32) -> u32 {
        self.next() % n
    }
}

/// What must be true of a record array after any sequence of operations.
fn check_well_formed(records: &[Record], which: &str, after: &str) {
    let mut seen: Vec<u16> = Vec::new();

    for (index, record) in records.iter().enumerate() {
        if record.packet_id == 0 {
            // An empty slot must be empty in every field, or a later compaction
            // would carry stale state forward into a live record.
            assert_eq!(
                record.qos,
                QoS::AtMostOnce,
                "{which}[{index}] is empty but keeps a QoS, after {after}"
            );
            assert_eq!(
                record.state,
                PublishState::Null,
                "{which}[{index}] is empty but keeps a state, after {after}"
            );
            continue;
        }

        assert!(
            !seen.contains(&record.packet_id),
            "{which} holds packet id {} twice, after {after} — two messages \
             would share one handshake",
            record.packet_id
        );
        seen.push(record.packet_id);

        assert_ne!(
            record.qos,
            QoS::AtMostOnce,
            "{which}[{index}] is occupied at QoS 0, which keeps no record, after {after}"
        );
        assert_ne!(
            record.state,
            PublishState::Null,
            "{which}[{index}] is occupied with no state, after {after}"
        );
    }
}

const QOSES: [QoS; 3] = [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce];
const ACKS: [AckType; 4] = [
    AckType::PubAck,
    AckType::PubRec,
    AckType::PubRel,
    AckType::PubComp,
];
const OPS: [Operation; 2] = [Operation::Send, Operation::Receive];

/// Arbitrary operation sequences, with packet ids an adversary would choose.
#[test]
fn arbitrary_operation_sequences_keep_the_records_well_formed() {
    for seed in 1..200u32 {
        let mut rng = Lcg::new(seed);

        let outgoing_count = (rng.below(4) + 1) as usize;
        let incoming_count = (rng.below(4) + 1) as usize;
        let mut outgoing = [Record::default(); 4];
        let mut incoming = [Record::default(); 4];
        let mut records = PublishRecords::new(
            &mut outgoing[..outgoing_count],
            &mut incoming[..incoming_count],
        );

        for step in 0..60 {
            // A small id space, so collisions and reuse happen constantly --
            // which is what a broker replaying a session looks like.
            let packet_id = rng.below(4) as u16;
            let qos = QOSES[rng.below(3) as usize];
            let ack = ACKS[rng.below(4) as usize];
            let op = OPS[rng.below(2) as usize];

            let what = match rng.below(6) {
                0 => {
                    let _ = records.reserve(packet_id, qos);
                    "reserve"
                }
                1 => {
                    let _ = records.update_publish(packet_id, op, qos);
                    "update_publish"
                }
                2 => {
                    let _ = records.update_ack(packet_id, ack, op);
                    "update_ack"
                }
                3 => {
                    let _ = records.remove(packet_id);
                    "remove"
                }
                4 => {
                    let mut cursor = Cursor::new();
                    while records.publish_to_resend(&mut cursor).is_some() {}
                    "publish_to_resend"
                }
                _ => {
                    let mut cursor = Cursor::new();
                    while records.pubrel_to_resend(&mut cursor).is_some() {}
                    "pubrel_to_resend"
                }
            };

            let after = format!("seed {seed} step {step} {what}");
            check_well_formed(records.outgoing(), "outgoing", &after);
            check_well_formed(records.incoming(), "incoming", &after);
        }
    }
}

/// The resend cursors always terminate, whatever the records hold.
///
/// Both walk the outgoing array with a cursor the CALLER owns, so a cursor that
/// failed to advance past a match would spin rather than fail. That is the same
/// hazard `rusty_rtos_sntp`'s retry loops have and `rusty_rtos_json`'s iterator
/// had, and it gets the same treatment: the bound is asserted, not assumed.
#[test]
fn the_resend_cursors_always_terminate() {
    for seed in 1..100u32 {
        let mut rng = Lcg::new(seed);
        let mut outgoing = [Record::default(); 4];
        let mut incoming = [Record::default(); 4];
        let mut records = PublishRecords::new(&mut outgoing, &mut incoming);

        // Fill the records with whatever a random session would leave behind.
        for _ in 0..20 {
            let packet_id = rng.below(5) as u16;
            let _ = records.reserve(packet_id, QOSES[1 + rng.below(2) as usize]);
            let _ = records.update_publish(
                packet_id,
                Operation::Send,
                QOSES[1 + rng.below(2) as usize],
            );
            let _ = records.update_ack(
                packet_id,
                ACKS[rng.below(4) as usize],
                OPS[rng.below(2) as usize],
            );
        }

        for kind in 0..2 {
            let mut cursor = Cursor::new();
            let mut steps = 0usize;

            loop {
                let found = if kind == 0 {
                    records.publish_to_resend(&mut cursor)
                } else {
                    records.pubrel_to_resend(&mut cursor)
                };

                if found.is_none() {
                    break;
                }

                steps += 1;
                assert!(
                    steps <= records.outgoing().len(),
                    "seed {seed}: the cursor produced {steps} results from {} records \
                     — it is not advancing",
                    records.outgoing().len()
                );
            }
        }
    }
}
