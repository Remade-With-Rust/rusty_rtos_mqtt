//! The publish state machine against `core_mqtt_state.c`, operation for
//! operation.
//!
//! The C arm is `oracle/state_driver.c` driving `core_mqtt_state.c` compiled
//! VERBATIM from the pinned checkout (v5.0.2 at `04845c6a`), and its trace is
//! checked in — so this runs in CI with no C toolchain.
//!
//! # What is compared
//!
//! Not only the status each call returns, but **both record arrays after every
//! operation**. The records are the state, and their ORDER is load-bearing:
//! MQTT 5.0 requires message ordering, so the library appends rather than
//! filling holes, compacts rather than fragmenting, and deliberately moves a
//! record to the end when a PUBREC arrives so that PUBRELs resend in the order
//! the publishes went out.
//!
//! A transcription that produced every correct status while ordering the array
//! differently would pass a status-only test and resend a session's backlog out
//! of order — on exactly the reconnect this whole module exists to survive.
//!
//! # The scenarios live in the trace
//!
//! Each begins with `cfg` lines holding the record sizes and the whole
//! operation script, so this replays the workload rather than keeping a second
//! copy of the driver's tables.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::{
    AckType, Cursor, Operation, PublishRecords, PublishState, QoS, Record, StateError,
};

const TRACE: &str = include_str!("../../../oracle/state.trace");

/// The driver's array bound.
const MAX_RECORDS: usize = 8;

const fn state_name(s: PublishState) -> &'static str {
    match s {
        PublishState::Null => "Null",
        PublishState::PublishSend => "PublishSend",
        PublishState::PubAckSend => "PubAckSend",
        PublishState::PubRecSend => "PubRecSend",
        PublishState::PubRelSend => "PubRelSend",
        PublishState::PubCompSend => "PubCompSend",
        PublishState::PubAckPending => "PubAckPending",
        PublishState::PubRecPending => "PubRecPending",
        PublishState::PubRelPending => "PubRelPending",
        PublishState::PubCompPending => "PubCompPending",
        PublishState::PublishDone => "PublishDone",
    }
}

const fn error_name(e: StateError) -> &'static str {
    match e {
        StateError::BadParameter => "BadParameter",
        StateError::NoMemory => "NoMemory",
        StateError::StateCollision => "StateCollision",
        StateError::IllegalState => "IllegalState",
        StateError::BadResponse => "BadResponse",
    }
}

const fn qos_of(n: u8) -> QoS {
    match n {
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}

const fn qos_number(q: QoS) -> u8 {
    match q {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}

const fn op_of(n: u8) -> Operation {
    if n == 0 {
        Operation::Send
    } else {
        Operation::Receive
    }
}

/// The C's `MQTTPubAckType_t` accepts any integer; ours has four values.
fn ack_of(n: u8) -> Option<AckType> {
    match n {
        0 => Some(AckType::PubAck),
        1 => Some(AckType::PubRec),
        2 => Some(AckType::PubRel),
        3 => Some(AckType::PubComp),
        _ => None,
    }
}

fn write_records(out: &mut String, tag: &str, records: &[Record]) {
    let _ = write!(out, "records {tag} {}", records.len());
    for r in records {
        let _ = write!(
            out,
            " {}:{}:{}",
            r.packet_id,
            qos_number(r.qos),
            state_name(r.state)
        );
    }
    let _ = writeln!(out);
}

/// Replay one scenario and append our lines to `out`.
fn run_scenario(
    out: &mut String,
    outgoing_count: usize,
    incoming_count: usize,
    ops: &[(char, u16, u8, u8)],
) {
    let mut outgoing = [Record::default(); MAX_RECORDS];
    let mut incoming = [Record::default(); MAX_RECORDS];
    let mut records = PublishRecords::new(
        &mut outgoing[..outgoing_count],
        &mut incoming[..incoming_count],
    );
    let mut cursor = Cursor::new();

    for (i, (kind, packet_id, a, b)) in ops.iter().enumerate() {
        match kind {
            'r' => {
                let status = match records.reserve(*packet_id, qos_of(*a)) {
                    Ok(()) => "Success",
                    Err(e) => error_name(e),
                };
                let _ = writeln!(out, "op {i} r -> {status}");
            }

            'p' => {
                let (status, state) =
                    match records.update_publish(*packet_id, op_of(*b), qos_of(*a)) {
                        Ok(s) => ("Success", s),
                        // The C leaves its out-parameter at the driver's
                        // initialiser on failure, which is Null.
                        Err(e) => (error_name(e), PublishState::Null),
                    };
                let _ = writeln!(out, "op {i} p -> {status} {}", state_name(state));
            }

            'a' => {
                let (status, state) = match ack_of(*a) {
                    // An ack type outside the enum cannot be expressed here at
                    // all -- `AckType` has four values and no fifth -- so the
                    // C's bounds check has no equivalent and its answer is
                    // reproduced directly. That is the transcription being
                    // FAITHFUL about a difference, not papering over one.
                    None => ("BadParameter", PublishState::Null),
                    Some(ack) => match records.update_ack(*packet_id, ack, op_of(*b)) {
                        Ok(s) => ("Success", s),
                        Err(e) => (error_name(e), PublishState::Null),
                    },
                };
                let _ = writeln!(out, "op {i} a -> {status} {}", state_name(state));
            }

            'x' => {
                let status = match records.remove(*packet_id) {
                    Ok(()) => "Success",
                    Err(e) => error_name(e),
                };
                let _ = writeln!(out, "op {i} x -> {status}");
            }

            'P' => {
                let found = records.publish_to_resend(&mut cursor).unwrap_or(0);
                let _ = writeln!(out, "op {i} P -> {found} cursor={}", cursor.position());
            }

            'R' => {
                let found = records.pubrel_to_resend(&mut cursor);
                // The C always reports PubRelSend on a hit, and leaves the
                // driver's initialiser (Null) on a miss.
                let state = if found.is_some() {
                    PublishState::PubRelSend
                } else {
                    PublishState::Null
                };
                let _ = writeln!(
                    out,
                    "op {i} R -> {} {} cursor={}",
                    found.unwrap_or(0),
                    state_name(state),
                    cursor.position()
                );
            }

            'C' => {
                cursor = Cursor::new();
                let _ = writeln!(out, "op {i} C -> cursor={}", cursor.position());
            }

            other => panic!("unknown operation {other:?}"),
        }

        write_records(out, "out", records.outgoing());
        write_records(out, "in", records.incoming());
    }
}

/// Build our whole side of the trace by replaying every scenario in it.
fn our_trace() -> String {
    let mut out = String::new();
    let mut lines = TRACE.lines();

    let geometry = lines.next().expect("a geometry line");
    let _ = writeln!(out, "{geometry}");
    assert!(
        geometry.contains(&format!("maxrecords={MAX_RECORDS}")),
        "the two arms disagree about the array bound: {geometry:?}"
    );

    let (mut outgoing_count, mut incoming_count) = (0usize, 0usize);

    for line in lines {
        let f: Vec<&str> = line.split_whitespace().collect();

        match f.first().copied() {
            Some("scenario") => {
                let _ = writeln!(out, "{line}");
            }
            Some("cfg") if f[1] == "records" => {
                let _ = writeln!(out, "{line}");
                outgoing_count = f[2].parse().unwrap();
                incoming_count = f[3].parse().unwrap();
            }
            Some("cfg") if f[1] == "ops" => {
                let _ = writeln!(out, "{line}");
                let ops: Vec<(char, u16, u8, u8)> = f[3..]
                    .iter()
                    .map(|e| {
                        let p: Vec<&str> = e.split(':').collect();
                        (
                            p[0].chars().next().unwrap(),
                            p[1].parse().unwrap(),
                            p[2].parse().unwrap(),
                            p[3].parse().unwrap(),
                        )
                    })
                    .collect();
                // `ops` is the last cfg line, so the scenario is complete.
                run_scenario(&mut out, outgoing_count, incoming_count, &ops);
            }
            Some("end") if f.len() > 1 => {
                let _ = writeln!(out, "{line}");
            }
            Some("end") => {
                let _ = writeln!(out, "end");
            }
            // Everything else is a line WE produce.
            _ => {}
        }
    }

    out
}

#[test]
fn our_state_machine_matches_the_c_operation_for_operation() {
    let ours = our_trace();
    let mut theirs = TRACE.lines();
    let mut our_lines = ours.lines();
    let mut n = 0usize;

    loop {
        match (theirs.next(), our_lines.next()) {
            (None, None) => break,
            (Some(t), Some(o)) => {
                assert_eq!(o, t, "line {n} diverged\n  the C: {t}\n  ours : {o}");
            }
            (Some(t), None) => panic!("our trace ran out at line {n}; the C still has {t:?}"),
            (None, Some(o)) => panic!("the C trace ran out at line {n}; we still have {o:?}"),
        }
        n += 1;
    }

    assert_eq!(n, 413, "the trace should be 413 lines, not {n}");
}

/// The guard: the scenarios must reach every outcome the module has.
///
/// heap_4's guard fails on too few refusals, heap_1's on never exhausting,
/// heap_5's on an unvisited region, backoff's on an unvisited branch, json's on
/// a corpus that stops being mostly rejections, sntp's on an unreached status.
/// This is the eighth shape and it says the same thing.
#[test]
fn the_scenarios_reach_every_outcome() {
    let seen: Vec<&str> = TRACE
        .lines()
        .filter(|l| l.starts_with("op "))
        .filter_map(|l| l.split_whitespace().nth(4))
        .collect();

    for required in [
        "Success",
        "BadParameter",
        "NoMemory",
        "StateCollision",
        "IllegalState",
        "BadResponse",
    ] {
        assert!(
            seen.contains(&required),
            "no scenario ever produces {required}"
        );
    }

    // And every state a record can be in must actually be observed in the
    // arrays, or a row of the transition table is untested.
    for state in [
        "PublishSend",
        "PubAckSend",
        "PubRecSend",
        "PubRelSend",
        "PubCompSend",
        "PubAckPending",
        "PubRecPending",
        "PubRelPending",
        "PubCompPending",
    ] {
        assert!(
            TRACE
                .lines()
                .any(|l| l.starts_with("records ") && l.contains(state)),
            "no record is ever in state {state}"
        );
    }
}

/// The ordering guarantee, stated as a test rather than left in a comment.
///
/// A PUBREC for an outgoing QoS 2 publish deletes the record and adds it back
/// at the END. With two publishes in flight that is visible: the acknowledged
/// one moves behind the unacknowledged one, so the PUBRELs it now owes resend
/// in the order the publishes went out.
///
/// This is the single behaviour a status-only differential would miss entirely,
/// which is why the records are compared after every operation.
#[test]
fn a_pubrec_moves_the_record_behind_the_ones_still_waiting() {
    let mut outgoing = [Record::default(); 4];
    let mut incoming = [Record::default(); 4];
    let mut records = PublishRecords::new(&mut outgoing, &mut incoming);

    for id in [1u16, 2] {
        records.reserve(id, QoS::ExactlyOnce).expect("room");
        records
            .update_publish(id, Operation::Send, QoS::ExactlyOnce)
            .expect("a reserved publish");
    }

    assert_eq!(
        records.outgoing()[0].packet_id,
        1,
        "the first publish should start first"
    );

    records
        .update_ack(1, AckType::PubRec, Operation::Receive)
        .expect("a PUBREC for an outgoing publish");

    let order: Vec<u16> = records
        .outgoing()
        .iter()
        .filter(|r| r.packet_id != 0)
        .map(|r| r.packet_id)
        .collect();

    assert_eq!(
        order,
        vec![2, 1],
        "packet 1 should have moved BEHIND packet 2 -- the PUBRELs it now owes \
         must resend in publish order"
    );

    // And the resend cursor walks them in that order.
    let mut cursor = Cursor::new();
    assert_eq!(records.pubrel_to_resend(&mut cursor), Some(1));
    assert_eq!(records.pubrel_to_resend(&mut cursor), None);
}
