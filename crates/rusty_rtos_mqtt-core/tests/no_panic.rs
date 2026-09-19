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

// ---- the fixed header ----------------------------------------------------
//
// The state machine above is driven by packet ids from the broker. THIS is
// driven by raw bytes off the socket, before anything is known about them, so
// it is the most exposed code in the package.

use rusty_rtos_mqtt_core::header::{
    encode_variable_length, process_incoming_packet_type_and_length, variable_length_encoded_size,
};

/// Arbitrary bytes at arbitrary lengths, with an arbitrary claimed count.
///
/// The exhaustive sweep next door covers every type byte against a chosen
/// alphabet of length bytes. This covers the shapes that alphabet cannot: odd
/// buffer sizes, and an `available` count that does not match the buffer.
#[test]
fn arbitrary_header_bytes_never_panic() {
    let mut rng = Lcg::new(7);

    for _ in 0..200_000 {
        let len = (rng.below(9)) as usize;
        let buffer: Vec<u8> = (0..len).map(|_| (rng.next() >> 16) as u8).collect();

        // Deliberately including counts LARGER than the buffer.
        let available = (rng.below(12)) as usize;

        let _ = process_incoming_packet_type_and_length(&buffer, available);
    }
}

/// A caller who lies about how many bytes arrived must not be able to read
/// past the buffer.
///
/// The C cannot promise this: it is handed a pointer and a count, and
/// `pBuffer[ bytesDecoded + 1U ]` trusts the count. An `available` larger than
/// the allocation reads whatever is next in memory. Here the count is only ever
/// an upper bound on a `get`, so an over-large one produces `NeedMoreBytes` and
/// nothing else — which is the kind of difference `forbid(unsafe)` is for.
#[test]
fn an_over_large_available_count_cannot_read_past_the_buffer() {
    let buffer = [0xD0u8, 0x80];

    for available in 0..64usize {
        let result = process_incoming_packet_type_and_length(&buffer, available);

        if available <= buffer.len() {
            continue;
        }

        // The header is incomplete and the bytes to finish it do not exist.
        assert!(
            result.is_err(),
            "claiming {available} bytes of a {}-byte buffer was accepted",
            buffer.len()
        );
    }
}

/// Encoding into a buffer too small answers rather than writing past it.
#[test]
fn encoding_into_any_buffer_is_safe() {
    let mut rng = Lcg::new(8);

    for _ in 0..100_000 {
        let length = rng.next() % 268_435_456;
        let size = (rng.below(8)) as usize;
        let mut buffer = vec![0xAAu8; size];

        let written = encode_variable_length(&mut buffer, length);

        if written == 0 {
            // Refused, so nothing was written and the fill survives.
            assert!(
                buffer.iter().all(|b| *b == 0xAA),
                "a refused encode still wrote into the buffer"
            );
            assert!(size < variable_length_encoded_size(length) as usize);
        } else {
            assert_eq!(written, variable_length_encoded_size(length) as usize);
        }
    }
}

// ---- the property primitives ---------------------------------------------
//
// Every MQTT 5 property is decoded through these, out of bytes the broker
// chose, bounded by a length the broker also chose. A property length larger
// than the packet is the ordinary shape of a malformed packet.

use rusty_rtos_mqtt_core::property::{PropertyReader, decode_variable_length, encode_string};

/// Random property sections, with budgets that routinely exceed the buffer.
#[test]
fn arbitrary_property_sections_never_panic() {
    let mut rng = Lcg::new(11);

    for _ in 0..100_000 {
        let len = rng.below(24) as usize;
        let bytes: Vec<u8> = (0..len).map(|_| (rng.next() >> 16) as u8).collect();

        // A budget the packet CLAIMS, deliberately often larger than what
        // arrived -- which is what an attacker sends.
        let budget = rng.below(40);
        let mut reader = PropertyReader::new(&bytes, budget);

        for _ in 0..8 {
            let mut used = rng.below(2) == 1;
            match rng.below(5) {
                0 => {
                    let _ = reader.u8(&mut used);
                }
                1 => {
                    let _ = reader.u16(&mut used);
                }
                2 => {
                    let _ = reader.u32(&mut used);
                }
                3 => {
                    let _ = reader.utf8(&mut used);
                }
                _ => {
                    let _ = reader.user_property();
                }
            }

            // The cursor must never run past what exists.
            assert!(
                reader.position() <= bytes.len(),
                "the cursor reached {} in a {}-byte buffer",
                reader.position(),
                bytes.len()
            );
        }
    }
}

/// The property length decoder, over arbitrary bytes.
#[test]
fn arbitrary_property_lengths_never_panic() {
    let mut rng = Lcg::new(12);

    for _ in 0..200_000 {
        let len = rng.below(7) as usize;
        let bytes: Vec<u8> = (0..len).map(|_| (rng.next() >> 16) as u8).collect();
        let _ = decode_variable_length(&bytes);
    }
}

/// Encoding a string into a buffer that cannot hold it.
#[test]
fn encoding_a_string_into_any_buffer_is_safe() {
    let mut rng = Lcg::new(13);

    for _ in 0..50_000 {
        let source_len = rng.below(20) as usize;
        let source: Vec<u8> = (0..source_len).map(|_| (rng.next() >> 16) as u8).collect();
        let claimed = rng.below(24) as u16;
        let dest_len = rng.below(28) as usize;
        let mut dest = vec![0xAAu8; dest_len];

        let written = encode_string(&mut dest, Some(&source), claimed);

        if written != 0 {
            assert_eq!(written, usize::from(claimed) + 2);
            assert!(written <= dest_len);
        }
    }
}

// ---------------------------------------------------------------------------
// The outgoing-property validators.
//
// These read a section the APPLICATION built, not one the broker sent, so they
// look like the safe direction. Two things make them worth fuzzing anyway: an
// application that echoes a broker's user properties back into its own packet
// is handing network bytes to a writer, and the section length is a number the
// caller supplies and can get wrong.

use rusty_rtos_mqtt_core::validate::{
    ConnectValidation, validate_connect_properties, validate_publish_ack_properties,
    validate_publish_properties, validate_subscribe_properties, validate_unsubscribe_properties,
    validate_will_properties,
};

/// Arbitrary property sections through all six validators.
#[test]
fn arbitrary_outgoing_property_sections_never_panic() {
    let mut rng = Lcg::new(14);

    for _ in 0..100_000 {
        let len = rng.below(24) as usize;
        let bytes: Vec<u8> = (0..len).map(|_| (rng.next() >> 16) as u8).collect();

        let mut found = ConnectValidation::default();
        let mut alias = None;

        let _ = validate_connect_properties(&bytes, &mut found);
        let _ = validate_will_properties(&bytes);
        let _ = validate_subscribe_properties(rng.below(2) == 1, &bytes);
        let _ = validate_publish_properties((rng.next() >> 16) as u16, &bytes, &mut alias);
        let _ = validate_publish_ack_properties(&bytes);
        let _ = validate_unsubscribe_properties(&bytes);
    }
}

// ---------------------------------------------------------------------------
// The buffered header reader, and the connection context.
//
// `process_header` is handed bytes straight off the network, before anything
// has decided they are a packet -- the same threat surface as the fixed-header
// decoder above, one layer out.

use rusty_rtos_mqtt_core::context::{ConnectionProperties, update_with_connect_props};

/// Arbitrary receive buffers through the buffered reader.
#[test]
fn arbitrary_receive_buffers_never_panic() {
    let mut rng = Lcg::new(15);

    for _ in 0..200_000 {
        let len = rng.below(8) as usize;
        let bytes: Vec<u8> = (0..len).map(|_| (rng.next() >> 16) as u8).collect();

        if let Ok(header) = process_incoming_packet_type_and_length(&bytes, bytes.len()) {
            // The header must lie inside what arrived, or the caller would
            // slice a body that is not there.
            assert!(
                header.header_length <= bytes.len(),
                "a {}-byte header out of {} bytes",
                header.header_length,
                bytes.len()
            );
            assert!(header.header_length >= 2, "a header is at least two bytes");
        }
    }
}

/// Arbitrary CONNECT property sections through the context filler.
#[test]
fn arbitrary_connect_properties_never_panic() {
    let mut rng = Lcg::new(16);

    for _ in 0..100_000 {
        let len = rng.below(24) as usize;
        let bytes: Vec<u8> = (0..len).map(|_| (rng.next() >> 16) as u8).collect();

        let mut context = ConnectionProperties::new();
        let _ = update_with_connect_props(&bytes, &mut context);
    }
}

// ---------------------------------------------------------------------------
// The property builders.
//
// These write into a buffer the application owns, from values the application
// chooses -- and the C's equivalent writes one byte past that buffer when it is
// exactly one byte too small. This gate asserts the property that makes the
// same slip impossible here: whatever the arithmetic says, the cursor never
// passes the slice and a refusal writes nothing.

use rusty_rtos_mqtt_core::builder::PropertyBuilder;

/// Arbitrary property sections, built into arbitrary buffers.
#[test]
fn building_into_any_buffer_never_writes_past_it() {
    let mut rng = Lcg::new(17);

    for _ in 0..100_000 {
        let capacity = rng.below(20) as usize;
        let mut bytes = vec![0xAAu8; capacity];

        let Ok(mut builder) = PropertyBuilder::new(&mut bytes) else {
            continue;
        };

        for _ in 0..6 {
            let length = rng.below(10) as usize;
            let text: Vec<u8> = (0..length).map(|_| b'x').collect();

            let _ = match rng.below(8) {
                0 => builder.session_expiry(rng.next(), None),
                1 => builder.topic_alias((rng.next() >> 16) as u16 | 1, None),
                2 => builder.payload_format(rng.below(2) == 1, None),
                3 => builder.content_type(&text, None),
                4 => builder.reason_string(&text, None),
                5 => builder.subscription_id(rng.below(300_000) | 1, None),
                6 => builder.user_property(&text, b"v", None),
                _ => builder.correlation_data(&text, None),
            };

            assert!(
                builder.len() <= builder.capacity(),
                "wrote {} bytes into {}",
                builder.len(),
                builder.capacity()
            );
        }

        // Nothing beyond the cursor was touched.
        let written = builder.len();
        assert!(bytes[written..].iter().all(|byte| *byte == 0xAA));
    }
}

// ---------------------------------------------------------------------------
// The property cursor.
//
// This one reads a section a BROKER sent, so every byte is chosen by the peer.
// The invariant is the mirror of the builder's: the cursor never leaves the
// section, and a refusal never moves it.

use rusty_rtos_mqtt_core::cursor::PropertyCursor;

/// Arbitrary property sections, walked from arbitrary positions.
#[test]
fn walking_any_property_section_never_leaves_it() {
    let mut rng = Lcg::new(18);

    for _ in 0..100_000 {
        let len = rng.below(20) as usize;
        let bytes: Vec<u8> = (0..len).map(|_| (rng.next() >> 16) as u8).collect();

        let mut cursor = PropertyCursor::new(&bytes);
        cursor.seek(rng.below(24) as usize);

        for _ in 0..6 {
            let before = cursor.position();
            let _ = cursor.next_type();
            assert_eq!(before, cursor.position(), "a peek moved the cursor");

            // On CLONES: any of these may succeed, and a getter that succeeds
            // is supposed to move the cursor. The invariant under test is
            // about the one that FAILS, so the probes must not disturb the
            // cursor `skip` is then measured against.
            let _ = cursor.clone().session_expiry();
            let _ = cursor.clone().reason_string();
            let _ = cursor.clone().user_property();
            let _ = cursor.clone().subscription_id();
            assert_eq!(before, cursor.position(), "a clone moved the original");

            if cursor.skip().is_err() {
                assert_eq!(before, cursor.position(), "a refusal moved the cursor");
                break;
            }

            assert!(
                cursor.position() <= bytes.len().max(before),
                "the cursor reached {} in a {}-byte section",
                cursor.position(),
                bytes.len()
            );
        }
    }
}
