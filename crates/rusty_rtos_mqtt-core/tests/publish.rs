//! The incoming PUBLISH against `core_mqtt_serializer.c`.
//!
//! The C arm is `oracle/publish_driver.c` driving the pinned v5.0.2 serializer
//! (`04845c6a`) verbatim, and its trace is checked in.
//!
//! # Two flag sweeps and six property sweeps
//!
//! Both for the reason the CONNACK needed five: a byte that selects between
//! values of different shapes cannot be swept with one body. QoS decides
//! whether a packet identifier is present, so a body with one is malformed at
//! QoS 0 and a body without one is malformed at QoS 1 — sweeping once would
//! refuse half the nibble for the wrong reason and prove nothing.
//!
//! # What is compared
//!
//! Every field the C hands back: the QoS, DUP and RETAIN bits, the packet
//! identifier, the topic name, the property length, the property section and
//! **the payload**. The payload is the point of the packet and the one number
//! another program will index with, so a differential that checked only the
//! status would be checking the wrong thing.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::ack::PacketInfo;
use rusty_rtos_mqtt_core::publish::{PublishError, PublishInfo, deserialize_publish};
use rusty_rtos_mqtt_core::state::QoS;

const TRACE: &str = include_str!("../../../oracle/publish.trace");

fn hex(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "-".to_owned();
    }

    let mut out = String::new();
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn unhex(text: &str) -> Vec<u8> {
    if text == "-" {
        return Vec::new();
    }

    assert!(text.len() % 2 == 0, "an odd number of hex digits: {text:?}");
    (0..text.len() / 2)
        .map(|i| u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).expect("hex"))
        .collect()
}

fn field<'a>(fields: &[&'a str], index: usize, name: &str) -> &'a str {
    fields[index].strip_prefix(name).unwrap_or_else(|| {
        panic!(
            "field {index} should start with {name:?}, got {:?}",
            fields[index]
        )
    })
}

/// The C's `MQTTQoS_t`, which is 0, 1 or 2.
fn qos_number(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}

fn outcome(result: Result<PublishInfo<'_>, PublishError>) -> String {
    match result {
        Err(PublishError::BadParameter) => " -> BadParameter".to_owned(),
        Err(PublishError::BadResponse) => " -> BadResponse".to_owned(),
        Ok(publish) => format!(
            " -> Success qos={} dup={} ret={} id={} topic={} proplen={} props={} payload={}",
            qos_number(publish.qos),
            u8::from(publish.dup),
            u8::from(publish.retain),
            // The C leaves its out-parameter at zero for a QoS 0 publish,
            // which carries no packet identifier at all.
            publish.packet_id.unwrap_or(0),
            hex(publish.topic_name),
            publish.properties.len(),
            hex(publish.properties),
            hex(publish.payload)
        ),
    }
}

fn replay(fields: &[&str], first: usize) -> String {
    let packet_type = u8::from_str_radix(field(fields, first, "type="), 16).expect("a type byte");
    let remaining_length: u32 = field(fields, first + 1, "rl=").parse().expect("rl");
    let max_packet_size: u32 = field(fields, first + 2, "max=").parse().expect("max");
    let topic_alias_max: u16 = field(fields, first + 3, "alias=").parse().expect("alias");
    let body = unhex(field(fields, first + 4, "in="));

    let packet = PacketInfo {
        packet_type,
        remaining_length,
        remaining_data: &body,
    };

    outcome(deserialize_publish(
        &packet,
        max_packet_size,
        topic_alias_max,
    ))
}

fn call(packet_type: u8, body: &[u8]) -> Result<PublishInfo<'_>, PublishError> {
    deserialize_publish(
        &PacketInfo {
            packet_type,
            remaining_length: u32::try_from(body.len()).unwrap(),
            remaining_data: body,
        },
        1024,
        10,
    )
}

fn our_trace() -> String {
    let mut out = String::new();

    for line in TRACE.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();

        match f.first().copied() {
            Some("geometry" | "end") => {
                let _ = writeln!(out, "{line}");
            }

            Some("case") => {
                let _ = writeln!(
                    out,
                    "case {} {} {} {} {} {} {}{}",
                    f[1],
                    f[2],
                    f[3],
                    f[4],
                    f[5],
                    f[6],
                    f[7],
                    replay(&f, 3)
                );
            }

            Some("flags-sweep") => {
                // The two body shapes the driver sweeps, keyed by its label.
                let body: &[u8] = match f[1] {
                    "no-packet-id" => &[0x00, 0x01, b'a', 0x00, b'h', b'i'],
                    "with-packet-id" => &[0x00, 0x01, b'a', 0x00, 0x2A, 0x00, b'h', b'i'],
                    other => panic!("the trace names a body this file does not know: {other:?}"),
                };

                let mut accepted = String::new();
                let (mut n, mut rejected) = (0usize, 0usize);

                for nibble in 0..16u8 {
                    if call(0x30 | nibble, body).is_ok() {
                        if n > 0 {
                            accepted.push(',');
                        }
                        let _ = write!(accepted, "{nibble:x}");
                        n += 1;
                    } else {
                        rejected += 1;
                    }
                }

                let _ = writeln!(
                    out,
                    "flags-sweep {} accepted={accepted} n={n} rejected={rejected}",
                    f[1]
                );
            }

            Some("property-sweep") => {
                let value: &[u8] = match f[1] {
                    "one-byte" => &[0x00],
                    "two-byte" => &[0x00, 0x01],
                    "four-byte" => &[0x00, 0x00, 0x00, 0x01],
                    "string" => &[0x00, 0x01, b'x'],
                    "varint" => &[0x07],
                    "user-property" => &[0x00, 0x01, b'k', 0x00, 0x01, b'v'],
                    other => panic!("the trace names a shape this file does not know: {other:?}"),
                };

                let mut accepted = String::new();
                let (mut n, mut rejected) = (0usize, 0usize);

                for id in 0..=255u8 {
                    let section_length = value.len() + 1;
                    let mut body =
                        vec![0x00, 0x01, b'a', u8::try_from(section_length).unwrap(), id];
                    body.extend_from_slice(value);

                    if call(0x30, &body).is_ok() {
                        if n > 0 {
                            accepted.push(',');
                        }
                        let _ = write!(accepted, "{id:02x}");
                        n += 1;
                    } else {
                        rejected += 1;
                    }
                }

                let _ = writeln!(
                    out,
                    "property-sweep {} accepted={accepted} n={n} rejected={rejected}",
                    f[1]
                );
            }

            _ => {}
        }
    }

    out
}

#[test]
fn our_publish_deserializer_matches_the_c_case_for_case() {
    let ours = our_trace();
    let mut theirs = TRACE.lines();
    let mut our_lines = ours.lines();
    let mut n = 0usize;

    loop {
        match (theirs.next(), our_lines.next()) {
            (None, None) => break,
            (Some(t), Some(o)) => assert_eq!(o, t, "line {n} diverged\n  the C: {t}\n  ours : {o}"),
            (Some(t), None) => panic!("our trace ran out at line {n}; the C still has {t:?}"),
            (None, Some(o)) => panic!("the C trace ran out at line {n}; we still have {o:?}"),
        }
        n += 1;
    }

    assert_eq!(n, 54, "the trace should be 54 lines, not {n}");
}

/// The guard: every QoS, both flag bits, and a payload that is actually there.
///
/// The fifteenth shape of the guard `heap_4` started, and this one has a job
/// the others did not: **a corpus of empty payloads would prove nothing about
/// the payload arithmetic**, which is the whole safety argument of this module.
#[test]
fn the_cases_exercise_every_qos_and_carry_real_payloads() {
    let cases: Vec<&str> = TRACE.lines().filter(|l| l.starts_with("case ")).collect();

    for qos in 0..=2u8 {
        let n = cases
            .iter()
            .filter(|l| l.contains(&format!(" qos={qos} ")))
            .count();
        assert!(n >= 2, "only {n} successful cases at QoS {qos}");
    }

    assert!(
        cases.iter().any(|l| l.contains(" dup=1 ")),
        "no case sets DUP"
    );
    assert!(
        cases.iter().any(|l| l.contains(" ret=1 ")),
        "no case sets RETAIN"
    );

    // A payload of at least four bytes, in at least three cases: the payload
    // length is a four-term subtraction and a corpus of empty ones would never
    // have moved it.
    let with_payload = cases
        .iter()
        .filter(|l| {
            l.split_whitespace()
                .find_map(|f| f.strip_prefix("payload="))
                .is_some_and(|p| p != "-" && p.len() >= 8)
        })
        .count();

    assert!(
        with_payload >= 3,
        "only {with_payload} cases carry a payload of four bytes or more"
    );

    assert!(
        cases
            .iter()
            .filter(|l| l.contains("-> BadResponse"))
            .count()
            >= 8,
        "too few cases are refused"
    );
    assert!(
        cases
            .iter()
            .filter(|l| l.contains("-> BadParameter"))
            .count()
            >= 2,
        "the two routing refusals must both be exercised"
    );
}

/// The property table is exactly MQTT 5.0 §3.3.2.3 — and one entry is a
/// divergence hiding in plain sight.
#[test]
fn the_publish_property_table_is_the_specification() {
    let expected = [
        // 0x0B appears in BOTH the one-byte and the varint sweep, because a
        // one-byte variable-length integer is a legal Subscription Identifier.
        // That is not a duplicate: it is the zero-identifier divergence below,
        // visible in the sweep.
        ("one-byte", "01,0b"),
        ("two-byte", "23"),
        ("four-byte", "02"),
        ("string", "03,08,09"),
        ("varint", "0b"),
        ("user-property", "26"),
    ];

    let mut ids = std::collections::BTreeSet::new();

    for (shape, set) in expected {
        let line = TRACE
            .lines()
            .find(|l| l.starts_with(&format!("property-sweep {shape} ")))
            .unwrap_or_else(|| panic!("no {shape} sweep in the trace"));

        assert!(
            line.contains(&format!("accepted={set} ")),
            "the {shape} property set changed: {line}"
        );

        for id in set.split(',') {
            ids.insert(id.to_owned());
        }
    }

    assert_eq!(
        ids.len(),
        8,
        "MQTT 5.0 §3.3.2.3 lists eight PUBLISH properties, not {}: {ids:?}",
        ids.len()
    );
}

/// The two places coreMQTT is more permissive than MQTT 5.0, read off the
/// trace.
///
/// Findings, not fixtures: both are written up in `docs/upstream/` for the
/// owner to file. Asserted from the CHECKED-IN TRACE rather than from our own
/// code, so the day the pinned oracle changes its mind, this says so.
#[test]
fn the_trace_records_two_divergences_from_the_specification() {
    // §3.3.2.3.4: a zero-length topic name is a protocol error unless a Topic
    // Alias is present. The C accepts it either way, so both cases succeed and
    // the packet with no alias is the defect.
    assert!(
        TRACE
            .lines()
            .any(|l| l.contains(" qos0-empty-topic ") && l.contains("topic=- ")),
        "a zero-length topic name with no Topic Alias is no longer accepted"
    );

    // §3.3.2.3.8: a Subscription Identifier of zero is a protocol error.
    assert!(
        TRACE
            .lines()
            .any(|l| l.contains(" subscription-id-zero ") && l.contains("props=0b00")),
        "a zero Subscription Identifier is no longer accepted"
    );

    // ...and the two-byte spelling of the same zero IS refused — by the
    // non-minimal-encoding check, not by any zero check. That is what shows
    // there is no zero check at all, rather than one with a gap.
    assert!(
        TRACE
            .lines()
            .any(|l| l.contains(" subscription-id-zero-two-byte ") && l.ends_with("BadResponse")),
        "the non-minimal spelling is now accepted too"
    );
}
