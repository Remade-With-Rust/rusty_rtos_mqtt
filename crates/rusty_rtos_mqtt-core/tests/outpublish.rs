//! The outgoing PUBLISH against `core_mqtt_serializer.c`.
//!
//! The C arm is `oracle/outpublish_driver.c` driving the pinned v5.0.2
//! serializer (`04845c6a`) verbatim, and its trace is checked in.
//!
//! # Three serializers, one case
//!
//! Each case runs the size calculator and then **all three** serializers into
//! the same shape of buffer, and prints all three results. The three share one
//! body in the C and must agree as prefixes, so running them apart would test
//! three functions and not the relationship between them — which is the thing a
//! caller on the vectored path depends on.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::outpublish::{
    OutgoingError, OutgoingPublish, publish_packet_size, serialize_publish,
    serialize_publish_header, serialize_publish_header_without_topic, update_duplicate_flag,
};
use rusty_rtos_mqtt_core::state::QoS;

const TRACE: &str = include_str!("../../../oracle/outpublish.trace");

fn hex(bytes: &[u8]) -> String {
    let mut out = String::new();
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// `-` absent, `.` present and empty, hex otherwise.
fn unopt(text: &str) -> Option<Vec<u8>> {
    match text {
        "-" => None,
        "." => Some(Vec::new()),
        body => {
            assert!(body.len() % 2 == 0, "an odd number of hex digits: {body:?}");
            Some(
                (0..body.len() / 2)
                    .map(|i| u8::from_str_radix(&body[i * 2..i * 2 + 2], 16).expect("hex"))
                    .collect(),
            )
        }
    }
}

fn field<'a>(fields: &[&'a str], index: usize, name: &str) -> &'a str {
    fields[index].strip_prefix(name).unwrap_or_else(|| {
        panic!(
            "field {index} should start with {name:?}, got {:?}",
            fields[index]
        )
    })
}

fn status(error: OutgoingError) -> &'static str {
    match error {
        OutgoingError::BadParameter => "BadParameter",
        OutgoingError::NoMemory => "NoMemory",
    }
}

fn qos_from(number: &str) -> QoS {
    match number {
        "0" => QoS::AtMostOnce,
        "1" => QoS::AtLeastOnce,
        "2" => QoS::ExactlyOnce,
        other => panic!("the trace names a QoS this file does not know: {other:?}"),
    }
}

/// FNV-1a/64, with the seed every driver in this package uses.
fn fnv(state: u64, byte: u8) -> u64 {
    (state ^ u64::from(byte)).wrapping_mul(1_099_511_628_211)
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
                let qos = qos_from(field(&f, 3, "qos="));
                let dup = field(&f, 4, "dup=") == "1";
                let retain = field(&f, 5, "ret=") == "1";
                let packet_id: u16 = field(&f, 6, "id=").parse().expect("id");
                let topic = unopt(field(&f, 7, "topic=")).expect("a topic");
                let payload = unopt(field(&f, 8, "payload=")).expect("a payload");
                let properties = unopt(field(&f, 9, "props=")).unwrap_or_default();
                let max: u32 = field(&f, 10, "max=").parse().expect("max");
                let buffer: usize = field(&f, 11, "buf=").parse().expect("buf");

                let publish = OutgoingPublish {
                    qos,
                    dup,
                    retain,
                    topic_name: &topic,
                    payload: &payload,
                    properties: &properties,
                };

                let mut tail = String::new();

                match publish_packet_size(&publish, max) {
                    Err(error) => {
                        let _ = write!(tail, " -> size {}", status(error));
                    }
                    Ok(size) => {
                        let _ = write!(
                            tail,
                            " -> size Success remaining={} packet={}",
                            size.remaining_length, size.packet_size
                        );

                        // 1. The whole packet.
                        let mut long = vec![0xCCu8; buffer];
                        match serialize_publish(
                            &mut long,
                            &publish,
                            packet_id,
                            size.remaining_length,
                        ) {
                            Err(error) => {
                                let _ = write!(tail, " | full {}", status(error));
                            }
                            Ok(_) => {
                                let _ = write!(
                                    tail,
                                    " | full Success bytes={}",
                                    hex(&long[..size.packet_size as usize])
                                );
                            }
                        }

                        // 2. Everything but the payload.
                        let mut middle = vec![0xCCu8; buffer];
                        match serialize_publish_header(
                            &mut middle,
                            &publish,
                            packet_id,
                            size.remaining_length,
                        ) {
                            Err(error) => {
                                let _ = write!(tail, " | hdr {}", status(error));
                            }
                            Ok(header_size) => {
                                let _ = write!(
                                    tail,
                                    " | hdr Success hsize={header_size} bytes={}",
                                    hex(&middle[..header_size])
                                );
                            }
                        }

                        // 3. The type byte, the length and the topic's length.
                        // Its own buffer, because the C hands it a raw pointer
                        // rather than a sized one and the driver gives it the
                        // full array.
                        let mut short = vec![0xCCu8; 128];
                        match serialize_publish_header_without_topic(
                            &mut short,
                            &publish,
                            size.remaining_length,
                        ) {
                            Err(error) => {
                                let _ = write!(tail, " | notopic {}", status(error));
                            }
                            Ok(header_size) => {
                                let _ = write!(
                                    tail,
                                    " | notopic Success hsize={header_size} bytes={}",
                                    hex(&short[..header_size])
                                );
                            }
                        }
                    }
                }

                let _ = writeln!(
                    out,
                    "case {} {} {} {} {} {} {} {} {} {} {}{tail}",
                    f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8], f[9], f[10], f[11]
                );
            }

            // contract <i> <name> rl= -> full ... | hdr ...
            //
            // The C's header serializer reports the size it COMPUTED from the
            // remaining length it was handed, not the bytes it wrote -- and the
            // two are equal only while the caller keeps the API contract of
            // calling the size function first. These cases break it on purpose,
            // which is the only way to tell the two numbers apart.
            Some("contract") => {
                let remaining_length: u32 = field(&f, 3, "rl=").parse().expect("rl");
                let publish = OutgoingPublish {
                    qos: QoS::AtMostOnce,
                    dup: false,
                    retain: false,
                    topic_name: b"a/b",
                    payload: b"hello",
                    properties: &[],
                };

                let mut tail = String::new();

                let mut long = vec![0xCCu8; 128];
                match serialize_publish(&mut long, &publish, 0, remaining_length) {
                    Err(error) => {
                        let _ = write!(tail, " -> full {}", status(error));
                    }
                    Ok(_) => {
                        let _ = write!(tail, " -> full Success");
                    }
                }

                let mut middle = vec![0xCCu8; 128];
                match serialize_publish_header(&mut middle, &publish, 0, remaining_length) {
                    Err(error) => {
                        let _ = write!(tail, " | hdr {}", status(error));
                    }
                    Ok(header_size) => {
                        let _ = write!(
                            tail,
                            " | hdr Success hsize={header_size} bytes={}",
                            hex(&middle[..header_size])
                        );
                    }
                }

                let _ = writeln!(out, "contract {} {} {}{tail}", f[1], f[2], f[3]);
            }

            Some("dup-sweep") => {
                let set = f[1] == "set";
                let (mut accepted, mut refused) = (0usize, 0usize);
                let mut digest = 1_469_598_103_934_665_603u64;

                for value in 0..=255u8 {
                    let mut header = value;

                    if update_duplicate_flag(&mut header, set).is_ok() {
                        accepted += 1;
                        digest = fnv(digest, header);
                    } else {
                        refused += 1;
                    }
                }

                let _ = writeln!(
                    out,
                    "dup-sweep {} accepted={accepted} refused={refused} digest={digest:016x}",
                    f[1]
                );
            }

            _ => {}
        }
    }

    out
}

#[test]
fn our_outgoing_publish_matches_the_c_case_for_case() {
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

    assert_eq!(n, 25, "the trace should be 25 lines, not {n}");
}

/// The guard: all three serializers must run, and the three must DISAGREE
/// somewhere.
///
/// The eighteenth shape of the guard `heap_4` started. Its own job: three
/// functions that always answered identically would need only one differential,
/// and a corpus that never separated them would hide the fact that they
/// validate differently — which is the thing a caller choosing between them
/// needs to know.
#[test]
fn the_cases_separate_the_three_serializers() {
    let cases: Vec<&str> = TRACE.lines().filter(|l| l.starts_with("case ")).collect();

    for name in ["full Success", "hdr Success", "notopic Success"] {
        let n = cases.iter().filter(|l| l.contains(name)).count();
        assert!(n >= 8, "only {n} cases reach `{name}`");
    }

    // At least one case where the loose serializer succeeds and a strict one
    // does not: that difference is the reason all three exist.
    let separated = cases
        .iter()
        .filter(|l| l.contains("notopic Success") && l.contains("full BadParameter"))
        .count();
    assert!(
        separated >= 3,
        "only {separated} cases separate the loose serializer from the strict ones"
    );

    // And at least one where the header fits and the whole packet does not,
    // which is the point of the middle one.
    assert!(
        cases
            .iter()
            .any(|l| l.contains("full NoMemory") && l.contains("hdr Success")),
        "no case has a buffer that holds the header but not the payload"
    );

    // Every QoS, and both of DUP and RETAIN.
    for qos in 0..=2u8 {
        assert!(
            cases.iter().any(|l| l.contains(&format!(" qos={qos} "))),
            "no case at QoS {qos}"
        );
    }
    assert!(cases.iter().any(|l| l.contains(" dup=1 ")));
    assert!(cases.iter().any(|l| l.contains(" ret=1 ")));

    // And the API contract must be broken in BOTH directions somewhere: the
    // header serializer's reported size and the bytes it wrote are equal only
    // while the contract holds, and a corpus that always keeps it cannot tell
    // the two numbers apart.
    let contract: Vec<&str> = TRACE
        .lines()
        .filter(|l| l.starts_with("contract "))
        .collect();
    assert!(
        contract.iter().any(|l| l.contains("rl=16")),
        "no case hands the serializer a remaining length LARGER than the packet"
    );
    assert!(
        contract.iter().any(|l| l.contains("rl=8 ")),
        "no case hands it one SMALLER than the packet"
    );
    assert!(
        contract.iter().any(|l| l.contains("hdr BadParameter")),
        "no case goes under the payload length, which is the one shape the          header serializer refuses outright"
    );
}

/// The three serializers' outputs are prefixes of each other, read off the
/// trace.
///
/// The per-function comparison proves each one matches the C. This proves the
/// three agree with **each other**, which no single-function differential can —
/// the same shape as the packet-size calculators' cross-slice test, inside one
/// module.
#[test]
fn the_three_outputs_nest_in_the_trace() {
    let mut checked = 0usize;

    for line in TRACE.lines().filter(|l| l.starts_with("case ")) {
        let bytes = |marker: &str| -> Option<String> {
            let after = line.split(marker).nth(1)?;
            after
                .split_whitespace()
                .find_map(|f| f.strip_prefix("bytes="))
                .map(str::to_owned)
        };

        let (Some(short), Some(middle), Some(long)) = (
            bytes("| notopic Success"),
            bytes("| hdr Success"),
            bytes("| full Success"),
        ) else {
            continue;
        };

        assert!(
            middle.starts_with(&short),
            "the short header is not a prefix of the middle one:\n  {line}"
        );
        assert!(
            long.starts_with(&middle),
            "the middle header is not a prefix of the whole packet:\n  {line}"
        );

        checked += 1;
    }

    assert!(
        checked >= 8,
        "only {checked} cases have all three outputs to compare"
    );
}
