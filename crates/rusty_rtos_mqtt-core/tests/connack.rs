//! The CONNACK deserializer against `core_mqtt_serializer.c`.
//!
//! The C arm is `oracle/connack_driver.c` driving the pinned v5.0.2 serializer
//! (`04845c6a`) verbatim, and its trace is checked in.
//!
//! # Five property sweeps, not one
//!
//! The property identifier is one byte, so it is enumerable — but a single
//! fixed body cannot sweep it, because each identifier introduces a value of a
//! different **width** and a body sized for one is malformed for the others.
//! Both arms would answer `BadResponse` for the wrong reason and the sweep
//! would discriminate nothing.
//!
//! So it is swept five times, once per value shape, and each prints its own
//! accepted set. Together the five sets are the property table *and* say which
//! identifier is which type — which is what a reader needs to check it against
//! MQTT 5.0 §3.2.2.3.
//!
//! # Values on Success AND on ServerRefused
//!
//! A non-zero reason code makes the C return `MQTTServerRefused` and parse the
//! property section anyway, because the Reason String that says *why* lives in
//! it. So both statuses carry out-parameters, and both print them.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::ack::PacketInfo;
use rusty_rtos_mqtt_core::connack::{ClientSettings, ConnAck, ConnAckError, deserialize_connack};

const TRACE: &str = include_str!("../../../oracle/connack.trace");

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

/// The C's status names, and everything it prints after a parsed one.
fn outcome(result: Result<ConnAck<'_>, ConnAckError>) -> String {
    match result {
        Err(ConnAckError::BadParameter) => " -> BadParameter".to_owned(),
        Err(ConnAckError::BadResponse) => " -> BadResponse".to_owned(),
        Ok(connack) => {
            // The C's third status. Both arms of it fill the out-parameters.
            let status = if connack.refused() {
                "ServerRefused"
            } else {
                "Success"
            };
            let s = &connack.server;

            format!(
                " -> {status} sp={} fields={:08x} exp={} rmax={} qos={} ret={} mps={} \
                 alias={} wild={} subid={} shared={} ka={} props={}",
                u8::from(connack.session_present),
                connack.fields_present,
                s.session_expiry,
                s.receive_max,
                s.max_qos,
                s.retain_available,
                s.max_packet_size,
                s.topic_alias_max,
                s.wildcard_available,
                s.subscription_id_available,
                s.shared_available,
                s.keep_alive,
                hex(connack.properties)
            )
        }
    }
}

fn replay(fields: &[&str], first: usize) -> String {
    let packet_type = u8::from_str_radix(field(fields, first, "type="), 16).expect("a type byte");
    let remaining_length: u32 = field(fields, first + 1, "rl=").parse().expect("rl");
    let max_packet_size: u32 = field(fields, first + 2, "max=").parse().expect("max");
    let request_response_info = field(fields, first + 3, "rri=") == "1";
    let body = unhex(field(fields, first + 4, "in="));

    let packet = PacketInfo {
        packet_type,
        remaining_length,
        remaining_data: &body,
    };
    let client = ClientSettings {
        max_packet_size,
        request_response_info,
    };

    outcome(deserialize_connack(&packet, &client))
}

fn call(body: &[u8]) -> Result<ConnAck<'_>, ConnAckError> {
    deserialize_connack(
        &PacketInfo {
            packet_type: 0x20,
            remaining_length: u32::try_from(body.len()).unwrap(),
            remaining_data: body,
        },
        &ClientSettings {
            max_packet_size: 1024,
            request_response_info: true,
        },
    )
}

/// The property section for one identifier at one value shape.
fn property_body(id: u8, value: &[u8]) -> Vec<u8> {
    let section_length = value.len() + 1;
    let mut body = vec![0x00, 0x00, u8::try_from(section_length).unwrap(), id];
    body.extend_from_slice(value);
    body
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

            Some("reason-code-sweep") => {
                let mut accepted = String::new();
                let (mut success, mut refused, mut rejected) = (0usize, 0usize, 0usize);

                for code in 0..=255u8 {
                    match call(&[0x00, code, 0x00]) {
                        Ok(connack) => {
                            if !accepted.is_empty() {
                                accepted.push(',');
                            }
                            let _ = write!(accepted, "{code:02x}");

                            if connack.refused() {
                                refused += 1;
                            } else {
                                success += 1;
                            }
                        }
                        Err(_) => rejected += 1,
                    }
                }

                let _ = writeln!(
                    out,
                    "reason-code-sweep accepted={accepted} n={} success={success} \
                     refused={refused} rejected={rejected}",
                    success + refused
                );
            }

            Some("property-sweep") => {
                // The shapes the driver sweeps, keyed by the label it prints.
                let value: &[u8] = match f[1] {
                    "one-byte" => &[0x00],
                    "two-byte" => &[0x00, 0x01],
                    "four-byte" => &[0x00, 0x00, 0x00, 0x01],
                    "string" => &[0x00, 0x01, b'x'],
                    "user-property" => &[0x00, 0x01, b'k', 0x00, 0x01, b'v'],
                    other => panic!("the trace names a shape this file does not know: {other:?}"),
                };

                let mut accepted = String::new();
                let (mut n, mut rejected) = (0usize, 0usize);

                for id in 0..=255u8 {
                    let body = property_body(id, value);

                    if call(&body).is_ok() {
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
fn our_connack_deserializer_matches_the_c_case_for_case() {
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

/// The guard: every status must appear, and the sweeps must stay tables.
///
/// The fourteenth shape of the guard `heap_4` started. This one has an extra
/// job: **`ServerRefused` is a third status** and a workload that never
/// produced it would leave a whole branch of the C untested while every line
/// still matched.
#[test]
fn the_cases_reach_every_status_including_the_third() {
    let cases: Vec<&str> = TRACE.lines().filter(|l| l.starts_with("case ")).collect();

    let success = cases.iter().filter(|l| l.contains("-> Success")).count();
    let server_refused = cases
        .iter()
        .filter(|l| l.contains("-> ServerRefused"))
        .count();
    let bad_response = cases
        .iter()
        .filter(|l| l.contains("-> BadResponse"))
        .count();
    let bad_parameter = cases
        .iter()
        .filter(|l| l.contains("-> BadParameter"))
        .count();

    assert!(success >= 20, "only {success} cases succeed");
    assert!(bad_response >= 15, "only {bad_response} are BadResponse");
    assert!(bad_parameter >= 2, "only {bad_parameter} are BadParameter");
    assert!(
        server_refused >= 1,
        "no case produces ServerRefused -- the C's third status, and the whole \
         reason a refused connection still parses its properties"
    );

    // Each sweep is a one-byte table, so it must accept a handful of 256.
    for line in TRACE.lines().filter(|l| l.contains("accepted=")) {
        let accepted: usize = line
            .split_whitespace()
            .find_map(|f| f.strip_prefix("n="))
            .expect("an n= field")
            .parse()
            .expect("a count");

        assert!(
            (1..=25).contains(&accepted),
            "a one-byte sweep accepting {accepted} of 256 is not a table any more: {line}"
        );
    }
}

/// **This time the tables ARE the specification**, and that is the result.
///
/// The [`ack`](rusty_rtos_mqtt_core::ack) slice swept the same shape and found
/// three divergences from MQTT 5.0. These two were swept the same way and
/// match §3.2.2.2 and §3.2.2.3 exactly. A sweep that confirms conformance is a
/// result, and asserting it from the CHECKED-IN TRACE is what makes it one: if
/// the pinned oracle ever drifts from the specification, this fails.
#[test]
fn the_connack_tables_are_exactly_the_specification() {
    // §3.2.2.2, the 22 CONNACK Reason Codes.
    let reason = TRACE
        .lines()
        .find(|l| l.starts_with("reason-code-sweep"))
        .expect("the reason-code sweep");
    let accepted = reason
        .split_whitespace()
        .find_map(|f| f.strip_prefix("accepted="))
        .expect("an accepted= field");

    assert_eq!(
        accepted, "00,80,81,82,83,84,85,86,87,88,89,8a,8c,90,95,97,99,9a,9b,9c,9d,9f",
        "the CONNACK reason-code table is no longer MQTT 5.0 §3.2.2.2"
    );

    // Exactly one of them is an acceptance; the other 21 are refusals with a
    // reason, which is the point of the third status.
    assert!(reason.contains("success=1 refused=21"), "{reason}");

    // §3.2.2.3, the 17 CONNACK properties — and which type each one is, which
    // is the part a single sweep could not have told anybody.
    let expected = [
        ("one-byte", "24,25,28,29,2a"),
        ("two-byte", "13,21,22"),
        ("four-byte", "11,27"),
        ("string", "12,15,16,1a,1c,1f"),
        ("user-property", "26"),
    ];

    let mut total = 0usize;

    for (shape, set) in expected {
        let line = TRACE
            .lines()
            .find(|l| l.starts_with(&format!("property-sweep {shape} ")))
            .unwrap_or_else(|| panic!("no {shape} sweep in the trace"));

        assert!(
            line.contains(&format!("accepted={set} ")),
            "the {shape} property set changed: {line}"
        );
        total += set.split(',').count();
    }

    assert_eq!(
        total, 17,
        "MQTT 5.0 §3.2.2.3 lists seventeen CONNACK properties"
    );
}
