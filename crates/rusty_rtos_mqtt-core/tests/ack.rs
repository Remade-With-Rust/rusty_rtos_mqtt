//! The acknowledgement deserializers against `core_mqtt_serializer.c`.
//!
//! The C arm is `oracle/ack_driver.c` driving the pinned v5.0.2 serializer
//! (`04845c6a`) verbatim, and its trace is checked in.
//!
//! # Every case carries its own bytes
//!
//! The trace prints the type byte, the claimed remaining length, the body in
//! hex, the maximum packet size and the request-problem flag, so this file
//! replays it rather than keeping a second copy of the C's table. The only
//! thing that could drift is the format itself, and a format change fails the
//! parse rather than silently testing something else.
//!
//! # And every case's claim equals its body
//!
//! The driver refuses to run a case whose `remainingLength` exceeds its body,
//! because the C would then index past its own buffer and its answer would
//! depend on what happened to be next in memory. The over-claim — which is the
//! attack — is pinned on this side alone, by
//! `a_claim_larger_than_the_buffer_is_refused` in `src/ack.rs`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::ack::{AckError, AckInfo, Limits, PacketInfo, deserialize_ack};

const TRACE: &str = include_str!("../../../oracle/ack.trace");

/// The C's `put_hex`: a dash for nothing, lowercase hex otherwise.
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

/// A field written `name=value`, checked rather than assumed.
fn field<'a>(fields: &[&'a str], index: usize, name: &str) -> &'a str {
    fields[index].strip_prefix(name).unwrap_or_else(|| {
        panic!(
            "field {index} should start with {name:?}, got {:?}",
            fields[index]
        )
    })
}

/// The C's status names, and what it prints after a successful one.
fn outcome(result: Result<AckInfo<'_>, AckError>) -> String {
    match result {
        Err(AckError::BadParameter) => " -> BadParameter".to_owned(),
        Err(AckError::BadResponse) => " -> BadResponse".to_owned(),
        Ok(ack) => {
            let id = match ack.packet_id {
                Some(id) => id.to_string(),
                None => "-".to_owned(),
            };
            format!(
                " -> Success id={id} rc={} props={}",
                hex(ack.reason_codes),
                hex(ack.properties)
            )
        }
    }
}

/// Run one `type=... rl=... max=... rp=... in=...` group.
fn replay(fields: &[&str], first: usize) -> String {
    let packet_type = u8::from_str_radix(field(fields, first, "type="), 16).expect("a type byte");
    let remaining_length: u32 = field(fields, first + 1, "rl=").parse().expect("rl");
    let max_packet_size: u32 = field(fields, first + 2, "max=").parse().expect("max");
    let request_problem_info = field(fields, first + 3, "rp=") == "1";
    let body = unhex(field(fields, first + 4, "in="));

    let packet = PacketInfo {
        packet_type,
        remaining_length,
        remaining_data: &body,
    };
    let limits = Limits {
        max_packet_size,
        request_problem_info,
    };

    outcome(deserialize_ack(&packet, &limits))
}

/// Sweep one byte position and report the accepted set, the C's way.
fn sweep<F>(mut build: F) -> (String, usize, usize, usize)
where
    F: FnMut(u8) -> (PacketInfo<'static>, Limits),
{
    // The bodies are built into a leaked box so the `PacketInfo` can borrow for
    // `'static`; a sweep of 256 four-byte buffers is not worth threading a
    // lifetime through, and the test process is about to exit anyway.
    let mut accepted = String::new();
    let (mut n, mut bad_param, mut bad_response) = (0usize, 0usize, 0usize);

    for value in 0..=255u8 {
        let (packet, limits) = build(value);

        match deserialize_ack(&packet, &limits) {
            Ok(_) => {
                if n > 0 {
                    accepted.push(',');
                }
                let _ = write!(accepted, "{value:02x}");
                n += 1;
            }
            Err(AckError::BadParameter) => bad_param += 1,
            Err(AckError::BadResponse) => bad_response += 1,
        }
    }

    (accepted, n, bad_param, bad_response)
}

fn body(bytes: Vec<u8>) -> &'static [u8] {
    Box::leak(bytes.into_boxed_slice())
}

fn our_trace() -> String {
    let mut out = String::new();

    for line in TRACE.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();

        match f.first().copied() {
            Some("geometry" | "end") => {
                let _ = writeln!(out, "{line}");
            }

            // case <i> <name> type= rl= max= rp= in= -> ...
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

            // long <name> type= rl= max= rp= in= -> ...
            Some("long") => {
                let _ = writeln!(
                    out,
                    "long {} {} {} {} {} {}{}",
                    f[1],
                    f[2],
                    f[3],
                    f[4],
                    f[5],
                    f[6],
                    replay(&f, 2)
                );
            }

            Some("suback-status-sweep") => {
                let (accepted, n, bad_param, bad_response) = sweep(|value| {
                    (
                        PacketInfo {
                            packet_type: 0x90,
                            remaining_length: 4,
                            remaining_data: body(vec![0x00, 0x2A, 0x00, value]),
                        },
                        Limits {
                            max_packet_size: 1024,
                            request_problem_info: true,
                        },
                    )
                });

                let _ = writeln!(
                    out,
                    "suback-status-sweep accepted={accepted} n={n} refused={}",
                    bad_param + bad_response
                );
            }

            Some("ack-reason-sweep") => {
                let packet_type = match f[1] {
                    "puback" => 0x40,
                    "pubrel" => 0x62,
                    other => panic!("the trace names a sweep this file does not know: {other:?}"),
                };

                let (accepted, n, bad_param, bad_response) = sweep(|value| {
                    (
                        PacketInfo {
                            packet_type,
                            remaining_length: 3,
                            remaining_data: body(vec![0x00, 0x2A, value]),
                        },
                        Limits {
                            max_packet_size: 1024,
                            request_problem_info: true,
                        },
                    )
                });

                let _ = writeln!(
                    out,
                    "ack-reason-sweep {} accepted={accepted} n={n} refused={}",
                    f[1],
                    bad_param + bad_response
                );
            }

            Some("type-sweep") => {
                let (accepted, n, bad_param, bad_response) = sweep(|value| {
                    (
                        PacketInfo {
                            packet_type: value,
                            remaining_length: 4,
                            remaining_data: body(vec![0x00, 0x2A, 0x00, 0x00]),
                        },
                        Limits {
                            max_packet_size: 1024,
                            request_problem_info: true,
                        },
                    )
                });

                let _ = writeln!(
                    out,
                    "type-sweep accepted={accepted} n={n} badparam={bad_param} badresponse={bad_response}"
                );
            }

            _ => {}
        }
    }

    out
}

#[test]
fn our_ack_deserializers_match_the_c_case_for_case() {
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

    assert_eq!(n, 57, "the trace should be 57 lines, not {n}");
}

/// The guard: the workload must reach every outcome and refuse most of it.
///
/// The thirteenth shape of the guard `heap_4` started. A deserializer whose
/// corpus it accepts wholesale is not deciding anything — and this one's whole
/// job is to refuse, since everything it reads was chosen by the other end.
#[test]
fn the_cases_reach_every_outcome_and_mostly_refuse() {
    let cases: Vec<&str> = TRACE
        .lines()
        .filter(|l| l.starts_with("case ") || l.starts_with("long "))
        .collect();

    let success = cases.iter().filter(|l| l.contains("-> Success")).count();
    let bad_response = cases.iter().filter(|l| l.ends_with("BadResponse")).count();
    let bad_parameter = cases.iter().filter(|l| l.ends_with("BadParameter")).count();

    assert!(success >= 15, "only {success} cases succeed");
    assert!(bad_response >= 15, "only {bad_response} are BadResponse");
    assert!(
        bad_parameter >= 2,
        "only {bad_parameter} are BadParameter -- the two reachable caller \
         errors, a CONNACK and a zero maximum, must both be exercised"
    );

    // And the three sweeps must each refuse far more than they accept: they are
    // one-byte tables with ten or twelve entries out of 256.
    for line in TRACE.lines().filter(|l| l.contains("accepted=")) {
        let accepted: usize = line
            .split_whitespace()
            .find_map(|f| f.strip_prefix("n="))
            .expect("an n= field")
            .parse()
            .expect("a count");

        assert!(
            (1..=20).contains(&accepted),
            "a one-byte sweep accepting {accepted} of 256 is not a table any \
             more: {line}"
        );
    }
}

/// The three places coreMQTT and MQTT 5.0 disagree, read straight off the
/// trace.
///
/// These are findings, not test fixtures: each is written up in
/// `docs/upstream/` for the owner to file. Asserting them from the CHECKED-IN
/// trace rather than from our own code means the day the pinned oracle changes
/// its mind, this test says so.
#[test]
fn the_trace_records_three_divergences_from_the_specification() {
    let suback_table = TRACE
        .lines()
        .find(|l| l.starts_with("suback-status-sweep"))
        .expect("the suback sweep");

    // 0x11, "No subscription existed", is a legal UNSUBACK reason code and is
    // not in the table -- because SUBACK and UNSUBACK share one.
    assert!(
        !suback_table.contains(",11") && !suback_table.contains("=11"),
        "0x11 is now accepted; the shared-table divergence may be fixed: \
         {suback_table}"
    );

    // And that same shared table takes granted-QoS bytes, which an UNSUBACK
    // cannot grant.
    assert!(suback_table.contains("01,02"), "{suback_table}");

    // A SUBACK with no reason codes at all is accepted, though MQTT 5.0 §3.9
    // requires one per topic filter. The C derives the count by subtraction
    // and never checks it is non-zero.
    assert!(
        TRACE
            .lines()
            .any(|l| l.contains("suback-zero-reason-codes") && l.contains("-> Success")),
        "a SUBACK with zero reason codes is no longer accepted"
    );
}
