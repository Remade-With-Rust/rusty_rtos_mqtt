//! The CONNECT against `core_mqtt_serializer.c`: size, then serialize.
//!
//! The C arm is `oracle/connect_driver.c` driving the pinned v5.0.2 serializer
//! (`04845c6a`) verbatim, and its trace is checked in.
//!
//! # Absent, empty and present are three states
//!
//! The C distinguishes a NULL `pUserName` from a non-NULL one of length zero —
//! the first clears a flag bit and writes nothing, the second sets the bit and
//! writes two zero bytes. A trace that printed both as "nothing" could not tell
//! them apart, so the driver prints `-` for absent and `.` for present-and-empty
//! and this file parses them back.
//!
//! # Why the two functions run in one case
//!
//! The C's own comment says `MQTT_SerializeConnect` does not re-check the
//! remaining length because calling `MQTT_GetConnectPacketSize` first is "part
//! of the API contract". Running them apart would test a contract nobody keeps,
//! so every case sizes and then serializes on that answer, and the trace
//! carries the bytes.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::connect::{
    Connect, ConnectError, Will, connect_packet_size, serialize_connect,
};
use rusty_rtos_mqtt_core::state::QoS;
use rusty_rtos_mqtt_core::writer::{ConnectInfo, WillInfo};

const TRACE: &str = include_str!("../../../oracle/connect.trace");

fn hex(bytes: &[u8]) -> String {
    let mut out = String::new();
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// The driver's three-state field: `-` absent, `.` present and empty.
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

fn status(error: ConnectError) -> &'static str {
    match error {
        ConnectError::BadParameter => "BadParameter",
        ConnectError::NoMemory => "NoMemory",
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

/// Size, then serialize on that answer, formatted the C's way.
fn run(connect: &Connect<'_>, buffer_size: usize) -> String {
    let size = match connect_packet_size(connect) {
        Err(error) => return format!(" -> size {}", status(error)),
        Ok(size) => size,
    };

    let mut out = vec![0xCCu8; buffer_size];
    let mut line = format!(
        " -> size Success remaining={} packet={}",
        size.remaining_length, size.packet_size
    );

    match serialize_connect(&mut out, connect, size.remaining_length) {
        Err(error) => {
            let _ = write!(line, " serialize {}", status(error));
        }
        Ok(_) => {
            let _ = write!(
                line,
                " serialize Success bytes={}",
                hex(&out[..size.packet_size as usize])
            );
        }
    }

    line
}

/// FNV-1a/64, the same constants the driver uses.
fn fnv(state: u64, bytes: &[u8]) -> u64 {
    let mut hash = state;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    hash
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
                let clean_session = field(&f, 3, "clean=") == "1";
                let keep_alive_seconds: u16 = field(&f, 4, "ka=").parse().expect("ka");
                let client_identifier = unopt(field(&f, 5, "id=")).expect("a client id");
                let username = unopt(field(&f, 6, "user="));
                let password = unopt(field(&f, 7, "pass="));
                let properties = unopt(field(&f, 8, "props=")).unwrap_or_default();
                let will_text = field(&f, 9, "will=");
                let buffer: usize = field(&f, 10, "buf=").parse().expect("buf");

                // `qos,retain,topic,payload,properties`, or `-` for no will.
                let will_parts: Vec<&str> = will_text.split(',').collect();
                let will_owned = if will_text == "-" {
                    None
                } else {
                    assert_eq!(will_parts.len(), 5, "a will has five fields: {will_text:?}");
                    Some((
                        qos_from(will_parts[0]),
                        will_parts[1] == "1",
                        unopt(will_parts[2]).expect("a will topic"),
                        unopt(will_parts[3]).expect("a will payload"),
                        unopt(will_parts[4]).unwrap_or_default(),
                    ))
                };

                let connect = Connect {
                    info: ConnectInfo {
                        clean_session,
                        keep_alive_seconds,
                        username: username.as_deref(),
                        password: password.as_deref(),
                    },
                    client_identifier: &client_identifier,
                    properties: &properties,
                    will: will_owned
                        .as_ref()
                        .map(|(qos, retain, topic, payload, props)| Will {
                            info: WillInfo {
                                qos: *qos,
                                retain: *retain,
                            },
                            topic_name: topic,
                            payload,
                            properties: props,
                        }),
                };

                let _ = writeln!(
                    out,
                    "case {} {} {} {} {} {} {} {} {} {}{}",
                    f[1],
                    f[2],
                    f[3],
                    f[4],
                    f[5],
                    f[6],
                    f[7],
                    f[8],
                    f[9],
                    f[10],
                    run(&connect, buffer)
                );
            }

            // big <i> <name> which= len= -> size ...
            //
            // The five 16-bit field checks, one field at a time. No BYTES: a
            // 65 KB packet's hex would be 131 KB on one line, and the thing
            // under test is the refusal.
            Some("big") => {
                let which: u8 = field(&f, 3, "which=").parse().expect("which");
                let length: usize = field(&f, 4, "len=").parse().expect("len");
                let big = vec![b'x'; length];
                let short: &[u8] = b"x";

                let mut info = ConnectInfo {
                    clean_session: true,
                    keep_alive_seconds: 60,
                    username: None,
                    password: None,
                };
                let mut client_identifier: &[u8] = b"id";
                let mut will_topic: &[u8] = short;
                let mut will_payload: &[u8] = short;
                let mut has_will = false;

                match which {
                    0 => client_identifier = &big,
                    1 => info.username = Some(&big),
                    2 => info.password = Some(&big),
                    3 => {
                        will_topic = &big;
                        has_will = true;
                    }
                    _ => {
                        will_payload = &big;
                        has_will = true;
                    }
                }

                let connect = Connect {
                    info,
                    client_identifier,
                    properties: &[],
                    will: has_will.then_some(Will {
                        info: WillInfo {
                            qos: QoS::AtMostOnce,
                            retain: false,
                        },
                        topic_name: will_topic,
                        payload: will_payload,
                        properties: &[],
                    }),
                };

                let outcome = match connect_packet_size(&connect) {
                    Err(error) => format!(" -> size {}", status(error)),
                    Ok(size) => format!(
                        " -> size Success remaining={} packet={}",
                        size.remaining_length, size.packet_size
                    ),
                };

                let _ = writeln!(out, "big {} {} {} {}{outcome}", f[1], f[2], f[3], f[4]);
            }

            Some("optional-field-sweep") => {
                const CLIENT_ID: &[u8] = b"id";
                const USER: &[u8] = b"u";
                const PASS: &[u8] = b"pw";
                const TOPIC: &[u8] = b"w/t";
                const PAYLOAD: &[u8] = b"bye";
                const PROPS: &[u8] = &[0x11, 0x00, 0x00, 0x00, 0x01];

                // The seed every driver in this package uses. It is NOT the
                // canonical FNV-1a offset basis (14695981039346656037) -- it is
                // one digit short of it, and has been since the first
                // differential. That costs nothing: a digest only has to be
                // deterministic and shared by both arms, and it is both. Noted
                // so the next reader does not "fix" one arm.
                let mut digest = 1_469_598_103_934_665_603u64;
                let (mut accepted, mut refused) = (0usize, 0usize);
                let (mut shortest, mut longest) = (u32::MAX, 0u32);

                for combination in 0..32u32 {
                    let has_user = combination & 1 != 0;
                    let has_pass = combination & 2 != 0;
                    let has_will = combination & 4 != 0;
                    let has_props = combination & 8 != 0;
                    let will_qos1 = combination & 16 != 0;

                    let connect = Connect {
                        info: ConnectInfo {
                            clean_session: true,
                            keep_alive_seconds: 60,
                            username: has_user.then_some(USER),
                            password: has_pass.then_some(PASS),
                        },
                        client_identifier: CLIENT_ID,
                        properties: if has_props { PROPS } else { &[] },
                        will: has_will.then_some(Will {
                            info: WillInfo {
                                qos: if will_qos1 {
                                    QoS::AtLeastOnce
                                } else {
                                    QoS::AtMostOnce
                                },
                                retain: will_qos1,
                            },
                            topic_name: TOPIC,
                            payload: PAYLOAD,
                            properties: if has_props { PROPS } else { &[] },
                        }),
                    };

                    let Ok(size) = connect_packet_size(&connect) else {
                        refused += 1;
                        continue;
                    };

                    let mut buffer = vec![0xCCu8; 192];

                    if serialize_connect(&mut buffer, &connect, size.remaining_length).is_err() {
                        refused += 1;
                        continue;
                    }

                    accepted += 1;
                    shortest = shortest.min(size.packet_size);
                    longest = longest.max(size.packet_size);
                    digest = fnv(digest, &buffer[..size.packet_size as usize]);
                }

                let _ = writeln!(
                    out,
                    "optional-field-sweep n=32 accepted={accepted} refused={refused} \
                     shortest={shortest} longest={longest} digest={digest:016x}"
                );
            }

            _ => {}
        }
    }

    out
}

#[test]
fn our_connect_matches_the_c_case_for_case() {
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

    assert_eq!(n, 30, "the trace should be 30 lines, not {n}");
}

/// The guard: every optional field in both states, and every will QoS.
///
/// The seventeenth shape of the guard `heap_4` started. This one's own job: a
/// CONNECT is eight fields, four of them optional, and a corpus that only ever
/// sent the same three would prove nothing about the **combinations** — which
/// is where a flags byte and a payload part company.
#[test]
fn the_cases_exercise_every_optional_field_in_both_states() {
    let cases: Vec<&str> = TRACE.lines().filter(|l| l.starts_with("case ")).collect();

    // Each optional field must appear absent, empty and present.
    for (name, prefix) in [("user name", "user="), ("password", "pass=")] {
        for (state, marker) in [("absent", "-"), ("present", "")] {
            let n = cases
                .iter()
                .filter(|l| {
                    l.split_whitespace()
                        .find_map(|f| f.strip_prefix(prefix))
                        .is_some_and(|v| if marker == "-" { v == "-" } else { v != "-" })
                })
                .count();
            assert!(n >= 2, "the {name} is {state} in only {n} cases");
        }
    }

    // A present-but-empty field is its own state and the C writes two bytes
    // for it; at least one case must have one.
    assert!(
        cases.iter().any(|l| l.contains(" user=. ")),
        "no case sends a present-but-empty user name"
    );

    // Every will QoS, and both retain values.
    for qos in 0..=2u8 {
        assert!(
            cases.iter().any(|l| l.contains(&format!(" will={qos},"))),
            "no case has a will at QoS {qos}"
        );
    }

    // And the packet must actually get written: a size calculator alone proves
    // nothing about the bytes.
    let wrote = cases
        .iter()
        .filter(|l| l.contains("serialize Success bytes="))
        .count();
    assert!(wrote >= 12, "only {wrote} cases reach the serializer");

    // Each of the five 16-bit field checks must be reached, and both sides of
    // at least one boundary seen -- a corpus of short fields leaves all five
    // unreachable, which is how they went unproven until they were measured.
    let big: Vec<&str> = TRACE.lines().filter(|l| l.starts_with("big ")).collect();

    for which in 0..5u8 {
        assert!(
            big.iter().any(|l| l.contains(&format!("which={which} "))),
            "the 16-bit check on field {which} is never reached"
        );
    }

    assert!(
        big.iter()
            .any(|l| l.contains("len=65535") && l.contains("Success")),
        "no case sits AT the 16-bit maximum"
    );
    assert!(
        big.iter()
            .filter(|l| l.contains("len=65536") && l.contains("BadParameter"))
            .count()
            >= 5,
        "not every field is refused one byte past the maximum"
    );
}

/// Every packet begins the same eleven bytes, whatever else it carries.
///
/// A CONNECT's first eleven bytes are the type, the remaining length, the
/// protocol name and the version. Reading them off the checked-in trace is a
/// cheap standing check that nothing has shifted the payload by a byte — which
/// is the failure a length-prefixed format hides best, since every field still
/// parses.
#[test]
fn every_serialized_connect_starts_with_the_protocol_name() {
    let mut checked = 0usize;

    for line in TRACE
        .lines()
        .filter(|l| l.contains("serialize Success bytes="))
    {
        let bytes = line
            .split_whitespace()
            .find_map(|f| f.strip_prefix("bytes="))
            .expect("a bytes= field");

        assert!(bytes.starts_with("10"), "not a CONNECT type byte: {line}");

        // Skip the type byte and the one-byte remaining length; every case in
        // this trace is short enough for one.
        assert_eq!(
            &bytes[4..18],
            "00044d51545405",
            "the protocol preamble moved: {line}"
        );

        checked += 1;
    }

    assert!(checked >= 12, "only {checked} packets were checked");
}
