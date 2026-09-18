//! The DISCONNECT, both directions, against `core_mqtt_serializer.c`.
//!
//! The C arm is `oracle/disconnect_driver.c` driving the pinned v5.0.2
//! serializer (`04845c6a`) verbatim, and its trace is checked in.
//!
//! # Why both directions are one differential
//!
//! `validateDisconnectResponse` takes an `incoming` flag and answers
//! differently for the same byte, so the two directions are not two independent
//! things to test — they are **one table read twice**, and the interesting
//! assertion is about the difference between the two readings. The reason code
//! is therefore swept 256 times in each direction and both accepted sets are
//! printed.
//!
//! # The outgoing arm sizes and then serializes
//!
//! Each outgoing case runs `MQTT_GetDisconnectPacketSize` and then
//! `MQTT_SerializeDisconnect` on its answer, and prints the bytes. The
//! two-slice agreement the packet-size calculators introduced as a separate
//! test is carried by the trace itself here.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::ack::PacketInfo;
use rusty_rtos_mqtt_core::disconnect::{
    DisconnectError, deserialize_disconnect, disconnect_packet_size, serialize_disconnect,
    validate_outgoing_properties,
};

const TRACE: &str = include_str!("../../../oracle/disconnect.trace");

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

fn status(error: DisconnectError) -> &'static str {
    match error {
        DisconnectError::BadParameter => "BadParameter",
        DisconnectError::BadResponse => "BadResponse",
        DisconnectError::NoMemory => "NoMemory",
    }
}

fn read(body: &[u8], max_packet_size: u32) -> String {
    let packet = PacketInfo {
        packet_type: 0xE0,
        remaining_length: u32::try_from(body.len()).unwrap(),
        remaining_data: body,
    };

    match deserialize_disconnect(&packet, max_packet_size) {
        Err(error) => format!(" -> {}", status(error)),
        Ok(disconnect) => {
            let reason = match disconnect.reason_code {
                Some(code) => format!("{code:02x}"),
                None => "-".to_owned(),
            };
            format!(
                " -> Success rc={reason} props={}",
                hex(disconnect.properties)
            )
        }
    }
}

/// One outgoing case: size, then serialize on that answer.
fn write_one(
    reason_code: Option<u8>,
    properties: &[u8],
    max_packet_size: u32,
    buffer_size: usize,
) -> String {
    let length = u32::try_from(properties.len()).unwrap();

    let size = match disconnect_packet_size(reason_code, length, max_packet_size) {
        Err(error) => return format!(" -> size {}", status(error)),
        Ok(size) => size,
    };

    let mut out = vec![0xCCu8; buffer_size];
    let mut line = format!(
        " -> size Success remaining={} packet={}",
        size.remaining_length, size.packet_size
    );

    match serialize_disconnect(&mut out, reason_code, properties, size.remaining_length) {
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

/// The five value shapes both sweeps use, keyed by the driver's label.
fn shape(label: &str) -> &'static [u8] {
    match label {
        "one-byte" => &[0x00],
        "two-byte" => &[0x00, 0x01],
        "four-byte" => &[0x00, 0x00, 0x00, 0x01],
        "string" => &[0x00, 0x01, b'x'],
        "user-property" => &[0x00, 0x01, b'k', 0x00, 0x01, b'v'],
        other => panic!("the trace names a shape this file does not know: {other:?}"),
    }
}

fn our_trace() -> String {
    let mut out = String::new();

    for line in TRACE.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();

        match f.first().copied() {
            Some("geometry" | "end") => {
                let _ = writeln!(out, "{line}");
            }

            // in <i> <name> rl= max= in= -> ...
            Some("in") => {
                let max: u32 = field(&f, 4, "max=").parse().expect("max");
                let body = unhex(field(&f, 5, "in="));

                let _ = writeln!(
                    out,
                    "in {} {} {} {} {}{}",
                    f[1],
                    f[2],
                    f[3],
                    f[4],
                    f[5],
                    read(&body, max)
                );
            }

            // out <i> <name> rc= proplen= max= buf= props= -> ...
            Some("out") => {
                let reason = field(&f, 3, "rc=");
                let reason_code = if reason == "-" {
                    None
                } else {
                    Some(u8::from_str_radix(reason, 16).expect("a reason code"))
                };
                let max: u32 = field(&f, 5, "max=").parse().expect("max");
                let buffer: usize = field(&f, 6, "buf=").parse().expect("buf");
                let properties = unhex(field(&f, 7, "props="));

                let _ = writeln!(
                    out,
                    "out {} {} {} {} {} {} {}{}",
                    f[1],
                    f[2],
                    f[3],
                    f[4],
                    f[5],
                    f[6],
                    f[7],
                    write_one(reason_code, &properties, max, buffer)
                );
            }

            // val <i> <name> connexp= props= -> ...
            Some("val") => {
                let expiry: u32 = field(&f, 3, "connexp=").parse().expect("connexp");
                let properties = unhex(field(&f, 4, "props="));

                let outcome = match validate_outgoing_properties(expiry, &properties) {
                    Ok(()) => " -> Success".to_owned(),
                    Err(error) => format!(" -> {}", status(error)),
                };

                let _ = writeln!(out, "val {} {} {} {}{}", f[1], f[2], f[3], f[4], outcome);
            }

            Some("reason-sweep") => {
                let incoming = f[1] == "incoming";
                let mut accepted = String::new();
                let (mut n, mut rejected) = (0usize, 0usize);

                for code in 0..=255u8 {
                    let ok = if incoming {
                        let packet = PacketInfo {
                            packet_type: 0xE0,
                            remaining_length: 1,
                            remaining_data: &[code],
                        };
                        deserialize_disconnect(&packet, 1024).is_ok()
                    } else {
                        disconnect_packet_size(Some(code), 0, 1024).is_ok()
                    };

                    if ok {
                        if n > 0 {
                            accepted.push(',');
                        }
                        let _ = write!(accepted, "{code:02x}");
                        n += 1;
                    } else {
                        rejected += 1;
                    }
                }

                let _ = writeln!(
                    out,
                    "reason-sweep {} accepted={accepted} n={n} rejected={rejected}",
                    f[1]
                );
            }

            Some("prop-sweep") => {
                let incoming = f[1] == "incoming";
                let value = shape(f[2]);
                let mut accepted = String::new();
                let (mut n, mut rejected) = (0usize, 0usize);

                for id in 0..=255u8 {
                    let mut section = vec![id];
                    section.extend_from_slice(value);

                    let ok = if incoming {
                        // reason code, property length, then the section.
                        let mut body = vec![0x00u8, u8::try_from(section.len()).unwrap()];
                        body.extend_from_slice(&section);

                        let packet = PacketInfo {
                            packet_type: 0xE0,
                            remaining_length: u32::try_from(body.len()).unwrap(),
                            remaining_data: &body,
                        };
                        deserialize_disconnect(&packet, 1024).is_ok()
                    } else {
                        validate_outgoing_properties(3600, &section).is_ok()
                    };

                    if ok {
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
                    "prop-sweep {} {} accepted={accepted} n={n} rejected={rejected}",
                    f[1], f[2]
                );
            }

            _ => {}
        }
    }

    out
}

#[test]
fn our_disconnect_matches_the_c_case_for_case() {
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

    assert_eq!(n, 58, "the trace should be 58 lines, not {n}");
}

/// The guard: both directions, all four statuses, and bytes actually written.
///
/// The sixteenth shape of the guard `heap_4` started. Its own job here: this is
/// the first slice that runs a **size calculator and a writer in the same
/// case**, so a corpus where no case reaches the writer would leave half the
/// slice unproven while every line still matched.
#[test]
fn the_cases_cover_both_directions_and_reach_the_writer() {
    let lines: Vec<&str> = TRACE.lines().collect();

    let incoming = lines.iter().filter(|l| l.starts_with("in ")).count();
    let outgoing = lines.iter().filter(|l| l.starts_with("out ")).count();
    let validate = lines.iter().filter(|l| l.starts_with("val ")).count();

    assert!(incoming >= 15, "only {incoming} incoming cases");
    assert!(outgoing >= 10, "only {outgoing} outgoing cases");
    assert!(validate >= 5, "only {validate} validation cases");

    // Every status the module can produce.
    for name in ["Success", "BadParameter", "BadResponse", "NoMemory"] {
        assert!(
            lines.iter().any(|l| l.contains(name)),
            "no case produces {name}"
        );
    }

    // And the writer must actually run: a size calculator that is never
    // followed by a serialize proves nothing about the pair.
    let wrote = lines
        .iter()
        .filter(|l| l.contains("serialize Success bytes="))
        .count();
    assert!(wrote >= 6, "only {wrote} cases reach the writer");
}

/// One table, two directions — and the incoming one is a byte short.
///
/// The outgoing accepted set is exactly MQTT 5.0 §3.14.2.1's client column. The
/// incoming set is the server column **minus `0x9F`**, "connection rate
/// exceeded" — so a client refuses a conformant disconnection as a malformed
/// packet. Asserted from the CHECKED-IN TRACE, so the day the pinned oracle
/// changes its mind, this says so.
#[test]
fn the_incoming_reason_code_table_is_missing_one() {
    let sweep = |direction: &str| -> String {
        TRACE
            .lines()
            .find(|l| l.starts_with(&format!("reason-sweep {direction} ")))
            .unwrap_or_else(|| panic!("no {direction} sweep"))
            .split_whitespace()
            .find_map(|f| f.strip_prefix("accepted="))
            .expect("an accepted= field")
            .to_owned()
    };

    // §3.14.2.1's client column, exactly.
    assert_eq!(
        sweep("outgoing"),
        "00,04,80,81,82,83,90,93,94,95,96,97,98,99",
        "the outgoing reason-code table is no longer the specification's"
    );

    // ...and the server column, less 0x9f.
    let incoming = sweep("incoming");
    assert!(
        !incoming.contains("9f"),
        "0x9f is now accepted from a server -- the upstream defect may be \
         fixed, and the draft needs revisiting: {incoming}"
    );
    assert!(
        incoming.contains("9e,a0"),
        "0x9f's neighbours have moved too, so this is a different table: \
         {incoming}"
    );

    // The two tables are genuinely different, not one a subset of the other.
    assert!(incoming.contains("8b") && !sweep("outgoing").contains("8b"));
    assert!(sweep("outgoing").contains("04") && !incoming.contains("04"));
}

/// Each direction has its own property table, and neither contains the other.
#[test]
fn the_two_property_tables_are_different() {
    let sweep = |direction: &str, shape: &str| -> String {
        TRACE
            .lines()
            .find(|l| l.starts_with(&format!("prop-sweep {direction} {shape} ")))
            .unwrap_or_else(|| panic!("no {direction} {shape} sweep"))
            .split_whitespace()
            .find_map(|f| f.strip_prefix("accepted="))
            .expect("an accepted= field")
            .to_owned()
    };

    // Incoming: a reason string, a server reference, user properties.
    assert_eq!(sweep("incoming", "string"), "1c,1f");
    assert_eq!(sweep("incoming", "user-property"), "26");
    assert_eq!(sweep("incoming", "four-byte"), "");

    // Outgoing: a reason string, a SESSION EXPIRY, user properties.
    assert_eq!(sweep("outgoing", "string"), "1f");
    assert_eq!(sweep("outgoing", "user-property"), "26");
    assert_eq!(sweep("outgoing", "four-byte"), "11");

    // Neither is a superset: the server reference is incoming-only and the
    // session expiry outgoing-only.
    assert!(!sweep("outgoing", "string").contains("1c"));
    assert!(sweep("incoming", "four-byte").is_empty());
}
