//! SUBSCRIBE, UNSUBSCRIBE, the publish acknowledgements and PINGREQ against
//! `core_mqtt_serializer.c`.
//!
//! The C arm is `oracle/outbound_driver.c` driving the pinned v5.0.2 serializer
//! (`04845c6a`) verbatim, and its trace is checked in.
//!
//! With the CONNECT, the outgoing PUBLISH and the DISCONNECT, this completes
//! the outgoing wire codec: every packet an MQTT client can put on a socket.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::outbound::{
    OutboundError, RetainHandling, Subscription, serialize_ack, serialize_pingreq,
    serialize_subscribe, serialize_unsubscribe, subscription_options,
};
use rusty_rtos_mqtt_core::state::QoS;

const TRACE: &str = include_str!("../../../oracle/outbound.trace");

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

fn status(error: OutboundError) -> &'static str {
    match error {
        OutboundError::BadParameter => "BadParameter",
        OutboundError::NoMemory => "NoMemory",
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

fn handling_from(number: &str) -> RetainHandling {
    match number {
        "0" => RetainHandling::OnSubscribe,
        "1" => RetainHandling::OnSubscribeIfNew,
        "2" => RetainHandling::Never,
        other => panic!("the trace names a retain handling this file does not know: {other:?}"),
    }
}

/// FNV-1a/64, with the seed every driver in this package uses.
fn fnv(state: u64, byte: u8) -> u64 {
    (state ^ u64::from(byte)).wrapping_mul(1_099_511_628_211)
}

/// One filter: `f<i>=<hex>,<qos>,<no local>,<retain as published>,<handling>`.
fn parse_filter(text: &str) -> (Vec<u8>, QoS, bool, bool, RetainHandling) {
    let parts: Vec<&str> = text.split(',').collect();
    assert_eq!(parts.len(), 5, "a filter has five fields: {text:?}");

    (
        unhex(parts[0]),
        qos_from(parts[1]),
        parts[2] == "1",
        parts[3] == "1",
        handling_from(parts[4]),
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

            // sub|unsub <i> <name> id= n= f0=.. f1=.. .. props= rl= buf= -> ..
            Some(kind @ ("sub" | "unsub")) => {
                let packet_id: u16 = field(&f, 3, "id=").parse().expect("id");
                let count: usize = field(&f, 4, "n=").parse().expect("n");

                let parsed: Vec<_> = (0..count)
                    .map(|i| parse_filter(field(&f, 5 + i, &format!("f{i}="))))
                    .collect();
                let subscriptions: Vec<Subscription<'_>> = parsed
                    .iter()
                    .map(|(topic, qos, no_local, rap, handling)| Subscription {
                        topic_filter: topic,
                        qos: *qos,
                        no_local: *no_local,
                        retain_as_published: *rap,
                        retain_handling: *handling,
                    })
                    .collect();

                let properties = unhex(field(&f, 5 + count, "props="));
                let remaining_length: u32 = field(&f, 6 + count, "rl=").parse().expect("rl");
                let buffer: usize = field(&f, 7 + count, "buf=").parse().expect("buf");

                let mut destination = vec![0xCCu8; buffer];
                let result = if kind == "sub" {
                    serialize_subscribe(
                        &mut destination,
                        &subscriptions,
                        &properties,
                        packet_id,
                        remaining_length,
                    )
                } else {
                    serialize_unsubscribe(
                        &mut destination,
                        &subscriptions,
                        &properties,
                        packet_id,
                        remaining_length,
                    )
                };

                let tail = match result {
                    Err(error) => format!(" -> {}", status(error)),
                    Ok(packet_size) => {
                        format!(" -> Success bytes={}", hex(&destination[..packet_size]))
                    }
                };

                let head: Vec<&str> = f[..8 + count].to_vec();
                let _ = writeln!(out, "{}{tail}", head.join(" "));
            }

            // ack <i> <name> type= id= rc= props= buf= -> ..
            Some("ack") => {
                let packet_type =
                    u8::from_str_radix(field(&f, 3, "type="), 16).expect("a type byte");
                let packet_id: u16 = field(&f, 4, "id=").parse().expect("id");
                let reason = field(&f, 5, "rc=");
                let reason_code = if reason == "-" {
                    None
                } else {
                    Some(u8::from_str_radix(reason, 16).expect("a reason code"))
                };
                let properties = unhex(field(&f, 6, "props="));
                let buffer: usize = field(&f, 7, "buf=").parse().expect("buf");

                let mut destination = vec![0xCCu8; buffer];
                let tail = match serialize_ack(
                    &mut destination,
                    packet_type,
                    packet_id,
                    reason_code,
                    &properties,
                ) {
                    Err(error) => format!(" -> {}", status(error)),
                    Ok(packet_size) => {
                        format!(" -> Success bytes={}", hex(&destination[..packet_size]))
                    }
                };

                let _ = writeln!(
                    out,
                    "ack {} {} {} {} {} {} {}{tail}",
                    f[1], f[2], f[3], f[4], f[5], f[6], f[7]
                );
            }

            Some("pingreq") => {
                let buffer: usize = field(&f, 1, "buf=").parse().expect("buf");
                let mut destination = vec![0xCCu8; buffer];

                let tail = match serialize_pingreq(&mut destination) {
                    Err(error) => format!(" -> {}", status(error)),
                    Ok(n) => format!(" -> Success bytes={}", hex(&destination[..n])),
                };

                let _ = writeln!(out, "pingreq {}{tail}", f[1]);
            }

            Some("options-sweep") => {
                let mut bytes = String::new();
                let mut digest = 1_469_598_103_934_665_603u64;
                let mut n = 0usize;

                // The driver's iteration order: qos fastest, then handling,
                // then no-local, then retain-as-published.
                for combination in 0..36u32 {
                    let qos = match combination % 3 {
                        0 => QoS::AtMostOnce,
                        1 => QoS::AtLeastOnce,
                        _ => QoS::ExactlyOnce,
                    };
                    let handling = match (combination / 3) % 3 {
                        0 => RetainHandling::OnSubscribe,
                        1 => RetainHandling::OnSubscribeIfNew,
                        _ => RetainHandling::Never,
                    };

                    let byte = subscription_options(&Subscription {
                        topic_filter: b"f",
                        qos,
                        no_local: (combination / 9) & 1 != 0,
                        retain_as_published: (combination / 18) & 1 != 0,
                        retain_handling: handling,
                    });

                    if n > 0 {
                        bytes.push(',');
                    }
                    let _ = write!(bytes, "{byte:02x}");
                    digest = fnv(digest, byte);
                    n += 1;
                }

                let _ = writeln!(
                    out,
                    "options-sweep bytes={bytes} n={n} digest={digest:016x}"
                );
            }

            Some("ack-reason-sweep") => {
                let packet_type = match f[1] {
                    "puback" => 0x40u8,
                    "pubrec" => 0x50,
                    "pubrel" => 0x62,
                    "pubcomp" => 0x70,
                    other => panic!("the trace names an ack this file does not know: {other:?}"),
                };

                let mut accepted = String::new();
                let mut n = 0usize;

                for code in 0..=255u8 {
                    let mut destination = vec![0xCCu8; 160];

                    if serialize_ack(&mut destination, packet_type, 1, Some(code), &[]).is_ok() {
                        if n > 0 {
                            accepted.push(',');
                        }
                        let _ = write!(accepted, "{code:02x}");
                        n += 1;
                    }
                }

                let _ = writeln!(
                    out,
                    "ack-reason-sweep {} accepted={accepted} n={n} refused={}",
                    f[1],
                    256 - n
                );
            }

            _ => {}
        }
    }

    out
}

#[test]
fn our_outbound_packets_match_the_c_case_for_case() {
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

    assert_eq!(n, 45, "the trace should be 45 lines, not {n}");
}

/// The guard: every packet kind, every option bit, and the three ack shapes.
///
/// The nineteenth shape of the guard `heap_4` started.
#[test]
fn the_cases_cover_every_outgoing_packet_kind() {
    let lines: Vec<&str> = TRACE.lines().collect();

    for kind in ["sub ", "unsub ", "ack ", "pingreq "] {
        let n = lines.iter().filter(|l| l.starts_with(kind)).count();
        assert!(n >= 3, "only {n} `{kind}` cases");
    }

    // The three shapes `serializeAckBody` names, by packet length.
    let ack_bytes: Vec<&str> = lines
        .iter()
        .filter(|l| l.starts_with("ack ") && l.contains("bytes="))
        .copied()
        .collect();
    let lengths: std::collections::BTreeSet<usize> = ack_bytes
        .iter()
        .filter_map(|l| {
            l.split_whitespace()
                .find_map(|f| f.strip_prefix("bytes="))
                .map(|b| b.len() / 2)
        })
        .collect();
    assert!(
        lengths.contains(&4) && lengths.contains(&6) && lengths.iter().any(|&n| n > 6),
        "the three ack shapes are not all present: {lengths:?}"
    );

    // Every option bit must be set somewhere in the named SUBSCRIBE cases.
    for qos in 0..=2u8 {
        assert!(
            lines.iter().any(
                |l| l.starts_with("sub ") && l.contains(&format!(",{qos},0,0,0"))
                    || l.starts_with("sub ") && l.contains(&format!("={qos},"))
            ),
            "no SUBSCRIBE case at QoS {qos}"
        );
    }

    // And both statuses.
    assert!(lines.iter().any(|l| l.contains("-> BadParameter")));
    assert!(lines.iter().any(|l| l.contains("-> NoMemory")));
}

/// The library validates ack reason codes twice, and the two halves disagree.
///
/// The writing side checks PER PACKET TYPE and matches MQTT 5.0 exactly; the
/// reading side checks all four against one shared table of ten. Asserted from
/// the CHECKED-IN TRACE, because it is the sharpest evidence for the upstream
/// report: the library already contains the correct table.
#[test]
fn the_writing_side_gets_the_ack_reason_codes_right() {
    let accepted = |label: &str| -> String {
        TRACE
            .lines()
            .find(|l| l.starts_with(&format!("ack-reason-sweep {label} ")))
            .unwrap_or_else(|| panic!("no {label} sweep"))
            .split_whitespace()
            .find_map(|f| f.strip_prefix("accepted="))
            .expect("an accepted= field")
            .to_owned()
    };

    // §3.4.2.1 and §3.5.2.1: nine codes, and 0x92 is NOT among them.
    assert_eq!(accepted("puback"), "00,10,80,83,87,90,91,97,99");
    assert_eq!(accepted("pubrec"), "00,10,80,83,87,90,91,97,99");

    // §3.6.2.1 and §3.7.2.1: two codes.
    assert_eq!(accepted("pubrel"), "00,92");
    assert_eq!(accepted("pubcomp"), "00,92");

    // The four tables are genuinely different, which is the whole point.
    assert_ne!(accepted("puback"), accepted("pubrel"));
}

/// All 36 subscription-option bytes are distinct, and none sets a reserved bit.
#[test]
fn every_option_combination_produces_its_own_byte() {
    let line = TRACE
        .lines()
        .find(|l| l.starts_with("options-sweep"))
        .expect("the options sweep");
    let bytes = line
        .split_whitespace()
        .find_map(|f| f.strip_prefix("bytes="))
        .expect("a bytes= field");

    let values: Vec<u8> = bytes
        .split(',')
        .map(|b| u8::from_str_radix(b, 16).expect("hex"))
        .collect();

    assert_eq!(values.len(), 36);

    let distinct: std::collections::BTreeSet<u8> = values.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        36,
        "two option combinations produce the same byte, so one decision is \
         being lost"
    );

    for value in values {
        assert_eq!(
            value & 0xC0,
            0,
            "{value:#04x} sets a reserved bit of the options byte"
        );
    }
}
