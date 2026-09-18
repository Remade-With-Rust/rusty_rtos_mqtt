//! The packet-size calculators against `core_mqtt_serializer.c`.
//!
//! The C arm is `oracle/size_driver.c` driving the pinned v5.0.2 serializer
//! (`04845c6a`) verbatim, and its trace is checked in.
//!
//! # Values only on success, and why
//!
//! The C writes its out-parameters *before* its final maximum-packet-size
//! check, so a failed call has still updated them. That is real behaviour a C
//! caller can observe — and a Rust `Result` structurally cannot reproduce it,
//! because there is nothing to hand back on the error path.
//!
//! Rather than invent a trace line one arm cannot produce, the driver prints
//! the values only on success. The divergence is recorded in the package plan
//! and pinned by `a_refusal_hands_the_caller_nothing` next door, rather than
//! quietly dropped.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::size::{
    PINGREQ_PACKET_SIZE, PacketSize, SizeError, ack_packet_size, subscribe_packet_size,
    unsubscribe_packet_size,
};

const TRACE: &str = include_str!("../../../oracle/size.trace");

/// The driver's subscription cases, keyed by the name it prints.
///
/// The topic filter lengths are the only part of a case the trace does not
/// carry, because a filter's *contents* never affect its size. Keeping them
/// here is the one table this differential duplicates, and the case NAMES in
/// the trace are what keep the two in step — a renamed or reordered case fails
/// the lookup rather than silently testing the wrong thing.
fn sub_case(name: &str) -> (&'static [usize], u32) {
    match name {
        "one-short-filter" => (&[5], 1000),
        "three-filters" => (&[5, 10, 20], 1000),
        "empty-filter" => (&[0], 1000),
        "max-length-filter" => (&[65535], 1_000_000),
        "filter-one-past-16-bits" => (&[65536], 1_000_000),
        "eight-filters" => (&[1, 2, 3, 4, 5, 6, 7, 8], 1000),
        "subscribe-exactly-at-the-max" => (&[5], 13),
        "subscribe-one-byte-over" => (&[5], 12),
        "zero-max-packet-size" => (&[5], 0),
        "many-large-filters" => (
            &[65535, 65535, 65535, 65535, 65535, 65535, 65535, 65535],
            1_000_000,
        ),
        // The 268,435,455 boundary, which only the property length can reach:
        // eight filters of 65,535 come to 524,280.
        "properties-just-under-the-limit" => (&[0], u32::MAX),
        "properties-at-the-limit" => (&[0], u32::MAX),
        "properties-one-past-the-limit" => (&[0], u32::MAX),
        "properties-then-one-filter-over" => (&[100], u32::MAX),
        "properties-then-one-filter-fits" => (&[100], u32::MAX),
        // Computed to land on each check EXACTLY; an overshooting case
        // cannot tell a `>=` from a `>`.
        "in-loop-check-at-exactly-the-invalid-value" => (&[100], u32::MAX),
        "final-check-at-exactly-the-maximum" => (&[0], u32::MAX),
        other => panic!("the trace names a case this file does not know: {other:?}"),
    }
}

fn outcome(result: Result<PacketSize, SizeError>) -> String {
    match result {
        Ok(size) => format!(
            " -> Success remaining={} size={}",
            size.remaining_length, size.packet_size
        ),
        Err(SizeError::BadParameter) => " -> BadParameter".to_owned(),
    }
}

fn our_trace() -> String {
    let mut out = String::new();
    let mut lines = TRACE.lines();

    let geometry = lines.next().expect("a geometry line");
    let _ = writeln!(out, "{geometry}");

    for line in lines {
        let f: Vec<&str> = line.split_whitespace().collect();

        match f.first().copied() {
            Some("pingreq") => {
                let _ = writeln!(out, "pingreq -> Success {PINGREQ_PACKET_SIZE}");
            }

            // ack <i> <max> <property length> -> ...
            Some("ack") => {
                let max: u32 = f[2].parse().unwrap();
                let property_length: usize = f[3].parse().unwrap();
                let _ = writeln!(
                    out,
                    "ack {} {} {}{}",
                    f[1],
                    f[2],
                    f[3],
                    outcome(ack_packet_size(max, property_length))
                );
            }

            // sub / unsub <i> <name> prop=<n> -> ...   (or "<kind> zero-count -> ...")
            Some(kind @ ("sub" | "unsub")) => {
                if f[1] == "zero-count" {
                    // An empty list, which both calculators refuse.
                    let result = if kind == "sub" {
                        subscribe_packet_size(&[], 0, 1000)
                    } else {
                        unsubscribe_packet_size(&[], 0, 1000)
                    };
                    let _ = writeln!(out, "{kind} zero-count{}", outcome(result));
                    continue;
                }

                let name = f[2];
                // The property length comes from the trace, so it is not
                // duplicated in the lookup below.
                let property_length: u32 = f[3]
                    .strip_prefix("prop=")
                    .expect("a prop= field")
                    .parse()
                    .unwrap();

                let (filters, max) = sub_case(name);
                let result = if kind == "sub" {
                    subscribe_packet_size(filters, property_length, max)
                } else {
                    unsubscribe_packet_size(filters, property_length, max)
                };

                let _ = writeln!(
                    out,
                    "{kind} {} {name} prop={property_length}{}",
                    f[1],
                    outcome(result)
                );
            }

            Some("end") => {
                let _ = writeln!(out, "end");
            }

            _ => {}
        }
    }

    out
}

#[test]
fn our_size_calculators_match_the_c_case_for_case() {
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

    assert_eq!(n, 53, "the trace should be 53 lines, not {n}");
}

/// The guard: the cases must refuse for every reason the calculators have.
///
/// A size calculator that only ever succeeds is not enforcing a limit. The
/// twelfth shape of the guard `heap_4` started.
#[test]
fn the_cases_refuse_for_every_reason() {
    let refused = TRACE
        .lines()
        .filter(|l| l.ends_with("BadParameter"))
        .count();
    let accepted = TRACE.lines().filter(|l| l.contains("-> Success")).count();

    assert!(refused >= 8, "only {refused} cases are refused");
    assert!(accepted >= 8, "only {accepted} cases succeed");

    // Each distinct reason must be reachable, checked directly rather than by
    // reading the trace, because the C collapses them all to one status.
    assert_eq!(
        ack_packet_size(0, 0),
        Err(SizeError::BadParameter),
        "a zero maximum packet size must be refused"
    );
    assert_eq!(
        ack_packet_size(u32::MAX, 268_435_456),
        Err(SizeError::BadParameter),
        "a property length past the limit must be refused"
    );
    assert_eq!(
        subscribe_packet_size(&[], 0, 1000),
        Err(SizeError::BadParameter),
        "an empty subscription list must be refused"
    );
    assert_eq!(
        subscribe_packet_size(&[65_536], 0, 1_000_000),
        Err(SizeError::BadParameter),
        "a topic filter too long for its 16-bit prefix must be refused"
    );
    assert_eq!(
        subscribe_packet_size(&[5], 0, 12),
        Err(SizeError::BadParameter),
        "a packet larger than the broker's maximum must be refused"
    );
}

/// The size a calculator reports is the size the writer produces.
///
/// The two slices are only useful together: a calculator that agreed with the C
/// and a writer that agreed with the C could still disagree with **each other**
/// about the same packet, and nothing in either differential would notice.
#[test]
fn the_calculated_size_matches_what_the_writer_lays_down() {
    use rusty_rtos_mqtt_core::writer::{serialize_ack_fixed, serialize_subscribe_header};

    for property_length in [0usize, 1, 127, 128, 1000] {
        let size = ack_packet_size(1_000_000, property_length).expect("a small ack");

        let mut buffer = [0u8; 32];
        let written = serialize_ack_fixed(&mut buffer, 0x40, 1, size.remaining_length, 0);

        // The writer lays down the header, the packet id and the reason code;
        // the property section is the caller's to append.
        assert_eq!(
            written as u32
                + property_length as u32
                + rusty_rtos_mqtt_core::header::variable_length_encoded_size(
                    property_length as u32
                ),
            size.packet_size,
            "the ack calculator and the ack writer disagree at property length \
             {property_length}"
        );
    }

    for filters in [&[5usize][..], &[5, 10], &[100, 200, 300]] {
        let size = subscribe_packet_size(filters, 0, 1_000_000).expect("a small list");

        let mut buffer = [0u8; 32];
        let written = serialize_subscribe_header(&mut buffer, size.remaining_length, 1);

        // The header is the type byte, the length and the packet id; the
        // remaining length counts the packet id too, so it is subtracted once.
        assert_eq!(
            written as u32 + size.remaining_length - 2,
            size.packet_size,
            "the subscribe calculator and the subscribe writer disagree"
        );
    }
}
