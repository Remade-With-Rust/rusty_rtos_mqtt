//! The two logging tables against the pinned `core_mqtt_serializer.c`.
//!
//! # The only two functions in the library with no observable behaviour
//!
//! `logConnackResponse` and `logAckResponse` spend their whole bodies calling
//! `LogError` and `LogDebug`, which `core_mqtt_config_defaults.h` defines as
//! **nothing**. On a stock build they are switches that do not do anything, so
//! there is no run to diff against — and turning the logging on would mean
//! compiling the pinned C with a configuration it does not ship, which is the
//! one thing every other arm of this oracle refuses to do.
//!
//! So the oracle for these two is the pinned **source text**. `oracle/logtable.py`
//! parses the two switches out of it and writes one line per reason code; this
//! diffs the crate's tables against that. A message that drifts on either side
//! fails the build, and when the pin moves the script is re-run like every
//! other trace generator.
//!
//! It is a weaker instrument than a differential and it is named as one. What
//! it does prove is exactly what these two functions contain: a mapping from
//! 256 reason codes to a set of strings, and which codes fall through.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use rusty_rtos_mqtt_core::ack::reason_message;
use rusty_rtos_mqtt_core::connack::reason_name;

const TRACE: &str = include_str!("../../../oracle/logtable.trace");

/// One row of the extracted table.
struct Row {
    function: &'static str,
    code: Option<u8>,
    message: &'static str,
}

fn rows() -> Vec<Row> {
    let mut out = Vec::new();

    for line in TRACE.lines() {
        let mut parts = line.splitn(4, ' ');

        let Some(function) = parts.next() else {
            continue;
        };

        if function != "logConnackResponse" && function != "logAckResponse" {
            continue;
        }

        let Some(code) = parts.next() else { continue };

        // The `rows=` header line has no code.
        if code.starts_with("rows=") {
            continue;
        }

        let _name = parts.next();
        let message = parts.next().unwrap_or("-");

        out.push(Row {
            function,
            code: code
                .strip_prefix("0x")
                .and_then(|hex| u8::from_str_radix(hex, 16).ok()),
            message: if message == "-" { "" } else { message },
        });
    }

    out
}

/// Every named reason code maps to the message the C gives it.
#[test]
fn the_tables_are_the_cs() {
    let rows = rows();

    assert!(rows.len() >= 30, "only {} rows extracted", rows.len());

    for row in &rows {
        let Some(code) = row.code else {
            // The `default:` branch, checked separately.
            continue;
        };

        let ours = match row.function {
            "logConnackResponse" => reason_name(code),
            _ => reason_message(code),
        };

        assert_eq!(
            ours, row.message,
            "{} 0x{code:02x} disagrees with the pinned source",
            row.function
        );
    }
}

/// The default branches, which are the other half of each table.
#[test]
fn the_default_branches_are_the_cs() {
    let rows = rows();

    let connack_default = rows
        .iter()
        .find(|row| row.function == "logConnackResponse" && row.code.is_none())
        .expect("the CONNACK default");
    let ack_default = rows
        .iter()
        .find(|row| row.function == "logAckResponse" && row.code.is_none())
        .expect("the ack default");

    // A code with no case falls to the default in both arms. 0x01 is in
    // neither table.
    assert_eq!(reason_name(0x01), connack_default.message);
    assert_eq!(reason_message(0x01), ack_default.message);
}

/// The whole of both tables, swept.
///
/// A named row proves the codes the C names; this proves the ones it does not,
/// which is the other 234 of 256.
#[test]
fn every_one_of_the_256_codes_agrees() {
    let rows = rows();

    let named = |function: &str| -> Vec<(u8, &'static str)> {
        rows.iter()
            .filter(|row| row.function == function)
            .filter_map(|row| row.code.map(|code| (code, row.message)))
            .collect()
    };

    let connack = named("logConnackResponse");
    let acks = named("logAckResponse");

    let connack_default = "Invalid reason code received.";

    for code in 0..=u8::MAX {
        let expected = connack
            .iter()
            .find(|(named, _)| *named == code)
            .map_or(connack_default, |(_, message)| *message);

        assert_eq!(reason_name(code), expected, "CONNACK 0x{code:02x}");

        let expected = acks
            .iter()
            .find(|(named, _)| *named == code)
            .map_or("", |(_, message)| *message);

        assert_eq!(reason_message(code), expected, "ack 0x{code:02x}");
    }
}

/// Two of the C's messages are wrong, and this arm reproduces both.
///
/// A transcription is not a correction. Pinning the defects means that if
/// upstream fixes them the extractor and this test disagree, and the fix is
/// noticed rather than silently absorbed.
#[test]
fn the_two_wrong_messages_are_reproduced() {
    // 0x80 is Unspecified Error; the message belongs to a CONNACK's 0x9F.
    assert_eq!(
        reason_message(0x80),
        "Publish refused with packet id %hu: Connection rate exceeded."
    );
    assert_eq!(
        reason_name(0x9F),
        "Connection refused: Connection rate exceeded."
    );

    // And a space before a full stop, which a tidy-up would have removed.
    assert!(reason_name(0x95).ends_with("large ."));
    assert!(reason_message(0x91).ends_with("in use. "));
}
