//! The six outgoing-property validators against `core_mqtt_serializer.c`.
//!
//! The C arm is `oracle/validate_driver.c` driving the pinned v5.0.2
//! serializer (`04845c6a`) verbatim, and its trace is checked in.
//!
//! # Thirty-six sweeps are the whole map
//!
//! Each validator is a switch on one property identifier, so each is swept over
//! all 256 of them at each of six value shapes and the accepted set is
//! **printed** rather than hashed — where the accepted set is small, printing
//! it makes a divergence legible instead of merely detectable.
//!
//! What a sweep cannot see is a rule that spans two properties, which is why
//! the fifty-six named cases carry [MQTT-3.1.2-32] and the repeats.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::validate::{
    ConnectValidation, ValidateError, validate_connect_properties, validate_publish_ack_properties,
    validate_publish_properties, validate_subscribe_properties, validate_unsubscribe_properties,
    validate_will_properties,
};

const TRACE: &str = include_str!("../../../oracle/validate.trace");

/// The driver's out-parameter sentinels: values no validator can produce, so
/// "left alone" is visible in the trace.
const NO_MAX_PACKET_SIZE: u32 = 0xAAAA_AAAA;
const NO_TOPIC_ALIAS: u16 = 0xAAAA;

/// What one validator answered, and everything it wrote on the way.
struct Answer {
    status: Result<(), ValidateError>,
    connect: ConnectValidation,
    topic_alias: Option<u16>,
}

fn run(which: &str, section: &[u8], subscription_id_available: bool, alias_max: u16) -> Answer {
    // Pre-set exactly as the driver pre-sets its C variables, including the
    // `true` that every CONNECT case then clears.
    let mut connect = ConnectValidation {
        request_problem_info: true,
        max_packet_size: None,
    };
    let mut topic_alias = None;

    let status = match which {
        "connect" => validate_connect_properties(section, &mut connect),
        "will" => validate_will_properties(section),
        "subscribe" => validate_subscribe_properties(subscription_id_available, section),
        "publish" => validate_publish_properties(alias_max, section, &mut topic_alias),
        "puback" => validate_publish_ack_properties(section),
        "unsubscribe" => validate_unsubscribe_properties(section),
        other => panic!("unknown validator {other:?}"),
    };

    Answer {
        status,
        connect,
        topic_alias,
    }
}

fn status_name(status: Result<(), ValidateError>) -> &'static str {
    match status {
        Ok(()) => "Success",
        Err(ValidateError::BadParameter) => "BadParameter",
        Err(ValidateError::BadResponse) => "BadResponse",
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

fn parse_hex(text: &str) -> Vec<u8> {
    if text == "-" {
        return Vec::new();
    }

    assert!(text.len() % 2 == 0, "a half byte in {text:?}");

    (0..text.len() / 2)
        .map(|i| u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).expect("a hex byte"))
        .collect()
}

/// The six value shapes, by the name the driver prints.
///
/// Neither integer shape is zero: several tables refuse a zero Receive Maximum,
/// Maximum Packet Size or Topic Alias, and a sweep must not confuse "unknown
/// identifier" with "known identifier, illegal value". The varint is 7 for the
/// same reason.
fn shape(name: &str) -> &'static [u8] {
    match name {
        "one-byte" => &[0x01],
        "two-byte" => &[0x00, 0x01],
        "four-byte" => &[0x00, 0x00, 0x00, 0x01],
        "string" => &[0x00, 0x01, b'x'],
        "varint" => &[0x07],
        "user-property" => &[0x00, 0x01, b'k', 0x00, 0x01, b'v'],
        other => panic!("unknown value shape {other:?}"),
    }
}

/// Every identifier through one validator at one value shape.
fn sweep(which: &str, shape_name: &str) -> (String, usize, usize) {
    let value = shape(shape_name);
    let mut accepted = String::new();
    let (mut n, mut refused) = (0usize, 0usize);

    for id in 0..=255u8 {
        let mut section = Vec::with_capacity(value.len() + 1);
        section.push(id);
        section.extend_from_slice(value);

        if run(which, &section, true, 10).status.is_ok() {
            if n > 0 {
                accepted.push(',');
            }
            let _ = write!(accepted, "{id:02x}");
            n += 1;
        } else {
            refused += 1;
        }
    }

    (accepted, n, refused)
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
                let which = field(&f, 3, "v=");
                let subid = field(&f, 4, "subid=") == "1";
                let alias_max: u16 = field(&f, 5, "alias=").parse().expect("an alias maximum");
                let section = parse_hex(field(&f, 6, "props="));

                let answer = run(which, &section, subid, alias_max);

                let _ = write!(
                    out,
                    "case {} {} {} {} {} {} -> {}",
                    f[1],
                    f[2],
                    f[3],
                    f[4],
                    f[5],
                    f[6],
                    status_name(answer.status)
                );

                // The out-parameters, printed on refused cases too: a validator
                // that answered the right status while writing the wrong number
                // would pass a status-only comparison.
                if which == "connect" {
                    let _ = write!(
                        out,
                        " rpi={} maxpkt={}",
                        u8::from(answer.connect.request_problem_info),
                        answer.connect.max_packet_size.unwrap_or(NO_MAX_PACKET_SIZE)
                    );
                } else if which == "publish" {
                    let _ = write!(
                        out,
                        " alias={}",
                        answer.topic_alias.unwrap_or(NO_TOPIC_ALIAS)
                    );
                }

                out.push('\n');
            }

            Some("sweep") => {
                let (accepted, n, refused) = sweep(f[1], f[2]);
                let _ = writeln!(
                    out,
                    "sweep {} {} accepted={accepted} n={n} refused={refused}",
                    f[1], f[2]
                );
            }

            _ => {}
        }
    }

    out
}

#[test]
fn our_validators_match_the_c_table_for_table() {
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

    assert_eq!(n, 94, "the trace should be 94 lines, not {n}");
}

/// The guard: six tables that cannot be told apart are ONE table with six
/// names.
///
/// The twenty-first shape of the guard `heap_4` started, and the one this slice
/// needs. Every other differential here proves a workload can fail; this one
/// has to prove the six workloads are **different from each other**, because a
/// transcription that routed all six validators to one table would still answer
/// correctly for every case aimed at that one. The sweeps make the check exact:
/// each validator's six accepted sets are its signature, and no two signatures
/// may be equal.
#[test]
fn no_two_validators_accept_the_same_set() {
    let validators = [
        "connect",
        "will",
        "subscribe",
        "publish",
        "puback",
        "unsubscribe",
    ];
    let shapes = [
        "one-byte",
        "two-byte",
        "four-byte",
        "string",
        "varint",
        "user-property",
    ];

    let signatures: Vec<(&str, String)> = validators
        .iter()
        .map(|which| {
            let mut signature = String::new();
            for name in shapes {
                let (accepted, _, _) = sweep(which, name);
                let _ = write!(signature, "[{accepted}]");
            }
            (*which, signature)
        })
        .collect();

    for (i, (a, left)) in signatures.iter().enumerate() {
        // A table that accepted every identifier, or none of them, is a table
        // that cannot refuse -- the older shape of this guard.
        assert!(
            left.contains("26"),
            "{a} accepts no user property, so its sweeps found nothing at all"
        );
        assert!(
            !left.contains("00,01,02"),
            "{a} accepts a run of consecutive identifiers, so it is refusing nothing"
        );

        for (b, right) in signatures.iter().skip(i + 1) {
            assert_ne!(
                left, right,
                "{a} and {b} accept exactly the same identifiers at every shape, so \
                 nothing in this differential could tell them apart"
            );
        }
    }
}

/// Both statuses, from every table that has both, and every table refusing
/// something.
#[test]
fn every_validator_both_succeeds_and_fails_in_the_named_cases() {
    let cases: Vec<&str> = TRACE.lines().filter(|l| l.starts_with("case ")).collect();

    for which in [
        "connect",
        "will",
        "subscribe",
        "publish",
        "puback",
        "unsubscribe",
    ] {
        let mine: Vec<&&str> = cases
            .iter()
            .filter(|l| l.contains(&format!("v={which} ")))
            .collect();

        assert!(
            mine.iter().any(|l| l.contains("-> Success")),
            "no case makes {which} succeed"
        );
        assert!(
            mine.iter().any(|l| l.contains("-> Bad")),
            "no case makes {which} fail"
        );
    }

    // And both statuses appear, since the split between them is per refusal
    // rather than per table.
    for status in ["-> BadParameter", "-> BadResponse"] {
        assert!(
            cases.iter().any(|l| l.contains(status)),
            "no case produces `{status}`"
        );
    }
}

/// The CONNECT out-parameters are compared on refused cases too.
///
/// A differential compares the state left behind, not only the answer — and
/// here the C leaves a Maximum Packet Size written when a later property fails.
/// This checks the TRACE exercises that, not just that we could.
#[test]
fn the_trace_pins_both_out_parameters_including_when_refused() {
    let cases: Vec<&str> = TRACE.lines().filter(|l| l.starts_with("case ")).collect();

    assert!(
        cases
            .iter()
            .any(|l| l.contains("v=connect ") && l.contains(" rpi=1")),
        "no case raises Request Problem Information, so the flag could be a constant"
    );
    assert!(
        cases
            .iter()
            .any(|l| l.contains("v=connect ") && !l.contains("maxpkt=2863311530")),
        "no case sets a Maximum Packet Size, so the out-parameter could be dead"
    );
    assert!(
        cases
            .iter()
            .any(|l| l.contains("-> BadParameter alias=") && !l.contains("alias=43690")),
        "no case refuses a Topic Alias AFTER writing it, which is the C's ordering"
    );
}
