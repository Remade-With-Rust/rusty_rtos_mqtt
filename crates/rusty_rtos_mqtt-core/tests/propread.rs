//! The MQTT 5 property reader against `core_mqtt_prop_deserializer.c`.
//!
//! The C arm is `oracle/propread_driver.c` driving the pinned v5.0.2
//! deserializer (`04845c6a`) verbatim, and its trace is checked in.
//!
//! # The cursor is the answer
//!
//! Every function here advances a caller-owned index, so a getter that
//! returned the right value and left the cursor one byte out would pass a
//! value-only comparison and desynchronise every call after it. Every line
//! prints the index after the call, and every case that reads twice is a case
//! where the second read can only succeed if the first left the cursor exactly
//! right.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::cursor::{CursorError, PropertyCursor};

const TRACE: &str = include_str!("../../../oracle/propread.trace");

/// What one call answered: its status, and the value if it had one.
struct Answer {
    status: &'static str,
    detail: String,
}

fn status(error: CursorError) -> &'static str {
    match error {
        CursorError::BadParameter => "BadParameter",
        CursorError::BadResponse => "BadResponse",
        CursorError::EndOfProperties => "EndOfProperties",
    }
}

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

/// Run one getter by the name the trace gives it.
fn call(cursor: &mut PropertyCursor<'_>, name: &str) -> Answer {
    macro_rules! number {
        ($method:ident) => {{
            match cursor.$method() {
                Ok(value) => Answer {
                    status: "Success",
                    detail: format!(" value={value}"),
                },
                Err(error) => Answer {
                    status: status(error),
                    detail: String::new(),
                },
            }
        }};
    }

    macro_rules! text {
        ($method:ident) => {{
            match cursor.$method() {
                Ok(value) => Answer {
                    status: "Success",
                    detail: format!(" text={}", hex(value)),
                },
                Err(error) => Answer {
                    status: status(error),
                    detail: String::new(),
                },
            }
        }};
    }

    match name {
        "session-expiry" => number!(session_expiry),
        "receive-max" => number!(receive_max),
        "max-qos" => number!(max_qos),
        "retain-available" => number!(retain_available),
        "max-packet-size" => number!(max_packet_size),
        "topic-alias-max" => number!(topic_alias_max),
        "wildcard-available" => number!(wildcard_available),
        "subs-id-available" => number!(subscription_id_available),
        "shared-sub-available" => number!(shared_sub_available),
        "server-keep-alive" => number!(server_keep_alive),
        "payload-format" => number!(payload_format),
        "message-expiry" => number!(message_expiry),
        "topic-alias" => number!(topic_alias),
        "subscription-id" => number!(subscription_id),

        "assigned-client-id" => text!(assigned_client_id),
        "reason-string" => text!(reason_string),
        "response-info" => text!(response_info),
        "server-ref" => text!(server_reference),
        "auth-method" => text!(auth_method),
        "auth-data" => text!(auth_data),
        "response-topic" => text!(response_topic),
        "correlation-data" => text!(correlation_data),
        "content-type" => text!(content_type),

        "user-prop" => match cursor.user_property() {
            Ok((key, value)) => Answer {
                status: "Success",
                detail: format!(" key={} val={}", hex(key), hex(value)),
            },
            Err(error) => Answer {
                status: status(error),
                detail: String::new(),
            },
        },

        "next-type" => match cursor.next_type() {
            Ok(value) => Answer {
                status: "Success",
                detail: format!(" value={value}"),
            },
            Err(error) => Answer {
                status: status(error),
                detail: String::new(),
            },
        },

        "skip" => match cursor.skip() {
            Ok(()) => Answer {
                status: "Success",
                detail: String::new(),
            },
            Err(error) => Answer {
                status: status(error),
                detail: String::new(),
            },
        },

        other => panic!("unknown getter {other:?}"),
    }
}

/// The body every sweep uses: wide enough for the widest property, so a
/// refusal is the table's and never the buffer's.
fn sweep_section(id: u8) -> [u8; 9] {
    [id, 0x00, 0x02, b'a', b'b', 0x00, 0x02, b'c', b'd']
}

fn fnv(state: u64, value: u64) -> u64 {
    (state ^ value).wrapping_mul(1_099_511_628_211)
}

/// `MQTTStatus_t`'s values, so a digest of statuses means the same thing in
/// both arms. See the note in `tests/context.rs`.
fn code(status: &str) -> u64 {
    match status {
        "Success" => 0,
        "BadParameter" => 1,
        "BadResponse" => 5,
        "EndOfProperties" => 12,
        other => panic!("no enumerator for {other}"),
    }
}

fn field_of<'a>(fields: &[&'a str], index: usize, name: &str) -> &'a str {
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

    (0..text.len() / 2)
        .map(|i| u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).expect("a hex byte"))
        .collect()
}

fn our_trace() -> String {
    let mut out = String::new();

    for line in TRACE.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();

        match f.first().copied() {
            Some("geometry" | "end") => {
                let _ = writeln!(out, "{line}");
            }

            Some("known" | "skippable") => {
                let skipping = f[0] == "skippable";
                let mut accepted = String::new();
                let mut n = 0usize;

                for id in 0..=255u8 {
                    let section = sweep_section(id);
                    let mut cursor = PropertyCursor::new(&section);

                    let ok = if skipping {
                        cursor.skip().is_ok()
                    } else {
                        cursor.next_type().is_ok()
                    };

                    if ok {
                        if n > 0 {
                            accepted.push(',');
                        }
                        let _ = write!(accepted, "{id:02x}");
                        n += 1;
                    }
                }

                let _ = writeln!(
                    out,
                    "{} accepted={} n={n}",
                    f[0],
                    if accepted.is_empty() {
                        "-".to_owned()
                    } else {
                        accepted
                    }
                );
            }

            Some("tables") => {
                let mut differ = 0usize;
                let mut digest = 1_469_598_103_934_665_603u64;

                for id in 0..=255u8 {
                    let section = sweep_section(id);

                    let peek = PropertyCursor::new(&section);
                    let a = match peek.next_type() {
                        Ok(_) => "Success",
                        Err(error) => status(error),
                    };

                    let mut skipper = PropertyCursor::new(&section);
                    let b = match skipper.skip() {
                        Ok(()) => "Success",
                        Err(error) => status(error),
                    };

                    digest = fnv(digest, code(a));
                    digest = fnv(digest, code(b));
                    digest = fnv(digest, skipper.position() as u64);

                    if (a == "Success") != (b == "Success") {
                        differ += 1;
                    }
                }

                let _ = writeln!(out, "tables differ={differ} digest={digest:016x}");
            }

            Some("getter") => {
                let name = f[1];
                let mut accepted = String::new();
                let mut n = 0usize;
                let mut digest = 1_469_598_103_934_665_603u64;

                for id in 0..=255u8 {
                    let section = sweep_section(id);
                    let mut cursor = PropertyCursor::new(&section);
                    let answer = call(&mut cursor, name);

                    digest = fnv(digest, code(answer.status));
                    digest = fnv(digest, cursor.position() as u64);

                    if answer.status == "Success" {
                        if n > 0 {
                            accepted.push(',');
                        }
                        let _ = write!(accepted, "{id:02x}");
                        n += 1;
                    }
                }

                let _ = writeln!(
                    out,
                    "getter {name} accepted={} n={n} digest={digest:016x}",
                    if accepted.is_empty() {
                        "-".to_owned()
                    } else {
                        accepted
                    }
                );
            }

            Some("read") => {
                let start: usize = field_of(&f, 3, "at=").parse().expect("a position");
                let section = parse_hex(field_of(&f, 4, "props="));

                let mut cursor = PropertyCursor::new(&section);
                cursor.seek(start);

                let _ = write!(out, "read {} {} {} {}", f[1], f[2], f[3], f[4]);

                for chunk in line.split(" | ").skip(1) {
                    let name = chunk.split("->").next().expect("a getter name");
                    let answer = call(&mut cursor, name);

                    let _ = write!(
                        out,
                        " | {name}->{}{} at={}",
                        answer.status,
                        answer.detail,
                        cursor.position()
                    );
                }

                out.push('\n');
            }

            _ => {}
        }
    }

    out
}

#[test]
fn our_property_reader_matches_the_c_cursor_for_cursor() {
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

    assert_eq!(n, 55, "the trace should be 55 lines, not {n}");
}

/// The guard, twenty-fourth shape: a cursor differential must contain a case
/// where the SECOND call can only work if the first left the cursor right.
///
/// Every line here prints an index, which is necessary and not sufficient: a
/// trace of single-call cases compares a number nothing depends on. What makes
/// the index load-bearing is a case that reads twice, because the second read
/// demands a particular identifier and gets one only if the cursor landed
/// exactly on it.
#[test]
fn the_trace_chains_reads_so_the_cursor_carries_weight() {
    let reads: Vec<&str> = TRACE.lines().filter(|l| l.starts_with("read ")).collect();

    let chained = reads
        .iter()
        .filter(|l| l.matches(" | ").count() >= 2)
        .count();
    assert!(chained >= 6, "only {chained} cases make more than one call");

    // And of those, the ones whose second call SUCCEEDS -- a second call that
    // fails would be satisfied by any wrong cursor.
    let productive = reads
        .iter()
        .filter(|l| {
            let mut calls = l.split(" | ").skip(1);
            calls
                .next()
                .is_some_and(|first| first.contains("->Success"))
                && calls.next().is_some_and(|next| next.contains("->Success"))
        })
        .count();
    assert!(
        productive >= 4,
        "only {productive} cases read twice and succeed twice"
    );

    // Every width must be skipped somewhere, because the skip table is five
    // groups and a width computed wrong lands the cursor inside a value.
    for shape in [
        "skip-then-read",
        "skip-a-string",
        "skip-a-user-prop",
        "skip-a-subscription-id",
    ] {
        assert!(
            reads.iter().any(|l| l.contains(shape)),
            "no case named {shape}"
        );
    }
}

/// Both tables accept the same alphabet, and the trace says so.
///
/// Five tables in this library disagree with a sibling — the acknowledgement
/// reason codes, the DISCONNECT codes, the PUBLISH property validator, the
/// CONNECT context filler and the builder's packet table. These two do not,
/// and a differential that only ever reported disagreement would be one nobody
/// believed when it reported agreement.
#[test]
fn the_two_tables_agree_and_the_trace_records_it() {
    let known = TRACE
        .lines()
        .find(|l| l.starts_with("known "))
        .expect("a known line");
    let skippable = TRACE
        .lines()
        .find(|l| l.starts_with("skippable "))
        .expect("a skippable line");

    let set = |line: &str| -> String {
        line.split_whitespace()
            .find_map(|f| f.strip_prefix("accepted="))
            .unwrap_or_default()
            .to_owned()
    };

    assert_eq!(
        set(known),
        set(skippable),
        "the identifier table and the width table no longer agree"
    );
    assert!(known.contains("n=27"), "MQTT 5 defines 27 identifiers");
    assert!(
        TRACE.contains("tables differ=0"),
        "the two tables now disagree somewhere"
    );

    // And every getter accepts exactly one, which is the other half: a getter
    // wired to the wrong constant would show as n=0 or as two getters sharing.
    let getters: Vec<&str> = TRACE.lines().filter(|l| l.starts_with("getter ")).collect();
    assert_eq!(getters.len(), 24, "24 getters, one line each");

    for line in &getters {
        assert!(
            line.contains(" n=1 "),
            "a getter that does not accept exactly one identifier: {line}"
        );
    }
}
