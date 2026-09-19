//! Topic matching against `core_mqtt.c`.
//!
//! The C arm is `oracle/topic_driver.c` driving the pinned v5.0.2
//! `core_mqtt.c` verbatim, and its trace is checked in.
//!
//! # A grid, because matching takes two strings
//!
//! Every other sweep in this package walks one byte over its 256 values.
//! Matching takes a topic **and** a filter, so the sweep is the same idea one
//! dimension up: every string over `{a, /, +}` to length three, 39 of them, as
//! a 39 × 39 matrix. The divergence this slice found is a **column pattern** in
//! that grid — legible rather than merely detectable, which is the whole reason
//! for printing an accepted set instead of hashing it.
//!
//! Then `{a, b, /, +, #, $}` to length four: 1,554 strings each way,
//! 2,414,916 pairs, digested.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::ack::{AckError, PacketInfo};
use rusty_rtos_mqtt_core::topic::{
    TopicError, matches, packet_type_name, status_name, suback_status_codes, unsuback_status_codes,
};

const TRACE: &str = include_str!("../../../oracle/topic.trace");

/// Every string of length 1..=`max` over `alphabet`, in the driver's order.
fn enumerate(alphabet: &[u8], max: usize) -> Vec<Vec<u8>> {
    let mut out = Vec::new();

    for length in 1..=max {
        let total = alphabet.len().pow(length as u32);

        for i in 0..total {
            let mut value = i;
            let mut word = vec![0u8; length];

            for position in 0..length {
                word[length - 1 - position] = alphabet[value % alphabet.len()];
                value /= alphabet.len();
            }

            out.push(word);
        }
    }

    out
}

fn hit(topic: &[u8], filter: &[u8]) -> bool {
    matches(topic, filter) == Ok(true)
}

fn fnv(state: u64, value: u64) -> u64 {
    (state ^ value).wrapping_mul(1_099_511_628_211)
}

fn field_of<'a>(fields: &[&'a str], index: usize, name: &str) -> &'a str {
    fields[index].strip_prefix(name).unwrap_or_else(|| {
        panic!(
            "field {index} should start with {name:?}, got {:?}",
            fields[index]
        )
    })
}

/// The ack cases, which carry bytes the trace cannot spell without help.
///
/// Unlike every other case table in this package, these are built here rather
/// than parsed: the trace prints a NAME and an ANSWER, and the packet is too
/// long to put on the line. The names are the join, and
/// `the_ack_cases_are_all_present` checks neither side has grown one.
fn ack_case(name: &str) -> (u8, u32, Vec<u8>, bool) {
    const SUBACK: u8 = 0x90;
    const UNSUBACK: u8 = 0xB0;

    let mut long = vec![0x00, 0x01, 0x82, 0x01, 0x1F, 0x00, 0x7F];
    long.resize(135, 0x00);

    match name {
        "suback-one-code" => (SUBACK, 4, vec![0x00, 0x01, 0x00, 0x01], true),
        "suback-three" => (SUBACK, 6, vec![0x00, 0x01, 0x00, 0x00, 0x01, 0x02], true),
        "suback-failure" => (SUBACK, 4, vec![0x00, 0x01, 0x00, 0x80], true),
        "suback-no-codes" => (SUBACK, 3, vec![0x00, 0x01, 0x00], true),
        "suback-with-props" => (
            SUBACK,
            8,
            vec![0x00, 0x01, 0x04, 0x1F, 0x00, 0x01, b'x', 0x01],
            true,
        ),
        "unsuback-one-code" => (UNSUBACK, 4, vec![0x00, 0x01, 0x00, 0x00], false),
        "unsuback-no-sub" => (UNSUBACK, 4, vec![0x00, 0x01, 0x00, 0x11], false),
        "unsuback-two" => (UNSUBACK, 5, vec![0x00, 0x01, 0x00, 0x00, 0x11], false),
        "suback-two-byte-property-length" => (SUBACK, 135, long, true),
        "suback-getter-on-unsuback" => (UNSUBACK, 4, vec![0x00, 0x01, 0x00, 0x00], true),
        "unsuback-getter-on-suback" => (SUBACK, 4, vec![0x00, 0x01, 0x00, 0x01], false),
        other => panic!("unknown ack case {other:?}"),
    }
}

fn our_trace() -> String {
    let mut out = String::new();
    let small = enumerate(b"a/+", 3);

    for line in TRACE.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();

        match f.first().copied() {
            Some("geometry" | "grid" | "end") => {
                let _ = writeln!(out, "{line}");
            }

            Some("grid-strings") => {
                let mut row = "grid-strings".to_owned();
                for word in &small {
                    let _ = write!(row, " {}", String::from_utf8_lossy(word));
                }
                let _ = writeln!(out, "{row}");
            }

            Some("grid-row") => {
                let filter = f[1].as_bytes();
                let mut row = format!("grid-row {} ", f[1]);

                for topic in &small {
                    row.push(if hit(topic, filter) { '1' } else { '.' });
                }

                let _ = writeln!(out, "{row}");
            }

            Some("sweep") => {
                let all = enumerate(b"ab/+#$", 4);
                let mut digest = 1_469_598_103_934_665_603u64;
                let mut hits = 0u64;

                for filter in &all {
                    for topic in &all {
                        let matched = hit(topic, filter);
                        digest = fnv(digest, u64::from(matched));

                        if matched {
                            hits += 1;
                        }
                    }
                }

                let n = all.len();
                let _ = writeln!(
                    out,
                    "sweep alphabet=ab/+#$ maxlen=4 n={n} calls={} matches={hits} \
                     digest={digest:016x}",
                    n as u64 * n as u64
                );
            }

            Some("match") => {
                let topic = field_of(&f, 2, "topic=");
                let filter = field_of(&f, 3, "filter=");

                let (status, answer) = match matches(topic.as_bytes(), filter.as_bytes()) {
                    Ok(true) => ("Success", "yes"),
                    Ok(false) => ("Success", "no"),
                    Err(TopicError::BadParameter) => ("BadParameter", "no"),
                };

                let _ = writeln!(
                    out,
                    "match {} topic={topic} filter={filter} -> {status} {answer}",
                    f[1]
                );
            }

            Some("refusal") => {
                let topic = b"a/b";
                let huge = vec![b'a'; 65_536];

                let status = match f[1] {
                    "empty-topic" => matches(b"", topic),
                    "empty-filter" => matches(topic, b""),
                    "huge-topic" => matches(&huge, topic),
                    _ => matches(topic, &huge),
                };

                let name = match status {
                    Ok(_) => "Success",
                    Err(TopicError::BadParameter) => "BadParameter",
                };

                let _ = writeln!(out, "refusal {} -> {name}", f[1]);
            }

            Some("ack") => {
                let (packet_type, remaining_length, bytes, suback) = ack_case(f[1]);
                let packet = PacketInfo {
                    packet_type,
                    remaining_length,
                    remaining_data: &bytes,
                };

                let result = if suback {
                    suback_status_codes(&packet)
                } else {
                    unsuback_status_codes(&packet)
                };

                let _ = write!(out, "ack {} {} -> ", f[1], f[2]);

                match result {
                    Ok(codes) => {
                        let _ = write!(out, "Success n={} codes=", codes.len());
                        for code in codes {
                            let _ = write!(out, "{code:02x}");
                        }
                    }
                    Err(AckError::BadParameter) => out.push_str("BadParameter"),
                    Err(AckError::BadResponse) => out.push_str("BadResponse"),
                }

                out.push('\n');
            }

            Some("status") => {
                let mut row = "status".to_owned();
                for code in 0..20u8 {
                    let _ = write!(row, " {code}={}", status_name(code));
                }
                let _ = writeln!(out, "{row}");
            }

            Some("types") => {
                let mut row = "types".to_owned();
                let mut digest = 1_469_598_103_934_665_603u64;

                for value in 0..=255u8 {
                    let name = packet_type_name(value);

                    for byte in name.bytes() {
                        digest = fnv(digest, u64::from(byte));
                    }
                    digest = fnv(digest, 0);

                    if value & 0x0F == 0 || value == 0x62 || value == 0x82 || value == 0xA2 {
                        let _ = write!(row, " {value:02x}={name}");
                    }
                }

                let _ = writeln!(out, "{row} digest={digest:016x}");
            }

            _ => {}
        }
    }

    out
}

#[test]
fn our_topic_matcher_matches_the_c() {
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

    assert_eq!(n, 109, "the trace should be 109 lines, not {n}");
}

/// The guard, twenty-fifth shape: a grid must have a `1` and a `.` in every
/// row and every column that can have one.
///
/// A matrix is only an instrument if it varies. A matcher that said "no" to
/// everything would give a diagonal — every string matches itself by the exact
/// path — and a matcher that said "yes" to everything would give a solid
/// block; both would still be a grid, and both would still be *compared*. So
/// the shape of the grid is asserted: the diagonal is there, the wildcard rows
/// are much fuller than the literal ones, and no row is solid.
#[test]
fn the_grid_varies_in_both_directions() {
    let rows: Vec<&str> = TRACE
        .lines()
        .filter(|l| l.starts_with("grid-row "))
        .collect();
    assert_eq!(
        rows.len(),
        39,
        "39 filters over {{a, /, +}} to length three"
    );

    fn cells(line: &str) -> &str {
        line.split_whitespace().nth(2).expect("a row")
    }

    for (i, line) in rows.iter().enumerate() {
        let row = cells(line);
        assert_eq!(row.len(), 39, "39 topics per row");

        // Every filter matches itself, because a filter is a legal topic name
        // and the exact-match path takes it.
        assert_eq!(
            row.as_bytes()[i],
            b'1',
            "row {i} does not match itself: {line}"
        );

        assert!(
            row.contains('.'),
            "row {i} matches every topic, so it cannot refuse: {line}"
        );
    }

    // A wildcard filter must match more than itself, or the wildcards do
    // nothing at all.
    let ones = |name: &str| -> usize {
        rows.iter()
            .find(|l| l.split_whitespace().nth(1) == Some(name))
            .map(|l| cells(l).matches('1').count())
            .unwrap_or(0)
    };

    assert!(ones("+") >= 6, "`+` matches {} topics", ones("+"));
    assert!(ones("+/+") >= 8, "`+/+` matches {} topics", ones("+/+"));
    assert_eq!(ones("a"), 1, "a literal filter matches only itself");
    assert_eq!(ones("aaa"), 1);
}

/// The divergence, asserted from the trace rather than from our own code.
///
/// A filter whose last level is `+` stops matching an empty last level once an
/// earlier `+` has been used — while the specification's own example,
/// `sport/+` against `sport/`, works. Both halves are in the trace, because a
/// report that showed only the failure would be answered with "that is what
/// `+` does".
#[test]
fn the_trace_holds_both_halves_of_the_empty_level_divergence() {
    fn line(topic: &str, filter: &str) -> &'static str {
        let want_topic = format!(" topic={topic} ");
        let want_filter = format!(" filter={filter} ");

        for candidate in TRACE.lines() {
            if candidate.starts_with("match ")
                && candidate.contains(&want_topic)
                && candidate.contains(&want_filter)
            {
                return candidate;
            }
        }

        panic!("no case for {topic} against {filter}");
    }

    // What works.
    assert!(line("sport/", "sport/+").ends_with("yes"));
    assert!(line("a/", "a/+").ends_with("yes"));
    assert!(line("/a", "+/+").ends_with("yes"));

    // What does not.
    assert!(line("a/", "+/+").ends_with("no"));
    assert!(line("/", "+/+").ends_with("no"));
    assert!(line("a//", "a/+/+").ends_with("no"));
}

/// Every ack case in the trace is one this test knows how to build.
///
/// The ack packets are built here rather than parsed from the line, which is
/// the one place in this package where a trace does not carry its own inputs —
/// 135 bytes will not fit on a line. So the join is the NAME, and a name in the
/// trace that this file does not know would otherwise panic at replay time with
/// nothing to say about why.
#[test]
fn the_ack_cases_are_all_present() {
    let names: Vec<&str> = TRACE
        .lines()
        .filter(|l| l.starts_with("ack "))
        .map(|l| l.split_whitespace().nth(1).expect("a name"))
        .collect();

    assert_eq!(names.len(), 11, "eleven ack cases");

    for name in &names {
        // Panics with the offending name if the table here has not grown with
        // the driver's.
        let _ = ack_case(name);
    }

    // And the property-length widths that matter: one byte and two.
    assert!(names.contains(&"suback-with-props"));
    assert!(names.contains(&"suback-two-byte-property-length"));
}
