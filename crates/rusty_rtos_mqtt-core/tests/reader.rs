//! The transport reader against `core_mqtt_serializer.c`.
//!
//! The C arm is `oracle/reader_driver.c` driving the pinned v5.0.2 serializer
//! (`04845c6a`) verbatim, and its trace is checked in.
//!
//! # The call sequence is the answer
//!
//! This is the one function in the package that takes a **callback** rather
//! than a buffer, so what it asked for — and how many times — is as much of the
//! behaviour as what it returned. Every case scripts the transport, logs each
//! call, and compares the log. A reader that consumed one byte too many from a
//! hostile stream keeps every status and changes every log.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::reader::{ReadError, Received, Recv, Sent, Transport, read_header};

const TRACE: &str = include_str!("../../../oracle/reader.trace");

/// A transport that answers from a script and logs what it was asked.
struct Script {
    steps: Vec<Received>,
    at: usize,
    calls: usize,
    log: Vec<String>,
}

impl Script {
    fn new(steps: Vec<Received>) -> Self {
        Self {
            steps,
            at: 0,
            calls: 0,
            log: Vec::new(),
        }
    }
}

impl Transport for Script {
    /// This differential never sends: `read_header` only reads. A script that
    /// was asked to would be a script driving the wrong function, so it says
    /// so rather than quietly succeeding.
    fn send(&mut self, _bytes: &[u8]) -> Sent {
        panic!("the reader differential asked the transport to send");
    }

    fn recv(&mut self, into: &mut [u8]) -> Recv {
        self.calls += 1;
        let step = self.steps.get(self.at).copied();
        self.at += 1;

        match step {
            Some(Received::Byte(byte)) => match into.first_mut() {
                Some(slot) => {
                    self.log.push(format!("{byte:02x}"));
                    *slot = byte;
                    Recv::Bytes(1)
                }
                None => Recv::Nothing,
            },
            Some(Received::Nothing) => {
                self.log.push("none".to_owned());
                Recv::Nothing
            }
            Some(Received::Failed) => {
                self.log.push("err".to_owned());
                Recv::Failed
            }
            // The script ran out: the reader asked for more than the case
            // described, which the C driver reports the same way.
            None => {
                self.log.push("OVERRUN".to_owned());
                Recv::Failed
            }
        }
    }
}

fn parse_script(text: &str) -> Vec<Received> {
    text.split(',')
        .map(|step| match step {
            "none" => Received::Nothing,
            "err" => Received::Failed,
            byte => Received::Byte(u8::from_str_radix(byte, 16).expect("a script byte")),
        })
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

fn status(error: ReadError) -> &'static str {
    match error {
        ReadError::NoDataAvailable => "NoDataAvailable",
        ReadError::RecvFailed => "RecvFailed",
        ReadError::BadResponse => "BadResponse",
    }
}

/// FNV-1a/64, with the seed every driver in this package uses.
fn fnv(state: u64, value: u64) -> u64 {
    (state ^ value).wrapping_mul(1_099_511_628_211)
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
                let mut transport = Script::new(parse_script(field(&f, 3, "script=")));
                let result = read_header(&mut transport);

                let log = if transport.log.is_empty() {
                    "-".to_owned()
                } else {
                    transport.log.join(",")
                };

                let tail = match result {
                    Err(error) => {
                        format!(" -> {} calls={} read={log}", status(error), transport.calls)
                    }
                    Ok(header) => format!(
                        " -> Success calls={} read={log} type={:02x} rl={}",
                        transport.calls, header.packet_type, header.remaining_length
                    ),
                };

                let _ = writeln!(out, "case {} {} {}{tail}", f[1], f[2], f[3]);
            }

            Some("type-sweep") => {
                let mut accepted = String::new();
                let (mut n, mut refused) = (0usize, 0usize);
                let mut digest = 1_469_598_103_934_665_603u64;

                for value in 0..=255u8 {
                    let mut transport =
                        Script::new(vec![Received::Byte(value), Received::Byte(0x00)]);
                    let result = read_header(&mut transport);

                    // The call COUNT is digested too: a type check moved after
                    // the length read would keep every status and change every
                    // count.
                    digest = fnv(digest, transport.calls as u64);

                    if result.is_ok() {
                        if n > 0 {
                            accepted.push(',');
                        }
                        let _ = write!(accepted, "{value:02x}");
                        n += 1;
                    } else {
                        refused += 1;
                    }
                }

                let _ = writeln!(
                    out,
                    "type-sweep accepted={accepted} n={n} refused={refused} \
                     calldigest={digest:016x}"
                );
            }

            _ => {}
        }
    }

    out
}

#[test]
fn our_reader_matches_the_c_call_for_call() {
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

    assert_eq!(n, 20, "the trace should be 20 lines, not {n}");
}

/// The guard: every status, every transport answer, and no case that OVERRUNS.
///
/// The twentieth shape of the guard `heap_4` started. Its own job: the reader
/// is the one function here that can ask for **more** than it should, and a
/// script that always had spare bytes would never notice. Every script is
/// exactly as long as the reader should need, and `OVERRUN` in a log is a
/// failure by construction.
#[test]
fn the_scripts_are_exact_and_nothing_overruns() {
    let cases: Vec<&str> = TRACE.lines().filter(|l| l.starts_with("case ")).collect();

    assert!(
        !TRACE.contains("OVERRUN"),
        "a case's script ran out, so the reader asked for more than it should"
    );

    for name in [
        "-> Success",
        "-> BadResponse",
        "-> NoDataAvailable",
        "-> RecvFailed",
    ] {
        assert!(
            cases.iter().any(|l| l.contains(name)),
            "no case produces `{name}`"
        );
    }

    // Both transport failures, at the type byte and past it.
    assert!(cases.iter().any(|l| l.contains("script=none ")));
    assert!(cases.iter().any(|l| l.contains("script=err ")));
    assert!(cases.iter().any(|l| l.contains("script=30,none ")));
    assert!(cases.iter().any(|l| l.contains("script=30,err ")));

    // And a remaining length of each width, so the call count varies.
    for calls in 2..=5usize {
        assert!(
            cases.iter().any(|l| l.contains(&format!("calls={calls} "))),
            "no case takes {calls} transport calls"
        );
    }
}

/// A bad packet type costs ONE call, and the trace says so.
///
/// The status is `BadResponse` whether the type is checked before or after the
/// length, so only the call count distinguishes them — and a reader that
/// checked afterwards would drain a malformed stream instead of stopping at its
/// first byte.
#[test]
fn a_refused_type_never_costs_more_than_one_call() {
    let mut checked = 0usize;

    for line in TRACE
        .lines()
        .filter(|l| l.starts_with("case ") && l.contains("-> BadResponse"))
    {
        let script = line
            .split_whitespace()
            .find_map(|f| f.strip_prefix("script="))
            .expect("a script= field");
        let calls: usize = line
            .split_whitespace()
            .find_map(|f| f.strip_prefix("calls="))
            .expect("a calls= field")
            .parse()
            .expect("a count");

        // A script whose FIRST byte is a type a client may not receive.
        let first = script.split(',').next().expect("a first step");

        if let Ok(byte) = u8::from_str_radix(first, 16) {
            if !rusty_rtos_mqtt_core::header::incoming_packet_valid(byte) {
                assert_eq!(
                    calls, 1,
                    "a refused type byte cost {calls} calls, not one:\n  {line}"
                );
                checked += 1;
            }
        }
    }

    assert!(
        checked >= 3,
        "only {checked} cases refuse on the type byte alone"
    );
}
