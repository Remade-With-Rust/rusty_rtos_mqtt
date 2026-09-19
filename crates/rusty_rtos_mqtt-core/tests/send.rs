//! The send plumbing against `core_mqtt.c`.
//!
//! The C arm is `oracle/send_driver.c` driving the pinned v5.0.2 `core_mqtt.c`
//! verbatim, and its trace is checked in.
//!
//! # The log is the answer
//!
//! A transport may take all the bytes it is offered, some of them, none, or
//! fail — and each answer puts the sender round its loop again with a different
//! offset. So what it **offers on every call** is as much of the behaviour as
//! how many bytes arrive:
//!
//! ```text
//! log=2:1,1:1     two bytes in two calls, the second offer correctly advanced
//! log=2:1,2:1     the same two bytes, with the first one sent twice
//! ```
//!
//! Both send two bytes. Only one is right, and only the log can tell them
//! apart. Same shape as the transport reader in slice 13, pointed the other
//! way.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::client::{ClientError, Clock, ConnectionStatus, MqttContext};
use rusty_rtos_mqtt_core::reader::{Recv, Sent, Transport};

const TRACE: &str = include_str!("../../../oracle/send.trace");

/// A transport that answers from a script and logs every call.
struct Script {
    steps: Vec<i32>,
    at: usize,
    calls: usize,
    sent: Vec<u8>,
    log: String,
}

impl Script {
    fn new(steps: Vec<i32>) -> Self {
        Self {
            steps,
            at: 0,
            calls: 0,
            sent: Vec::new(),
            log: String::new(),
        }
    }
}

impl Transport for Script {
    fn recv(&mut self, _into: &mut [u8]) -> Recv {
        panic!("the send differential asked the transport to receive");
    }

    fn send(&mut self, bytes: &[u8]) -> Sent {
        self.calls += 1;

        // The script is held at its last step once exhausted, so a sender that
        // will not terminate shows as a long log rather than as a hang.
        let step = if self.at < self.steps.len() {
            let value = self.steps[self.at];
            self.at += 1;
            value
        } else {
            *self.steps.last().expect("a script step")
        };

        // A transport never takes more than it was offered.
        let answer = step.min(bytes.len() as i32);

        if !self.log.is_empty() {
            self.log.push(',');
        }
        let _ = write!(self.log, "{}:{}", bytes.len(), answer);

        if answer > 0 {
            let count = answer as usize;
            self.sent.extend_from_slice(&bytes[..count]);
            Sent::Bytes(count)
        } else if answer == 0 {
            Sent::Nothing
        } else {
            Sent::Failed
        }
    }
}

/// A clock that starts where it is told and advances by a fixed step per read.
struct Ticking {
    now: u32,
    step: u32,
    reads: usize,
}

impl Clock for Ticking {
    fn now_ms(&mut self) -> u32 {
        let now = self.now;
        self.reads += 1;
        self.now = self.now.wrapping_add(self.step);
        now
    }
}

fn status(result: Result<(), ClientError>) -> &'static str {
    match result {
        Ok(()) => "Success",
        Err(ClientError::BadParameter) => "BadParameter",
        Err(ClientError::NotConnected) => "StatusNotConnected",
        Err(ClientError::DisconnectPending) => "StatusDisconnectPending",
        Err(ClientError::SendFailed) => "SendFailed",
        Err(ClientError::PublishStoreFailed) => "PublishStoreFailed",
        Err(ClientError::RecvFailed) => "RecvFailed",
        Err(ClientError::BadResponse) => "BadResponse",
        Err(ClientError::ServerRefused) => "ServerRefused",
        Err(ClientError::StatusConnected) => "StatusConnected",
        Err(ClientError::PublishRetrieveFailed) => "PublishRetrieveFailed",
        Err(ClientError::NoDataAvailable) => "NoDataAvailable",
        Err(ClientError::NeedMoreBytes) => "NeedMoreBytes",
        Err(ClientError::EventCallbackFailed) => "EventCallbackFailed",
        Err(ClientError::KeepAliveTimeout) => "KeepAliveTimeout",
        Err(ClientError::State(_)) => "StateError",
    }
}

fn state(status: ConnectionStatus) -> u8 {
    match status {
        ConnectionStatus::NotConnected => 0,
        ConnectionStatus::Connected => 1,
        ConnectionStatus::DisconnectPending => 2,
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

fn field_of<'a>(fields: &[&'a str], index: usize, name: &str) -> &'a str {
    fields[index].strip_prefix(name).unwrap_or_else(|| {
        panic!(
            "field {index} should start with {name:?}, got {:?}",
            fields[index]
        )
    })
}

fn parse_script(text: &str) -> Vec<i32> {
    text.split(',')
        .map(|step| step.parse().expect("a script step"))
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

            Some(kind @ ("ping" | "vec")) => {
                let script = parse_script(field_of(&f, 3, "script="));
                let step: u32 = field_of(&f, 4, "step=").parse().expect("a step");
                let connect = match field_of(&f, 5, "connected=") {
                    "1" => ConnectionStatus::Connected,
                    "2" => ConnectionStatus::DisconnectPending,
                    _ => ConnectionStatus::NotConnected,
                };

                let mut transport = Script::new(script);
                let mut clock = Ticking {
                    now: 0,
                    step,
                    reads: 0,
                };

                let mut buffer = [0u8; 256];
                let mut client = MqttContext::new(&mut buffer);

                client.connect_status = connect;

                let result = if kind == "ping" {
                    client.ping(&mut transport, &mut clock)
                } else {
                    client.disconnect(&mut transport, &mut clock, None, &[])
                };

                let _ = write!(
                    out,
                    "{kind} {} {} {} {} {} -> {} calls={} log={} bytes={}",
                    f[1],
                    f[2],
                    f[3],
                    f[4],
                    f[5],
                    status(result),
                    transport.calls,
                    if transport.log.is_empty() {
                        "-".to_owned()
                    } else {
                        transport.log.clone()
                    },
                    hex(&transport.sent)
                );

                if kind == "ping" {
                    let _ = writeln!(
                        out,
                        " connect={} waiting={} txtime={}",
                        state(client.connect_status),
                        u8::from(client.waiting_for_ping_resp()),
                        client.last_packet_tx_time()
                    );
                } else {
                    let _ = writeln!(
                        out,
                        " connect={} reads={}",
                        state(client.connect_status),
                        clock.reads
                    );
                }
            }

            Some("elapsed") => {
                let start: u32 = field_of(&f, 2, "start=").parse().expect("a start");
                let step: u32 = field_of(&f, 3, "step=").parse().expect("a step");

                let mut transport = Script::new(vec![0]);
                let mut clock = Ticking {
                    now: start,
                    step,
                    reads: 0,
                };

                let mut buffer = [0u8; 256];
                let mut client = MqttContext::new(&mut buffer);
                client.connect_status = ConnectionStatus::Connected;

                let result = client.ping(&mut transport, &mut clock);

                let _ = writeln!(
                    out,
                    "elapsed {} {} {} -> {} calls={} reads={}",
                    f[1],
                    f[2],
                    f[3],
                    status(result),
                    transport.calls,
                    clock.reads
                );
            }

            _ => {}
        }
    }

    out
}

#[test]
fn our_send_plumbing_matches_the_c_call_for_call() {
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

    assert_eq!(n, 29, "the trace should be 29 lines, not {n}");
}

/// The guard, twenty-sixth shape: a sender differential needs a case where the
/// transport takes PART of a vector.
///
/// Counting bytes is not enough. A sender that re-offered a whole vector after
/// a partial take would put the same number of bytes on the wire and log a
/// different sequence — so the trace must contain a call whose offer is smaller
/// than the vector it came from, and a case where a partial take lands exactly
/// on a vector boundary, because those two are the branches of the advance.
#[test]
fn the_trace_takes_part_of_a_vector_and_all_of_one() {
    let vectors: Vec<&str> = TRACE.lines().filter(|l| l.starts_with("vec ")).collect();

    let line = |name: &str| -> &&str {
        vectors
            .iter()
            .find(|l| l.split_whitespace().nth(2) == Some(name))
            .unwrap_or_else(|| panic!("no vector case named {name}"))
    };

    // Stops exactly at the end of the first vector: the whole-vector advance.
    assert!(line("stops-on-a-boundary").contains("log=2:2,1:1"));

    // Stops inside it: the offer after must be the REMAINDER of that vector,
    // not the whole of it again.
    let mid = line("stops-mid-vector");
    assert!(
        mid.contains("log=2:1,1:1,1:1"),
        "the mid-vector case no longer re-offers the remainder: {mid}"
    );

    // And every case that succeeds put the same three bytes out, whatever the
    // call sequence -- which is the invariant the log is there to qualify.
    for line in &vectors {
        if line.contains("-> Success") {
            assert!(
                line.contains(" bytes=e00100 "),
                "a successful DISCONNECT sent something else: {line}"
            );
        }
    }
}

/// The elapsed-time arithmetic is right across the 32-bit wrap.
///
/// `calculateElapsedTime` is `later - start` on `uint32_t`, and it is correct
/// across the wrap only because both sides are unsigned. A signed subtraction,
/// or a `later > start` guard added "for safety", would stop a client timing
/// out after 49.7 days of uptime. The three `elapsed` lines start at zero, one
/// step below the wrap, and at the wrap itself, and must be identical.
#[test]
fn the_timeout_survives_the_clock_wrapping() {
    let lines: Vec<&str> = TRACE
        .lines()
        .filter(|l| l.starts_with("elapsed "))
        .collect();

    assert_eq!(lines.len(), 3, "three clock origins");

    let answer =
        |line: &str| -> String { line.split(" -> ").nth(1).expect("an answer").to_owned() };

    let first = answer(lines[0]);

    for line in &lines {
        assert_eq!(
            answer(line),
            first,
            "the timeout depends on where the clock started: {line}"
        );
    }

    // And it is the timeout, not a send failure.
    assert!(first.starts_with("SendFailed"), "{first}");
    assert!(first.contains("calls=2"), "{first}");
}
