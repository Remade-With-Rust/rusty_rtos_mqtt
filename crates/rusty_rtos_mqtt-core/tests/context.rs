//! The last of `core_mqtt_serializer.c`, against the C.
//!
//! The C arm is `oracle/context_driver.c` driving the pinned v5.0.2 serializer
//! (`04845c6a`) verbatim, and its trace is checked in.
//!
//! # One job, done twice
//!
//! Two of the four things proven here are second copies of something this
//! package has already diffed against the C, and that is the point of running
//! them together:
//!
//! * `MQTT_ProcessIncomingPacketTypeAndLength` reads the fixed header out of a
//!   **buffer**; `MQTT_GetIncomingPacketTypeAndLength` reads it off a
//!   **callback**. Every `dual` line drives both over the same bytes.
//! * `updateContextWithConnectProps` walks the CONNECT property table, which
//!   `MQTT_ValidateConnectProperties` walked in the previous slice. Both
//!   traces print their accepted sets, so the two tables are a diff.
//!
//! A correct arm is the best instrument for finding a wrong one, and it found
//! two: a reader that cannot say "not yet", and a context filler that stores
//! what the validator calls a protocol error.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::builder::{BuilderError, PropertyBuilder};
use rusty_rtos_mqtt_core::context::{
    ConnectionProperties, ContextError, update_with_connect_props,
};
use rusty_rtos_mqtt_core::header::{HeaderError, process_incoming_packet_type_and_length};
use rusty_rtos_mqtt_core::outpublish::OutgoingPublish;
use rusty_rtos_mqtt_core::reader::{ReadError, Received, Transport, read_header};
use rusty_rtos_mqtt_core::state::QoS;
use rusty_rtos_mqtt_core::validate::{ValidateError, validate_publish_params};

const TRACE: &str = include_str!("../../../oracle/context.trace");

/// The transport the `get` arm is driven through: the case's bytes, then
/// nothing — which is what a socket holding `available` bytes does.
struct Arrived<'a> {
    bytes: &'a [u8],
    at: usize,
    calls: usize,
}

impl Transport for Arrived<'_> {
    fn recv_one(&mut self) -> Received {
        self.calls += 1;

        match self.bytes.get(self.at).copied() {
            Some(byte) => {
                self.at += 1;
                Received::Byte(byte)
            }
            None => Received::Nothing,
        }
    }
}

fn process_status(error: HeaderError) -> &'static str {
    match error {
        HeaderError::NoDataAvailable => "NoDataAvailable",
        HeaderError::NeedMoreBytes => "NeedMoreBytes",
        HeaderError::BadResponse => "BadResponse",
    }
}

fn read_status(error: ReadError) -> &'static str {
    match error {
        ReadError::NoDataAvailable => "NoDataAvailable",
        ReadError::RecvFailed => "RecvFailed",
        ReadError::BadResponse => "BadResponse",
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

    (0..text.len() / 2)
        .map(|i| u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).expect("a hex byte"))
        .collect()
}

/// The rolling digest every driver in this package uses.
///
/// FNV-1a's multiplier with a seed one digit short of its offset basis; see
/// the note in `tests/connect.rs` for why that is deliberate.
fn fnv(state: u64, value: u64) -> u64 {
    (state ^ value).wrapping_mul(1_099_511_628_211)
}

/// `MQTTStatus_t`'s values, so a digest of statuses means the same thing in
/// both arms.
///
/// The C digests the enumerator, not a name, so these are its numbers and not
/// an order of our choosing: `MQTTSuccess = 0`, then `BadParameter`,
/// `NoMemory`, `SendFailed`, `RecvFailed`, `BadResponse`, `ServerRefused`,
/// `NoDataAvailable`, `IllegalState`, `StateCollision`, `KeepAliveTimeout`,
/// `NeedMoreBytes`.
fn code(status: Result<(), &'static str>) -> u64 {
    match status {
        Ok(()) => 0,
        Err("BadParameter") => 1,
        Err("RecvFailed") => 4,
        Err("BadResponse") => 5,
        Err("NoDataAvailable") => 7,
        Err("NeedMoreBytes") => 11,
        Err(other) => panic!("no enumerator for {other}"),
    }
}

/// Both readers over one byte string, as the trace prints them.
fn dual(bytes: &[u8]) -> (String, String, u64, u64) {
    let mut process = String::new();
    let mut get = String::new();

    let a = match process_incoming_packet_type_and_length(bytes, bytes.len()) {
        Ok(header) => {
            let _ = write!(
                process,
                "process=Success type={:02x} rl={} hl={}",
                header.packet_type, header.remaining_length, header.header_length
            );
            Ok(())
        }
        Err(error) => {
            let _ = write!(process, "process={}", process_status(error));
            Err(process_status(error))
        }
    };

    let mut transport = Arrived {
        bytes,
        at: 0,
        calls: 0,
    };

    let b = match read_header(&mut transport) {
        Ok(header) => {
            let _ = write!(
                get,
                "get=Success type={:02x} rl={}",
                header.packet_type, header.remaining_length
            );
            Ok(())
        }
        Err(error) => {
            let _ = write!(get, "get={}", read_status(error));
            Err(read_status(error))
        }
    };

    let _ = write!(get, " calls={}", transport.calls);

    (process, get, code(a), code(b))
}

fn builder_line(name: &str, length: usize) -> String {
    let mut bytes = vec![0u8; length];
    let mut line = format!("builder {name} length={length} -> ");

    match PropertyBuilder::new(&mut bytes) {
        Ok(builder) => {
            let _ = write!(
                line,
                "Success index={} buflen={} fieldset={}",
                builder.len(),
                builder.capacity(),
                builder.fields()
            );
        }
        // Every one of these is `MQTTBadParameter` in the C except
        // `NoMemory`, which is its own status.
        Err(BuilderError::NoMemory) => line.push_str("NoMemory"),
        Err(_) => line.push_str("BadParameter"),
    }

    line
}

fn context_status(status: Result<(), ContextError>) -> &'static str {
    match status {
        Ok(()) => "Success",
        Err(ContextError::BadParameter) => "BadParameter",
        Err(ContextError::BadResponse) => "BadResponse",
    }
}

fn run_context(section: &[u8]) -> (&'static str, ConnectionProperties) {
    let mut context = ConnectionProperties::new();
    let status = context_status(update_with_connect_props(section, &mut context));
    (status, context)
}

/// The six value shapes, as the validator differential uses them.
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

fn params(
    retain: bool,
    retain_available: u8,
    qos: QoS,
    max_qos: u8,
    alias: u16,
    topic_length: usize,
    max_packet_size: u32,
) -> Result<(), ValidateError> {
    let topic = b"abcdefgh";
    let publish = OutgoingPublish {
        qos,
        dup: false,
        retain,
        topic_name: &topic[..topic_length],
        payload: b"",
        properties: b"",
    };

    validate_publish_params(&publish, retain_available, max_qos, alias, max_packet_size)
}

fn our_trace() -> String {
    let mut out = String::new();

    for line in TRACE.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();

        match f.first().copied() {
            Some("geometry" | "end") => {
                let _ = writeln!(out, "{line}");
            }

            Some("dual") => {
                let available: usize = field(&f, 3, "avail=").parse().expect("a count");
                let bytes = parse_hex(field(&f, 4, "bytes="));
                // The C takes the count and the buffer separately; one slice
                // carries both, so the count IS the slice.
                let (process, get, _, _) = dual(&bytes[..available]);

                let _ = writeln!(
                    out,
                    "dual {} {} {} {} -> {process} | {get}",
                    f[1], f[2], f[3], f[4]
                );
            }

            Some("dual-sweep") => {
                let mut accepted = String::new();
                let (mut n, mut refused, mut differ) = (0usize, 0usize, 0usize);
                let mut digest = 1_469_598_103_934_665_603u64;

                for value in 0..=255u8 {
                    let (_, _, a, b) = dual(&[value, 0x00]);

                    digest = fnv(digest, a);
                    digest = fnv(digest, b);

                    if a != b {
                        differ += 1;
                    }

                    if a == 0 {
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
                    "dual-sweep accepted={accepted} n={n} refused={refused} \
                     differ={differ} digest={digest:016x}"
                );
            }

            Some("truncated-sweep") => {
                let mut differ = 0usize;
                let mut digest = 1_469_598_103_934_665_603u64;

                for value in 0..=255u8 {
                    // One byte available: the length has not arrived.
                    let (_, _, a, b) = dual(&[value]);

                    digest = fnv(digest, a);
                    digest = fnv(digest, b);

                    if a != b {
                        differ += 1;
                    }
                }

                let _ = writeln!(out, "truncated-sweep differ={differ} digest={digest:016x}");
            }

            Some("init") => {
                let c = ConnectionProperties::new();
                let _ = writeln!(
                    out,
                    "init Success sessionexp={} recvmax={} maxpkt={} aliasmax={} rri={} \
                     rpi={} srecvmax={} smaxqos={} retain={} smaxpkt={} saliasmax={} \
                     wildcard={} subid={} shared={} keepalive={}",
                    c.session_expiry,
                    c.client.receive_max,
                    c.client.max_packet_size,
                    c.client.topic_alias_max,
                    u8::from(c.client.request_response_info),
                    u8::from(c.client.request_problem_info),
                    c.server.receive_max,
                    c.server.max_qos,
                    c.server.retain_available,
                    c.server.max_packet_size,
                    c.server.topic_alias_max,
                    c.server.wildcard_available,
                    c.server.subscription_id_available,
                    c.server.shared_available,
                    c.server.keep_alive
                );
            }

            Some("builder") => {
                let length: usize = field(&f, 2, "length=").parse().expect("a length");
                let _ = writeln!(out, "{}", builder_line(f[1], length));
            }

            Some("ctx") => {
                let section = parse_hex(field(&f, 3, "props="));
                let (status, c) = run_context(&section);

                let _ = writeln!(
                    out,
                    "ctx {} {} {} -> {status} sessionexp={} recvmax={} maxpkt={} aliasmax={}",
                    f[1],
                    f[2],
                    f[3],
                    c.session_expiry,
                    c.client.receive_max,
                    c.client.max_packet_size,
                    c.client.topic_alias_max
                );
            }

            Some("ctx-sweep") => {
                let value = shape(f[1]);
                let mut accepted = String::new();
                let (mut n, mut refused) = (0usize, 0usize);

                for id in 0..=255u8 {
                    let mut section = Vec::with_capacity(value.len() + 1);
                    section.push(id);
                    section.extend_from_slice(value);

                    if run_context(&section).0 == "Success" {
                        if n > 0 {
                            accepted.push(',');
                        }
                        let _ = write!(accepted, "{id:02x}");
                        n += 1;
                    } else {
                        refused += 1;
                    }
                }

                let _ = writeln!(
                    out,
                    "ctx-sweep {} accepted={accepted} n={n} refused={refused}",
                    f[1]
                );
            }

            Some("params") => {
                let qos = match field(&f, 4, "qos=") {
                    "0" => QoS::AtMostOnce,
                    "1" => QoS::AtLeastOnce,
                    _ => QoS::ExactlyOnce,
                };

                let status = params(
                    field(&f, 2, "retain=") == "1",
                    field(&f, 3, "avail=").parse().expect("a flag"),
                    qos,
                    field(&f, 5, "maxqos=").parse().expect("a qos"),
                    field(&f, 6, "alias=").parse().expect("an alias"),
                    field(&f, 7, "topiclen=").parse().expect("a length"),
                    field(&f, 8, "maxpkt=").parse().expect("a size"),
                );

                let name = match status {
                    Ok(()) => "Success",
                    Err(ValidateError::BadParameter) => "BadParameter",
                    Err(ValidateError::BadResponse) => "BadResponse",
                };

                let _ = writeln!(
                    out,
                    "params {} {} {} {} {} {} {} {} -> {name}",
                    f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8]
                );
            }

            Some("params-sweep") => {
                let (mut n, mut refused) = (0usize, 0usize);
                let mut digest = 1_469_598_103_934_665_603u64;

                for retain in [false, true] {
                    for available in 0..=1u8 {
                        for qos in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce] {
                            for max_qos in 0..=2u8 {
                                for alias in 0..=1u16 {
                                    for topic_length in [0usize, 1, 8] {
                                        for size in [0u32, 1024] {
                                            let status = params(
                                                retain,
                                                available,
                                                qos,
                                                max_qos,
                                                alias,
                                                topic_length,
                                                size,
                                            );

                                            digest = fnv(
                                                digest,
                                                code(status.map_err(|_| "BadParameter")),
                                            );

                                            if status.is_ok() {
                                                n += 1;
                                            } else {
                                                refused += 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                let _ = writeln!(
                    out,
                    "params-sweep n={n} refused={refused} digest={digest:016x}"
                );
            }

            _ => {}
        }
    }

    out
}

#[test]
fn the_last_of_the_serializer_matches_the_c() {
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

    assert_eq!(n, 56, "the trace should be 56 lines, not {n}");
}

/// The guard, twenty-second shape: two arms that never disagree are one arm
/// driven twice.
///
/// This differential's whole instrument is that the same header is read two
/// ways. If the `dual` lines happened to agree everywhere, they would prove
/// nothing that `reader.trace` had not already proven, and the second arm
/// would be decoration. The trace must therefore contain **both** a case where
/// the two agree and a case where they part — and the truncated sweep says the
/// parting is systematic rather than a single awkward input.
#[test]
fn the_two_readers_are_compared_where_they_agree_and_where_they_do_not() {
    let dual_lines: Vec<&str> = TRACE.lines().filter(|l| l.starts_with("dual ")).collect();

    let agreeing = dual_lines
        .iter()
        .filter(|l| l.contains("process=Success") && l.contains("get=Success"))
        .count();
    let parting = dual_lines
        .iter()
        .filter(|l| l.contains("process=NeedMoreBytes") && l.contains("get=BadResponse"))
        .count();

    assert!(
        agreeing >= 5,
        "only {agreeing} cases have both readers agree"
    );
    assert!(
        parting >= 2,
        "only {parting} cases have the two readers part"
    );

    // And the sweeps: identical on whole headers, different on every truncated
    // one. 168 is the number of packet types a client may receive, so the
    // parting is the whole accepted set rather than an awkward corner.
    assert!(
        TRACE.contains("dual-sweep") && TRACE.contains(" differ=0 "),
        "the type sweep no longer agrees, so the two readers differ on whole headers too"
    );
    assert!(
        TRACE.contains("truncated-sweep differ=168"),
        "the truncated sweep no longer parts on every acceptable type"
    );
}

/// The two walks of the CONNECT property table, as a diff of two traces.
///
/// The validator's accepted sets live in `validate.trace` and this walker's in
/// `context.trace`, and the interesting thing is where they differ: the
/// context filler accepts `0x16` with no method, and accepts `0x17`/`0x19`
/// carrying a value the validator rejects. Both differences say the same
/// thing — **this walk checks no values** — and both are asserted from the
/// checked-in traces rather than from our own code.
#[test]
fn the_two_connect_walks_accept_the_same_identifiers_and_not_the_same_values() {
    const VALIDATE: &str = include_str!("../../../oracle/validate.trace");

    let accepted = |trace: &str, prefix: &str, shape: &str| -> String {
        trace
            .lines()
            .find(|l| l.starts_with(&format!("{prefix} {shape} ")))
            .and_then(|l| {
                l.split_whitespace()
                    .find_map(|f| f.strip_prefix("accepted="))
            })
            .unwrap_or_default()
            .to_owned()
    };

    // The same nine identifiers, so neither table is a different table.
    for shape in ["one-byte", "two-byte", "four-byte", "user-property"] {
        assert_eq!(
            accepted(VALIDATE, "sweep connect", shape),
            accepted(TRACE, "ctx-sweep", shape),
            "the two CONNECT walks now differ at the {shape} shape"
        );
    }

    // And the two shapes where a VALUE decides. The validator refuses
    // authentication data alone and a Request Problem/Response of 7; the
    // context filler stores whatever it decodes.
    assert_eq!(accepted(VALIDATE, "sweep connect", "string"), "15");
    assert_eq!(
        accepted(TRACE, "ctx-sweep", "string"),
        "15,16",
        "the context filler no longer accepts authentication data alone"
    );

    assert_eq!(accepted(VALIDATE, "sweep connect", "varint"), "");
    assert_eq!(
        accepted(TRACE, "ctx-sweep", "varint"),
        "17,19",
        "the context filler no longer accepts a Request Problem Information of 7"
    );
}
