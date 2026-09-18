//! The MQTT 5 property builders against `core_mqtt_prop_serializer.c`.
//!
//! The C arm is `oracle/propbuild_driver.c` driving the pinned v5.0.2 builder
//! (`04845c6a`) verbatim, and its trace is checked in.
//!
//! # The one place the two arms disagree on purpose
//!
//! Every other differential in this package matches line for line, divergences
//! from the specification included, because the C is the oracle. **This one has
//! three lines it cannot match**, and they are the point of the slice:
//! `addPropUtf8` sizes a property as its two length bytes plus its body and
//! forgets the identifier byte, so given a buffer exactly one byte too small it
//! reports success and writes one byte past the end. The C driver marks those
//! lines `OVERFLOW`; this arm answers `NoMemory` and writes nothing, because a
//! `&mut [u8]` under `forbid(unsafe)` leaves it no other option.
//!
//! So the comparison is: **every line matches, except that every line the C
//! marked `OVERFLOW` must be one where we refused** — and there must be no
//! other difference anywhere. An exception that is itself checked.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::builder::{BuilderError, PropertyBuilder, allowed_properties};
use rusty_rtos_mqtt_core::connack::field;

const TRACE: &str = include_str!("../../../oracle/propbuild.trace");

/// The eighteen adders, in the order the driver's enumeration gives them.
const ADDERS: [(&str, u32); 18] = [
    ("session-expiry", field::SESSION_EXPIRY_INTERVAL),
    ("receive-max", field::RECEIVE_MAXIMUM),
    ("max-packet-size", field::MAX_PACKET_SIZE),
    ("topic-alias-max", field::TOPIC_ALIAS_MAX),
    ("request-resp-info", field::REQUEST_RESPONSE_INFO),
    ("request-prob-info", field::REQUEST_PROBLEM_INFO),
    ("auth-method", field::AUTHENTICATION_METHOD),
    ("auth-data", field::AUTHENTICATION_DATA),
    ("payload-format", field::PAYLOAD_FORMAT_INDICATOR),
    ("message-expiry", field::MESSAGE_EXPIRY_INTERVAL),
    ("will-delay", field::WILL_DELAY),
    ("topic-alias", field::TOPIC_ALIAS),
    ("response-topic", field::RESPONSE_TOPIC),
    ("correlation-data", field::CORRELATION_DATA),
    ("content-type", field::CONTENT_TYPE),
    ("reason-string", field::REASON_STRING),
    ("subscription-id", field::SUBSCRIPTION_ID),
    ("user-prop", field::USER_PROP),
];

/// Run one adder with the value the driver uses.
///
/// The values are chosen so that only the TABLE can refuse them, which is what
/// makes the mask a reading of the table rather than of the values.
fn add(
    builder: &mut PropertyBuilder<'_>,
    name: &str,
    variant: &str,
    packet_type: Option<u8>,
) -> Result<(), BuilderError> {
    const TEXT: &[u8] = b"ab";
    const WILDCARD: &[u8] = b"a/#";

    match (name, variant) {
        ("session-expiry", _) => builder.session_expiry(30, packet_type),
        ("receive-max", "zero") => builder.receive_max(0, packet_type),
        ("receive-max", _) => builder.receive_max(10, packet_type),
        ("max-packet-size", "zero") => builder.max_packet_size(0, packet_type),
        ("max-packet-size", _) => builder.max_packet_size(1024, packet_type),
        ("topic-alias-max", _) => builder.topic_alias_max(5, packet_type),
        ("request-resp-info", _) => builder.request_response_info(true, packet_type),
        ("request-prob-info", _) => builder.request_problem_info(true, packet_type),
        ("auth-method", _) => builder.auth_method(TEXT, packet_type),
        ("auth-data", _) => builder.auth_data(TEXT, packet_type),
        ("payload-format", _) => builder.payload_format(true, packet_type),
        ("message-expiry", _) => builder.message_expiry(60, packet_type),
        ("will-delay", _) => builder.will_delay(15, packet_type),
        ("topic-alias", "zero") => builder.topic_alias(0, packet_type),
        ("topic-alias", _) => builder.topic_alias(7, packet_type),
        ("response-topic", "wildcard") => builder.response_topic(WILDCARD, packet_type),
        ("response-topic", _) => builder.response_topic(TEXT, packet_type),
        ("correlation-data", _) => builder.correlation_data(TEXT, packet_type),
        ("content-type", "empty") => builder.content_type(b"", packet_type),
        ("content-type", _) => builder.content_type(TEXT, packet_type),
        ("reason-string", "empty") => builder.reason_string(b"", packet_type),
        ("reason-string", _) => builder.reason_string(TEXT, packet_type),
        ("subscription-id", "zero") => builder.subscription_id(0, packet_type),
        ("subscription-id", "max") => builder.subscription_id(268_435_455, packet_type),
        ("subscription-id", "over") => builder.subscription_id(268_435_456, packet_type),
        ("subscription-id", _) => builder.subscription_id(3, packet_type),
        ("user-prop", "empty") => builder.user_property(b"k", b"", packet_type),
        ("user-prop", _) => builder.user_property(b"k", b"v", packet_type),
        (other, _) => panic!("unknown adder {other:?}"),
    }
}

fn status(result: Result<(), BuilderError>) -> &'static str {
    match result {
        Ok(()) => "Success",
        // Every variant but `NoMemory` is `MQTTBadParameter`.
        Err(BuilderError::NoMemory) => "NoMemory",
        Err(_) => "BadParameter",
    }
}

/// The mask of adders a packet type accepts, read the way the C driver reads
/// it: one adder at a time, through the public surface.
fn mask_for(packet_type: u8) -> u32 {
    let mut mask = 0u32;

    for (which, (name, _)) in ADDERS.iter().enumerate() {
        let mut bytes = [0u8; 64];
        let mut builder = PropertyBuilder::new(&mut bytes).expect("a builder");

        if *name == "auth-data" {
            // A method first, with no packet type, so only the data's own
            // table check can refuse it.
            let _ = builder.auth_method(b"ab", None);
        }

        if add(&mut builder, name, "-", Some(packet_type)).is_ok() {
            mask |= 1u32 << which;
        }
    }

    mask
}

fn fnv(state: u64, value: u64) -> u64 {
    (state ^ value).wrapping_mul(1_099_511_628_211)
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

/// Replay one `add` line: the line carries the adder, the odd value if any and
/// the packet type, so nothing here is a second copy of the driver's table.
fn replay(line: &str) -> String {
    let mut chunks = line.split(" | ");
    let head = chunks.next().expect("a head");
    let head_fields: Vec<&str> = head.split_whitespace().collect();
    let capacity: usize = field_of(&head_fields, 3, "cap=")
        .parse()
        .expect("a capacity");

    let mut bytes = vec![0u8; capacity];
    let mut out = format!("add {} {} cap={capacity}", head_fields[1], head_fields[2]);

    // A zero-length buffer is refused by the constructor, which no case in this
    // trace asks for.
    let mut builder = PropertyBuilder::new(&mut bytes).expect("a builder");

    for chunk in chunks {
        let (call, tail) = match chunk.split_once("->") {
            Some(pair) => pair,
            None => panic!("a step should contain `->`: {chunk:?}"),
        };

        let name = call.split('(').next().expect("an adder name");
        let args = call
            .split_once('(')
            .and_then(|(_, rest)| rest.strip_suffix(')'))
            .unwrap_or_else(|| panic!("a step should look like name(variant,type): {chunk:?}"));
        let (variant, kind) = args.split_once(',').expect("a variant and a type");

        let packet_type = if kind == "-" {
            None
        } else {
            Some(u8::from_str_radix(kind, 16).expect("a packet type"))
        };

        let result = add(&mut builder, name, variant, packet_type);
        let _ = write!(out, " | {name}({variant},{kind})->{}", status(result));

        // The last chunk carries the trailing fields after its status.
        let _ = tail;
    }

    let _ = write!(
        out,
        " index={} fieldset={:08x} bytes={}",
        builder.len(),
        builder.fields(),
        hex(builder.section())
    );

    out
}

fn our_trace() -> String {
    let mut out = String::new();

    for line in TRACE.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();

        match f.first().copied() {
            Some("geometry" | "end") => {
                let _ = writeln!(out, "{line}");
            }

            Some("allowed") => {
                let packet_type = u8::from_str_radix(field_of(&f, 2, "type="), 16).expect("a type");
                let mask = mask_for(packet_type);

                let names: Vec<&str> = ADDERS
                    .iter()
                    .enumerate()
                    .filter(|(which, _)| mask & (1u32 << which) != 0)
                    .map(|(_, (name, _))| *name)
                    .collect();

                let _ = writeln!(
                    out,
                    "allowed {} {} mask={mask:05x} props={}",
                    f[1],
                    f[2],
                    if names.is_empty() {
                        "-".to_owned()
                    } else {
                        names.join(",")
                    }
                );
            }

            Some("allowed-sweep") => {
                let mut digest = 1_469_598_103_934_665_603u64;

                for packet_type in 0..=255u8 {
                    digest = fnv(digest, u64::from(mask_for(packet_type)));
                }

                let _ = writeln!(out, "allowed-sweep types=256 digest={digest:016x}");
            }

            Some("add") => {
                let _ = writeln!(out, "{}", replay(line));
            }

            _ => {}
        }
    }

    out
}

/// The C's lines, with the three it marked `OVERFLOW` set aside.
fn expected() -> (Vec<String>, Vec<String>) {
    let mut ordinary = Vec::new();
    let mut overflowing = Vec::new();

    for line in TRACE.lines() {
        if let Some(rest) = line.strip_suffix(" OVERFLOW") {
            overflowing.push(rest.to_owned());
        } else {
            ordinary.push(line.to_owned());
        }
    }

    (ordinary, overflowing)
}

#[test]
fn our_builders_match_the_c_except_where_it_overflows() {
    let ours = our_trace();
    let our_lines: Vec<&str> = ours.lines().collect();
    let all: Vec<&str> = TRACE.lines().collect();

    assert_eq!(
        our_lines.len(),
        all.len(),
        "the two traces have different lengths"
    );

    let mut refused = 0usize;

    for (n, (theirs, ours)) in all.iter().zip(our_lines.iter()).enumerate() {
        if let Some(without) = theirs.strip_suffix(" OVERFLOW") {
            // The C wrote past the buffer it was given. We refuse, so the line
            // must differ -- and it must differ in exactly that way.
            assert_ne!(
                *ours, without,
                "line {n} no longer differs, so this arm has started overflowing too"
            );
            assert!(
                ours.contains("->NoMemory"),
                "line {n} should be refused for want of room, got:\n  {ours}"
            );
            assert!(
                ours.contains(" index=0 ") || ours.ends_with(" bytes=-"),
                "line {n} refused and still wrote something:\n  {ours}"
            );
            refused += 1;
        } else {
            assert_eq!(
                ours, theirs,
                "line {n} diverged\n  the C: {theirs}\n  ours : {ours}"
            );
        }
    }

    assert_eq!(
        refused, 3,
        "the C overflows on {refused} lines, not the 3 this test was written for"
    );
    assert_eq!(
        all.len(),
        73,
        "the trace should be 73 lines, not {}",
        all.len()
    );
}

/// The overflow, stated as arithmetic rather than as a trace line.
///
/// `addPropUtf8` checks for `length + 2` bytes and writes `length + 3`. Three
/// widths, three sizes of buffer, and in each one the C reports success on a
/// buffer one byte short of what it then writes.
#[test]
fn a_string_property_needs_three_bytes_more_than_its_body() {
    for body in [1usize, 2, 8, 40] {
        let needed = body + 3;

        // Exactly enough: accepted, and the section is exactly that long.
        let mut bytes = vec![0u8; needed];
        let mut builder = PropertyBuilder::new(&mut bytes).unwrap();
        assert_eq!(builder.content_type(&vec![b'x'; body], None), Ok(()));
        assert_eq!(builder.len(), needed);

        // One byte short -- which is the size the C accepts -- refused here.
        let mut bytes = vec![0u8; needed - 1];
        let mut builder = PropertyBuilder::new(&mut bytes).unwrap();
        assert_eq!(
            builder.content_type(&vec![b'x'; body], None),
            Err(BuilderError::NoMemory)
        );
        assert_eq!(builder.len(), 0, "a refusal must write nothing");
    }
}

/// The guard, twenty-third shape: an exception must be smaller than the rule.
///
/// This differential has a documented exception — three lines where the C is
/// memory-unsafe and this arm refuses instead. An exception list is a hole in a
/// comparison, so it has to be bounded from both ends: it must be exactly as
/// large as it claims, every member must be a line the C itself marked, and the
/// lines around it must still be compared.
#[test]
fn the_exception_is_bounded() {
    let (ordinary, overflowing) = expected();

    assert_eq!(
        overflowing.len(),
        3,
        "the exception list has changed size: {overflowing:?}"
    );
    assert!(
        ordinary.len() > 60,
        "only {} lines are compared without exception",
        ordinary.len()
    );

    // Each one is a string property in a buffer one byte short, which is the
    // whole of the defect and not a general licence.
    for line in &overflowing {
        assert!(
            line.contains("cap=4"),
            "an overflowing line that is not the four-byte case: {line}"
        );
        assert!(
            line.contains("->Success"),
            "an overflowing line the C did not accept: {line}"
        );
        assert!(
            line.contains("index=5"),
            "an overflowing line that did not write five bytes: {line}"
        );
    }
}

/// A fourth table, and where it disagrees with the third.
///
/// The builder lets a PUBLISH carry a Subscription Identifier.
/// `validate_publish_properties` refuses one, and [MQTT-3.3.4-6] says a client
/// must not send one — the C's own comment beside the arm says "only in
/// server-to-client PUBLISH" and sets the bit anyway.
#[test]
fn the_builders_table_disagrees_with_the_validators() {
    use rusty_rtos_mqtt_core::header::packet;
    use rusty_rtos_mqtt_core::validate::{ValidateError, validate_publish_properties};

    assert_ne!(
        allowed_properties(packet::PUBLISH) & (1u32 << field::SUBSCRIPTION_ID),
        0,
        "the builder now refuses a subscription identifier in a PUBLISH"
    );

    let mut alias = None;
    assert_eq!(
        validate_publish_properties(10, &[0x0B, 0x07], &mut alias),
        Err(ValidateError::BadParameter),
        "the validator now accepts one"
    );

    // And the two agree everywhere it matters for SUBSCRIBE, which is where a
    // subscription identifier belongs.
    assert_ne!(
        allowed_properties(packet::SUBSCRIBE) & (1u32 << field::SUBSCRIPTION_ID),
        0
    );
}

/// A section this crate builds is a section this crate validates.
///
/// The cross-slice check, and the one that matters most to a user: the bytes
/// the builder produces go straight into the validator that decides whether
/// they may be sent, and it must accept them. Two arms that each agree with the
/// C can still disagree with each other.
#[test]
fn what_the_builder_writes_the_validator_accepts() {
    use rusty_rtos_mqtt_core::header::packet;
    use rusty_rtos_mqtt_core::validate::{ConnectValidation, validate_connect_properties};

    let mut bytes = [0u8; 64];
    let mut builder = PropertyBuilder::new(&mut bytes).unwrap();

    builder
        .session_expiry(3600, Some(packet::CONNECT))
        .expect("a session expiry");
    builder
        .receive_max(20, Some(packet::CONNECT))
        .expect("a receive maximum");
    builder
        .max_packet_size(65_536, Some(packet::CONNECT))
        .expect("a maximum packet size");
    builder
        .auth_method(b"SCRAM", Some(packet::CONNECT))
        .expect("an authentication method");
    builder
        .auth_data(b"secret", Some(packet::CONNECT))
        .expect("authentication data");
    builder
        .user_property(b"k", b"v", Some(packet::CONNECT))
        .expect("a user property");

    let mut found = ConnectValidation::default();
    let section = builder.section().to_vec();

    assert_eq!(
        validate_connect_properties(&section, &mut found),
        Ok(()),
        "the validator refused a section the builder wrote:\n  {}",
        hex(&section)
    );
    assert_eq!(found.max_packet_size, Some(65_536));

    // And the will's table, which takes a different set through the same
    // buffer.
    let mut bytes = [0u8; 64];
    let mut builder = PropertyBuilder::new(&mut bytes).unwrap();
    builder.will_delay(30, None).expect("a will delay");
    builder
        .payload_format(true, None)
        .expect("a payload format");
    builder
        .content_type(b"text/plain", None)
        .expect("a content type");

    let section = builder.section().to_vec();
    assert_eq!(
        rusty_rtos_mqtt_core::validate::validate_will_properties(&section),
        Ok(()),
        "the will validator refused a section the builder wrote:\n  {}",
        hex(&section)
    );
}
