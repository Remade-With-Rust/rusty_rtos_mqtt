//! The client context against `core_mqtt.c`.
//!
//! The C arm is `oracle/client_driver.c` driving the pinned v5.0.2
//! `core_mqtt.c` verbatim, and its trace is checked in.
//!
//! # One line the two arms cannot agree on
//!
//! `checkWildcardSubscriptions` reaches for a topic filter with `strchr`, which
//! runs to a NUL — and `MQTTSubscribeInfo_t` carries a pointer *and* a length,
//! with nothing requiring the bytes to be terminated. A filter of `"abc"` with
//! length 3, sitting in a buffer that reads `"abc#"`, is refused as containing
//! a wildcard.
//!
//! A `&[u8]` has no bytes past its length, so this arm answers `Ok`. The trace
//! marks that case and `the_exception_is_bounded` checks the exception is
//! exactly one line and exactly that one — the same arrangement as
//! `tests/propbuild.rs`, which is the other place the C does something the
//! language forbids.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::client::{
    ClientError, ConnectionStatus, MqttContext, RetainHandling, Subscription, SubscriptionType,
};
use rusty_rtos_mqtt_core::state::QoS;

const TRACE: &str = include_str!("../../../oracle/client.trace");

/// The case whose answer the two arms cannot share, and why.
const OVERRUN_CASE: &str = "wildcard-past-the-length";

fn status(result: Result<(), ClientError>) -> &'static str {
    match result {
        Ok(()) => "Success",
        Err(ClientError::BadParameter) => "BadParameter",
        Err(ClientError::NotConnected) => "StatusNotConnected",
        Err(ClientError::DisconnectPending) => "StatusDisconnectPending",
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

/// The filter each shape names, and how much of it the subscription claims.
///
/// `hidden-wild` is the one that matters: three bytes of filter inside a
/// four-byte buffer whose fourth byte is `#`.
fn shape(name: &str) -> (&'static [u8], usize) {
    match name {
        "plain" => (b"a/b", 3),
        "wild" => (b"a/#", 3),
        "plus" => (b"a/+", 3),
        "shared" => (b"$share/g/a/b", 12),
        "shared-no-name" => (b"$share//a/b", 11),
        "shared-no-topic" => (b"$share/g/", 9),
        "shared-wild-name" => (b"$share/g#/a/b", 13),
        "share-prefix" => (b"$share/", 7),
        "hidden-wild" => (b"abc#", 3),
        "empty" => (b"a/b", 0),
        other => panic!("unknown shape {other:?}"),
    }
}

fn qos_of(value: &str) -> Option<QoS> {
    match value {
        "0" => Some(QoS::AtMostOnce),
        "1" => Some(QoS::AtLeastOnce),
        "2" => Some(QoS::ExactlyOnce),
        // The C takes a QoS of 3, which `MQTTQoS_t` can hold and `QoS` cannot.
        _ => None,
    }
}

fn retain_of(value: &str) -> Option<RetainHandling> {
    match value {
        "0" => Some(RetainHandling::OnSubscribe),
        "1" => Some(RetainHandling::OnSubscribeIfNew),
        "2" => Some(RetainHandling::Never),
        _ => None,
    }
}

/// Build the subscription list a `shapes=` field describes.
///
/// Returns `None` when a SUBSCRIBE names a QoS or a retain-handling value that
/// `QoS` and `RetainHandling` cannot represent — which is how the type answers
/// the C's `qos > 2` and `retainHandlingOption > 2` refusals.
///
/// An UNSUBSCRIBE is different, and the difference is the C's: both checks live
/// inside `if( subscriptionType == MQTT_TYPE_SUBSCRIBE )`, so an unsubscribe
/// never looks at either field. A value the type cannot hold is therefore a
/// value nothing reads, and any stand-in gives the same answer.
fn parse_list(shapes: &str, which: SubscriptionType) -> Option<Vec<Subscription<'static>>> {
    let unread = which == SubscriptionType::Unsubscribe;
    let mut list = Vec::new();

    for entry in shapes.split(',') {
        let parts: Vec<&str> = entry.split(':').collect();
        let (filter, length) = shape(parts[0]);

        let qos = match qos_of(parts[1]) {
            Some(value) => value,
            None if unread => QoS::AtMostOnce,
            None => return None,
        };

        let retain_handling = match retain_of(parts[2]) {
            Some(value) => value,
            None if unread => RetainHandling::OnSubscribe,
            None => return None,
        };

        list.push(Subscription {
            topic_filter: &filter[..length],
            qos,
            retain_handling,
            no_local: parts[3] == "1",
            retain_as_published: false,
        });
    }

    Some(list)
}

fn our_trace() -> String {
    let mut out = String::new();

    for line in TRACE.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();

        match f.first().copied() {
            Some("geometry" | "end") => {
                let _ = writeln!(out, "{line}");
            }

            Some("init") => {
                let mut buffer = [0u8; 256];
                let client = MqttContext::new(&mut buffer);
                let p = &client.properties;

                let _ = writeln!(
                    out,
                    "init Success connect={} nextid=1 recvmax={} maxpkt={} rpi={} \
                     smaxqos={} retain={} keepalive={} outrecords={} inrecords={} \
                     ackprops={}",
                    match client.status() {
                        ConnectionStatus::NotConnected => 0,
                        ConnectionStatus::Connected => 1,
                        ConnectionStatus::DisconnectPending => 2,
                    },
                    p.client.receive_max,
                    p.client.max_packet_size,
                    u8::from(p.client.request_problem_info),
                    p.server.max_qos,
                    p.server.retain_available,
                    p.server.keep_alive,
                    client.outgoing_records(),
                    client.incoming_records(),
                    u8::from(client.ack_properties() != 0)
                );
            }

            Some("stateful") => {
                let outgoing: usize = field_of(&f, 2, "out=")
                    .split('/')
                    .nth(1)
                    .and_then(|n| n.parse().ok())
                    .expect("a count");
                let incoming: usize = field_of(&f, 3, "in=")
                    .split('/')
                    .nth(1)
                    .and_then(|n| n.parse().ok())
                    .expect("a count");
                let properties: usize = field_of(&f, 4, "props=")
                    .split('/')
                    .nth(1)
                    .and_then(|n| n.parse().ok())
                    .expect("a length");

                let mut buffer = [0u8; 256];
                let mut client = MqttContext::new(&mut buffer);
                client.enable_qos(outgoing, incoming, properties);

                let _ = writeln!(
                    out,
                    "stateful {} {} {} {} {} -> Success outmax={} inmax={} ackbuf={}",
                    f[1],
                    f[2],
                    f[3],
                    f[4],
                    f[5],
                    client.outgoing_records(),
                    client.incoming_records(),
                    client.ack_properties()
                );
            }

            Some("connectstatus") => {
                let mut buffer = [0u8; 16];
                let mut client = MqttContext::new(&mut buffer);

                client.connect_status = match f[1] {
                    "connected" => ConnectionStatus::Connected,
                    "disconnect-pending" => ConnectionStatus::DisconnectPending,
                    _ => ConnectionStatus::NotConnected,
                };

                let name = match client.status() {
                    ConnectionStatus::Connected => "StatusConnected",
                    ConnectionStatus::NotConnected => "StatusNotConnected",
                    ConnectionStatus::DisconnectPending => "StatusDisconnectPending",
                };

                let _ = writeln!(out, "connectstatus {} -> {name}", f[1]);
            }

            Some("packetid") => {
                let mut buffer = [0u8; 16];
                let mut client = MqttContext::new(&mut buffer);

                if f[1] == "at-the-wrap" {
                    // Walk to 65,534 the only way the API allows.
                    while client.next_packet_id() != 65_533 {}
                }

                let mut row = format!("packetid {}", f[1]);
                for _ in 0..4 {
                    let _ = write!(row, " {}", client.next_packet_id());
                }

                // The next one, without consuming it.
                let peek = client.next_packet_id();
                let _ = writeln!(out, "{row} next={peek}");
            }

            Some("sub") => {
                let unsubscribe = field_of(&f, 2, "which=") == "unsubscribe";
                let packet_id: u16 = field_of(&f, 4, "id=").parse().expect("an id");
                let stateful = field_of(&f, 5, "stateful=") == "1";
                let wildcard: u8 = field_of(&f, 6, "wildcard=").parse().expect("a flag");
                let shared: u8 = field_of(&f, 7, "shared=").parse().expect("a flag");
                let shapes = field_of(&f, 8, "shapes=");

                let mut buffer = [0u8; 256];
                let mut client = MqttContext::new(&mut buffer);

                if stateful {
                    client.enable_qos(4, 4, 0);
                }

                client.properties.server.wildcard_available = wildcard;
                client.properties.server.shared_available = shared;

                let which = if unsubscribe {
                    SubscriptionType::Unsubscribe
                } else {
                    SubscriptionType::Subscribe
                };

                let answer = match parse_list(shapes, which) {
                    // A QoS or retain-handling value the type cannot hold is
                    // the C's `> 2` refusal, answered by the type.
                    None => "BadParameter",
                    Some(list) => status(if unsubscribe {
                        client.unsubscribe(&list, packet_id)
                    } else {
                        client.subscribe(&list, packet_id, None)
                    }),
                };

                let _ = writeln!(
                    out,
                    "sub {} {} {} {} {} {} {} {} -> {answer}",
                    f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8]
                );
            }

            Some("pub") => {
                let qos = qos_of(field_of(&f, 2, "qos=")).expect("a qos");
                let packet_id: u16 = field_of(&f, 3, "id=").parse().expect("an id");
                let payload = field_of(&f, 4, "payload=");
                let topic_length: usize = field_of(&f, 5, "topiclen=").parse().expect("a length");
                let stateful = field_of(&f, 6, "stateful=") == "1";

                let has_payload = payload.starts_with('1');
                let payload_length: usize = payload
                    .split('/')
                    .nth(1)
                    .and_then(|n| n.parse().ok())
                    .expect("a length");

                let mut buffer = [0u8; 256];
                let mut client = MqttContext::new(&mut buffer);

                if stateful {
                    client.enable_qos(4, 4, 0);
                }

                // `payload=0/5` is the C's non-null-length-with-a-null-pointer
                // refusal, which a slice cannot express: the length IS the
                // slice's. The type answers it.
                let answer = if !has_payload && payload_length > 0 {
                    "BadParameter"
                } else {
                    let topic = vec![b'a'; topic_length.min(8)];
                    let topic = if topic_length > 8 {
                        // The 65,536-byte case, which only the length matters
                        // for.
                        vec![b'a'; topic_length]
                    } else {
                        topic
                    };
                    let body = vec![b'x'; payload_length];

                    status(client.publish(qos, packet_id, &topic, &body))
                };

                let _ = writeln!(
                    out,
                    "pub {} {} {} {} {} {} -> {answer}",
                    f[1], f[2], f[3], f[4], f[5], f[6]
                );
            }

            _ => {}
        }
    }

    out
}

#[test]
fn our_client_matches_the_c_except_where_it_reads_past_a_filter() {
    let ours = our_trace();
    let our_lines: Vec<&str> = ours.lines().collect();
    let all: Vec<&str> = TRACE.lines().collect();

    assert_eq!(
        our_lines.len(),
        all.len(),
        "the traces are different lengths"
    );

    let mut excepted = 0usize;

    for (n, (theirs, mine)) in all.iter().zip(our_lines.iter()).enumerate() {
        if theirs.contains(OVERRUN_CASE) {
            assert!(
                theirs.ends_with("-> BadParameter"),
                "line {n}: the C no longer refuses the hidden wildcard"
            );
            assert!(
                mine.ends_with("-> StatusNotConnected"),
                "line {n}: this arm should accept a filter whose wildcard is \
                 outside it, got:\n  {mine}"
            );
            excepted += 1;
            continue;
        }

        assert_eq!(
            mine, theirs,
            "line {n} diverged\n  the C: {theirs}\n  ours : {mine}"
        );
    }

    assert_eq!(excepted, 1, "the exception should be exactly one line");
    assert_eq!(
        all.len(),
        41,
        "the trace should be 41 lines, not {}",
        all.len()
    );
}

/// The exception is exactly one line, and it is the one it claims to be.
///
/// The same arrangement as `tests/propbuild.rs`: a documented exception is a
/// hole in a comparison, so it is bounded from both ends. Here the hole is one
/// case, its name says what it is, and the rest of the `sub` cases are compared
/// without exception.
#[test]
fn the_exception_is_bounded() {
    let subs: Vec<&str> = TRACE.lines().filter(|l| l.starts_with("sub ")).collect();

    let excepted: Vec<&&str> = subs.iter().filter(|l| l.contains(OVERRUN_CASE)).collect();
    assert_eq!(
        excepted.len(),
        1,
        "one excepted case, not {}",
        excepted.len()
    );

    // It is the hidden-wildcard shape, with wildcards forbidden -- the only
    // combination in which `strchr` can see something a slice cannot.
    let line = excepted[0];
    assert!(line.contains("shapes=hidden-wild:"), "{line}");
    assert!(line.contains(" wildcard=0 "), "{line}");

    // The exception must stay small against what IS compared without one.
    assert!(
        subs.len() >= 15 && excepted.len() * 10 < subs.len(),
        "{} excepted of {} subscription cases is no longer an exception",
        excepted.len(),
        subs.len()
    );

    // And the same shape with wildcards ALLOWED is not excepted, because then
    // `strchr` finds nothing that matters either.
    assert!(
        !subs
            .iter()
            .any(|l| l.contains("hidden-wild") && l.contains(" wildcard=1 ")),
        "an unexcepted hidden-wildcard case has appeared; check it still agrees"
    );
}

/// Both halves of the last-one-wins defect are in the trace.
///
/// A report that showed only `bad-then-good` would be answered with "that list
/// is fine"; it is the pair that makes the point.
#[test]
fn the_trace_holds_both_halves_of_the_last_one_wins_defect() {
    let line = |name: &str| -> &'static str {
        for candidate in TRACE.lines() {
            if candidate.starts_with("sub ") && candidate.split_whitespace().nth(1) == Some(name) {
                return candidate;
            }
        }
        panic!("no case named {name}");
    };

    // The same two entries, in both orders, with opposite answers.
    assert!(line("bad-then-good").ends_with("-> StatusNotConnected"));
    assert!(line("good-then-bad").ends_with("-> BadParameter"));

    // And the loop that DOES break, so the two are distinguishable: a QoS 1
    // entry FIRST, with no records, is refused -- where an empty filter first
    // is not.
    assert!(line("qos1-then-qos0-without-records").ends_with("-> BadParameter"));

    // The singleton, so nobody can say the shape was always accepted.
    assert!(line("empty-filter").ends_with("-> BadParameter"));
}
