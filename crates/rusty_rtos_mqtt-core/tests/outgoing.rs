//! The outgoing packets against `core_mqtt.c`.
//!
//! The C arm is `oracle/outgoing_driver.c` driving the pinned v5.0.2
//! `core_mqtt.c` verbatim, and its trace is checked in.
//!
//! # A packet is one stream and several GATHERS
//!
//! SUBSCRIBE and UNSUBSCRIBE are built out of a fixed array of four vectors,
//! and a list of filters longer than that array is sent as **several calls to
//! the vector sender** — one MQTT packet, several gathers. Which filters land
//! in which gather is not visible in the bytes at all; it is only visible in
//! the call log, and it is not symmetric:
//!
//! ```text
//! sub   one-filter   log=4:4,1:1,  2:2,3:3,1:1        header alone, then the filter
//! unsub one-filter   log=4:4,1:1,2:2,3:3               header AND the filter, one gather
//! ```
//!
//! A SUBSCRIBE spends three vectors on a topic and an UNSUBSCRIBE two, and the
//! header's own two are never reset before the filter loop — so the same guard
//! `used <= 4 - per_topic` lets one of them carry a filter in its first gather
//! and the other not. Both put the same bytes on the wire.
//!
//! # And the copy that is stored is not the copy that is sent
//!
//! A QoS 1 or 2 PUBLISH is handed to the retransmit store **with the DUP flag
//! raised**, and then sent with it clear. `pub qos1-stored` asserts both:
//! `bytes=320d…` went out and `stored=3a0d…` was kept, one bit apart.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::client::{
    ClientError, Clock, ConnectionStatus, MqttContext, NoStore, RetainHandling, Store, Subscription,
};
use rusty_rtos_mqtt_core::outpublish::OutgoingPublish;
use rusty_rtos_mqtt_core::reader::{Received, Sent, Transport};
use rusty_rtos_mqtt_core::state::{PublishState, QoS, Record};

const TRACE: &str = include_str!("../../../oracle/outgoing.trace");

/// A transport that answers with one fixed step and logs every call.
///
/// The C's is the same: a single `g_step_value`, clamped to what it was
/// offered. What varies between cases is the packet, not the script.
struct Script {
    step: i32,
    calls: usize,
    sent: Vec<u8>,
    log: String,
}

impl Script {
    fn new(step: i32) -> Self {
        Self {
            step,
            calls: 0,
            sent: Vec::new(),
            log: String::new(),
        }
    }

    /// What the C does between the two publishes of a `collide` case.
    fn forget(&mut self) {
        self.calls = 0;
        self.sent.clear();
        self.log.clear();
    }
}

impl Transport for Script {
    fn recv_one(&mut self) -> Received {
        panic!("the outgoing differential asked the transport to receive");
    }

    fn send(&mut self, bytes: &[u8]) -> Sent {
        self.calls += 1;

        // A transport never takes more than it was offered.
        let answer = self.step.min(bytes.len() as i32);

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

/// The same script, with `writev` implemented.
///
/// Two types rather than one flag, on purpose: `Script` leaves `writev` alone
/// and so proves the trait's DEFAULT — the fallback the C reaches for when the
/// pointer is null — and this one proves the override. A single type with a
/// boolean would have to write the fallback out again, and then the default
/// would be the one thing in the trait nothing tested.
struct Gathering(Script);

impl Transport for Gathering {
    fn recv_one(&mut self) -> Received {
        self.0.recv_one()
    }

    fn send(&mut self, bytes: &[u8]) -> Sent {
        self.0.send(bytes)
    }

    fn writev(&mut self, parts: &[&[u8]]) -> Sent {
        self.0.calls += 1;

        let bytes: usize = parts.iter().map(|part| part.len()).sum();
        let answer = self.0.step.min(bytes as i32);

        if !self.0.log.is_empty() {
            self.0.log.push(',');
        }
        let _ = write!(self.0.log, "v{}/{}:{answer}", parts.len(), bytes);

        if answer <= 0 {
            return if answer == 0 {
                Sent::Nothing
            } else {
                Sent::Failed
            };
        }

        let mut left = answer as usize;

        for part in parts {
            if left == 0 {
                break;
            }

            let take = part.len().min(left);
            self.0.sent.extend_from_slice(&part[..take]);
            left -= take;
        }

        Sent::Bytes(answer as usize)
    }
}

/// The C's clock never moves in this driver, because its transport always
/// answers.
struct Frozen;

impl Clock for Frozen {
    fn now_ms(&mut self) -> u32 {
        0
    }
}

/// The retransmit store, and with it the only door to the two vector helpers.
struct Keeper {
    answer: bool,
    calls: usize,
    stored: Vec<u8>,
}

impl Store for Keeper {
    fn store(&mut self, _packet_id: u16, parts: &[&[u8]]) -> bool {
        self.calls += 1;

        // `MQTT_GetBytesInMQTTVec` then `MQTT_SerializeMQTTVec`, which is what
        // the C's callback does and the only way either is reachable.
        if let Ok(needed) = rusty_rtos_mqtt_core::client::vector_bytes(parts) {
            let mut flat = vec![0u8; needed];
            let written = rusty_rtos_mqtt_core::client::serialize_vector(&mut flat, parts);
            flat.truncate(written);
            self.stored = flat;
        }

        self.answer
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
        Err(ClientError::State(error)) => match error {
            rusty_rtos_mqtt_core::state::StateError::StateCollision => "StateCollision",
            rusty_rtos_mqtt_core::state::StateError::BadParameter => "BadParameter",
            rusty_rtos_mqtt_core::state::StateError::NoMemory => "NoMemory",
            rusty_rtos_mqtt_core::state::StateError::IllegalState => "IllegalState",
            rusty_rtos_mqtt_core::state::StateError::BadResponse => "BadResponse",
        },
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

fn from_hex(text: &str) -> Vec<u8> {
    if text == "-" {
        return Vec::new();
    }

    text.as_bytes()
        .chunks(2)
        .map(|pair| {
            u8::from_str_radix(std::str::from_utf8(pair).expect("ascii"), 16).expect("a hex byte")
        })
        .collect()
}

fn field_of<'a>(fields: &[&'a str], index: usize, prefix: &str) -> &'a str {
    fields
        .get(index)
        .and_then(|field| field.strip_prefix(prefix))
        .unwrap_or_else(|| panic!("field {index} is not {prefix}"))
}

/// The four filter shapes the C names.
fn filter_of(shape: &str) -> &'static [u8] {
    match shape {
        "a" => b"a",
        "ab" => b"ab",
        "long" => b"aaaaaaaa",
        // A shared subscription with an EMPTY share name: the SUBSCRIBE
        // validator refuses it and the UNSUBSCRIBE one never looks.
        "badshare" => b"$share//a/b",
        _ => b"a/b",
    }
}

fn qos_of(text: &str) -> QoS {
    match text {
        "1" => QoS::AtLeastOnce,
        "2" => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}

fn subscription(shape: &str, qos: QoS) -> Subscription<'static> {
    Subscription {
        topic_filter: filter_of(shape),
        qos,
        no_local: false,
        retain_as_published: false,
        retain_handling: RetainHandling::OnSubscribe,
    }
}

fn context<'a>(
    buffer: &'a mut [u8],
    outgoing: &'a mut [Record],
    incoming: &'a mut [Record],
    connected: bool,
) -> MqttContext<'a> {
    let mut client = MqttContext::new(buffer);
    client.enable_qos(outgoing, incoming, 0);

    if connected {
        client.connect_status = ConnectionStatus::Connected;
    }

    client
}

/// `rec=<id>/<state>`: the first outgoing record, which is where a QoS 1 or 2
/// PUBLISH leaves its mark.
///
/// Without this the wire cannot tell a publish that reserved a record and then
/// advanced it from one that only reserved it. `pub qos1-store-refuses` is
/// `rec=5/PublishSend` and every successful QoS 1 publish is
/// `rec=5/PubAckPending`, and nothing else in the line differs.
fn record_of(client: &MqttContext<'_>) -> String {
    let (packet_id, state) = client
        .outgoing()
        .first()
        .map_or((0, PublishState::Null), |record| {
            (record.packet_id, record.state)
        });

    format!("{packet_id}/{}", state_name(state))
}

const fn state_name(state: PublishState) -> &'static str {
    match state {
        PublishState::Null => "Null",
        PublishState::PublishSend => "PublishSend",
        PublishState::PubAckSend => "PubAckSend",
        PublishState::PubRecSend => "PubRecSend",
        PublishState::PubRelSend => "PubRelSend",
        PublishState::PubCompSend => "PubCompSend",
        PublishState::PubAckPending => "PubAckPending",
        PublishState::PubRecPending => "PubRecPending",
        PublishState::PubRelPending => "PubRelPending",
        PublishState::PubCompPending => "PubCompPending",
        PublishState::PublishDone => "PublishDone",
    }
}

/// One call, whichever transport the line named.
fn drive<T: Transport>(
    client: &mut MqttContext<'_>,
    transport: &mut T,
    clock: &mut Frozen,
    unsubscribe: bool,
    list: &[Subscription<'_>],
    packet_id: u16,
    properties: &[u8],
) -> Result<(), ClientError> {
    if unsubscribe {
        client.unsubscribe(transport, clock, list, packet_id, properties)
    } else {
        client.subscribe(transport, clock, list, packet_id, properties)
    }
}

fn send_one<T: Transport>(
    client: &mut MqttContext<'_>,
    transport: &mut T,
    clock: &mut Frozen,
    store: Option<&mut Keeper>,
    info: &OutgoingPublish<'_>,
    packet_id: u16,
) -> Result<(), ClientError> {
    client.publish(transport, clock, store, info, packet_id)
}

fn rust_arm() -> String {
    let mut out = String::new();

    for line in TRACE.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();

        match fields.first().copied() {
            Some("geometry") | Some("end") => {
                let _ = writeln!(out, "{line}");
            }

            Some(kind @ ("sub" | "unsub")) => {
                let unsubscribe = kind == "unsub";
                let shapes = field_of(&fields, 2, "shapes=");
                let count: usize = field_of(&fields, 3, "n=").parse().expect("a count");
                let qos = qos_of(field_of(&fields, 4, "qos="));
                let packet_id: u16 = field_of(&fields, 5, "id=").parse().expect("an id");
                let step: i32 = field_of(&fields, 6, "step=").parse().expect("a step");
                let properties = from_hex(field_of(&fields, 7, "props="));
                let is_connected = field_of(&fields, 8, "conn=") == "1";
                let gathered = field_of(&fields, 9, "wv=") == "1";

                let list: Vec<Subscription<'_>> = shapes
                    .split(',')
                    .take(count)
                    .map(|shape| subscription(shape, qos))
                    .collect();

                let mut buffer = [0u8; 512];
                let mut outgoing = [Record::default(); 8];
                let mut incoming = [Record::default(); 8];
                let mut clock = Frozen;
                let mut client = context(&mut buffer, &mut outgoing, &mut incoming, is_connected);

                let (answer, script) = if gathered {
                    let mut transport = Gathering(Script::new(step));
                    let answer = drive(
                        &mut client,
                        &mut transport,
                        &mut clock,
                        unsubscribe,
                        &list,
                        packet_id,
                        &properties,
                    );
                    (answer, transport.0)
                } else {
                    let mut transport = Script::new(step);
                    let answer = drive(
                        &mut client,
                        &mut transport,
                        &mut clock,
                        unsubscribe,
                        &list,
                        packet_id,
                        &properties,
                    );
                    (answer, transport)
                };

                let _ = writeln!(
                    out,
                    "{kind} {} {} {} {} {} {} {} {} {} -> {} calls={} log={} bytes={}",
                    fields[1],
                    fields[2],
                    fields[3],
                    fields[4],
                    fields[5],
                    fields[6],
                    fields[7],
                    fields[8],
                    fields[9],
                    status(answer),
                    script.calls,
                    if script.log.is_empty() {
                        "-"
                    } else {
                        &script.log
                    },
                    hex(&script.sent)
                );
            }

            Some("pub") => {
                let qos = qos_of(field_of(&fields, 2, "qos="));
                let packet_id: u16 = field_of(&fields, 3, "id=").parse().expect("an id");
                let topic = field_of(&fields, 4, "topic=");
                let payload = field_of(&fields, 5, "payload=");
                let step: i32 = field_of(&fields, 6, "step=").parse().expect("a step");
                let retransmits = field_of(&fields, 7, "retx=") == "1";
                let store_answer = field_of(&fields, 8, "store=") == "1";
                let dup = field_of(&fields, 9, "dup=") == "1";
                let retain = field_of(&fields, 10, "retain=") == "1";
                let properties = from_hex(field_of(&fields, 11, "props="));
                let is_connected = field_of(&fields, 12, "conn=") == "1";
                let gathered = field_of(&fields, 13, "wv=") == "1";

                let body: &[u8] = if payload == "-" {
                    b""
                } else {
                    payload.as_bytes()
                };

                let info = OutgoingPublish {
                    qos,
                    dup,
                    retain,
                    topic_name: topic.as_bytes(),
                    payload: body,
                    properties: &properties,
                };

                let mut buffer = [0u8; 512];
                let mut outgoing = [Record::default(); 8];
                let mut incoming = [Record::default(); 8];
                let mut clock = Frozen;
                let mut keeper = Keeper {
                    answer: store_answer,
                    calls: 0,
                    stored: Vec::new(),
                };
                let mut client = context(&mut buffer, &mut outgoing, &mut incoming, is_connected);

                let (answer, script) = if gathered {
                    let mut transport = Gathering(Script::new(step));
                    let answer = send_one(
                        &mut client,
                        &mut transport,
                        &mut clock,
                        retransmits.then_some(&mut keeper),
                        &info,
                        packet_id,
                    );
                    (answer, transport.0)
                } else {
                    let mut transport = Script::new(step);
                    let answer = send_one(
                        &mut client,
                        &mut transport,
                        &mut clock,
                        retransmits.then_some(&mut keeper),
                        &info,
                        packet_id,
                    );
                    (answer, transport)
                };

                let _ = writeln!(
                    out,
                    "pub {} {} {} {} {} {} {} {} {} {} {} {} {} -> {} calls={} log={} \
                     bytes={} storecalls={} stored={} rec={}",
                    fields[1],
                    fields[2],
                    fields[3],
                    fields[4],
                    fields[5],
                    fields[6],
                    fields[7],
                    fields[8],
                    fields[9],
                    fields[10],
                    fields[11],
                    fields[12],
                    fields[13],
                    status(answer),
                    script.calls,
                    if script.log.is_empty() {
                        "-"
                    } else {
                        &script.log
                    },
                    hex(&script.sent),
                    keeper.calls,
                    hex(&keeper.stored),
                    record_of(&client)
                );
            }

            Some("collide") => {
                let qos = qos_of(field_of(&fields, 2, "qos="));
                let packet_id: u16 = field_of(&fields, 3, "id=").parse().expect("an id");
                let dup = field_of(&fields, 4, "dup=") == "1";

                let first = OutgoingPublish {
                    qos,
                    dup: false,
                    retain: false,
                    topic_name: b"a/b",
                    payload: b"hello",
                    properties: &[],
                };
                let second = OutgoingPublish { dup, ..first };

                let mut buffer = [0u8; 512];
                let mut outgoing = [Record::default(); 8];
                let mut incoming = [Record::default(); 8];
                let mut transport = Script::new(64);
                let mut clock = Frozen;
                let mut client = context(&mut buffer, &mut outgoing, &mut incoming, true);

                let first_answer = status(client.publish(
                    &mut transport,
                    &mut clock,
                    None::<&mut NoStore>,
                    &first,
                    packet_id,
                ));

                // Only the SECOND publish's calls are interesting.
                transport.forget();

                let answer = status(client.publish(
                    &mut transport,
                    &mut clock,
                    None::<&mut NoStore>,
                    &second,
                    packet_id,
                ));

                let _ = writeln!(
                    out,
                    "collide {} {} {} {} first={first_answer} -> {answer} calls={} \
                     log={} bytes={} rec={}",
                    fields[1],
                    fields[2],
                    fields[3],
                    fields[4],
                    transport.calls,
                    if transport.log.is_empty() {
                        "-"
                    } else {
                        &transport.log
                    },
                    hex(&transport.sent),
                    record_of(&client)
                );
            }

            Some("cancel") => {
                let stateful = field_of(&fields, 2, "stateful=") == "1";
                let published = field_of(&fields, 3, "published=");
                let cancel_id: u16 = field_of(&fields, 4, "id=").parse().expect("an id");

                let publish_first = published.starts_with('1');
                let publish_id: u16 = published
                    .split('/')
                    .nth(1)
                    .and_then(|n| n.parse().ok())
                    .expect("an id");

                let mut buffer = [0u8; 512];
                let mut outgoing = [Record::default(); 8];
                let mut incoming = [Record::default(); 8];
                let mut transport = Script::new(64);
                let mut clock = Frozen;
                let mut client = MqttContext::new(&mut buffer);

                if stateful {
                    client.enable_qos(&mut outgoing, &mut incoming, 0);
                }

                client.connect_status = ConnectionStatus::Connected;

                if publish_first {
                    let info = OutgoingPublish {
                        qos: QoS::AtLeastOnce,
                        dup: false,
                        retain: false,
                        topic_name: b"a/b",
                        payload: b"hello",
                        properties: &[],
                    };

                    let _ = client.publish(
                        &mut transport,
                        &mut clock,
                        None::<&mut NoStore>,
                        &info,
                        publish_id,
                    );
                }

                let answer = status(client.cancel_callback(cancel_id));

                let _ = writeln!(
                    out,
                    "cancel {} {} {} {} -> {answer}",
                    fields[1], fields[2], fields[3], fields[4]
                );
            }

            _ => {}
        }
    }

    out
}

/// Every line of the C's trace, reproduced.
#[test]
fn the_outgoing_packets_agree_with_the_c() {
    let ours = rust_arm();

    for (theirs, ours) in TRACE.lines().zip(ours.lines()) {
        assert_eq!(theirs, ours, "the two arms disagree");
    }

    assert_eq!(
        TRACE.lines().count(),
        ours.lines().count(),
        "the two arms produced a different number of lines"
    );
}

/// The trace must contain the cases the module note claims.
#[test]
fn the_trace_carries_the_cases_it_argues_from() {
    for needed in [
        // The asymmetry: a SUBSCRIBE's first gather has no filter in it and an
        // UNSUBSCRIBE's does.
        "sub one-filter ",
        "unsub one-filter ",
        // A list that does not fit one gather, both ways round.
        "sub three-filters ",
        "unsub two-filters ",
        // A transport that takes one byte at a time, so every offset shows.
        "sub one-byte-at-a-time ",
        "pub one-byte-at-a-time ",
        // The stored copy, the one that was already a duplicate, and a refusal.
        "pub qos1-stored ",
        "pub qos1-stored-already-dup ",
        "pub qos1-store-refuses ",
        // The packet id that is already in flight.
        "collide qos1-same-id ",
        "collide qos1-same-id-dup ",
        // And forgetting one.
        "cancel the-one-in-flight ",
    ] {
        assert!(
            TRACE.lines().any(|line| line.starts_with(needed)),
            "the trace has lost `{needed}`"
        );
    }
}

/// The stored copy differs from the sent one **only** in the DUP bit.
///
/// The trace asserts both strings, which would also pass if the sender were
/// storing something else entirely that happened to match a second recorded
/// string. This reads the two out of the same line and checks the relationship
/// the module note claims.
#[test]
fn the_stored_copy_is_the_sent_one_with_dup_raised() {
    let mut checked = 0usize;

    for line in TRACE.lines() {
        if !line.starts_with("pub ") || line.contains(" storecalls=0 ") {
            continue;
        }

        let Some(sent) = line
            .split_whitespace()
            .find_map(|field| field.strip_prefix("bytes="))
        else {
            continue;
        };
        let Some(stored) = line
            .split_whitespace()
            .find_map(|field| field.strip_prefix("stored="))
        else {
            continue;
        };

        if sent == "-" {
            continue;
        }

        let sent = from_hex(sent);
        let stored = from_hex(stored);

        assert_eq!(stored.len(), sent.len(), "{line}");
        assert_eq!(
            stored[1..],
            sent[1..],
            "past the header byte they must match"
        );
        assert_eq!(
            stored[0] | 0x08,
            stored[0],
            "the stored copy must carry DUP"
        );
        assert_eq!(stored[0] & !0x08, sent[0] & !0x08, "{line}");

        checked += 1;
    }

    assert!(checked >= 3, "only {checked} stored publishes in the trace");
}

/// A SUBSCRIBE carries one filter per gather and an UNSUBSCRIBE two, and the
/// difference is only in the log.
///
/// The twenty-first shape of the guard says six tables that cannot be told
/// apart are one table with six names. This is its converse: two senders that
/// put the SAME bytes on the wire are still two senders, and the only place the
/// difference exists is the call count.
#[test]
fn the_two_list_senders_gather_differently_and_send_the_same() {
    let sub = TRACE
        .lines()
        .find(|line| line.starts_with("sub one-filter "))
        .expect("the case");
    let unsub = TRACE
        .lines()
        .find(|line| line.starts_with("unsub one-filter "))
        .expect("the case");

    let calls = |line: &str| -> usize {
        line.split_whitespace()
            .find_map(|field| field.strip_prefix("calls="))
            .and_then(|n| n.parse().ok())
            .expect("a count")
    };

    // Five vectors against four, for one filter each.
    assert_eq!(
        calls(sub),
        5,
        "a SUBSCRIBE's first gather has no filter in it"
    );
    assert_eq!(calls(unsub), 4, "an UNSUBSCRIBE's first gather does");

    // And the payloads are the same shape: both end in the filter.
    for line in [sub, unsub] {
        let bytes = line
            .split_whitespace()
            .find_map(|field| field.strip_prefix("bytes="))
            .expect("the bytes");

        assert!(bytes.contains("0003612f62"), "{line} lost its filter");
    }
}
