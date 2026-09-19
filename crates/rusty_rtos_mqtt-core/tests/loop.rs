//! The receive loop against `core_mqtt.c`.
//!
//! The C arm is `oracle/loop_driver.c` driving the pinned v5.0.2 `core_mqtt.c`
//! verbatim, and its trace is checked in.
//!
//! `MQTT_ProcessLoop` has **three** inputs, not two: the transport in both
//! directions, the clock, and the **application callback** — which decides
//! whether the packet was accepted, what reason code the acknowledgement
//! carries, and whether it carries properties. So the callback is scripted too,
//! and every packet it is handed is logged.
//!
//! # An acknowledgement with properties and no reason code is never sent
//!
//! `qos1-with-property` adds one Reason String to a PUBACK and sets no reason
//! code. The C's sentinel for "the application did not set one" is `0xFF`, and
//! `validatePublishAckReasonCode`'s switch has no case for it — so it falls to
//! the default, answers `MQTTBadParameter`, and **nothing goes on the wire**.
//! The broker never hears about the publish and the handshake stops where it
//! stood. Reproduced, and drafted for upstream.
//!
//! # `?` in the callback log is a defect, not a gap
//!
//! The log's fourth field is how many reason codes the callback was handed.
//! For a PUBLISH and a PINGRESP it is `?`, because `handleIncomingPublish` and
//! the PINGRESP branch declare `MQTTDeserializedInfo_t deserializedInfo;` with
//! no initialiser and fill in three of its four members — so `pReasonCode` is
//! whatever was on the stack. This driver printed four different garbage values
//! before that was noticed.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::client::{
    AckReply, ClientError, Clock, ConnectionStatus, Event, EventHandler, MqttContext, NoStore,
    Store,
};
use rusty_rtos_mqtt_core::reader::{Recv, Sent, Transport};
use rusty_rtos_mqtt_core::state::{PublishState, QoS, Record};

const TRACE: &str = include_str!("../../../oracle/loop.trace");

/// Both directions of one transport, each scripted.
struct Script {
    send_step: i32,
    recv_script: Vec<i32>,
    recv_at: usize,
    incoming: Vec<u8>,
    at: usize,
    send_calls: usize,
    recv_calls: usize,
    sent: Vec<u8>,
    send_log: String,
    recv_log: String,
}

impl Transport for Script {
    fn recv(&mut self, into: &mut [u8]) -> Recv {
        self.recv_calls += 1;

        let left = self.incoming.len() - self.at;
        let mut answer = if self.recv_at < self.recv_script.len() {
            let step = self.recv_script[self.recv_at];
            self.recv_at += 1;
            step
        } else {
            self.recv_script.last().copied().unwrap_or(0)
        };

        if answer > 0 {
            answer = answer.min(into.len() as i32).min(left as i32);
        }

        if !self.recv_log.is_empty() {
            self.recv_log.push(',');
        }
        let _ = write!(self.recv_log, "{}:{answer}", into.len());

        if answer > 0 {
            let count = answer as usize;
            into[..count].copy_from_slice(&self.incoming[self.at..self.at + count]);
            self.at += count;
            Recv::Bytes(count)
        } else if answer == 0 {
            Recv::Nothing
        } else {
            Recv::Failed
        }
    }

    fn send(&mut self, bytes: &[u8]) -> Sent {
        self.send_calls += 1;

        let answer = self.send_step.min(bytes.len() as i32);

        if !self.send_log.is_empty() {
            self.send_log.push(',');
        }
        let _ = write!(self.send_log, "{}:{answer}", bytes.len());

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

struct Ticking {
    now: u32,
    step: u32,
}

impl Clock for Ticking {
    fn now_ms(&mut self) -> u32 {
        let now = self.now;
        self.now = self.now.wrapping_add(self.step);
        now
    }
}

/// The scripted application.
struct App {
    accept: bool,
    reason: i32,
    add_property: bool,
    calls: usize,
    log: String,
}

/// Which packet types the C's handlers actually SET `pReasonCode` for.
///
/// For the others it is read off the stack, so this arm prints `?` where the C
/// prints `?` and neither pretends to a number.
fn sets_reason_code(packet_type: u8) -> bool {
    matches!(
        packet_type & 0xF0,
        0x40 | 0x50 | 0x60 | 0x70 | 0x90 | 0xB0 | 0xE0
    )
}

impl EventHandler for App {
    fn on_event(&mut self, event: &Event<'_>, reply: &mut AckReply<'_>) -> bool {
        self.calls += 1;

        if !self.log.is_empty() {
            self.log.push(',');
        }

        let offered = u8::from(reply.accepted());

        if sets_reason_code(event.packet_type) {
            let _ = write!(
                self.log,
                "{:02x}/{}/{offered}/{}",
                event.packet_type,
                event.packet_id,
                event.reason_codes.len()
            );
        } else {
            let _ = write!(
                self.log,
                "{:02x}/{}/{offered}/?",
                event.packet_type, event.packet_id
            );
        }

        if self.reason >= 0 {
            reply.set_reason_code(self.reason as u8);
        }

        if self.add_property {
            reply.with_properties(|builder| {
                let _ = builder.reason_string(b"no", None);
            });
        }

        self.accept
    }
}

/// The retransmit store, logging every key it is handed.
struct Keeper {
    answer: bool,
    store_calls: usize,
    clear_calls: usize,
    keys: Vec<u32>,
}

impl Store for Keeper {
    fn store(&mut self, packet_id: u32, _parts: &[&[u8]]) -> bool {
        self.store_calls += 1;
        self.keys.push(packet_id);
        self.answer
    }

    fn retrieve(&mut self, _packet_id: u32) -> Option<&[u8]> {
        None
    }

    fn clear(&mut self, packet_id: u32) {
        self.clear_calls += 1;
        self.keys.push(packet_id);
    }
}

fn status(result: &Result<(), ClientError>) -> &'static str {
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

fn state_number(state: PublishState) -> u8 {
    match state {
        PublishState::Null => 0,
        PublishState::PublishSend => 1,
        PublishState::PubAckSend => 2,
        PublishState::PubRecSend => 3,
        PublishState::PubRelSend => 4,
        PublishState::PubCompSend => 5,
        PublishState::PubAckPending => 6,
        PublishState::PubRecPending => 7,
        PublishState::PubRelPending => 8,
        PublishState::PubCompPending => 9,
        PublishState::PublishDone => 10,
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

/// The C's `seed_records`.
fn seed(outgoing: &mut [Record], incoming: &mut [Record], kind: u8) {
    match kind {
        1 => {
            outgoing[0] = Record {
                packet_id: 5,
                qos: QoS::AtLeastOnce,
                state: PublishState::PubAckPending,
            };
        }
        2 => {
            outgoing[0] = Record {
                packet_id: 5,
                qos: QoS::ExactlyOnce,
                state: PublishState::PubRecPending,
            };
        }
        3 => {
            incoming[0] = Record {
                packet_id: 6,
                qos: QoS::ExactlyOnce,
                state: PublishState::PubRelPending,
            };
        }
        4 => {
            outgoing[0] = Record {
                packet_id: 6,
                qos: QoS::ExactlyOnce,
                state: PublishState::PubCompPending,
            };
        }
        5 => {
            incoming[0] = Record {
                packet_id: 5,
                qos: QoS::AtLeastOnce,
                state: PublishState::PubAckSend,
            };
        }
        6 => {
            incoming[0] = Record {
                packet_id: 6,
                qos: QoS::ExactlyOnce,
                state: PublishState::PubRecSend,
            };
        }
        _ => {}
    }
}

fn rust_arm() -> String {
    let mut out = String::new();

    for line in TRACE.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let kind = fields.first().copied();

        if !matches!(kind, Some("proc" | "recv")) {
            let _ = writeln!(out, "{line}");
            continue;
        }

        let process = kind == Some("proc");
        let incoming_bytes = from_hex(field_of(&fields, 2, "in="));
        let send_step: i32 = field_of(&fields, 3, "sstep=").parse().expect("a step");
        let recv_script: Vec<i32> = field_of(&fields, 4, "rscript=")
            .split('/')
            .map(|step| step.parse().expect("a step"))
            .collect();
        let clock_step: u32 = field_of(&fields, 5, "cstep=").parse().expect("a step");

        let cb: Vec<&str> = field_of(&fields, 6, "cb=").split('/').collect();
        let accept = cb[0] == "1";
        let reason: i32 = cb[1].parse().expect("a reason");
        let add_property = cb[2] == "1";

        let use_ack_props = field_of(&fields, 7, "props=") == "1";
        let store_spec = field_of(&fields, 8, "store=");
        let have_store = store_spec.starts_with('1');
        let store_answer = store_spec.ends_with('1');
        let waiting_ping = field_of(&fields, 9, "ping=") == "1";
        let last_tx: u32 = field_of(&fields, 10, "tx=").parse().expect("a time");
        let last_rx: u32 = field_of(&fields, 11, "rx=").parse().expect("a time");
        let seed_kind: u8 = field_of(&fields, 12, "seed=").parse().expect("a seed");
        let ping_time: u32 = field_of(&fields, 13, "pingtime=").parse().expect("a time");
        let calls: usize = field_of(&fields, 14, "calls=").parse().expect("a count");

        let mut transport = Script {
            send_step,
            recv_script,
            recv_at: 0,
            incoming: incoming_bytes,
            at: 0,
            send_calls: 0,
            recv_calls: 0,
            sent: Vec::new(),
            send_log: String::new(),
            recv_log: String::new(),
        };
        let mut clock = Ticking {
            now: 0,
            step: clock_step,
        };
        let mut app = App {
            accept,
            reason,
            add_property,
            calls: 0,
            log: String::new(),
        };
        let mut keeper = Keeper {
            answer: store_answer,
            store_calls: 0,
            clear_calls: 0,
            keys: Vec::new(),
        };

        let mut buffer = [0u8; 64];
        let mut ack_properties = [0u8; 32];
        let mut outgoing = [Record::default(); 4];
        let mut incoming = [Record::default(); 4];

        let mut client = MqttContext::new(&mut buffer);

        if use_ack_props {
            client.enable_qos(&mut outgoing, &mut incoming, &mut ack_properties);
        } else {
            client.enable_qos(&mut outgoing, &mut incoming, &mut []);
        }

        client.connect_status = ConnectionStatus::Connected;
        client.set_keep_alive_seconds(60);
        client.set_waiting_for_ping_resp(waiting_ping);
        client.set_last_packet_tx_time(last_tx);
        client.set_last_packet_rx_time(last_rx);
        client.set_ping_req_send_time(ping_time);

        {
            let (out_records, in_records) = client.records_mut();
            seed(out_records, in_records, seed_kind);
        }

        // A packet arriving in pieces needs TWO calls to finish, and the
        // second one is the only thing that can see where the first left the
        // index.
        let mut answer = Ok(());

        for _ in 0..calls {
            answer = if have_store {
                if process {
                    client.process_loop(&mut transport, &mut clock, Some(&mut keeper), &mut app)
                } else {
                    client.receive_loop(&mut transport, &mut clock, Some(&mut keeper), &mut app)
                }
            } else if process {
                client.process_loop(&mut transport, &mut clock, None::<&mut NoStore>, &mut app)
            } else {
                client.receive_loop(&mut transport, &mut clock, None::<&mut NoStore>, &mut app)
            };
        }

        let (out0, out0s) = client
            .outgoing()
            .first()
            .map_or((0, 0), |r| (r.packet_id, state_number(r.state)));
        let (in0, in0s) = client
            .incoming()
            .first()
            .map_or((0, 0), |r| (r.packet_id, state_number(r.state)));

        let keys = if keeper.keys.is_empty() {
            "-".to_owned()
        } else {
            keeper
                .keys
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join("/")
        };

        let _ = writeln!(
            out,
            "{} {} {} {} {} {} {} {} {} {} {} {} {} {} {} -> {} state={} index={} scalls={} \
             slog={} sbytes={} rcalls={} rlog={} cbcalls={} cblog={} waiting={} sent={} \
             out0={out0}/{out0s} in0={in0}/{in0s} store={} clear={} keys={keys}",
            fields[0],
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
            fields[14],
            status(&answer),
            state(client.connect_status),
            client.index(),
            transport.send_calls,
            if transport.send_log.is_empty() {
                "-"
            } else {
                &transport.send_log
            },
            hex(&transport.sent),
            transport.recv_calls,
            if transport.recv_log.is_empty() {
                "-"
            } else {
                &transport.recv_log
            },
            app.calls,
            if app.log.is_empty() { "-" } else { &app.log },
            u8::from(client.waiting_for_ping_resp()),
            u8::from(client.control_packet_sent()),
            keeper.store_calls,
            keeper.clear_calls,
        );
    }

    out
}

/// Every line of the C's trace, reproduced.
#[test]
fn the_receive_loop_agrees_with_the_c() {
    let ours = rust_arm();

    for (n, (theirs, ours)) in TRACE.lines().zip(ours.lines()).enumerate() {
        assert_eq!(
            ours, theirs,
            "line {n} diverged\n  the C: {theirs}\n  ours : {ours}"
        );
    }

    assert_eq!(
        TRACE.lines().count(),
        ours.lines().count(),
        "the two arms produced a different number of lines"
    );
}

/// The trace must carry the cases the module note argues from.
#[test]
fn the_trace_carries_the_cases_it_argues_from() {
    for needed in [
        // Reassembly, both ways, and across two calls.
        "proc two-publishes-one-read ",
        "proc half-then-the-rest ",
        "proc half-a-publish ",
        "proc one-byte-at-a-time ",
        // Every acknowledgement, in both directions.
        "proc puback ",
        "proc pubrec ",
        "proc pubrel ",
        "proc pubcomp ",
        // The one packet the two loops treat differently.
        "proc pingresp ",
        "recv pingresp ",
        // A duplicate is forwarded at QoS 1 and not at QoS 2.
        "proc qos1-collision ",
        "proc qos2-collision ",
        // And a reason code the acknowledgement being SENT may not carry.
        "proc pubrel-bad-reason ",
        "proc pubrel-good-reason ",
        // The application's three powers.
        "proc qos1-callback-refuses ",
        "proc qos1-with-reason ",
        "proc qos1-with-property ",
        // And keep alive, which only runs when nothing arrived.
        "proc keepalive-tx-timeout ",
        "proc keepalive-rx-timeout ",
        "proc keepalive-not-checked-when-busy ",
    ] {
        assert!(
            TRACE.lines().any(|line| line.starts_with(needed)),
            "the trace has lost `{needed}`"
        );
    }
}

/// An acknowledgement with properties and no reason code is never sent.
///
/// The sentinel the C uses for "the application set none" is `0xFF`, and the
/// reason-code validator has no case for it. One property is enough to reach
/// that path, so adding a Reason String to a PUBACK silently costs the PUBACK.
#[test]
fn a_property_without_a_reason_code_costs_the_acknowledgement() {
    let line = TRACE
        .lines()
        .find(|line| line.starts_with("proc qos1-with-property "))
        .expect("the case");

    assert!(line.contains("-> BadParameter "), "{line}");
    assert!(line.contains(" sbytes=- "), "nothing may go out: {line}");

    // The same publish with a reason code set DOES get its acknowledgement.
    let ok = TRACE
        .lines()
        .find(|line| line.starts_with("proc qos1-with-reason "))
        .expect("the case");

    assert!(ok.contains("-> Success "), "{ok}");
    assert!(
        ok.split_whitespace()
            .any(|field| field.starts_with("sbytes=40")),
        "the PUBACK must go out: {ok}"
    );
}

/// Keep alive is checked only when nothing arrived.
#[test]
fn a_busy_connection_never_pings() {
    let busy = TRACE
        .lines()
        .find(|line| line.starts_with("proc keepalive-not-checked-when-busy "))
        .expect("the case");
    let idle = TRACE
        .lines()
        .find(|line| line.starts_with("proc keepalive-rx-timeout "))
        .expect("the case");

    // The idle one sends a PINGREQ; the busy one sends nothing, though its
    // transmit time is just as stale.
    assert!(idle.contains(" sbytes=c000 "), "{idle}");
    assert!(busy.contains(" sbytes=- "), "{busy}");
}
