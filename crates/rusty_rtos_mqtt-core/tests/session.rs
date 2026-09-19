//! Opening a connection, against `core_mqtt.c`.
//!
//! The C arm is `oracle/session_driver.c` driving the pinned v5.0.2
//! `core_mqtt.c` verbatim, and its trace is checked in.
//!
//! `MQTT_Connect` is the first function in the library that both **sends and
//! receives**, so this differential scripts both directions and logs both — and
//! compares the whole connection context afterwards, because a CONNACK's job is
//! to set that context and a status alone would bless a reader that dropped
//! every property it carried.
//!
//! # Five bytes the application never asked for
//!
//! A CONNECT with no properties does not go out with an empty property section.
//! coreMQTT builds one containing a single Maximum Packet Size, set to the size
//! of the network buffer — `05 27 00 00 01 00` for a 256-byte one — on the
//! grounds that otherwise a server may send a packet the library cannot read.
//! It is in `sbytes` on nearly every line here.
//!
//! # And a clean session leaks every stored PUBREL
//!
//! `clean-pubrel-only-with-store` has a retransmit store wired up, one PUBREL
//! in flight, and a clean session — and the clear callback is **not called**.
//! `handleCleanSession` zeroes the outgoing record array and then asks
//! `MQTT_PubrelToResend`, which reads that array, what to clear. The resumed
//! case one line below proves the record is findable: it re-sends exactly that
//! PUBREL. Reproduced, and drafted for upstream.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::client::{
    ClientError, Clock, ConnectionStatus, MqttContext, NoStore, Store,
};
use rusty_rtos_mqtt_core::connect::{Connect, Will};
use rusty_rtos_mqtt_core::reader::{Recv, Sent, Transport};
use rusty_rtos_mqtt_core::state::{PublishState, QoS, Record};
use rusty_rtos_mqtt_core::writer::{ConnectInfo, WillInfo};

const TRACE: &str = include_str!("../../../oracle/session.trace");

/// Both directions of one transport, each scripted by a single step.
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

        // The last step is held once the script runs out.
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

/// A clock that advances by a fixed step per read.
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

/// The three retransmit callbacks, counted.
struct Keeper {
    answer: bool,
    packet: Vec<u8>,
    retrieve_calls: usize,
    retrieved: Vec<u32>,
    clear_calls: usize,
    cleared: Vec<u32>,
}

impl Store for Keeper {
    fn store(&mut self, _packet_id: u32, _parts: &[&[u8]]) -> bool {
        true
    }

    fn retrieve(&mut self, packet_id: u32) -> Option<&[u8]> {
        self.retrieve_calls += 1;

        // The KEY, which is the only place the incoming-publish flag shows.
        self.retrieved.push(packet_id);

        if self.answer {
            Some(&self.packet)
        } else {
            None
        }
    }

    fn clear(&mut self, packet_id: u32) {
        self.clear_calls += 1;
        self.cleared.push(packet_id);
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

/// The C's `seed_records`, which puts messages in flight before connecting.
fn seed(outgoing: &mut [Record], kind: u8) {
    match kind {
        1 => {
            outgoing[0] = Record {
                packet_id: 5,
                qos: QoS::AtLeastOnce,
                state: PublishState::PubAckPending,
            };
            outgoing[1] = Record {
                packet_id: 6,
                qos: QoS::ExactlyOnce,
                state: PublishState::PubCompPending,
            };
        }
        2 => {
            outgoing[0] = Record {
                packet_id: 6,
                qos: QoS::ExactlyOnce,
                state: PublishState::PubCompPending,
            };
        }
        _ => {}
    }
}

fn rust_arm() -> String {
    let mut out = String::new();

    for line in TRACE.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();

        if !matches!(fields.first().copied(), Some("conn")) {
            let _ = writeln!(out, "{line}");
            continue;
        }

        let client_id = field_of(&fields, 2, "id=");
        let clean_session = field_of(&fields, 3, "clean=") == "1";
        let keep_alive: u16 = field_of(&fields, 4, "ka=").parse().expect("a keep alive");
        let properties = from_hex(field_of(&fields, 5, "props="));
        let with_will = field_of(&fields, 6, "will=") == "1";
        let timeout_ms: u32 = field_of(&fields, 7, "timeout=").parse().expect("a timeout");
        let send_step: i32 = field_of(&fields, 8, "sstep=").parse().expect("a step");
        let recv_script: Vec<i32> = field_of(&fields, 9, "rscript=")
            .split('/')
            .map(|step| step.parse().expect("a step"))
            .collect();
        let clock_step: u32 = field_of(&fields, 10, "cstep=").parse().expect("a step");
        let pre_connected = field_of(&fields, 11, "pre=") == "1";
        let store_spec = field_of(&fields, 12, "store=");
        let seed_kind: u8 = field_of(&fields, 13, "seed=").parse().expect("a seed");
        let connack = from_hex(field_of(&fields, 14, "connack="));

        let have_store = store_spec.starts_with('1');
        let retrieve_answer = store_spec.ends_with('1');

        let will = with_will.then_some(Will {
            info: WillInfo {
                qos: QoS::AtMostOnce,
                retain: false,
            },
            topic_name: b"w/t",
            payload: b"bye",
            properties: &[],
        });

        let connect = Connect {
            info: ConnectInfo {
                clean_session,
                keep_alive_seconds: keep_alive,
                username: None,
                password: None,
            },
            client_identifier: client_id.as_bytes(),
            properties: &properties,
            will,
        };

        let mut transport = Script {
            send_step,
            recv_script,
            recv_at: 0,
            incoming: connack,
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
        let mut keeper = Keeper {
            answer: retrieve_answer,
            // The C's `g_stored_packet`: a PUBREL for packet 5.
            packet: vec![0x62, 0x02, 0x00, 0x05],
            retrieve_calls: 0,
            retrieved: Vec::new(),
            clear_calls: 0,
            cleared: Vec::new(),
        };

        let mut buffer = [0u8; 256];
        let mut outgoing = [Record::default(); 4];
        let mut incoming = [Record::default(); 4];

        let mut client = MqttContext::new(&mut buffer);
        client.enable_qos(&mut outgoing, &mut incoming, &mut []);

        // AFTER `enable_qos`, because that zeroes the arrays — exactly as
        // `MQTT_InitStatefulQoS` does, which is why the C driver seeds after
        // its `fresh()` too.
        seed(client.outgoing_mut(), seed_kind);

        if pre_connected {
            client.connect_status = ConnectionStatus::Connected;
        }

        let mut session_present = false;
        let answer = if have_store {
            client.connect(
                &mut transport,
                &mut clock,
                Some(&mut keeper),
                &connect,
                timeout_ms,
                &mut session_present,
            )
        } else {
            client.connect(
                &mut transport,
                &mut clock,
                None::<&mut NoStore>,
                &connect,
                timeout_ms,
                &mut session_present,
            )
        };

        let session = u8::from(session_present);
        let server = client.properties.server;
        let records = client.outgoing();
        let (id0, state0) = records
            .first()
            .map_or((0, 0), |r| (r.packet_id, state_number(r.state)));
        let (id1, state1) = records
            .get(1)
            .map_or((0, 0), |r| (r.packet_id, state_number(r.state)));

        let list = |keys: &[u32]| -> String {
            if keys.is_empty() {
                "-".to_owned()
            } else {
                keys.iter()
                    .map(u32::to_string)
                    .collect::<Vec<_>>()
                    .join("/")
            }
        };
        let cleared = list(&keeper.cleared);
        let retrieved = list(&keeper.retrieved);

        let _ = writeln!(
            out,
            "conn {} {} {} {} {} {} {} {} {} {} {} {} {} {} -> {} session={session} \
             state={} scalls={} slog={} sbytes={} rcalls={} rlog={} ka={} out={} in={} \
             maxqos={} retain={} maxpkt={} alias={} wild={} subid={} shared={} recvmax={} \
             expiry={} retrieve={} retrieved={retrieved} clear={} cleared={cleared} rec0={id0}/{state0} \
             rec1={id1}/{state1}",
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
            client.keep_alive_seconds(),
            client.outgoing_records(),
            client.incoming_records(),
            server.max_qos,
            server.retain_available,
            server.max_packet_size,
            server.topic_alias_max,
            server.wildcard_available,
            server.subscription_id_available,
            server.shared_available,
            server.receive_max,
            client.properties.session_expiry,
            keeper.retrieve_calls,
            keeper.clear_calls,
        );
    }

    out
}

/// Every line of the C's trace, reproduced.
#[test]
fn the_connection_agrees_with_the_c() {
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
        // Both directions in pieces, and both failing.
        "conn send-one-byte-at-a-time ",
        "conn recv-one-byte-at-a-time ",
        "conn send-fails ",
        "conn recv-fails ",
        // The two ways the CONNACK wait can end.
        "conn no-connack-retry-count ",
        "conn no-connack-timeout ",
        // A body that never finishes arriving.
        "conn truncated-body ",
        // What a CONNACK does to the context.
        "conn server-limits ",
        "conn small-receive-max ",
        // And the session paths.
        "conn resumed-seeded-with-store ",
        "conn resumed-seeded-no-store ",
        "conn resumed-seeded-retrieve-fails ",
        "conn clean-pubrel-only-with-store ",
        "conn resumed-pubrel-only-no-store ",
    ] {
        assert!(
            TRACE.lines().any(|line| line.starts_with(needed)),
            "the trace has lost `{needed}`"
        );
    }
}

/// A clean session does not clear a stored PUBREL, and a resumed one re-sends
/// the same record.
///
/// Two lines of the trace read together. `clean-pubrel-only-with-store` has a
/// store, a PUBREL in flight and `clear=0`; `resumed-pubrel-only-no-store` has
/// the same record and sends a PUBREL for it. The second is what makes the
/// first a **defect** rather than an empty array.
#[test]
fn a_clean_session_leaves_a_stored_pubrel_behind() {
    let clean = TRACE
        .lines()
        .find(|line| line.starts_with("conn clean-pubrel-only-with-store "))
        .expect("the case");
    let resumed = TRACE
        .lines()
        .find(|line| line.starts_with("conn resumed-pubrel-only-no-store "))
        .expect("the case");

    let field = |line: &str, name: &str| -> String {
        line.split_whitespace()
            .find_map(|field| field.strip_prefix(name))
            .expect("the field")
            .to_owned()
    };

    assert!(
        clean.contains(" store=1/1 "),
        "the clean case must have a store wired up"
    );
    assert_eq!(field(clean, "clear="), "0", "{clean}");
    assert_eq!(field(clean, "cleared="), "-", "{clean}");

    // The same record, found and acted on, one line later.
    assert!(
        field(resumed, "sbytes=").ends_with("62020006"),
        "the resumed case must re-send the PUBREL: {resumed}"
    );
}

/// A CONNECT with no properties still carries a property section.
///
/// Five bytes the application never asked for: a Maximum Packet Size equal to
/// the network buffer. A test rather than a comment, because it is the sort of
/// thing a later simplification would delete as dead.
#[test]
fn a_connect_with_no_properties_still_announces_a_maximum_packet_size() {
    let plain = TRACE
        .lines()
        .find(|line| line.starts_with("conn plain "))
        .expect("the case");
    let bytes = plain
        .split_whitespace()
        .find_map(|field| field.strip_prefix("sbytes="))
        .expect("the bytes");

    // `05` property length, `27` Maximum Packet Size, then 256 big-endian —
    // which is the driver's network buffer size, not anything the caller said.
    assert!(
        bytes.contains("0527000001 00".replace(' ', "").as_str()),
        "the invented property section is missing: {plain}"
    );
}
