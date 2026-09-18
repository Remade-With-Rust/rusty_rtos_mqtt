#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]
//! `rusty_rtos_mqtt` — the first slice of coreMQTT, remade in Rust.
//!
//! **What is here.** Nine slices, each diffed against the C.
//!
//! `core_mqtt_state.c` — the QoS 1 and QoS 2 delivery state machine, in both
//! directions, across 24 scenarios. The differential compares both record
//! arrays after every operation, not just the status, because their ORDER is
//! the order a resumed session resends in.
//!
//! The **fixed header** out of `core_mqtt_serializer.c` — the packet type and
//! the variable-byte remaining length every packet starts with — over
//! 6,291,456 calls: every one of the 256 type bytes against every length
//! pattern at every claimed length.
//!
//! The **MQTT 5 property primitives** out of `core_mqtt_serializer_private.c`
//! — the bounded integer, string and user-property reads every property in
//! every packet goes through. The differential compares the cursor and the
//! budget after every read, because a refused string has already consumed its
//! length bytes and a transcription that tidied that up would disagree with the
//! C on every malformed packet.
//!
//! The **fixed-header writers** — the header of every outgoing packet type,
//! byte for byte, including the CONNECT flags byte that packs six independent
//! decisions and is swept exhaustively.
//!
//! The **packet-size calculators** that feed those writers — where the
//! remaining length a writer is handed comes from. Proven up to and across the
//! 268,435,455 boundary, at the exact value each check tests, and reconciled
//! against the writers themselves.
//!
//! The **acknowledgement deserializers** — the first code here whose whole
//! input a broker chooses. Every ack a client can receive except a CONNACK,
//! with all three of its single-byte decision tables swept over 256 values and
//! the accepted set printed rather than hashed, which is how three divergences
//! from MQTT 5.0 came out of it.
//!
//! The **CONNACK** — the first packet a broker sends and the only one that sets
//! connection-wide state. Its reason-code and property tables are swept over
//! 256 values each, the property identifier five times over because each one
//! introduces a value of a different width, and both tables turn out to be
//! exactly MQTT 5.0's.
//!
//! The **incoming PUBLISH** — the last packet a broker can send, and the only
//! one that carries application data. Its payload length is a four-term
//! subtraction on numbers a broker chose, and an identity test asserts that the
//! parts reconstruct the packet rather than merely fitting inside it. Two more
//! divergences from MQTT 5.0 came out of it.
//!
//! The **DISCONNECT, both directions** — the packet MQTT 5 made bidirectional,
//! and the one the line above claimed too early. One validation table read
//! twice with different answers, a property table per direction, and a server
//! reason code a stock client refuses.
//!
//! **What is not:** the OUTGOING packet bodies (~3,447 lines plus 2,056 of
//! outgoing MQTT 5 property tables) and the connection state machine
//! (`core_mqtt.c`, 5,618 lines). 27.9 % of coreMQTT is remade. This crate can
//! read a session and end one; it cannot yet start one.
//!
//! Zero allocation, `no_std`, `forbid(unsafe)`.
//!
//! This is the facade: it re-exports the `no_std` core. Depend on this crate;
//! reach into the sub-crates only when you are building a port or a backend.
//!
//! Part of Kairos (Remade With Rust). Plan: `docs/plans/rusty_rtos_mqtt.md`.

pub use rusty_rtos_mqtt_core::*;

/// The names a firmware wants in scope.
pub mod prelude {
    pub use rusty_rtos_mqtt_core::prelude::*;
}
