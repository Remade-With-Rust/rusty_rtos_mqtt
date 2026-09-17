#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]
//! `rusty_rtos_mqtt` — the first slice of coreMQTT, remade in Rust.
//!
//! **What is here.** Four slices, each diffed against the C.
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
//! **What is not:** the packet BODIES (~5,870 lines plus 2,056 of MQTT 5
//! property tables) and the connection state machine (`core_mqtt.c`, 5,618
//! lines). 12.4 % of coreMQTT is remade. This crate has the pieces; it does not
//! yet put a packet together.
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
