#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]
//! `rusty_rtos_mqtt` — the first slice of coreMQTT, remade in Rust.
//!
//! **What is here.** Two slices, both diffed against the C.
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
//! **What is not:** the rest of the wire codec (~5,870 lines plus 2,056 of MQTT
//! 5 property codecs) and the connection state machine (`core_mqtt.c`, 5,618
//! lines). This crate can recognise a packet arriving and track what is in
//! flight; it cannot yet build one.
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
