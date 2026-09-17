#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]
//! `rusty_rtos_mqtt` — the first slice of coreMQTT, remade in Rust.
//!
//! **What is here:** `core_mqtt_state.c` — the QoS 1 and QoS 2 delivery state
//! machine, in both directions, diffed against the C operation for operation
//! across 24 scenarios. The differential compares both record arrays after
//! every operation, not just the status, because their ORDER is the order a
//! resumed session resends in.
//!
//! **What is not:** the wire serializer (`core_mqtt_serializer.c` and the MQTT 5
//! property codecs, 8,819 lines) and the connection state machine
//! (`core_mqtt.c`, 5,618 lines). This crate tracks what is in flight; it cannot
//! yet put anything on the wire.
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
