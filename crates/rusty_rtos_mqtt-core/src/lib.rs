#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]
//! `rusty_rtos_mqtt-core` — the pure heart of `rusty_rtos_mqtt`.
//!
//! Rules this crate lives by (from the Kairos mission plan):
//!
//! 1. `no_std` by default; `alloc` is a feature, never an assumption.
//! 2. No CPU, no registers, no allocator, no operating system. Ports and
//!    backends are separate WRAP crates.
//! 3. Every type that crosses to another Kairos package comes from
//!    `rusty_rtos_core`, so packages compose without conversions.
//! 4. Handles are indices, never pointers; nothing on a hot path allocates.
//! 5. `forbid(unsafe)`. The C kernel's trace is the oracle; the scalar path
//!    is the oracle; any faster path is gated identical against it.

#[cfg(feature = "alloc")]
extern crate alloc;

pub use rusty_rtos_core as rtos_core;

/// The names a firmware wants in scope.
pub mod prelude {
    pub use rusty_rtos_core::prelude::*;
}

/// Crate version, for manifests and logs.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// `core_mqtt_state.c`: the QoS publish state machine.
pub mod state;

/// The MQTT fixed header: a packet type, and a variable-byte length.
pub mod header;

/// The MQTT 5 property primitives: bounded reads out of a packet.
pub mod property;

/// The fixed header of every outgoing MQTT packet.
pub mod writer;

/// How big an outgoing packet will be.
pub mod size;

/// Reading an acknowledgement off the wire.
pub mod ack;

pub use ack::{
    AckError, AckInfo, Limits, PINGRESP_REMAINING_LENGTH, PUBREL, PacketInfo, deserialize_ack,
};
pub use header::{
    HeaderError, MAX_REMAINING_LENGTH, PacketHeader, REMAINING_LENGTH_INVALID,
    encode_variable_length, incoming_packet_valid, process_incoming_packet_type_and_length,
    variable_length_encoded_size,
};
pub use property::{PropertyError, PropertyReader, decode_variable_length, encode_string};
pub use size::{
    ListPacket, PINGREQ_PACKET_SIZE, PacketSize, SizeError, ack_packet_size, list_packet_size,
    subscribe_packet_size, unsubscribe_packet_size,
};
pub use state::{
    AckType, Cursor, Operation, PACKET_ID_INVALID, PublishRecords, PublishState, QoS, Record,
    StateError, calculate_state_ack, calculate_state_publish,
};
pub use writer::{
    ConnectInfo, PINGREQ, VERSION_5, WillInfo, serialize_ack_fixed, serialize_connect_fixed_header,
    serialize_disconnect_fixed, serialize_subscribe_header, serialize_unsubscribe_header,
};
