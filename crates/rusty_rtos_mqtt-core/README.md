# rusty_rtos_mqtt-core

[![Remade With Rust](https://img.shields.io/badge/Remade%20With-Rust-000?logo=rust&logoColor=fff)](https://github.com/remade-with-rust)
[![By Mata Network](https://img.shields.io/badge/by-Mata%20Network-5b2be0)](https://www.mata.network)
[![crates.io](https://img.shields.io/crates/v/rusty_rtos_mqtt-core.svg)](https://crates.io/crates/rusty_rtos_mqtt-core)
[![docs.rs](https://docs.rs/rusty_rtos_mqtt-core/badge.svg)](https://docs.rs/rusty_rtos_mqtt-core)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

The pure `no_std` core of [`rusty_rtos_mqtt`](https://github.com/Remade-With-Rust/rusty_rtos_mqtt): a complete
MQTT 5 client, the Kairos remake of coreMQTT. No CPU, no allocator, no
operating system. `#![forbid(unsafe_code)]`.

**All 218 of coreMQTT v5.0.2's 218 functions**, counted from the pinned source
by [a checked-in script](https://github.com/Remade-With-Rust/rusty_rtos_mqtt/blob/main/oracle/coverage.py) —
`python oracle/coverage.py --check` fails if the ledger names a function the
source does not have.

- **The QoS 1 and 2 state machine**, both directions, diffed against the C
  **operation for operation** across 24 scenarios as a 413-line trace —
  comparing *both record arrays after every operation*, not just each status.
- **The fixed header** over **6,291,456 calls**: every one of the 256 type bytes
  against every length pattern at every claimed length.
- **Every packet a broker can send and every packet a client can send**, plus
  the connection, the receive loop and the acknowledgements.
- **Zero allocation**: two caller-supplied record arrays, sized independently,
  exactly as the C does it.

## Conformance

Twenty-four proven slices, each diffed against coreMQTT v5.0.2 compiled
**verbatim** at `04845c6a`. Every C arm's trace is checked in, so CI needs no C
toolchain.

```sh
cargo test -p rusty_rtos_mqtt-core      # 203 tests
python oracle/coverage.py --check       # 218 / 218, 100 %
```

Every differential that passed first time was then deliberately broken, and the
poison rows are in
[`docs/LEDGER.md`](https://github.com/Remade-With-Rust/rusty_rtos_mqtt/blob/main/docs/LEDGER.md) beside the
numbers they defend.

## Part of Remade With Rust

This crate is part of **[Kairos](https://github.com/Remade-With-Rust/kairos)** — FreeRTOS remade in memory-safe
Rust, as independent packages that expose the API a FreeRTOS developer already
knows and prove every scheduling decision against the C kernel's own trace.

**Where this sits for Mata.** Kairos is the real-time layer on the device
itself, and [`rusty_rtos_mqtt`](https://crates.io/crates/rusty_rtos_mqtt) is the way out of it.
Paired with the **MATA distributed cloud**, robotics and sensor data has two
routes — read it on the machine, or reach it through the cloud — with the same
memory-safe crates at both ends.

The family:
[`rusty_rtos_core`](https://crates.io/crates/rusty_rtos_core) (the shared vocabulary),
[`rusty_rtos_kernel`](https://crates.io/crates/rusty_rtos_kernel) (the scheduler),
[`rusty_rtos_port`](https://crates.io/crates/rusty_rtos_port) (the architecture seam),
[`rusty_rtos_heap`](https://crates.io/crates/rusty_rtos_heap) (the allocators),
[`rusty_rtos_json`](https://crates.io/crates/rusty_rtos_json) (coreJSON),
[`rusty_rtos_sntp`](https://crates.io/crates/rusty_rtos_sntp) (coreSNTP),
[`rusty_rtos_mqtt`](https://crates.io/crates/rusty_rtos_mqtt) (coreMQTT),
[`rusty_rtos_backoff`](https://crates.io/crates/rusty_rtos_backoff) (backoffAlgorithm),
[`rusty_rtos-capi`](https://crates.io/crates/rusty_rtos-capi) (the C ABI) and
[`rusty_rtos_demo`](https://crates.io/crates/rusty_rtos_demo) (the conformance corpus).
All ten are on crates.io. Also check out
the rest of **[github.com/remade-with-rust](https://github.com/remade-with-rust)**.

## About Mata Network

<!-- ORG BOILERPLATE — keep identical across repos -->

**[Mata Network](https://www.mata.network/)** builds sovereign, self-hostable
privacy infrastructure — *"stop sacrificing your privacy for convenience"*:
wallet & identity, a password manager, a contact manager, and a browser
extension that stops your information leaking as you browse.

**Remade With Rust** is our open-source home for the permissively-licensed
building blocks that work depends on — including
[remade_ffmpeg_rs](https://github.com/Remade-With-Rust/remade_ffmpeg_rs) (the
FFmpeg alternative) and [FFAI](https://github.com/Remade-With-Rust/FFAI) (the
AI media toolkit).

→ **[www.mata.network](https://www.mata.network/)**

<!-- /ORG BOILERPLATE -->

## License

MIT OR Apache-2.0, at your option. FreeRTOS is MIT-licensed by Amazon.com, Inc.
or its affiliates; this crate remakes its API and behaviour from the published
sources and links no FreeRTOS code.
