### In The Wild with 21 Active Installs

FREE RAG Converter Online -- <a href="https://RAGconverter.com">RAGconverter.com</a>

# rusty_rtos_mqtt

[![Remade With Rust](https://img.shields.io/badge/Remade%20With-Rust-000?logo=rust&logoColor=fff)](https://github.com/remade-with-rust)
[![By Mata Network](https://img.shields.io/badge/by-Mata%20Network-5b2be0)](https://www.mata.network)
[![crates.io](https://img.shields.io/crates/v/rusty_rtos_mqtt.svg)](https://crates.io/crates/rusty_rtos_mqtt)
[![docs.rs](https://docs.rs/rusty_rtos_mqtt/badge.svg)](https://docs.rs/rusty_rtos_mqtt)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

A `no_std` MQTT 5 client: the publish state machine, the whole wire codec in
both directions, the connection, the receive loop and the acknowledgements.
**All 218 of coreMQTT v5.0.2's 218 functions**, counted from the pinned source
by [a checked-in script](https://github.com/Remade-With-Rust/rusty_rtos_mqtt/blob/main/oracle/coverage.py).

Too big to remake in one go, so it was remade in twenty-four slices, each one
diffed against the C.

- **Every packet, both directions.** Builds and reads every MQTT packet, off a
  socket or out of a buffer; assembles, checks and walks back every property
  section either end may send; opens a connection, runs its receive loop,
  answers its acknowledgements and keeps it alive.
- **The hard part is the delivery state machine.** QoS 1 and QoS 2 in both
  directions, diffed against the C operation for operation and compared on
  *both record arrays after every operation*, not just the status returned.
- **Zero allocation**: two caller-supplied record arrays, sized independently,
  exactly as the C does it. `forbid(unsafe)`.

**No known gaps.** What is left is not coreMQTT but the things around it: its
CMock vectors, and a real broker.

- This package's plan: [docs/plans/rusty_rtos_mqtt.md](https://github.com/Remade-With-Rust/rusty_rtos_mqtt/blob/main/docs/plans/rusty_rtos_mqtt.md)
- Every number: [docs/LEDGER.md](https://github.com/Remade-With-Rust/rusty_rtos_mqtt/blob/main/docs/LEDGER.md)
- What each slice found: [docs/SLICES.md](https://github.com/Remade-With-Rust/rusty_rtos_mqtt/blob/main/docs/SLICES.md)
- The family plan: Kairos [`docs/plans/rtos-mission.md`](https://github.com/Remade-With-Rust/kairos/blob/main/docs/plans/rtos-mission.md)

**Claims discipline:** this README makes no performance or capability claim that
is not backed by a test, a benchmark ledger entry, or a kill test recorded in the
plan. "Scaffold" means scaffold. "Sim only" means the sim port; "builds, not
flashed" means no chip has run it.

## Status

**Complete.** Twenty-four proven slices and 203 tests, and
`python oracle/coverage.py --check` fails if the ledger names a function the
pinned source does not have.

Every packet a broker can send is read, off a socket or out of a buffer, and
every packet a client can send is written.

## What it is

- A pure-Rust remake of the corresponding FreeRTOS component. Same job, same
  names, same semantics, new code, permissive licence, `forbid(unsafe)` in
  the core.
- Arch-agnostic: the core crate is `no_std` (+ `alloc`) and knows nothing about
  a CPU, an allocator or an operating system. Ports and backends are thin,
  feature-gated WRAP crates.

## What it is not

- Not a fork of FreeRTOS and not a binding to it. The C kernel is the
  **oracle** this package is measured against, never a dependency.
- Not a rewrite of a radio blob, a ROM or a vendor driver. Where silicon must
  be touched, a port crate **wraps** `cortex-m-rt` / `riscv-rt` / `esp-hal`
  and says so.

## Conformance

**413 trace lines across 24 scenarios agree with `core_mqtt_state.c`**,
compiled verbatim from the pinned checkout (v5.0.2 at `04845c6a`).

```sh
cargo test -p rusty_rtos_mqtt-core
```

Twenty-four slices, each diffed against coreMQTT v5.0.2 compiled **verbatim**
from the pinned checkout at `04845c6a`. Every C arm's trace is checked in, so CI
needs no C toolchain.

```sh
cargo test -p rusty_rtos_mqtt-core      # 203 tests
python oracle/coverage.py --check       # 218 / 218
```

| | |
|---|---|
| the publish state machine | **413 trace lines** across 24 scenarios, comparing *both record arrays after every operation*, not just each status |
| the fixed header | **6,291,456 calls** — all 256 type bytes against every length pattern at every claimed length |
| topic matching | a 39x39 grid, printed in full |
| the rest | ~900 further trace lines across the packet writers, the size calculators, the acknowledgement deserializers, CONNACK, the incoming PUBLISH, DISCONNECT in both directions, CONNECT, the outgoing PUBLISH, SUBSCRIBE/UNSUBSCRIBE, the transport reader, the property validators, builders and reader, the client context, the send plumbing, the connection and the receive loop |

Every differential that passed first time was then deliberately broken. The
poison rows are in [`docs/LEDGER.md`](docs/LEDGER.md) beside the numbers they
defend, and the slice-by-slice account of what each one found is in
[`docs/SLICES.md`](docs/SLICES.md).

### What the differential found in the C

Each of these came from being unable to make our arm agree with the C, then
reading the C to find out why.

- A **one-byte buffer overflow** in the MQTT 5 property builders.
- The library **validates acknowledgement reason codes twice and disagrees
  with itself**.
- A **clean session leaks every stored PUBREL**.
- A **wildcard filter silently misses a whole class of topic**, visible as a
  column in the 39x39 grid.
- An acknowledgement with properties and no reason code **is never sent**.
- Several places MQTT 5.0 names a protocol error that coreMQTT does not
  enforce.


## The gate

This module takes no bytes from the network, so it looks safer than a parser. It
is not: **the packet ids come from the broker**. Every PUBACK, PUBREC, PUBREL and
PUBCOMP carries an id chosen by whoever is on the other end of the socket, and
each one indexes a record array.

| test | what it does |
|---|---|
| arbitrary operation sequences | 200 seeds x 60 operations over arrays of 1..4, with a four-id space so collisions and reuse happen constantly — which is what a broker replaying a session looks like |
| well-formedness after every step | no duplicate packet id in either array, no occupied record at QoS 0 or in state `Null`, no empty slot keeping stale fields |
| cursor termination | both resend cursors must finish; one that failed to advance past a match would spin rather than fail, the same hazard `rusty_rtos_json`'s iterator and `rusty_rtos_sntp`'s retry loops have |
| arbitrary header bytes | 200,000 buffers of 0..8 bytes with a claimed count of 0..12 — deliberately including counts LARGER than the buffer, which is the shape the C cannot survive |
| encoding into any buffer | 100,000 lengths into buffers of 0..7 bytes; a refused encode must leave the fill untouched |
| arbitrary property sections | 100,000 readers over 0..24 bytes with budgets up to 40 — routinely larger than the buffer, which is what an attacker sends — asserting the cursor never passes what exists |
| arbitrary property lengths | 200,000 buffers of 0..7 bytes through the property length decoder |
| encoding a string anywhere | 50,000 encodes with a claimed length independent of the source |
| **writing past the reported length** | all five writers at seven remaining lengths, into a filled buffer, asserting no byte beyond the returned count was touched — the claim a trace of the output cannot make |

A duplicate packet id in the records would make two messages share one
handshake, which is why that invariant is checked after every operation rather
than at the end.

## Using it

```rust
use rusty_rtos_mqtt::{AckType, Operation, PublishRecords, QoS, Record};

// The application sizes the arrays; on a device these are two statics.
let mut outgoing = [Record::default(); 8];
let mut incoming = [Record::default(); 4];
let mut records = PublishRecords::new(&mut outgoing, &mut incoming);

// Claim a packet id, then tell the records the PUBLISH went out.
records.reserve(42, QoS::ExactlyOnce)?;
let state = records.update_publish(42, Operation::Send, QoS::ExactlyOnce)?;

// Later, the broker's PUBREC arrives.
records.update_ack(42, AckType::PubRec, Operation::Receive)?;
```

After a session is resumed, walk what is still in flight:

```rust
use rusty_rtos_mqtt::Cursor;

let mut cursor = Cursor::new();
while let Some(packet_id) = records.publish_to_resend(&mut cursor) {
    // resend this PUBLISH, in this order
}
```

## Performance

No rows. Nothing here has been benchmarked and nothing has run on a chip. The
ledger carries this crate's counts.

## Portability

Builds `no_std` with no default features on `thumbv7em-none-eabihf` and
`riscv32imac-unknown-none-elf` (both verified), and CI holds it to
`thumbv8m.main-none-eabihf` and `riscv32imafc-unknown-none-elf` as well.

## Layout

```text
crates/rusty_rtos_mqtt          facade: re-exports + prelude; the crate you depend on
crates/rusty_rtos_mqtt-core     no_std (+ alloc); forbid(unsafe); types, traits, algorithms
firmware/                per-chip example projects, excluded from the workspace
docs/plans/              this package's plan and its hardening audit
docs/LEDGER.md           every number, with its method line
```

## Build

```sh
cargo test --workspace                                   # host: the tests
cargo check -p rusty_rtos_mqtt-core --no-default-features \
  --target thumbv7em-none-eabihf                         # Cortex-M4F class, no alloc
cargo check -p rusty_rtos_mqtt-core --no-default-features --features alloc \
  --target riscv32imac-unknown-none-elf                  # ESP32-C6 class, with alloc
```

CI holds the core to `thumbv7em-none-eabihf`, `thumbv8m.main-none-eabihf`,
`riscv32imac-unknown-none-elf` and `riscv32imafc-unknown-none-elf`, with and
without `alloc`, plus `cargo deny check`. Firmware examples (Xtensa needs the
esp toolchain; Cortex-M and RISC-V work on stable) are built from their own
directories under `firmware/`.

## Part of Remade With Rust

This crate is part of **[Kairos](https://github.com/Remade-With-Rust/kairos)** —
FreeRTOS remade in memory-safe Rust, as independent packages that expose the API
a FreeRTOS developer already knows and prove every scheduling decision against
the C kernel's own trace. `rusty_rtos_mqtt` is the K7 library that is **finished**:
all 218 of coreMQTT v5.0.2's 218 functions, counted from the pinned source by a
checked-in script.

**Where this sits for Mata.** Kairos is the real-time layer on the device
itself, and [`rusty_rtos_mqtt`](https://github.com/Remade-With-Rust/rusty_rtos_mqtt) is the way out of it.
Paired with the **MATA distributed cloud**, robotics and sensor data has two
routes — read it on the machine, or reach it through the cloud — with the same
memory-safe crates at both ends.

The family:
[`rusty_rtos_core`](https://crates.io/crates/rusty_rtos_core) (the shared vocabulary),
[`rusty_rtos_kernel`](https://crates.io/crates/rusty_rtos_kernel) (the scheduler),
[`rusty_rtos_port`](https://crates.io/crates/rusty_rtos_port) (the architecture seam),
[`rusty_rtos_heap`](https://crates.io/crates/rusty_rtos_heap) (the allocators),
[`rusty_rtos_json`](https://github.com/Remade-With-Rust/rusty_rtos_json) (coreJSON),
[`rusty_rtos_sntp`](https://github.com/Remade-With-Rust/rusty_rtos_sntp) (coreSNTP),
[`rusty_rtos_mqtt`](https://github.com/Remade-With-Rust/rusty_rtos_mqtt) (coreMQTT),
[`rusty_rtos_backoff`](https://github.com/Remade-With-Rust/rusty_rtos_backoff) (backoffAlgorithm),
[`rusty_rtos-capi`](https://github.com/Remade-With-Rust/rusty_rtos-capi) (the C ABI) and
[`rusty_rtos_demo`](https://github.com/Remade-With-Rust/rusty_rtos_demo) (the conformance corpus).
The last six are on GitHub and not yet on crates.io. Also check out
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

MIT OR Apache-2.0, at your option. FreeRTOS is MIT-licensed by Amazon.com,
Inc. or its affiliates; this crate remakes its API and behaviour from the
published sources and links no FreeRTOS code.

---

<!-- HARDENING-TABLE:BEGIN generated by use-protection-please — edit docs/plans/use-protection-please.md, not this block -->
## Hardening status

**Tier** critical-path · **Audited** unrecorded (survey) · **v1.0.0 gates** 6/16 · [Full checklist](https://github.com/Remade-With-Rust/rusty_rtos_mqtt/blob/main/docs/plans/use-protection-please.md)

`████░░░░░░░░░░░░░░░░` **22%** &nbsp;·&nbsp; 8 Completed · 0 Scheduled · 28 Incomplete · 19 N/A

| Phase | ✅ Completed | 🗓 Scheduled | ⬜ Incomplete | · N/A |
|---|--:|--:|--:|--:|
| 0 — Threat modeling | 0 | 0 | 2 | 0 |
| 1 — Toolchain | 2 | 0 | 2 | 0 |
| 2 — Supply chain | 2 | 0 | 6 | 0 |
| 3 — Code level | 3 | 0 | 4 | 0 |
| 4 — Static analysis | 0 | 0 | 1 | 0 |
| 5 — Dynamic analysis | 0 | 0 | 3 | 0 |
| 6 — Fuzzing and properties | 0 | 0 | 4 | 0 |
| 7 — Formal verification | 0 | 0 | 1 | 0 |
| 8 — Build and binary | 0 | 0 | 1 | 1 |
| 9 — Runtime privilege | 0 | 0 | 0 | 1 |
| 10 — Cryptography | 0 | 0 | 0 | 3 |
| 11 — CI/CD, release, and operations | 1 | 0 | 4 | 0 |
| 12 — Compliance controls | 0 | 0 | 0 | 14 |
| **Total** | **8** | **0** | **28** | **19** |

**Architect** — [Tim Almond](https://github.com/Ttimmahlax) — accountable for this unit's security design; rendered
<!-- HARDENING-TABLE:END -->
