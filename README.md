# rusty_rtos_mqtt

[![crates.io](https://img.shields.io/crates/v/rusty_rtos_mqtt.svg)](https://crates.io/crates/rusty_rtos_mqtt)
[![docs.rs](https://docs.rs/rusty_rtos_mqtt/badge.svg)](https://docs.rs/rusty_rtos_mqtt)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

A `no_std` MQTT publish state machine, fixed-header codec and MQTT 5 property
primitives — three proven slices of the Kairos remake of coreMQTT.
MIT OR Apache-2.0.

**K7's fourth library, and the first one too big to remake in one go.** coreMQTT
v5.0.2 is **21,102 lines**. `core_mqtt_state.c` is 1,206 of them and includes
nothing but its own header — no bytes, no transport, no clock — so it is a
complete, provable unit on its own, and it is where MQTT's hardest correctness
lives.

- **Proven**: the QoS 1 and QoS 2 delivery state machine, in both directions,
  diffed against the C **operation for operation** across 24 scenarios as a
  413-line trace — and that trace compares **both record arrays after every
  operation**, not just the status each call returns.
- **Proven**: the fixed header — the packet type and the variable-byte
  remaining length every packet starts with — over **6,291,456 calls**: every
  one of the 256 type bytes against every length pattern at every claimed
  length, compared by per-status counts and an FNV-1a digest.
- **Proven**: the MQTT 5 property primitives — the bounded integer, string and
  user-property reads every property in every packet goes through — over 104
  trace lines and a 6,480-call sweep, comparing the cursor and the budget after
  every read, not just the answer.
- **Zero allocation**: two caller-supplied arrays, sized independently, exactly
  as the C does it. `forbid(unsafe)`.

**Known gaps, and they are still most of coreMQTT.** The packet bodies —
CONNECT, PUBLISH, SUBSCRIBE and the property *tables* that sit on top of the
primitives — and the connection state machine (`core_mqtt.c`, 5,618 lines) are
**not written**. This crate can recognise a packet arriving, read its properties
and track what is in flight; it cannot yet build one.


Part of **Kairos**, the Remade-With-Rust programme that rebuilds the FreeRTOS
portfolio in memory-safe Rust, as independent packages that expose the API a
FreeRTOS developer already knows and prove every scheduling decision against
the C kernel's own trace.

- This package's plan: [docs/plans/rusty_rtos_mqtt.md](docs/plans/rusty_rtos_mqtt.md)
- Every number: [docs/LEDGER.md](docs/LEDGER.md)
- The family plan: Kairos `docs/plans/rtos-mission.md` (umbrella repo)

**Claims discipline:** this README makes no performance or capability claim that
is not backed by a test, a benchmark ledger entry, or a kill test recorded in the
plan. "Scaffold" means scaffold. "Sim only" means the sim port; "builds, not
flashed" means no chip has run it.

## Status

**Three slices built and proven; the rest of coreMQTT is not.** 413 trace lines
agree with `core_mqtt_state.c`, 6,291,456 calls with the fixed-header codec, and
104 lines plus a 6,480-call sweep with the MQTT 5 property primitives — all at
the pinned v5.0.2. **11.2 % of the library.** 25 tests. This crate can recognise
a packet arriving and read its properties; it cannot yet build one.

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

### The order of the records is the thing

MQTT's delivery guarantees are a state machine per in-flight message: QoS 1 is a
two-step handshake, QoS 2 a four-step one, in both directions — and all of it
has to survive the connection dropping in the middle. When a session resumes,
whatever was in flight is resent **from these records**.

So MQTT 5.0 requires message ordering, and these arrays are not a free list:

* a new record is **appended** after the last occupied slot, never dropped into
  the first hole;
* a full array of holes is **compacted**, preserving relative order;
* and a PUBREC for an outgoing QoS 2 publish deliberately **deletes the record
  and re-adds it at the end**, so the PUBRELs it now owes resend in the order
  the publishes went out.

That last one is the behaviour a status-only differential would miss entirely: a
transcription could return every correct status and still resend a session's
backlog out of order, on exactly the reconnect this module exists to survive.
It has its own test, and the differential compares both arrays after every
operation.

**Poison-proven on nine behaviours, all caught:** filling a hole instead of
appending, never compacting, a PUBREC that does not move the record, dropping
either of the duplicate-packet self-transitions, a resend cursor that does not
advance, a PUBREL routed to the wrong array, dropping the ack/QoS sanity check,
and rewriting a record on a self-transition.

Two of those needed a scenario adding before they would fire, and one of them is
worth stating. The ack/QoS check — which refuses a PUBACK unless the record is
QoS 1, and everything else unless it is QoS 2 — is **invisible for most
mismatches**, because the transition would have failed anyway. It is visible in
exactly one shape: a QoS 1 publish sitting in `PubAckPending` handed a PUBCOMP
computes `PublishDone`, and that transition is *legal*. Without the check, a
handshake that never happened completes quietly. The scenario for it was added
because the poison did not fire.

### The self-transitions are reconnect stories

Four transitions in the table go to the state the record is already in, and each
one is a specific recovery: a duplicate PUBLISH whose PUBREC was lost, a
duplicate PUBREL whose PUBCOMP was lost, a PUBREL being resent, an outgoing
publish being resent. They are not slack in the table. A test names them, so
removing one is a failure rather than a silent narrowing of what a session can
recover from.

## The fixed header

**6,291,456 calls agree with `core_mqtt_serializer.c`** — every one of the 256
possible type bytes, against every remaining-length byte pattern, at every
claimed length.

Every MQTT packet begins with one type byte and a **remaining length** encoded
as one to four bytes, seven bits at a time. It is the first thing a device
parses off a socket, before it knows what kind of packet it is holding, and it
is the classic place to attack an MQTT implementation. There are three ways to
lie about a length and the C refuses all three:

1. **Too many bytes.** The multiplier is checked *before* each byte, so a fifth
   continuation byte is refused rather than shifted off the top.
2. **Too large.** Four bytes can express more than the 268,435,455 the
   specification allows.
3. **Non-minimal.** `0x80 0x00` decodes to zero, and so does `0x00` — but only
   the second is minimal. The C rejects the first by comparing the bytes it
   consumed against the size the answer *should* have taken.

The third is the same shape as the over-long UTF-8 rule `rusty_rtos_json` had to
get right, and for the same reason: a decoder that accepts non-minimal encodings
hands an attacker two spellings of one length, which is how a length check one
layer up gets bypassed.

### Named cases and an exhaustive sweep

The differential does both, deliberately. 30 named cases print every observable,
so a divergence names itself. The sweep compares per-status **counts** plus an
FNV-1a **digest** of all 6.29 million answers — exhaustive coverage that a
reader can still check, where a digest alone would say only that something moved.

**Poison-proven on eight behaviours, all caught first time:** dropping the
non-minimal check, allowing a fifth length byte, an off-by-one on the
available-bytes test, a PUBREL without its reserved bit, accepting a
client-only packet type, an off-by-one in the size helper, the continuation bit
on the wrong byte, and a header length that omits the type byte.

### A guarantee the C cannot make

`MQTT_ProcessIncomingPacketTypeAndLength` takes a pointer and a count, and
`pBuffer[ bytesDecoded + 1U ]` trusts the count: an `available` larger than the
allocation reads whatever is next in memory. Here the count is only ever an
upper bound on a `get`, so an over-large one produces `NeedMoreBytes` and
nothing else. There is a test for exactly that.

## The property primitives

**104 trace lines plus a 6,480-call sweep agree with
`core_mqtt_serializer_private.c`.**

MQTT 5 adds properties to almost every packet, and every one of them is decoded
through the same handful of primitives: a one-, two- or four-byte integer, a
length-prefixed string, or a user property, which is two strings. They carry two
rules between them, and both are protocol requirements:

1. **A property may appear once.** A repeat is a protocol error, not
   last-one-wins.
2. **Every read is bounded by the property length**, which was itself decoded
   from the packet a moment earlier — so a string claiming more bytes than the
   property has left must be refused *before* the read.

### A failed read still moves the cursor

The differential compares the status, the value, **and where the cursor and the
budget were left**. That third part is the one that matters. `decodeUtf8`
consumes its two length bytes and charges them to the budget *before* it
discovers the body does not fit, so a refusal leaves the cursor two bytes on and
the budget two smaller.

A transcription that tidied that up would look more correct and would disagree
with the C on every malformed packet. It is reproduced deliberately, it has its
own test, and the "tidy" version is one of the poisons.

### Two variable-length decoders, not one

This slice's `decode_variable_length` is the **property** length decoder.
[The fixed header](#the-fixed-header) has its own. They are not the same
function: one starts at index 0 and is bounded by a buffer length, the other
starts at index 1 and is bounded by a count of bytes received, and they differ
in how they treat an out-of-range value. Reusing either for the other is the
mistake a single-function test cannot see, so both are transcribed and both get
their own exhaustive sweep.

**Poison-proven on eight behaviours, seven caught:** allowing a duplicate
property, charging the length bytes only after the body fits, a one-byte budget
for a two-byte length, a three-byte budget for a four-byte integer, dropping the
non-minimal check, a little-endian string length, and a user property that does
not reset its seen flag between key and value.

The eighth did not fire and is **provably dead code**. The property length
decoder has an in-loop range check against 268,435,456 that can never be
reached: the multiplier guard stops the loop after four bytes, and four bytes of
seven bits reach exactly 268,435,455 — one less. The check stays because the C
has it and a differential arm does not tidy its oracle, and a unit test pins the
arithmetic, because the bound is a relationship between two constants that could
move.

### A second bound the C does not have

`decodeUtf8` checks the claimed length against the property **budget** and then
indexes. If the packet claims a budget larger than the bytes actually received —
which an attacker chooses freely — the C reads past the buffer. Here the budget
is checked first, exactly as the C does, and then the slice is taken with `get`.
There is a test for it, and it is the second instance of the category the fixed
header found: a differential proves we match the C's *answers*, and says nothing
about what the C does on inputs that violate its own preconditions.

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

## License

MIT OR Apache-2.0, at your option. FreeRTOS is MIT-licensed by Amazon.com,
Inc. or its affiliates; this crate remakes its API and behaviour from the
published sources and links no FreeRTOS code.

---

<!-- HARDENING-TABLE:BEGIN generated by use-protection-please — edit docs/plans/use-protection-please.md, not this block -->
## Hardening status

**Tier** critical-path · **Audited** unrecorded (survey) · **v1.0.0 gates** 6/16 · [Full checklist](docs/plans/use-protection-please.md)

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
