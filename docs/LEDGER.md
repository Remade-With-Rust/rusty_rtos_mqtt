# rusty_rtos_mqtt — the ledger

Every number this package claims, with the run that produced it. A row
without a method is not a number. Counters before clocks; an external oracle
before a self-metric; the method line names the machine, the pinning, the arm
order and the null-arm floor for anything timed.

## Conformance (2026-09-17) — the publish state machine

| quantity | value | method |
|---|---|---|
| trace lines agreeing with the C | **413 / 413** | `cargo test -p rusty_rtos_mqtt-core --test state`. C arm: `oracle/state_driver.c` driving `core_mqtt_state.c` compiled verbatim from v5.0.2 at `04845c6a` with `-DMQTT_DO_NOT_USE_CUSTOM_CONFIG`, the library's own switch for its shipped defaults. Our arm replays the scenarios from the trace's own `cfg` lines. |
| scenarios | **24** | both QoS 1 round trips, both QoS 2 round trips, collision, both arrays exhausted, compaction, the PUBREC reordering (alone and on a full array), the resend cursors, duplicate publish and duplicate PUBREL, resends, illegal transitions, acks without records, removals, packet id zero, a one-record array, packet id reuse, an out-of-range ack type, and the two ack/QoS mismatches. |
| what is compared per operation | **the status, the new state, and BOTH record arrays** | the records are the state, and their ORDER is the resend order. A status-only comparison would bless a transcription that reordered a session's backlog. |
| outcomes reached | **6 of 6** | Success, BadParameter, NoMemory, StateCollision, IllegalState, BadResponse. A standing test fails if any is never produced, and if any of the nine occupied states is never observed in the arrays. |
| poison rows | **9 caught, 0 missed** | filling a hole instead of appending, never compacting, a PUBREC that does not move the record, dropping either duplicate-packet self-transition, a cursor that does not advance, a PUBREL routed to the wrong array, dropping the ack/QoS check, and rewriting a record on a self-transition. |

**Two poisons needed work before they would fire, and the reasons differ.** The
ack/QoS check was a WORKLOAD gap: it is invisible for most mismatches because
the transition would fail anyway, and visible only for a QoS 1 publish in
`PubAckPending` handed a PUBCOMP — where the computed `PublishDone` is a legal
transition and the handshake would complete quietly. A scenario was added. The
self-transition guard is genuinely an optimisation; a unit test pins why.

## Conformance (2026-09-17) — the fixed header

| quantity | value | method |
|---|---|---|
| sweep calls agreeing with the C | **6,291,456 / 6,291,456** | `cargo test -p rusty_rtos_mqtt-core --test header`. Every one of the 256 type bytes x 8^4 remaining-length patterns x 6 claimed lengths. Compared by per-status COUNTS plus an FNV-1a DIGEST of every answer, so a single divergence anywhere moves the digest. |
| named cases | **30** | printed in full, so a divergence names itself: the sweep proves agreement, the cases say where. |
| statuses reached | **4 of 4** | Success 1,924,608, BadResponse 2,028,032, NeedMoreBytes 1,290,240, NoDataAvailable 1,048,576. |
| refusal share | **69 %** | a standing test fails if the sweep ever accepts more than half its inputs — a fixed-header decoder that accepts most of what it is shown is not checking anything. |
| encoder probes | **16** | every boundary of the 1/2/3/4-byte encoding, round-tripped. |
| poison rows | **8 caught, 0 missed** | the non-minimal check, a fifth length byte, the available-bytes off-by-one, a PUBREL without its reserved bit, a client-only type accepted, the size helper's boundary, the continuation bit on the wrong byte, and a header length omitting the type byte. All fired first time. |

**The three ways to lie about a length**, each with a named test: too many bytes,
too large, and non-minimal. The last is the same shape as the over-long UTF-8
rule in `rusty_rtos_json` — two spellings of one value is how a length check one
layer up gets bypassed.

**A guarantee the C cannot make.** `MQTT_ProcessIncomingPacketTypeAndLength`
takes a pointer and a count and trusts the count, so an `available` larger than
the allocation reads past the buffer. Ours uses the count only as an upper bound
on a `get`. There is a test for it.

## Conformance (2026-09-17) — the property primitives

| quantity | value | method |
|---|---|---|
| trace lines agreeing with the C | **104 / 104** | `cargo test -p rusty_rtos_mqtt-core --test property`. C arm: `oracle/property_driver.c` driving `core_mqtt_serializer_private.c` verbatim from v5.0.2 at `04845c6a`. |
| scenarios | **13** | one of each integer width, a duplicate property, each width against a budget one byte short, five string shapes including one claiming 65,535 bytes, three user-property shapes, and a mixed run ending in exhaustion. |
| what is compared per read | **the status, the value, the CURSOR and the BUDGET** | a failed `decodeUtf8` has already consumed its two length bytes, so it leaves the cursor moved and the budget smaller. Comparing only the status would bless a transcription that tidied that up and then disagreed with the C on every malformed packet. |
| `decodeVariableLength` sweep | **6,480 calls** | every 4-byte pattern from a six-value alphabet at every buffer length 0..4, compared by counts (3,510 accepted, 2,970 refused) and an FNV-1a digest. |
| poison rows | **8 introduced, 7 caught** | duplicate property allowed, length bytes charged late, a one-byte budget for a two-byte length, a three-byte budget for a four-byte integer, the non-minimal check dropped, a little-endian string length, and a user property that does not reset its seen flag. |

**The eighth poison is provably dead code.** The property length decoder has an
in-loop check against 268,435,456 that can never fire: the multiplier guard
stops the loop after four bytes, and four bytes of seven bits reach exactly
268,435,455 — one less. Verified as arithmetic, kept because the C keeps it, and
pinned by a unit test because the bound is a relationship between two constants.

**A second bound the C does not have.** `decodeUtf8` checks the claimed length
against the property BUDGET and then indexes, so a packet claiming a budget
larger than the bytes received makes the C read past the buffer. Ours checks the
budget first, exactly as the C does, then takes the slice with `get`. Second
instance of the category the fixed header found.

## The gate (2026-09-17)

The packet ids driving this module come from the broker, so they are
attacker-chosen even though no bytes are parsed here.

| quantity | value | method |
|---|---|---|
| random state operations | **12,000** | 200 seeds x 60 operations over arrays of 1..4 slots, with a four-id space so collisions and reuse happen constantly. |
| well-formedness checks | **24,000** | both arrays after every operation: no duplicate packet id, no occupied record at QoS 0 or in state `Null`, no empty slot keeping stale fields. A duplicate id would make two messages share one handshake. |
| cursor termination | **asserted** | 99 seeds x 2 cursors; a cursor that failed to advance past a match would spin rather than fail. |

## The build fact (2026-09-17)

| gate | result |
|---|---|
| `cargo test -p rusty_rtos_mqtt-core` | 25 passed, 0 failed (4 unit, 8 gate, 3 state, 5 header, 5 property) |
| `cargo clippy --all-targets --all-features` under the workspace lint policy | clean, 0 warnings |
| `cargo build -p rusty_rtos_mqtt --no-default-features --target thumbv7em-none-eabihf` | passes |
| `cargo build -p rusty_rtos_mqtt --no-default-features --target riscv32imac-unknown-none-elf` | passes |

## Scope, in lines (2026-09-17)

A count that belongs here because the README's honesty depends on it.

| part of coreMQTT v5.0.2 | lines | state |
|---|---:|---|
| `core_mqtt_state.c` | 1,206 | **remade and proven** |
| `core_mqtt_serializer.c`, the fixed-header codec | ~240 | **remade and proven** |
| `core_mqtt_serializer_private.c`, the property primitives | ~309 | **remade and proven** |
| `core_mqtt_serializer.c`, the rest | ~5,870 | not written |
| `core_mqtt_serializer_private.c`, the rest | ~344 | not written |
| `core_mqtt_prop_serializer.c` | 1,176 | not written |
| `core_mqtt_prop_deserializer.c` | 880 | not written |

| `core_mqtt.c` | 5,618 | not written |
| **total** | **15,643** (plus 5,459 of headers) | **11.2 % remade** |

No speed number and no size number: nothing here has been benchmarked, and
nothing has run on a chip.
