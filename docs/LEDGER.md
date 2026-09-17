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

## The gate (2026-09-17)

The packet ids driving this module come from the broker, so they are
attacker-chosen even though no bytes are parsed here.

| quantity | value | method |
|---|---|---|
| random operations | **12,000** | 200 seeds x 60 operations over arrays of 1..4 slots, with a four-id space so collisions and reuse happen constantly. |
| well-formedness checks | **24,000** | both arrays after every operation: no duplicate packet id, no occupied record at QoS 0 or in state `Null`, no empty slot keeping stale fields. A duplicate id would make two messages share one handshake. |
| cursor termination | **asserted** | 99 seeds x 2 cursors; a cursor that failed to advance past a match would spin rather than fail. |

## The build fact (2026-09-17)

| gate | result |
|---|---|
| `cargo test -p rusty_rtos_mqtt-core` | 7 passed, 0 failed (2 unit, 2 gate, 3 differential) |
| `cargo clippy --all-targets --all-features` under the workspace lint policy | clean, 0 warnings |
| `cargo build -p rusty_rtos_mqtt --no-default-features --target thumbv7em-none-eabihf` | passes |
| `cargo build -p rusty_rtos_mqtt --no-default-features --target riscv32imac-unknown-none-elf` | passes |

## Scope, in lines (2026-09-17)

A count that belongs here because the README's honesty depends on it.

| part of coreMQTT v5.0.2 | lines | state |
|---|---:|---|
| `core_mqtt_state.c` | 1,206 | **remade and proven** |
| `core_mqtt_serializer.c` | 6,110 | not written |
| `core_mqtt_prop_serializer.c` | 1,176 | not written |
| `core_mqtt_prop_deserializer.c` | 880 | not written |
| `core_mqtt_serializer_private.c` | 653 | not written |
| `core_mqtt.c` | 5,618 | not written |
| **total** | **15,643** (plus 5,459 of headers) | **7.7 % remade** |

No speed number and no size number: nothing here has been benchmarked, and
nothing has run on a chip.
