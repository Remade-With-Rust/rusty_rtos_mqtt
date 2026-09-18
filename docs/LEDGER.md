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

## Conformance (2026-09-17) — the fixed-header writers

| quantity | value | method |
|---|---|---|
| trace lines agreeing with the C | **51 / 51** | `cargo test -p rusty_rtos_mqtt-core --test writer`, byte for byte. C arm: `oracle/writer_driver.c` driving `core_mqtt_serializer_private.c` verbatim from v5.0.2 at `04845c6a`. |
| CONNECT flag combinations | **1,536** | every combination of clean session, will present, will QoS (3), will retain, username and password, across four keep-alive values and four remaining lengths, compared by an FNV-1a digest of the length and the flags byte — with eight printed in full so a mismatch is localisable. |
| named cases | **7 acks, 12 sub/unsub, 20 disconnects, 8 connects** | each printed with its full output bytes. |
| poison rows | **8 introduced, 7 caught** | flags swapped, will QoS 2 on the wrong bit, clean session on the reserved bit, a little-endian keep alive, a DISCONNECT that always writes a reason, UNSUBSCRIBE with the SUBSCRIBE type byte, and a protocol name without its length prefix. |

**One poison found a gap in the GATE rather than the code.** Making
`serialize_disconnect_fixed` always write a reason code — so it writes one byte
*beyond the length it returns* — passed every test including the byte-for-byte
differential, because the differential only ever looks at `buffer[..n]`. A
caller packing something after the header would have had it clobbered. "Wrote
the right bytes" and "wrote only those bytes" are two claims and a trace can
only make the first; a test now makes the second, and catches that poison.

**The eighth is an equivalence.** Writing the will QoS as a shifted number
rather than two flags produces identical bytes, because the two bits are
adjacent and the legal QoS values are 0, 1 and 2. The C's form is kept and a
unit test pins the adjacency.

## Conformance (2026-09-18) — the packet-size calculators

| quantity | value | method |
|---|---|---|
| trace lines agreeing with the C | **53 / 53** | `cargo test -p rusty_rtos_mqtt-core --test size`. C arm: `oracle/size_driver.c` driving `core_mqtt_serializer.c` verbatim from v5.0.2 at `04845c6a`. |
| named cases | **14 acks, 17 subscribes, 17 unsubscribes, 1 pingreq, 2 empty lists** | each printed with its remaining length and its packet size, so a divergence names itself. |
| what is compared per case | **the status, and on success BOTH out-parameters** | see the row below on what a `Result` cannot reproduce. |
| refusal reasons reachable | **5 of 5** | a zero maximum, a property length past the limit, an empty list, a topic filter too long for its 16-bit prefix, and a packet larger than the broker's maximum. Checked directly rather than read off the trace, because the C collapses all five to `MQTTBadParameter`. |
| poison rows | **9 introduced, 7 caught** | the ack's property-length term, the ack's two fixed bytes, the subscribe options byte, the two-byte length prefix on each filter, the packet id, the filter-length upper bound, and the final maximum-packet-size check. |
| cross-slice agreement | **8 packets** | each calculator's answer fed straight into the matching [writer](#conformance-2026-09-17--the-fixed-header-writers) and the bytes reconciled. Neither differential alone can catch the two slices agreeing with the C and disagreeing with each other. |

**The three poisons that did not fire all had the same cause, and the fix was
arithmetic rather than more cases.** Eight topic filters of 65,535 bytes come to
524,280 and the limit is 268,435,455 — three orders of magnitude away, so
neither limit check was reachable at all. The property length is the only input
that can bridge that, and the C takes it through an `MQTTPropBuilder_t` whose
`currentIndex` the caller sets. Cases *near* the limit were still not enough:
each check lands on an EXACT value, and a case that overshoots cannot tell a
`>=` from a `>`. The two boundary cases are computed — `prop=268435348` puts the
in-loop check at exactly 268,435,456, `prop=268435446` puts the final check at
exactly 268,435,455.

**One of the three was a real gap; the other two are genuine properties, and one
of those is an asymmetry between the arms.** The final check is now caught; the
in-loop one is not, because in the C
it is LOAD-BEARING (a `uint32_t` accumulator whose additions wrap, so a long
enough list would wrap past zero and come out under the limit) and here the
additions SATURATE, so an overflowing list ends at `u32::MAX` and the final
check refuses it anyway. Kept for fidelity; pinned by
`the_in_loop_check_is_subsumed_by_saturating_arithmetic`, which records that
removing it would be safe here and unsafe there. The early zero-maximum check is
the same shape — the smallest packet is four bytes — and is pinned the same way.

**A refusal hands the caller nothing, and that is a divergence we chose.** The C
writes both out-parameters BEFORE its final maximum-packet-size check, so a
failed call has still updated them, and a caller who ignored the status would
serialize with a length the library had just refused. A `Result` has no error
path to hand a value back on, so the misuse has no Rust equivalent. The driver
therefore prints values only on success. Fourth member of the family this
package keeps finding: **a differential bounds what the C answers, never what it
touches, and never what it leaves behind.**

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
| `cargo test -p rusty_rtos_mqtt-core` | 40 passed, 0 failed (11 unit, 8 gate, 3 state, 5 header, 5 property, 5 writer, 3 size) |
| `cargo clippy --all-targets --all-features` under the workspace lint policy | clean, 0 warnings |
| `cargo build -p rusty_rtos_mqtt --no-default-features --target thumbv7em-none-eabihf` | passes |
| `cargo build -p rusty_rtos_mqtt --no-default-features --target riscv32imac-unknown-none-elf` | passes |

## Scope, in lines (2026-09-18)

A count that belongs here because the README's honesty depends on it.

| part of coreMQTT v5.0.2 | lines | state |
|---|---:|---|
| `core_mqtt_state.c` | 1,206 | **remade and proven** |
| `core_mqtt_serializer.c`, the fixed-header codec | ~240 | **remade and proven** |
| `core_mqtt_serializer_private.c`, the property primitives | ~309 | **remade and proven** |
| `core_mqtt_serializer_private.c`, the fixed-header writers | ~180 | **remade and proven** |
| `core_mqtt_serializer.c`, the packet-size calculators | ~300 | **remade and proven** |
| `core_mqtt_serializer.c`, the rest | ~5,570 | not written |
| `core_mqtt_serializer_private.c`, the rest | ~164 | not written |
| `core_mqtt_prop_serializer.c` | 1,176 | not written |
| `core_mqtt_prop_deserializer.c` | 880 | not written |

| `core_mqtt.c` | 5,618 | not written |
| **total** | **15,643** (plus 5,459 of headers) | **14.3 % remade** |

No speed number and no size number: nothing here has been benchmarked, and
nothing has run on a chip.
