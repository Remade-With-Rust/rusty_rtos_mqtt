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

## Conformance (2026-09-18) — the acknowledgement deserializers

| quantity | value | method |
|---|---|---|
| trace lines agreeing with the C | **57 / 57** | `cargo test -p rusty_rtos_mqtt-core --test ack`. C arm: `oracle/ack_driver.c` driving `core_mqtt_serializer.c` verbatim from v5.0.2 at `04845c6a`. |
| named cases | **51** | every publish-ack shape, both list acks, PINGRESP, the routing refusals and the maximum-packet-size boundary, plus two generated long packets that exercise a TWO-byte remaining length at exactly the maximum and one under. |
| what is compared per case | **the status, and on success the packet id, the reason codes AND the property section** | the three things the C hands back. A status-only comparison would bless a transcription that answered correctly while pointing at the wrong bytes. |
| single-byte sweeps | **3 x 256 calls** | the SUBACK/UNSUBACK status table, the publish-ack reason table (twice, at `0x40` and `0x62`), and the packet-type routing switch. Each prints its ACCEPTED SET in full rather than a digest, because twelve values out of 256 is a table a reader can check. |
| divergences from MQTT 5.0 found | **3** | drafted at `kairos-upstream/drafts/coremqtt-unsuback-reason-code-table.md` and asserted from the CHECKED-IN trace, so the suite reports it if the pinned oracle ever changes its mind. |
| poison rows | **11 introduced, 10 caught** | `0x11` accepted, the granted-QoS arm dropped, the reason code read one byte early, a PUBREL routed by its `0x60` nibble, `requestProblemInfo` ignored, a zero packet id allowed, the type byte dropped from the packet size, a repeated reason string allowed, a CONNACK refused as a bad packet, and the pub-ack property section treated as a bound rather than an exact fit. |

**The three divergences, because they are the point of the slice.**
`readSubackStatus` is the SUBACK table and it serves UNSUBACK too, so it accepts
granted-QoS bytes an UNSUBACK cannot grant and refuses **`0x11`, "No
subscription existed"**, which MQTT 5.0 §3.11.3 lists as legal — a client
unsubscribing from a filter it is not subscribed to gets `MQTTBadResponse` and
its callback never fires. A SUBACK with **zero** reason codes is accepted,
because the count is derived by subtraction and never checked. And `0x92` is
accepted in a PUBACK, where the specification lists it only for PUBREL and
PUBCOMP. All three are transcribed exactly; the Rust arm is a transcription and
its oracle is the C.

**The poison that needed a case was the exact-fit check**, and the shape the
workload was missing is worth naming: a property section that parses **cleanly**
followed by one extra byte. Without the exact fit the section reads fine and the
trailing byte is never looked at, so a broker could carry data inside a packet
the client believes it has read whole. A section that is merely malformed does
not show it, because the property walk refuses that anyway.

**The eleventh is a genuine property, and the second of its kind here.** The
SUB/UNSUBACK property bound is the same predicate as the slice that follows it;
the C needs the check precisely because it has no slice, only pointer
arithmetic. Loosening it changes no answer, so no test fails when it is
loosened — and that is the finding.
`the_sub_ack_bound_is_the_slice_bound_restated` pins the equivalence over 768
combinations, and fails the day it stops holding.

**One case cannot be a differential at all.** `MQTTPacketInfo_t` carries a
pointer and a claimed length, and making them disagree is the attack — but it is
also the one input where the C reads past its own buffer, so its answer depends
on memory rather than on the library. The driver ASSERTS that every case's claim
equals its body, and the over-claim is pinned on the Rust side alone by
`a_claim_larger_than_the_buffer_is_refused`. Third instance of the category,
after the fixed header's byte count and the property reader's budget.

## Conformance (2026-09-18) — the CONNACK

| quantity | value | method |
|---|---|---|
| trace lines agreeing with the C | **54 / 54** | `cargo test -p rusty_rtos_mqtt-core --test connack`. C arm: `oracle/connack_driver.c` driving `core_mqtt_serializer.c` verbatim from v5.0.2 at `04845c6a`. |
| named cases | **46** | the shape of the packet, each of the seventeen properties on its own with a real value, the two zero-limit protocol errors, the malformed property sections, a realistic multi-property CONNACK, and the routing and maximum-packet-size boundaries. |
| what is compared per case | **the status, the session-present flag, the field bitmap, all ten server settings AND the property slice** | `fields_present` is part of the answer, not decoration: a Maximum QoS of 0 and an ABSENT Maximum QoS mean opposite things (QoS 0 only, versus QoS 2), and the value alone cannot tell them apart. |
| single-byte sweeps | **6 x 256 calls** | the reason code, and the property identifier at each of five value SHAPES. |
| statuses reached | **4 of 4** | Success, `MQTTServerRefused`, BadResponse, BadParameter. The third is new to this package and has its own guard assertion, because a workload that never produced it would leave a whole branch of the C untested while every line still matched. |
| divergences from MQTT 5.0 found | **0, and that is the result** | both tables are exactly §3.2.2.2 and §3.2.2.3, asserted from the CHECKED-IN TRACE so a drift under the pin fails the suite. |
| poison rows | **13 introduced, 12 caught** | any flags byte, a resumed session with a refusal, a zero Receive Maximum, a zero Maximum Packet Size, a non-boolean flag, the property section bounded rather than exactly fitted, Response Information ungated, two properties at the wrong width, a repeated reason string, a field under the wrong bit, and one reason code dropped. |

**One sweep could not have done it.** The property identifier is one byte and so
enumerable, but each identifier introduces a value of a different WIDTH — a body
sized for a two-byte property is malformed for a four-byte one, both arms refuse
for the wrong reason, and the sweep discriminates nothing. Sweeping it five
times, once per value shape, gives five accepted sets whose union is the table
AND whose membership says which identifier is which type:

```
one-byte      24,25,28,29,2a      two-byte   13,21,22       four-byte 11,27
string        12,15,16,1a,1c,1f   user-prop  26                     total 17
```

**The thirteenth poison is a genuine property, and a THIRD shape of one.**
Lowering the three-byte minimum to two changes no answer: two bytes is what the
flags and the reason code need, and the third is what the property-length
decoder needs — and that decoder refuses a zero-length buffer by itself, in
**both** arms (`decodeVariableLength` answers `MQTTBadResponse` when
`localBufferLength` is zero). The size calculators' in-loop check is load-bearing
in the C and subsumed here because our arithmetic saturates; the ack
deserializers' property bound is load-bearing in the C and subsumed here because
we take a slice; **this one is redundant in the C too.** Kept because it states
the packet's shape in one place, pinned by
`the_three_byte_minimum_is_stated_not_load_bearing`.

**A refusal is `Ok`, and that is a chosen divergence in SHAPE, not in answer.**
The C's `MQTTServerRefused` means the packet parsed and the broker said no, and
the C fills the out-parameters on it — because the Reason String that says why
is in the property section. `ConnAck::refused()` is that status; making it an
`Err` would have looked tidier and discarded the explanation, which is the one
thing a refused client needs. The trace prints values for both statuses and
compares them.

## Conformance (2026-09-18) — the incoming PUBLISH

| quantity | value | method |
|---|---|---|
| trace lines agreeing with the C | **54 / 54** | `cargo test -p rusty_rtos_mqtt-core --test publish`. C arm: `oracle/publish_driver.c` driving `core_mqtt_serializer.c` verbatim from v5.0.2 at `04845c6a`. |
| named cases | **44** | every QoS, both flag bits, each of the eight properties on its own, the topic-alias bounds, the malformed property sections, the packet-id rules and the routing and maximum-packet-size boundaries. |
| what is compared per case | **the status, QoS, DUP, RETAIN, the packet id, the topic name, the property length, the property section AND THE PAYLOAD** | the payload is the point of the packet and the one number another program will index with. A differential that compared only the status would be checking the wrong thing. |
| flag sweeps | **2 x 16 calls** | the low nibble of the type byte, against a body WITH a packet identifier and a body without. |
| property sweeps | **6 x 256 calls** | one per value shape, including the variable-length integer no other packet carries. |
| divergences from MQTT 5.0 found | **2** | drafted at `kairos-upstream/drafts/coremqtt-publish-protocol-errors.md` and asserted from the CHECKED-IN TRACE. |
| poison rows | **15 introduced, 12 caught** | QoS 3 accepted, DUP and RETAIN swapped, a packet id read at QoS 0, a zero packet id, two of the payload subtraction's four terms dropped, a Payload Format Indicator above 1, a zero Topic Alias, the alias maximum ignored, a repeated content type, an exact property fit demanded, and the type matched as a whole byte. |

**The two divergences, both too permissive.** MQTT 5.0 §3.3.2.3.4 makes a
zero-length topic name a protocol error unless a Topic Alias is present, and the
C never links the two — so an application can be handed a message with no topic
and no alias to resolve one. §3.3.2.3.8 makes a Subscription Identifier of zero
a protocol error, and the value is never checked, though both its neighbours'
are. Transcribed exactly; the Rust arm's oracle is the C, not the
specification.

**The second showed itself in the sweep, which is the argument for printing.**
`property-sweep one-byte accepted=01,0b` lists `0x0B` because the one-byte value
swept is `0x00` — the line says in passing that a zero Subscription Identifier
is accepted. A digest would have said only that the arms agree.

**QoS moves the body, so both sweeps had to be doubled.** A packet identifier
sits between the topic and the properties at QoS 1 and 2 and not at QoS 0, so a
body carrying one is malformed at QoS 0 and a body without one is malformed at
QoS 1. One flags sweep would have refused half the nibble for the wrong reason:

```
flags-sweep no-packet-id   accepted=0,1,8,9                 n=4  rejected=12
flags-sweep with-packet-id accepted=0,1,2,3,4,5,8,9,a,b,c,d n=12 rejected=4
```

The four missing from the second are the QoS 3 nibbles, the only combination
MQTT forbids.

**The three poisons that did not fire are ONE finding.** They are the three
`checkPublishRemainingLength` calls, and each is the same predicate as the slice
that follows it — the C needs them because it indexes with a length it was
handed, and every read here goes through `bounded` instead. Fourth appearance of
the family, and the first where three checks collapse into one reason.
`the_remaining_length_floors_are_the_slice_bounds_restated` pins it.

**And the test that pins it asserts an IDENTITY, because the obvious assertion
was vacuous.** "The payload is not larger than the body" is satisfied by a
payload silently emptied — measured: that exact mutation passed. What has teeth
is that the PARTS RECONSTRUCT THE PACKET: two topic-length bytes plus the topic
plus the packet id plus the encoded property length plus the properties plus the
payload equals the remaining length, over 3,000-odd shapes including claims
larger than the buffer. Both halves of the test assert their own
non-vacuity.

## Conformance (2026-09-18) — the DISCONNECT, both directions

| quantity | value | method |
|---|---|---|
| trace lines agreeing with the C | **58 / 58** | `cargo test -p rusty_rtos_mqtt-core --test disconnect`. C arm: `oracle/disconnect_driver.c` driving `core_mqtt_serializer.c` verbatim from v5.0.2 at `04845c6a`. |
| named cases | **21 incoming, 14 outgoing, 9 validation** | the outgoing cases run `MQTT_GetDisconnectPacketSize` and then `MQTT_SerializeDisconnect` on its answer and print the BYTES, so the trace carries the size/writer agreement itself rather than leaving it to a test on our side alone. |
| reason-code sweeps | **2 x 256 calls** | the same table, once per direction. |
| property sweeps | **10 x 256 calls** | five value shapes x two directions, because the two directions have DIFFERENT property tables and neither contains the other. |
| statuses reached | **4 of 4** | Success, BadParameter, BadResponse and `MQTTNoMemory` — the last is new to this package and comes from a caller buffer too small for a packet the size calculator already agreed to. |
| divergences from MQTT 5.0 found | **1** | drafted at `kairos-upstream/drafts/coremqtt-disconnect-reason-code.md`. |
| poison rows | **15 introduced, 14 caught** | the direction flag, server codes both ways, the missing `0x9F` ADDED, an empty body, a one-byte body, the exact property fit, a session expiry from a server, a repeated reason string, a server reference going out, the session-expiry rule, properties without a reason code, two terms of the size, and the property length written before the reason code. |

**This slice exists because the previous one's claim was wrong.** After the
PUBLISH slice the README said the crate could read every packet a broker can
send. It could not: MQTT 5 made the DISCONNECT bidirectional and
`MQTT_DeserializeDisconnect` was not covered. Recorded rather than quietly
corrected.

**One table, two directions, and the comparison is the finding.** The outgoing
accepted set is exactly §3.14.2.1's client column. The incoming set is the
server column **minus `0x9F`**, "connection rate exceeded" — so a client refuses
a conformant disconnection as a malformed packet. Second reason-code table in
this library to be one entry short of the specification, after the UNSUBACK's.

**A bare DISCONNECT is correct by coincidence.** With no reason code the C still
charges a byte for the encoded property length and emits `E0 01 00`, where
§3.14.2.1 allows `E0 00`. The byte the writer intends as a property length is
read by a broker as the reason code; they agree only because a property length
may be non-zero only when a reason code is present, which forces it to zero.
Two sides agreeing on the bytes for different reasons is the fragile kind, so it
has its own test.

**Two poisons needed a case, and both for the same cause: the case meant to
catch them was malformed a SECOND way.** `repeated-reason-string` carried a
trailing byte, so the walk refused it before the duplicate check could;
`property-length-too-short` was malformed inside the section, so the same.
A case that fails for the wrong reason is worse than no case, because it looks
like coverage. Both were rebuilt to be wrong in exactly one way.

**The fifteenth is a genuine property.** The up-front buffer check in
`MQTT_SerializeDisconnect` is the same predicate as the three slices that follow
it — the C has pointer writes and a `memcpy` where this has bounded slices.
Fifth appearance of the family, and the first on the WRITING side.
`the_buffer_check_is_the_slice_bounds_restated` walks every buffer size from
zero to three past the packet, for four packet shapes, and asserts both that the
answer matches and that nothing past the reported size was touched.

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
| `cargo test -p rusty_rtos_mqtt-core` | 81 passed, 0 failed (38 unit, 8 gate, 3 state, 5 header, 5 property, 5 writer, 3 size, 3 ack, 3 connack, 4 publish, 4 disconnect) |
| `cargo clippy --all-targets --all-features` under the workspace lint policy | clean, 0 warnings |
| `cargo build -p rusty_rtos_mqtt --no-default-features --target thumbv7em-none-eabihf` | passes |
| `cargo build -p rusty_rtos_mqtt --no-default-features --target riscv32imac-unknown-none-elf` | passes |

## Scope, in lines (2026-09-18, revised)

A count that belongs here because the README's honesty depends on it.

| part of coreMQTT v5.0.2 | lines | state |
|---|---:|---|
| `core_mqtt_state.c` | 1,206 | **remade and proven** |
| `core_mqtt_serializer.c`, the fixed-header codec | ~240 | **remade and proven** |
| `core_mqtt_serializer_private.c`, the property primitives | ~309 | **remade and proven** |
| `core_mqtt_serializer_private.c`, the fixed-header writers | ~180 | **remade and proven** |
| `core_mqtt_serializer.c`, the packet-size calculators | ~300 | **remade and proven** |
| `core_mqtt_serializer.c`, the acknowledgement deserializers | ~586 | **remade and proven** |
| `core_mqtt_serializer.c`, the CONNACK path | ~566 | **remade and proven** |
| `core_mqtt_serializer.c`, the incoming PUBLISH | ~466 | **remade and proven** |
| `core_mqtt_serializer.c`, the DISCONNECT, both directions | ~505 | **remade and proven** |
| `core_mqtt_serializer.c`, the rest | ~3,447 | not written |
| `core_mqtt_serializer_private.c`, the rest | ~164 | not written |
| `core_mqtt_prop_serializer.c` | 1,176 | not written |
| `core_mqtt_prop_deserializer.c` | 880 | not written |

| `core_mqtt.c` | 5,618 | not written |
| **total** | **15,643** (plus 5,459 of headers) | **27.9 % remade** |

The CONNACK row excludes `logConnackResponse`'s 102 lines, which are a `static
void` of `LogError` calls with no observable behaviour. They are counted as not
written rather than claimed, because a remake that produces no log line has not
remade a logger.

No speed number and no size number: nothing here has been benchmarked, and
nothing has run on a chip.
