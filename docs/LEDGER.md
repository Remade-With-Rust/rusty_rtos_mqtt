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

## Conformance (2026-09-18) — the CONNECT

| quantity | value | method |
|---|---|---|
| trace lines agreeing with the C | **30 / 30** | `cargo test -p rusty_rtos_mqtt-core --test connect`, byte for byte. C arm: `oracle/connect_driver.c` driving `core_mqtt_serializer.c` verbatim from v5.0.2 at `04845c6a`. |
| named cases | **20** | every optional field absent, empty and present; all three will QoS values; both property sections; and the buffer at, one under and far under the packet size. |
| 16-bit boundary cases | **7** | one field at a time at 65,535 and at 65,536. These exist because **all five of the C's 16-bit checks were unreachable** from a table of short fields, and a poison on each of them passed. They print the status and the sizes, not the bytes: a 65 KB packet's hex would be 131 KB on one line. |
| whole-packet sweep | **32 combinations** | every combination of user name, password, will and properties, at two will QoS values, compared by an FNV-1a digest of the WHOLE serialized packet plus its shortest and longest length. |
| poison rows | **13 introduced, 12 caught** | the NUL rule, a nine-byte header, a field's length prefix, each property section's encoded length, the will counted when absent, FOUR orderings, an absent user name written as empty, and three 16-bit limits. |

**The ordering poisons are why this is byte-for-byte.** Every field in a
CONNECT's payload is length-prefixed, so swapping the will topic with the will
payload, or the user name with the password, still **parses** — it publishes the
will to the wrong topic and sends the password as the user name. Four such swaps
are in the poison set and all four are caught; nothing but a byte comparison
would have seen them.

**The flags byte and the payload are one claim in two places.** The writers'
slice swept the flags byte exhaustively and proved every bit, and could prove
nothing about whether the payload matches it. The 32-combination sweep digests
the WHOLE packet, so a field written when its bit is clear moves the bytes.

**A client identifier may not begin with NUL**, which is incidental. The C's
check is one expression meant to catch a length/pointer mismatch, and it
dereferences the pointer: `*pClientIdentifier == '\0'`. So a first byte of zero
is refused and a zero anywhere else is not. MQTT 5.0 §1.5.4 forbids U+0000
anywhere in a UTF-8 string, so the refusal is defensible — but it is one byte,
and by accident. Transcribed and pinned.

**One check the differential structurally cannot reach, and that is the
finding.** The C refuses a total past 268,435,455, and no field can get within
four orders of magnitude; the property section can, because the function reads
the builder's `currentIndex` and never touches `pBuffer` — a caller may claim 268
million bytes while pointing at eight, and the driver did exactly that at the
exact boundary (`currentIndex = 268435437` lands on 268,435,455) before the
cases were withdrawn. **A `&[u8]` cannot make that claim**, so the class of input
that makes the check load-bearing does not exist here. The check stays and a
test pins its operator, which is `>` where
[the DISCONNECT's](#conformance-2026-09-18--the-disconnect-both-directions) is
`>=` — two calculators one function apart in the same C file disagreeing about
whether 268,435,455 is legal.

## Conformance (2026-09-18) — the outgoing PUBLISH

| quantity | value | method |
|---|---|---|
| trace lines agreeing with the C | **25 / 25** | `cargo test -p rusty_rtos_mqtt-core --test outpublish`, byte for byte. C arm: `oracle/outpublish_driver.c` driving `core_mqtt_serializer.c` verbatim from v5.0.2 at `04845c6a`. |
| named cases | **17** | every QoS, DUP and RETAIN, properties with and without a payload, a two-byte remaining length, what each serializer refuses, and the buffer at, below and far below the packet size. |
| broken-contract cases | **4** | the serializers handed a remaining length that is NOT the one the calculator produced — the only way to tell the header size it REPORTS from the bytes it WRITES. |
| serializers per case | **3** | all three run on the same inputs and all three results printed, so the trace carries the prefix relationship between them. |
| dup-patch sweeps | **2 x 256 calls** | `MQTT_UpdateDuplicatePublishFlag` in both directions, compared by count and by an FNV-1a digest of the RESULTING bytes — a function that took the right inputs and set the wrong bit would pass a status-only sweep. |
| poison rows | **16 introduced, 16 caught** | the QoS bits swapped, DUP and RETAIN swapped, a packet id at QoS 0, a little-endian packet id, the properties before the packet id, the payload copied in the header-only serializer, three size terms dropped, three validations removed, the loose serializer made strict, the header size reported as the bytes written, and the DUP patch on the wrong bit and on any byte. |

**Three serializers, one relationship.** The short one writes four or five
bytes, the middle one everything but the payload, the long one the lot — and a
caller on the vectored path depends on the first being a prefix of the second
and the second of the third. Three separate differentials could each pass while
that broke, so all three run on every case and the trace carries their outputs
side by side.

**They validate differently, and the loose one validates almost nothing**: no
topic, no packet identifier, no DUP rule. It is the one reached for when
performance matters, and its looseness is the C's choice. One poison is making
it strict, and it fires.

**An assumption of mine was refuted on the first run of the new cases.** The
header serializer reports the size it COMPUTED, not the bytes it wrote; I wrote
that down and added `debug_assert!(written <= header_size)` beside it, reasoning
that an inflated remaining length could only over-report. It differs in BOTH
directions — remaining length 16 for an 11-byte packet reports 13 and writes 8;
remaining length 8 reports 5 and writes 8. No case had broken the C's stated API
contract ("call the size function first"), so nothing had ever exercised it. The
assertion is gone. **A comment that states a relationship is a claim, and a
claim in a comment is one nobody runs.**

**The two halves of the library disagree about an empty topic.** The reading
side accepts a zero-length topic name with no Topic Alias — a divergence from
MQTT 5.0 that the C has and this package reproduces — and the writing side
refuses the same packet. Recorded by a test that fails if either side changes.

## Conformance (2026-09-18) — SUBSCRIBE, UNSUBSCRIBE, the acks and PINGREQ

| quantity | value | method |
|---|---|---|
| trace lines agreeing with the C | **45 / 45** | `cargo test -p rusty_rtos_mqtt-core --test outbound`, byte for byte. C arm: `oracle/outbound_driver.c` driving `core_mqtt_serializer.c` verbatim from v5.0.2 at `04845c6a`. |
| named cases | **19 list, 16 ack, 3 pingreq** | every subscription option, three filters at once, the shared validator's four refusals, the three ack shapes, and the buffer at and below each threshold. |
| options sweep | **36 combinations** | every combination of QoS, no-local, retain-as-published and retain handling, with each byte printed and digested. |
| ack reason sweeps | **4 x 256 calls** | one per publish-acknowledgement type, because the C validates them PER TYPE on the way out. |
| poison rows | **16 introduced, 16 caught** | three option-bit swaps, the options byte before its filter, an options byte on UNSUBSCRIBE, three validator refusals removed, the two checks reordered, the ack tables merged, a PUBACK allowed `0x92`, a dropped property-length byte, a wrong remaining length, the buffer statuses unified, a zero ack packet id, and a wrong PINGREQ type byte. |

**The library validates ack reason codes TWICE and disagrees with itself.** The
writing side checks per packet type — nine values for a PUBACK and a PUBREC, two
for a PUBREL and a PUBCOMP — which is exactly MQTT 5.0 §§3.4.2.1, 3.5.2.1,
3.6.2.1 and 3.7.2.1. The reading side checks all four against one shared table
of ten. So coreMQTT will not SEND a PUBACK carrying `0x92` and will ACCEPT one.

This is the sharpest evidence yet for the upstream report already drafted on
that reading-side table, and it changes the argument: the fix is not "write a
table", it is "use the one three thousand lines up". The draft has been amended.

**A too-small buffer gets two different statuses**, a hundred lines apart:
`MQTTNoMemory` under four bytes from `MQTT_SerializeAck`, and `MQTTBadParameter`
at four bytes with a reason code from `serializeAckBody`. Transcribed as each
has it, with a case for each.

**The shared list validator checks the BUFFER before the FILTERS**, so a call
that is wrong about both reports `NoMemory`. The order is observable; the first
poison written for it removed the check instead of moving it, which is a
different experiment — rewritten, and then caught.

## Conformance (2026-09-18) — the transport reader

| quantity | value | method |
|---|---|---|
| trace lines agreeing with the C | **20 / 20** | `cargo test -p rusty_rtos_mqtt-core --test reader`. C arm: `oracle/reader_driver.c` driving `core_mqtt_serializer.c` verbatim from v5.0.2 at `04845c6a`, with a scripted transport. |
| what is compared per case | **the status, THE CALL COUNT, the log of every byte asked for, and the answer** | this is the one function in the package that takes a CALLBACK, so what it asked for is as much of the behaviour as what it returned. |
| named cases | **17** | each remaining-length width, three refused type bytes, the two length malformations, and each of the transport's three answers at both the type byte and past it. |
| type sweep | **256 calls** | every type byte through the READER rather than through `incoming_packet_valid` directly, with the CALL COUNTS digested as well as the statuses. |
| poison rows | **7 introduced, 6 caught** | the type checked after the length, the two failures distinguished everywhere, the multiplier guard moved after the read, the non-minimal check dropped, the type byte dropped from the header length, and the two statuses swapped. |

**Two orderings that only the call count can show.** The type is checked BEFORE
the length is read, so a packet a client may not receive costs one call and not
a drained header; and the multiplier guard runs BEFORE the read, so a fifth
continuation byte is refused without asking the transport for it. Both are
`MQTTBadResponse` either way — the status cannot tell them apart, and the log
can. Both are poisons and both are caught.

**Two statuses the library uses nowhere else**, and only at the first byte.
`MQTTNoDataAvailable` and `MQTTRecvFailed` are distinguished at the type byte;
one byte in, both become `MQTTBadResponse`, because a header that started must
finish. A distinction the C draws once and then drops.

**The seventh poison is the out-of-range check, which cannot fire.** The
multiplier guard bounds the value to exactly 268,435,455, one less than the
constant it tests. Third instance of that arithmetic in this package, after the
property length decoder's and the fixed header's, and pinned the same way.

**And one number the C does not produce.**
`MQTT_GetIncomingPacketTypeAndLength` leaves `headerLength` as the caller left
it — only `processRemainingLength` sets it, and this function does not call
that. `IncomingHeader::header_length` is therefore an ADDITION: the bytes were
counted anyway, so the number is free. It is not in the trace, because the C has
nothing to compare it against.

## Conformance (2026-09-18) — the outgoing property validators

| quantity | value | method |
|---|---|---|
| trace lines agreeing with the C | **94 / 94** | `cargo test -p rusty_rtos_mqtt-core --test validate`. C arm: `oracle/validate_driver.c` driving `core_mqtt_serializer.c` verbatim from v5.0.2 at `04845c6a`. |
| identifier sweeps | **36** (6 validators x 6 value shapes, 256 identifiers each) | **9,216 calls**, with each accepted set PRINTED rather than hashed — where the accepted set is small, the print says which identifier moved. |
| named cases | **56** | the repeats, the boundary values, the truncations, and the three [MQTT-3.1.2-32] pairs. |
| what is compared per case | **the status AND both out-parameters, on refused cases too** | the C writes its Maximum Packet Size and Topic Alias DURING the walk, so a refusal can leave one written. |
| poison rows | **9 introduced, 9 caught** | the publish table made to dedupe, the topic alias written after its bound is checked, Request Problem Information left at the caller's value, a malformed subscription id given the table's status, the alias bound made inclusive, the zero subscription id accepted, [MQTT-3.1.2-32] checked in the arm, the acknowledgement flag moved inside its loop, and a zero Receive Maximum accepted. |

**Three places the writing side is laxer than the reading side**, all in the
PUBLISH table: a Topic Alias of **zero** passes, a Payload Format Indicator
**above 1** passes, and it **deduplicates nothing but the Topic Alias** — its
`used` flag is declared inside the property loop, so the flag is false at every
property. The will validator carries five of the same seven identifiers and
refuses all three; the acknowledgement validator is the same loop one brace
apart and dedupes. Drafted for upstream.

**A sweep says what a table ACCEPTS; only reading the C says what it
REMEMBERS.** The driver was written from the sweeps and produced 49 cases, all
of which passed. Transcribing the C added seven more, and **three of the nine
poisons are caught only by those seven** — measured by rerunning them against
the 49-case trace:

| poison | 49 cases | 56 cases |
|---|---|---|
| a malformed subscription id given the table's own status | missed | **caught** |
| the topic alias bound made inclusive | missed | **caught** |
| the acknowledgement table stops deduplicating | missed | **caught** |

Each spans two properties or two checks — a flag that survives an iteration, a
boundary, a status passed through from a shared decoder — and a sweep sends one
property at a time at one value. The sweep is what found the three defects
above; the two instruments answer different questions, and a slice needs both.

**One identifier no sweep can see.** The CONNECT table accepts nine properties
and its sweeps report eight: authentication data (`0x16`) is refused at every
shape because [MQTT-3.1.2-32] needs an authentication method, and the C checks
that AFTER the walk. Three paired cases carry it.

**The guard, twenty-first shape: six tables that cannot be told apart are ONE
table with six names.** Every other differential here proves a workload can
fail. This one must also prove the six workloads differ from each other, since a
transcription that routed all six validators to one table would answer correctly
for every case aimed at that one. Each validator's six accepted sets are its
signature, and `no_two_validators_accept_the_same_set` asserts no two signatures
are equal.

**And two accumulators rather than return values.** The C writes its Maximum
Packet Size and Topic Alias through pointers as it walks, so a refusal can leave
one written — `publish-topic-alias-over` refuses with `alias=11` left behind.
`ConnectValidation` and the `&mut Option<u16>` are taken by reference for that
reason, the way `Read::read_to_end` takes its buffer, and the trace compares
them on refused cases too. A differential compares the state left behind, not
only the answer.

## Conformance (2026-09-18) — the connection context, and the last of the serializer

| quantity | value | method |
|---|---|---|
| trace lines agreeing with the C | **56 / 56** | `cargo test -p rusty_rtos_mqtt-core --test context`. C arm: `oracle/context_driver.c` driving `core_mqtt_serializer.c` verbatim from v5.0.2 at `04845c6a`. |
| what is compared per header case | **BOTH readers, over the same bytes** | `MQTT_ProcessIncomingPacketTypeAndLength` takes the header out of a buffer and `MQTT_GetIncomingPacketTypeAndLength` pulls it off a callback. 13 named cases plus two 256-value sweeps drive both and print both answers. |
| CONNECT property sweeps | **6 x 256** | the THIRD walk of that table in the library. Its accepted sets are printed, so `validate.trace` and `context.trace` are a diff. |
| parameter combinations | **432** | every combination of retain, retain-available, QoS, maximum QoS, topic alias, topic-name length and maximum packet size, digested. |
| poison rows | **13 introduced, 13 caught** | five in the buffered reader, three in the context filler, one in each constructor, and three in the parameter validator. |

**Three findings, all from running one job's two implementations side by side.**

1. **Only one of the two readers can say "not yet".** `truncated-sweep
   differ=168`: for every packet type a client may receive, a type byte with no
   length behind it yet is `MQTTNeedMoreBytes` from the buffered reader and
   `MQTTBadResponse` from the callback-driven one. The doxygen for the latter
   shows a non-blocking loop ending in `assert( status == MQTTSuccess )`, so an
   ordinary TCP segment boundary inside a header trips it.
2. **`updateContextWithConnectProps` stores what
   `MQTT_ValidateConnectProperties` refuses**, including a Maximum Packet Size
   of zero — which makes **nine** functions answer `MQTTBadParameter` for ever,
   three of them deserializers, so the session is inert in both directions. The
   helper is public and documented with a worked example.
3. **`MQTT_ValidatePublishParams` compares QoS against zero rather than the
   maximum**, so QoS 2 goes to a broker that announced Maximum QoS 1.

Drafted in `kairos-upstream/drafts/coremqtt-two-readers-one-header.md`.

**And a line count that was wrong, found by the same instrument.** Driving the
two header readers side by side is what showed that this slice had written a
SECOND Rust copy of `MQTT_ProcessIncomingPacketTypeAndLength` and
`processRemainingLength` — 138 C lines the fixed-header slice had already
remade, and counted again. The duplicate (`reader::process_header`) was deleted,
the differential re-pointed at
`header::process_incoming_packet_type_and_length`, and it passed unchanged,
which is itself the proof that the two were the same function. The slice is 230
lines, not 368, and the package is **44.3 %**, not 45.2 %. The general rule is
one the house already has for C code and had not applied to its own: **before
writing a function, grep the crate for its shape.**

**A trace should ask only what both arms can answer.** Three of the C's
refusals have no reachable equivalent in Rust, all of the same shape: a pointer
and a length that must agree and are never checked against each other
(`MQTTPropertyBuilder_Init`'s buffer and length, `MQTT_ValidatePublishParams`'s
topic name and its length). A slice carries both, so the question cannot be
asked. They were **removed from the driver** rather than faked, because printing
a sentinel in both columns is a constant compared with itself; the one bound
that is real but needs a 256 MB buffer to reach is pinned by a unit test on the
arithmetic instead.

**The guard, twenty-second shape: two arms that never disagree are ONE arm
driven twice.** The whole instrument of this slice is that the same header is
read two ways; if the `dual` lines agreed everywhere they would prove nothing
`reader.trace` had not. So the trace must contain cases where the two agree AND
cases where they part, and the sweeps must show the parting is systematic
rather than one awkward input. `the_two_readers_are_compared_where_they_agree_and_where_they_do_not`
asserts all three.

**And the digest seed, recorded rather than fixed.** Every driver here seeds its
rolling digest with `1469598103934665603`, which is one digit short of FNV-1a's
offset basis. This slice's driver was written with the real basis and its first
run disagreed with the Rust arm on nothing but the digest. The seed is arbitrary
— a digest need only be deterministic and shared — so the new driver was moved
to the house constant rather than five checked-in traces being regenerated. The
note in `tests/connect.rs` that predicted exactly this is why it took one run to
find.

## Conformance (2026-09-18) — the MQTT 5 property builders

| quantity | value | method |
|---|---|---|
| trace lines agreeing with the C | **70 / 73, and 3 refused on purpose** | `cargo test -p rusty_rtos_mqtt-core --test propbuild`. C arm: `oracle/propbuild_driver.c` driving `core_mqtt_prop_serializer.c` verbatim from v5.0.2 at `04845c6a`. |
| packet-type table | **256 types x 18 adders = 4,608 calls** | `isValidPropertyInPacketType` is `static`, so it is asked through the public adders. Sixteen named types printed, all 256 digested. |
| named cases | **54** | every adder once, every width at and below the buffer it needs, every value each adder refuses itself, the repeats, and the table through seven packet types. |
| poison rows | **14 introduced, 12 caught** | the two that could not fire are the finding below. |

**A buffer overflow, found by being unable to reproduce it.** `addPropUint8`,
`addPropUint16` and `addPropUint32` size themselves as the identifier byte plus
the value. `addPropUtf8` sizes itself as the two length bytes plus the body and
**forgets the identifier**, then writes it — so a buffer of exactly
`propertyLength + 2` gets `MQTTSuccess` and one byte past its end. Six public
adders route through it. This arm writes through a `&mut [u8]` under
`forbid(unsafe)` and answers `NoMemory`, so three trace lines cannot match:

```
add 38 utf8-in-four-bytes cap=4 | content-type(-,-)->Success index=5 ... OVERFLOW
```

This is the **first** place in K7 where the transcription rule — reproduce the C
exactly, divergences included — could not be followed, and the reason it could
not is the reason the project exists. Drafted in
`kairos-upstream/drafts/coremqtt-addproputf8-off-by-one.md`.

**The two poisons that could not fire, and the property they proved.** Sizing a
four-byte property as 4 instead of 5, and sizing a string property the way
`addPropUtf8` does, changed **no answer at all**. Both still refuse; they refuse
from `get_mut` instead of from the size check. So this crate has **two
independent bounds on every write**, and the second is not code that can be got
wrong. Kind (b) of the four kinds of silent poison — a genuine property — and it
is pinned by `no_arithmetic_error_can_write_past_the_slice`, a sweep of every
adder against every buffer size from 1 to 23 asserting that the cursor never
passes the buffer and that a refusal writes nothing.

**The guard, twenty-third shape: an exception must be smaller than the rule.**
A documented exception is a hole in a comparison, so `the_exception_is_bounded`
checks it from both ends: exactly three lines, every one of them a line the C
itself marked, every one a four-byte buffer accepting a five-byte write, and
more than sixty lines still compared without exception.

**A fourth copy of which property may go in which packet**, and it disagrees
with the third: the builder's table allows a **Subscription Identifier in a
PUBLISH**, which [MQTT-3.3.4-6] forbids a client to send and
`MQTT_ValidatePublishProperties` refuses. The C's comment beside that arm says
"only in server-to-client PUBLISH" and the next line sets the bit. In the same
draft.

**And the cross-slice check.** A section this crate builds is a section this
crate validates: `what_the_builder_writes_the_validator_accepts` runs the bytes
straight from the builder into the CONNECT and will validators. Two arms that
each agree with the C can still disagree with each other — the shape the
packet-size calculators and the writers established in slice 5.

## Conformance (2026-09-18) — the MQTT 5 property reader

| quantity | value | method |
|---|---|---|
| trace lines agreeing with the C | **55 / 55** | `cargo test -p rusty_rtos_mqtt-core --test propread`. C arm: `oracle/propread_driver.c` driving `core_mqtt_prop_deserializer.c` verbatim from v5.0.2 at `04845c6a`. |
| getter sweeps | **24 x 256 = 6,144 calls** | every getter against every identifier, with each accepted set printed and each cursor position digested. |
| table sweeps | **2 x 256** | the identifier table and the width table over the same alphabet, both accepted sets printed. |
| named cases | **26** | one of every width, the wrong getter for the property under the cursor, the cursor at and past the end, a skip of every width, and a truncated value for each. |
| what is compared per call | **the status, the value AND the cursor** | every function advances a caller-owned index; a getter that answered right and left the cursor one byte out would desynchronise every call after it. |
| poison rows | **10 introduced, 8 caught** | the two that could not fire are the property below. |

**Every getter accepts exactly one identifier**, all twenty-two of them, across
6,144 calls. A getter is an assertion about what is under the cursor, not a
search, so a section read in the wrong order fails rather than handing back a
number from the wrong property. `n=1` on every `getter` line is that rule.

**Two tables over one alphabet, and this time they agree.**
`MQTT_GetNextPropertyType` lists twenty-seven identifiers;
`MQTT_SkipNextProperty` sorts the same bytes into five width groups. Both
accepted sets are printed and they are identical (`tables differ=0`). Five
tables in this library disagree with a sibling — the acknowledgement reason
codes, the DISCONNECT codes, the PUBLISH property validator, the CONNECT context
filler and the builder's packet table — so **a differential that only ever
reported disagreement would be one nobody believed when it reported
agreement**, and the agreement is asserted rather than assumed. In the Rust arm
one `width_of` serves both callers, so they cannot drift.

**The guard, twenty-fourth shape: a cursor differential needs a case where the
SECOND call can only work if the first left the cursor right.** Printing an
index is necessary and not sufficient — a trace of single-call cases compares a
number nothing depends on. `the_trace_chains_reads_so_the_cursor_carries_weight`
requires at least four cases that read twice and succeed twice, and one skip of
each of the five widths.

**And the property the two silent poisons proved, third instance.** A budget one
byte too generous, and a cursor allowed to sit exactly on the end, change no
answer: the read that follows is bounded by the slice as well as by the
arithmetic. Same shape as the builder's two, and the same conclusion — **an
off-by-one in a length calculation is a bug in C and a redundancy here.** Not an
argument for sloppy arithmetic: the budget is transcribed exactly and a budget
that was too SMALL would show immediately. It is the reason the slip that
overflows `addPropUtf8` cannot overflow anything in this crate. Pinned by
`no_budget_error_can_read_past_the_section`.

## Conformance (2026-09-18) — topic matching

| quantity | value | method |
|---|---|---|
| trace lines agreeing with the C | **109 / 109** | `cargo test -p rusty_rtos_mqtt-core --test topic`. C arm: `oracle/topic_driver.c` driving `core_mqtt.c` verbatim from v5.0.2 at `04845c6a`. |
| the grid | **39 x 39 = 1,521 pairs, PRINTED** | every string over `{a, /, +}` to length three, as a matrix of which filter matches which topic. |
| the sweep | **1,554 x 1,554 = 2,414,916 pairs** | every string over `{a, b, /, +, #, $}` to length four, digested. Both wildcards and the `$` rule are in that alphabet. |
| named cases | **48** | the specification's own examples from §4.7.1.2, §4.7.1.3 and §4.7.2, the empty-level probes, and wildcard characters out of position. |
| poison rows | **13 introduced, 12 caught** | the one that could not fire is the `strncmp` fast path. |

**A divergence from MQTT 5.0, and the grid showed it as a column.** A filter
whose last level is `+` stops matching a topic whose last level is empty, once
an earlier `+` has been used — `a/+` matches `a/`, and `+/+` does not. §4.7.1.3
makes `+` match exactly one level and §4.7.3 makes an empty level a legal one, so
a client subscribed to `+/+` silently never receives a message published to
`a/`. Row `a/+` has a `1` in the `a/` column; row `+/+` has a `.`. Drafted in
`kairos-upstream/drafts/coremqtt-topic-filter-empty-last-level.md`, with the
missing `MQTTEndOfProperties` entry in `MQTT_Status_strerror`.

**A grid is to a string algorithm what a printed accepted set is to a table.**
Every sweep in this package so far walks one byte over 256 values and prints
what it accepted, because a digest says a table changed and a printed set says
which entry moved. Matching takes two strings, so the same idea needs two
dimensions — and the payoff was the same: the defect is a **pattern** in the
matrix rather than a case somebody had to suspect first.

**The guard, twenty-fifth shape: a grid must vary in both directions.** A
matrix is only an instrument if it is not constant. A matcher that refused
everything would still give a diagonal, because every filter is a legal topic
name and matches itself by the exact path; one that accepted everything would
give a solid block. Both would still be compared, and neither would prove
anything. `the_grid_varies_in_both_directions` asserts the diagonal is present,
that no row is solid, and that the wildcard rows are substantially fuller than
the literal ones.

**And one thing the sweep caught in our own arm.**
`MQTT_GetPacketTypeString` matches PUBLISH on its **nibble** — the low four bits
are its QoS, DUP and RETAIN flags — and every other type on the **whole byte**,
because a PUBREL's reserved bit must be set and `0x60` is malformed rather than
a PUBREL. The first transcription used the whole byte throughout, which is the
tidier-looking rule and wrong for sixteen values; the 256-value digest failed on
the first run. Recorded because the asymmetry is the kind a reader smooths out.

**The silent poison, and what it proved.** Deleting the `strncmp` fast path in
`MQTT_MatchTopic` changed no answer anywhere in the differential: the general
walk matches every string against itself without help, over all 780 strings up
to length four. So the shortcut is what its name says — an optimisation, not a
rule — which is worth knowing, because it looks at first like the only reason a
filter containing a literal `+` can match at all.

## Conformance (2026-09-18) — the client context

| quantity | value | method |
|---|---|---|
| trace lines agreeing with the C | **40 / 41, and 1 refused on purpose** | `cargo test -p rusty_rtos_mqtt-core --test client`. C arm: `oracle/client_driver.c` driving `core_mqtt.c` verbatim from v5.0.2 at `04845c6a`. |
| named cases | **21 subscription, 8 publish, plus the constructors** | `MQTT_Subscribe` validates before it looks at the connection, so an unconnected context tells a malformed list (`BadParameter`) from a well-formed one (`StatusNotConnected`). |
| poison rows | **15 introduced, 15 caught** | two needed cases added first; both gaps had real behaviour behind them. |

**Only the last entry in a subscription list decides whether the list is
valid.** `validateSubscribeUnsubscribeParams` ends in a loop that assigns its
status and never breaks, so every earlier entry's verdict is written over — and
the loop three lines above it, over the same list, does break. The same two
subscriptions in both orders give opposite answers. Everything the per-entry
validator checks is lost this way, so the client builds and sends a SUBSCRIBE
carrying a filter like `$share//a/b`.

**And `checkWildcardSubscriptions` searches a filter past its length**, with
`strchr`, where every other function that touches a topic filter uses
`topicFilterLength`. Second thing in this library the Rust arm cannot
reproduce, for the same reason as the first: a `&[u8]` has no bytes past its
length. One trace line is a bounded exception, and `the_exception_is_bounded`
checks it is one line, that it is the hidden-wildcard case, and that the
exception stays under a tenth of the subscription cases.

Both drafted in
`kairos-upstream/drafts/coremqtt-subscription-list-validation.md`.

**The type as a refusal, which is a new member of an old family.** Five of the
C's refusals here have no Rust counterpart: `MQTT_Init`'s null pointers,
`MQTT_InitStatefulQoS`'s pointer-versus-count pairs, and — this is the new one —
a QoS of 3 and a retain-handling option of 3. `MQTTQoS_t` can hold them;
`QoS` cannot, so **the type refuses before any validator runs**. That is the
null-pointer family one level up, and it means `validateTopicFilter`'s two
`> 2` checks are unreachable here rather than transcribed. Those cases were
removed from the driver, and the last-one-wins defect is demonstrated with an
empty filter, which both arms can express.

**Two poisons that needed cases, both with behaviour behind them.** An
UNSUBSCRIBE ignores every subscription option, so a shared subscription a
SUBSCRIBE refuses goes through unexamined — without a case for it, deleting the
whole unsubscribe short-circuit changed no answer. And a filter of exactly
`$share/` is **not** a shared subscription, because the C tests `length > 7`
before comparing seven bytes; nothing else in the trace told `> 7` from `>= 7`.

## Conformance (2026-09-18) — the send plumbing

| quantity | value | method |
|---|---|---|
| trace lines agreeing with the C | **28 / 28** | `cargo test -p rusty_rtos_mqtt-core --test send`. C arm: `oracle/send_driver.c` driving `core_mqtt.c` verbatim from v5.0.2 at `04845c6a`. |
| what is compared per case | **the status, the CALL COUNT, the log of every offer and its answer, the bytes that arrived, and the context afterwards** | a transport chooses how much of each offer it takes, so the sender's offsets are driven from outside the library. |
| named cases | **12 buffer, 11 vector, 3 clock origins** | every way a transport can answer, at every position in a packet. |
| poison rows | **12 introduced, 12 caught** | two of them by HANGING rather than failing. |

**This slice is the harness.** Everything left in `core_mqtt.c` talks to a
transport, so the instrument comes first: a scripted transport whose every call
is logged and a scripted clock. `log=2:1,1:1` is a sender that pushed two bytes
in two calls with the second offer correctly advanced; `log=2:1,2:1` is one that
sent the first byte twice. Both put two bytes on the wire.

**A wrong elapsed-time function does not answer wrongly — it never answers.**
`calculateElapsedTime` is `later - start` on two `uint32_t`, correct across the
32-bit wrap because both sides are unsigned. Replacing it with a saturating
subtraction makes a client that has been up for 49.7 days report zero elapsed
for ever, and a send against a busy transport never returns. So that poison is
caught in the strongest sense **and the differential cannot see it**, because a
hang is not a failing assertion: it is pinned by
`the_elapsed_time_wraps_and_a_guarded_subtraction_would_never_time_out`, and the
trace drives the clock from zero, from one step below the wrap and from the wrap
itself and requires all three answers identical.

A second poison hangs the same way — dropping the whole-vector advance in the
vector sender — and the poison runner now bounds every run and kills the process
tree, reporting `HANGS` as its own verdict. **A differential harness has to
survive the code it is breaking.**

**The guard, twenty-sixth shape: a sender differential needs a case where the
transport takes PART of a vector.** Counting bytes is not enough — a sender that
re-offered a whole vector after a partial take would put the same bytes on the
wire and log a different sequence. `stops-on-a-boundary` and `stops-mid-vector`
are the two branches of the advance, and both are asserted.

**Three poisons needed cases first, and all three gaps had behaviour behind
them.** A clock that actually moves, or the recorded transmit time is always
zero and dropping it changes nothing. A step that lands the elapsed time
**exactly** on the timeout, or `>=` and `>` agree. And a context whose transport
has already failed: `MQTT_Disconnect` refuses only `NotConnected`, so a dying
connection is still allowed to try the one packet it might manage — which is
right, and which nothing tested until there was a case for it.

**And one case removed rather than added.** A frozen clock with a transport that
never accepts loops for ever *by construction*, and coreMQTT's own config header
says that if the time function is a no-op then `MQTT_SEND_TIMEOUT_MS` must be
zero. That is a documented precondition, not a defect, and **a differential case
that cannot terminate is not a case.**

## Conformance (2026-09-19) — the outgoing packets

| quantity | value | method |
|---|---|---|
| trace lines agreeing with the C | **59 / 59** | `cargo test -p rusty_rtos_mqtt-core --test outgoing`. C arm: `oracle/outgoing_driver.c` driving `core_mqtt.c` verbatim from v5.0.2 at `04845c6a`. |
| what is compared per case | **the status, the call count, the log of every offer and its answer, the bytes that arrived, the stored copy, and the outgoing RECORD** | the record is what makes the state wiring observable; without it a publish that reserved a record and never advanced it is identical on the wire to one that did both. |
| transports driven | **two** | a per-vector one, which proves the trait's default, and a gathered `writev`, which is the only thing that can see a gather boundary. |
| poison rows | **27 introduced, 25 caught** | the two that remain are dead at `MQTT_SUB_UNSUB_MAX_VECTORS = 4`, and `the_gather_geometry_is_decided_by_a_constant` says why. |
| plus the send plumbing | **5 more poisons, 5 caught** | four of them on behaviour this crate had transcribed wrongly until this slice. |

**A packet is one stream and several GATHERS, and the split is not symmetric.**
A SUBSCRIBE spends three vectors on a topic and an UNSUBSCRIBE two, against a
four-vector array whose count is never reset before the filter loop. So a
SUBSCRIBE's first gather carries no filter at all and an UNSUBSCRIBE's carries
one:

```
sub   one-filter   v2/5:5, v3/6:6
unsub one-filter   v4/10:10
```

**The guard, twenty-seventh shape: a gather boundary is invisible to a transport
that is offered one vector at a time.** Three deliberate breakages of the gather
arithmetic — mis-counting the per-topic cost either way, and a strict inequality
in the guard — changed **no line** of the trace, because the vectors are offered
one at a time in the same order however they are grouped. Implementing `writev`,
which the C calls with the outstanding vectors *and their count*, made all three
fail. That was not an optimisation: `sendMessageVector` was already claimed as
remade and its `writev` branch was not there, so the claim was half true.

**And the twenty-eighth: a poison that a CONSTANT makes dead is not a workload
gap.** Two of the twenty-seven still do not fire, and neither is a missing case.
At `MQTT_SUB_UNSUB_MAX_VECTORS = 4` a SUBSCRIBE's room is **one** vector and the
cheapest filter costs **two**, so the first filter ends the gather whether or not
the options byte is counted — the arithmetic cannot be wrong in a way that shows.
`the_gather_geometry_is_decided_by_a_constant` states that in terms of the three
constants rather than the code, which is where the property actually lives.

**Reading the C properly turned up two defects in the previous slice.**
`sendMessageVector` compares elapsed time with `>` and reads the clock only when
it will go round again — including not reading it after a transport failure —
where `sendBuffer`, thirty lines away, compares with `>=` and reads it every
turn. This crate had given both senders `sendBuffer`'s shape and the trace
agreed, because nothing asked. Two new cases ask: a step that lands elapsed time
exactly on the timeout in the *vector* sender (`calls=3` against the buffer
sender's `calls=2`) and a `reads=` column on every vector line.

**A duplicate type deleted.** `client::Subscription` and
`outbound::Subscription` had the same five fields; slice 19 wrote the second
without looking for the first. The twenty-second shape of the guard, pointed at
a struct: *two types that cannot be told apart are one type with two names.* The
same pass replaced the client's `outgoing_records: usize` with the real
`PublishRecords`, so `MQTT_CancelCallback` and the publish state machine are
wired rather than counted.

**One case removed rather than added, again.** `MQTT_CancelCallback` passes a
packet id of zero straight to `findInRecord`, which asserts on it — so on a build
with assertions live the C **aborts** rather than answering, and the driver died
with it. A documented precondition, not a defect, and a differential case the C
cannot complete is not a case.

**Two upstream findings drafted**, in `kairos-upstream/drafts/`: a CWE-787 stack
write out of bounds reachable by setting `MQTT_SUB_UNSUB_MAX_VECTORS` below 3
(the guard's `4U - 3U` is unsigned), and the two senders' disagreement above.

## Conformance (2026-09-19) — opening a connection

| quantity | value | method |
|---|---|---|
| trace lines agreeing with the C | **39 / 39** | `cargo test -p rusty_rtos_mqtt-core --test session`. C arm: `oracle/session_driver.c` driving `core_mqtt.c` verbatim from v5.0.2 at `04845c6a`. |
| what is compared per case | **the status, BOTH call logs, the bytes that went out, the whole connection context, the record array, and the retransmit store's calls AND KEYS** | a CONNACK's job is to set the context, so a status alone would bless a reader that dropped every property in it. |
| directions scripted | **two** | the send step as in the send slice, and a receive SCRIPT — because one number cannot express a transport that alternates between delivering and not, which is the only shape that can see a polling timeout that resets. |
| poison rows | **26 introduced, 24 caught** | one is unreachable until the process loop exists, and one is subsumed by the slice bound. |

**The first function that both sends and receives**, and so the first that can
deadlock, time out, or answer a packet that never came.

**A CONNECT with no properties still carries five.** coreMQTT builds a property
section containing a single Maximum Packet Size set to the size of the network
buffer, because "otherwise the server can send a bigger packet which cannot be
processed by the coreMQTT library". It is on the wire in nearly every line of
the trace and the application supplied none of it.

**Two timeouts, and only one resets.** The CONNACK's header is retried against
either a clock or a RETRY COUNT depending on whether the caller passed a
non-zero timeout, and the count is checked *before* it is incremented, so a
maximum of zero tries once. The body then goes through `recvExact`, whose
ten-millisecond timeout restarts on every byte that arrives — so it bounds the
GAP between bytes, not the packet. `dribbled-body` delivers one byte every nine
milliseconds and succeeds; one millisecond more and it fails on the first gap.

**A clean session leaks every stored PUBREL.** `handleCleanSession` clears the
stored PUBLISHes, zeroes the outgoing record array, and then asks
`MQTT_PubrelToResend` — which reads that array — what PUBRELs to clear.
`clean-pubrel-only-with-store` has a store wired up, one PUBREL in flight, and
`clear=0`; the resumed case below it re-sends exactly that record, which is what
makes the first a defect rather than an empty array. Transcribed, and drafted
for upstream.

**`session_present` is an out-parameter because the C's really does survive the
failure.** `MQTT_DeserializeConnAck` fills it in before the clean-session check
can reject the packet, so a caller that asked for a clean session and got a
resumed one is handed `MQTTBadResponse` *and* a `true` flag. A
`Result<bool, _>` would have lost that.

**Two poisons remain, and both are explained.** `recvExact`'s failure branch
sets the connection to disconnect-pending only when it is already connected —
which it never is during `MQTT_Connect`, so the branch has no caller yet and the
process loop is where it will be proven. And refusing a packet larger than the
network buffer is subsumed by the slice bound: `get_mut(0..257)` on a 256-byte
buffer is `None` either way. Same family as the property builder's advisory size
check three slices ago.

**And a dedup that would have been a defect.** `connack::ServerSettings` and
`context::ServerLimits` have field for field the same nine members, which looks
exactly like the twenty-second shape of the guard. Merging them was tried and
reverted: **their zero means different things.** An absent Maximum QoS is 2 in a
session and 0 in a packet, so assigning one to the other would have capped every
publish at QoS 0 whenever a broker left the property out. The merge is written
out instead, field by field, gated on `fields_present`.

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
| `cargo test -p rusty_rtos_mqtt-core` | 193 passed, 0 failed (97 unit, 15 gate, 3 state, 5 header, 5 property, 5 writer, 3 size, 3 ack, 3 connack, 4 publish, 4 disconnect, 3 connect, 3 outpublish, 4 outbound, 3 reader, 4 validate, 3 context, 5 propbuild, 3 propread, 4 topic, 3 client, 3 send, 4 outgoing, 4 session) |
| `cargo clippy --all-targets --all-features` under the workspace lint policy | clean, 0 warnings |
| `cargo build -p rusty_rtos_mqtt --no-default-features --target thumbv7em-none-eabihf` | passes |
| `cargo build -p rusty_rtos_mqtt --no-default-features --target riscv32imac-unknown-none-elf` | passes |

## Scope, in lines (2026-09-18, revised)

A count that belongs here because the README's honesty depends on it.

| part of coreMQTT v5.0.2 | lines | state |
|---|---:|---|
## How much of coreMQTT is remade

Counted by `oracle/coverage.py`, which reads the pinned source, enumerates every
function definition in it, and checks each against `oracle/REMADE.txt`. That is
the only place the number comes from; `--check` fails if the list names a
function the pinned source does not have.

```
functions      203 / 218    93.1 %
function lines 11739 / 13066  89.8 %
all lines      11739 / 15643  75.0 %   (2577 lines are preamble and cannot be remade)
```

**The headline is the first line.** A function is the unit that can be
transcribed and diffed. The third is reported only so nobody reconstructs it and
believes it: 2,577 of coreMQTT's lines are includes, macros, doxygen and
`/*---*/` rules, and there is nothing in a doxygen block to remake, so that
figure cannot reach 100 % however much is done.

| file | functions remade |
|---|---|
| `core_mqtt_state.c` | 19 / 19 |
| `core_mqtt_serializer.c` | 68 / 70 |
| `core_mqtt_serializer_private.c` | 15 / 15 |
| `core_mqtt_prop_serializer.c` | 23 / 23 |
| `core_mqtt_prop_deserializer.c` | 31 / 31 |
| `core_mqtt.c` | 47 / 60 |

The two outstanding in `core_mqtt_serializer.c` are `logConnackResponse` and
`logAckResponse`: `static void`s of `LogError` calls with no observable
behaviour. They are counted as not written rather than claimed, because a remake
that produces no log line has not remade a logger. The 13 outstanding in
`core_mqtt.c` are the receive loop and the acknowledgement handling.

**This replaces the earlier figure, which was wrong.** Until 2026-09-18 this
table divided a hand-maintained sum of per-slice line counts by all 15,643
lines. The numerators were measured with a looser boundary than the denominator
— they counted the comment blocks *between* functions — so the two were never on
the same basis, the result (61.2 %) was inflated against the strict count, and
it could never have reached 100 %. Same class of error as the double-counted
reader in slice 15, and the same fix: one script, checked in, run on demand.

No speed number and no size number: nothing here has been benchmarked, and
nothing has run on a chip.
