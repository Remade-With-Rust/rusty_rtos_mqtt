# rusty_rtos_mqtt — package plan

**One sentence:** coreMQTT remade in Rust — the QoS publish state machine
(done, and diffed against the C operation for operation) and, in time, the
wire serializer and the connection state machine (neither written), zero
allocation, no_std, forbid(unsafe).

Family plan: Kairos `docs/plans/rtos-mission.md` (umbrella repo) — its §2.1
names what this package remakes, wraps and never touches; its §6 carries the
phase this package's kill test belongs to. This file obeys that one.

Written 2026-09-17. Status: **the publish state machine is built and
proven** — 413 trace lines across 24 scenarios agree with
`core_mqtt_state.c` at the pinned v5.0.2, comparing both record arrays after
every operation. The serializer and the connection state machine are not
written; they are 11,728 more lines of C.

---

## 1. What it is, what it is not

**Is:** the Rust remake of the FreeRTOS component named above, exposing the
names a FreeRTOS developer already knows, with the C original as the oracle.

**Is not:** a binding to the C code, a fork of it, or a place where a chip's
registers are touched (that is a port crate).

## 2. The laws this package encodes

1. The core is `no_std` (+ `alloc`), `forbid(unsafe)`, arch-agnostic.
2. Every parser that takes bytes from a wire, a store or a bus has a
   `tests/no_panic.rs` from the day it exists.
3. Every claim has a kill test or a ledger row; the README copies this plan
   and never upgrades it.
4. Feature ladder `std` ⊃ `alloc` ⊃ core-only; CI proves the two bare-metal
   rungs on four targets on every push.

## 3. The surface as built

`core_mqtt_state.c`, whole; out of `core_mqtt_serializer.c` the fixed-header
codec (~240 lines) and the packet-size calculators (~300); and, out of
`core_mqtt_serializer_private.c`, the property primitives (~309 lines) and the
fixed-header writers (~180). 14.3 % of the library.

| ours | coreMQTT | note |
|---|---|---|
| `PublishRecords::new(outgoing, incoming)` | the four `MQTTContext_t` record fields | borrowed and independently sized, as the C's are |
| `reserve(packet_id, qos)` | `MQTT_ReserveState` | |
| `update_publish(packet_id, op, qos)` | `MQTT_UpdateStatePublish` | |
| `update_ack(packet_id, ack, op)` | `MQTT_UpdateStateAck` | the PUBREC move lives here |
| `remove(packet_id)` | `MQTT_RemoveStateRecord` | |
| `publish_to_resend(&mut Cursor)` | `MQTT_PublishToResend` | `Option<u16>` rather than a zero sentinel |
| `pubrel_to_resend(&mut Cursor)` | `MQTT_PubrelToResend` | the C's out-parameter state is always `MQTTPubRelSend`, so it is dropped |
| `calculate_state_publish`, `calculate_state_ack` | the same | |
| `AckType` | `MQTTPubAckType_t` | four values and no fifth, so the C's range check has no equivalent |
| `StateError` | the `MQTTStatus_t` subset this module returns | |

The size calculators, in `size`:

| ours | coreMQTT | note |
|---|---|---|
| `ack_packet_size(max, property_length)` | `MQTT_GetAckPacketSize` | |
| `PINGREQ_PACKET_SIZE` | `MQTT_GetPingreqPacketSize` | a constant, because the C function takes no input that can change its answer and its only failure is a null pointer |
| `subscribe_packet_size(filters, property_length, max)` | `MQTT_GetSubscribePacketSize` | takes the filter LENGTHS, since a filter's contents cannot change its size |
| `unsubscribe_packet_size(...)` | `MQTT_GetUnsubscribePacketSize` | |
| `list_packet_size(kind, ...)` | `calculateSubscriptionPacketSize` | the C's `static` helper, exposed because the two callers differ by one byte per filter and hiding that would duplicate it |
| `PacketSize { remaining_length, packet_size }` | the two out-parameters | returned together, so a refusal cannot hand back a half-updated pair |

**Not built:** the rest of `core_mqtt_serializer.c` (~5,570 lines),
`core_mqtt_prop_*.c` (2,056 for MQTT 5 properties) and `core_mqtt.c` (5,618).
This crate can recognise a packet arriving, read its properties and size an
outgoing one; it cannot yet fill in the body.

## 4. Roadmap

| Milestone | Adds | Driven by | Kill test |
|---|---|---|---|
| scaffold | the shape | K0 | a clean clone builds alone; CI green ✅ |
| **publish state** | the QoS 1 and 2 delivery state machine | K7 | **413 trace lines across 24 scenarios agree with `core_mqtt_state.c`, both record arrays compared after every operation** ✅ |
| **fixed header** | the packet type and the variable-byte remaining length | K7 | **6,291,456 calls agree — every type byte against every length pattern at every claimed length, by per-status counts and an FNV-1a digest** ✅ |
| **property primitives** | the bounded integer, string and user-property reads | K7 | **104 trace lines plus a 6,480-call sweep agree, comparing the cursor and the budget after every read** ✅ |
| **fixed-header writers** | the header of every outgoing packet type | K7 | **51 trace lines plus a 1,536-call exhaustive CONNECT-flags sweep agree, byte for byte** ✅ |
| **packet sizes** | the remaining length and packet size every writer is handed | K7 | **53 trace lines agree, including the 268,435,455 boundary at the exact value each check tests, and the calculators reconcile with the writers** ✅ |
| CONNECT / PUBLISH / SUBSCRIBE | the rest of the wire codec | K7 | a byte-for-byte differential against `core_mqtt_serializer.c` |
| MQTT 5 properties | `core_mqtt_prop_*.c` | K7 | the same, over a property corpus |
| the connection | `core_mqtt.c` over a transport | K7 | a callback-for-callback differential, the shape `rusty_rtos_sntp`'s client established |
| a real broker | — | K7 | one hour against `rumqttd`, zero lost keep-alives (the family plan's kill test) |

## 5. Deliberately absent

| Absent | Why |
|---|---|
| an owned buffer or socket | the C takes both from the application, and so will this. A library that owns a socket cannot be used by an application that already has one. |
| a packet-id allocator | the C's `MQTT_GetPacketId` lives in `core_mqtt.c` and belongs with the connection, not with the records. |
| a range check on the ack type | `AckType` has four values and no fifth, so `MQTTBadParameter` for an out-of-range packet type cannot be reached. The differential reproduces the C's answer directly and says so. |
| any `unsafe` | `forbid(unsafe_code)`, and a state machine over two slices never needed it. |

## 6. Risks

| Risk | Mitigation |
|---|---|
| **The ORDER of the records is a correctness property, and a status-only test cannot see it.** A transcription could return every correct status and still resend a session's backlog out of order — on exactly the reconnect this module exists to survive. | The differential compares **both arrays after every operation**, and the PUBREC move has its own test. Poisoning the append, the compaction or the move all fail the run. |
| The packet ids driving this come from the broker, so they are attacker-chosen. | `tests/no_panic.rs` drives 200 x 60 random operations over a four-id space and checks well-formedness after every step: no duplicate id, no occupied record at QoS 0, no empty slot keeping stale fields. |
| A resend cursor that failed to advance would spin rather than fail. | Asserted, the same way `rusty_rtos_json`'s iterator and `rusty_rtos_sntp`'s retry loops are. |
| A guard whose effect is invisible in every scenario looks like dead code and gets removed. | Two were found by poisons that did not fire. One (the ack/QoS check) was a WORKLOAD gap and got a scenario; the other (the self-transition guard) is genuinely an optimisation, and a unit test pins why. |
| **This is 2,235 lines of a 21,102-line library.** Claiming "coreMQTT remade" on the strength of it would be false. | The README, the crate description and this plan all name what is not written, in lines. |
| A size calculator's limit checks sit three orders of magnitude above anything a plausible workload reaches, so they look proven and are not exercised at all. | The property length is the one input that can bridge the gap, and two cases are computed to land on each check EXACTLY. A case that overshoots cannot tell a `>=` from a `>`. |
| The calculator and the writer could each agree with the C and disagree with **each other** about the same packet, and neither differential would see it. | A standing test feeds each calculator's answer into the matching writer and reconciles the bytes. |

## 7. Decision log

| Date | Decision |
|---|---|
| 2026-09-17 | Stamped from the Kairos template; obeys the family plan. |
| 2026-09-17 | **`core_mqtt_state.c` is the first slice, because it is the only self-contained one.** It includes nothing but its own header — no bytes, no transport, no clock — so it can be diffed exactly today, and it is where MQTT's hardest correctness lives. The serializer is four times the size and the connection machine needs it; neither is a unit that can be finished and proven on its own. |
| 2026-09-17 | **The differential compares the RECORD ARRAYS, not just the statuses.** The relative order of the records is the resend order, which MQTT 5.0 requires, so a status-only comparison would bless a transcription that reordered a session's backlog. |
| 2026-09-17 | **A poison that did not fire found a workload gap, again.** The ack/QoS sanity check is invisible for most mismatches because the transition would fail anyway; it is visible only for a QoS 1 publish in `PubAckPending` handed a PUBCOMP, where the computed `PublishDone` is a LEGAL transition and the handshake would complete without it. A scenario was added. The mirror case is kept too, so the asymmetry is recorded rather than inferred. |
| 2026-09-17 | **An exhaustive sweep AND named cases, rather than either alone.** The fixed header's input space is small enough to enumerate — 256 type bytes x an 8-value length alphabet^4 x 6 claimed lengths is 6.29 million calls — so the differential compares per-status COUNTS and an FNV-1a DIGEST of every answer, alongside 30 named cases printed in full. The digest proves agreement everywhere; the named cases say WHERE when it breaks. All eight poisons fired first time, which is the first slice in K7 where none needed a scenario adding — and that is the sweep's doing, not luck. |
| 2026-09-17 | **A trace of the output cannot say "and nothing else".** A poison that made a writer emit one byte BEYOND the length it reports passed the byte-for-byte differential, because the differential only ever looks at `buffer[..n]`. A caller packing data after the header would have had it clobbered. "Wrote the right bytes" and "wrote only those bytes" are two claims; every writer now has a test for the second. |
| 2026-09-17 | **A failed read that moves the cursor is BEHAVIOUR, not a bug to tidy.** `decodeUtf8` consumes its two length bytes and charges them to the budget before it discovers the body does not fit. A transcription that checked first would look more correct and would disagree with the C on every malformed packet, so the differential compares the cursor and the budget after every read and the tidy version is one of the poisons. |
| 2026-09-17 | **Provably dead code in the oracle is kept, and pinned.** The property length decoder's in-loop range check cannot fire — the multiplier guard bounds the value to exactly one less than the constant it tests against. A differential arm does not tidy its oracle, so it stays; a unit test pins the arithmetic, because the bound is a relationship between two constants that could move. Third non-firing poison in this package, and the second that was a genuine property rather than a workload gap. |
| 2026-09-17 | **The header codec makes a guarantee the C cannot.** `MQTT_ProcessIncomingPacketTypeAndLength` takes a pointer and a count and trusts the count, so an `available` larger than the allocation reads past the buffer. Ours uses the count only as an upper bound on a `get`, and a test pins it. Worth recording because it is the first place in K7 where the Rust is not merely equivalent but strictly safer on the same inputs. |
| 2026-09-17 | **The `current != new` guard in `update_ack` is an optimisation, not behaviour**, and that is now a unit test rather than a coincidence. The record is deleted only on `PublishDone` or `PubRelSend`, and neither is reachable as a **legal** self-transition. The first version of the test missed the word "legal" and failed, which is the useful half of the story: `calculate_state_ack` will compute `PublishDone` for a record already there, and only `validate_transition_ack` stops it. |
| 2026-09-18 | **A boundary case must land EXACTLY on the comparison.** Property-length cases *near* 268,435,455 left the limit-check poisons still passing, because a case that overshoots a check refuses for the same reason whether the operator is `>=` or `>`. The two that matter are computed backwards from the arithmetic — `prop=268435348` puts the in-loop check at exactly 268,435,456, `prop=268435446` puts the final one at exactly 268,435,455 — and they are named for what they test, so the next reader does not have to re-derive them. |
| 2026-09-18 | **A check that is load-bearing in the C can be redundant in the transcription, when the arithmetic differs.** The in-loop overflow check guards a `uint32_t` accumulator whose additions WRAP: without it a long enough subscription list wraps past zero and comes out under the limit. Ours SATURATE, so the same list ends at `u32::MAX` and the final check refuses it regardless. The check is kept — a differential arm does not tidy its oracle — and a unit test records that removing it would be safe here and unsafe there, which is the part that would otherwise be lost. Fourth non-firing poison in this package, and the third that was a genuine property. |
| 2026-09-18 | **A `Result` cannot reproduce a half-written out-parameter, so the trace does not pretend to.** The C writes both out-parameters before its final maximum-packet-size check, so a failed call has still updated them and a caller who ignored the status would serialize with a length the library had just refused. There is no error-path value in a `Result` to hand back, so the driver prints values only on success and the divergence is recorded rather than papered over. This is the same family as the writer's write-past-reported-length and the header's over-large count: **a differential bounds what the C answers, never what it touches, and never what it leaves behind.** |
| 2026-09-18 | **Two slices that each agree with the C still need a test that they agree with EACH OTHER.** The calculator says how many bytes a packet takes and the writer lays down its header; both differentials could pass while the pair disagreed about the same packet, because neither arm ever sees the other. `the_calculated_size_matches_what_the_writer_lays_down` feeds each calculator's answer straight into the matching writer and reconciles the total. The first cross-slice test in K7, and the shape every later pair of slices should copy. |
