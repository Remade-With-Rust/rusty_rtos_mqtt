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
codec (~240 lines), the packet-size calculators (~300), the acknowledgement
deserializers (~586), the CONNACK path (~566) and the incoming PUBLISH (~466);
and, out of `core_mqtt_serializer_private.c`, the property primitives
(~309 lines) and the fixed-header writers (~180). 24.6 % of the library — and
**every packet a broker can send**.

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

The acknowledgement deserializers, in `ack`:

| ours | coreMQTT | note |
|---|---|---|
| `deserialize_ack(&PacketInfo, &Limits)` | `MQTT_DeserializeAck` | returns the three out-parameters together, so a refusal cannot leave one updated and the others not |
| `PacketInfo { packet_type, remaining_length, remaining_data }` | `MQTTPacketInfo_t` | keeps the CLAIMED length and the bytes that EXIST apart, which is where the attack lives |
| `Limits { max_packet_size, request_problem_info }` | the two `MQTTConnectionProperties_t` fields this path reads | naming them keeps the CONNACK slice from being a prerequisite |
| `AckInfo { packet_id, reason_codes, properties }` | `pPacketId`, `MQTTReasonCodeInfo_t`, `MQTTPropBuilder_t` | `packet_id` is `Option`, because only a PINGRESP may have none |
| `AckError::{BadParameter, BadResponse}` | the two statuses this path returns | five of the C's `MQTTBadParameter` paths are NULL checks with no Rust equivalent |
| `PUBREL` | `MQTT_PACKET_TYPE_PUBREL` | `0x62`, spelled out rather than reusing the `0x60` nibble the header codec masks to |

The CONNACK, in `connack`:

| ours | coreMQTT | note |
|---|---|---|
| `deserialize_connack(&PacketInfo, &ClientSettings)` | `MQTT_DeserializeConnAck` | |
| `ClientSettings { max_packet_size, request_response_info }` | the two `MQTTConnectionProperties_t` fields that are INPUTS | the C writes the server's answers into the same struct; splitting them stops a caller feeding the broker's numbers back as its own |
| `ServerSettings` | the ten `server*` fields | every limit the rest of the session runs under |
| `ConnAck { session_present, reason_code, server, fields_present, properties }` | the out-parameters plus `MQTTPropBuilder_t::fieldSet` | `fields_present` distinguishes "the server said 0" from "the server said nothing", which for Maximum QoS are opposite claims |
| `ConnAck::refused()` | `MQTTServerRefused` | a refusal is `Ok`, because the Reason String that says WHY is in the properties and an `Err` would discard it |
| `connack::property`, `connack::field` | the property ids and the `fieldSet` bit positions | the C's numbering exactly; these cross the API in a `u32` |

The incoming PUBLISH, in `publish`:

| ours | coreMQTT | note |
|---|---|---|
| `deserialize_publish(&PacketInfo, max_packet_size, topic_alias_max)` | `MQTT_DeserializePublish` | |
| `PublishInfo { qos, dup, retain, packet_id, topic_name, properties, payload }` | `MQTTPublishInfo_t` plus the packet-id out-parameter | `packet_id` is `Option`, because QoS 0 carries none; `payload` is an empty slice where the C uses a NULL pointer |
| `publish::flag` | `MQTT_PUBLISH_FLAG_*` | the four bits of the type byte's low nibble |
| `publish::property` | the eight ids of §3.3.2.3 | including the only VARIABLE-length one in the library |
| `PropertyReader::variable_length` | the C's inline `decodeVariableLength` in the subscription-id arm | bounded by the buffer as well as the budget |

**Not built:** the OUTGOING packet bodies (~3,952 lines of
`core_mqtt_serializer.c`), `core_mqtt_prop_*.c` (2,056 for the outgoing MQTT 5
property tables) and `core_mqtt.c` (5,618). This crate can read a session; it
cannot yet start one.

## 4. Roadmap

| Milestone | Adds | Driven by | Kill test |
|---|---|---|---|
| scaffold | the shape | K0 | a clean clone builds alone; CI green ✅ |
| **publish state** | the QoS 1 and 2 delivery state machine | K7 | **413 trace lines across 24 scenarios agree with `core_mqtt_state.c`, both record arrays compared after every operation** ✅ |
| **fixed header** | the packet type and the variable-byte remaining length | K7 | **6,291,456 calls agree — every type byte against every length pattern at every claimed length, by per-status counts and an FNV-1a digest** ✅ |
| **property primitives** | the bounded integer, string and user-property reads | K7 | **104 trace lines plus a 6,480-call sweep agree, comparing the cursor and the budget after every read** ✅ |
| **fixed-header writers** | the header of every outgoing packet type | K7 | **51 trace lines plus a 1,536-call exhaustive CONNECT-flags sweep agree, byte for byte** ✅ |
| **packet sizes** | the remaining length and packet size every writer is handed | K7 | **53 trace lines agree, including the 268,435,455 boundary at the exact value each check tests, and the calculators reconcile with the writers** ✅ |
| **acknowledgement deserializers** | every ack a broker can send, except CONNACK | K7 | **57 trace lines plus three 256-value sweeps agree; the sweeps' accepted sets are printed in full, and reading them found three divergences from MQTT 5.0** ✅ |
| **the CONNACK** | the packet that sets every connection-wide limit | K7 | **54 trace lines plus six 256-value sweeps agree; the reason-code and property tables are exactly MQTT 5.0 §3.2.2.2 and §3.2.2.3, asserted from the trace** ✅ |
| **the incoming PUBLISH** | the last packet a broker can send, and the only one carrying application data | K7 | **54 trace lines, two flag sweeps and six property sweeps agree; the payload arithmetic is pinned by an IDENTITY over 3,000-odd shapes, and two more divergences from MQTT 5.0 came out of it** ✅ |
| CONNECT / PUBLISH / SUBSCRIBE | the rest of the wire codec, outgoing | K7 | a byte-for-byte differential against `core_mqtt_serializer.c` |
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
| **This is 3,853 lines of a 21,102-line library.** Claiming "coreMQTT remade" on the strength of it would be false. | The README, the crate description and this plan all name what is not written, in lines. |
| **A PUBLISH's payload length is a four-term subtraction on attacker-chosen numbers**, and a wrap would hand the APPLICATION a length near four billion pointing into a packet of a few bytes. | Three growing remaining-length checks make it unreachable, and a test asserts that the parts RECONSTRUCT the packet rather than merely fitting in it — the weaker assertion was measured to be vacuous. |
| A one-byte table sweep looks exhaustive and can discriminate NOTHING, when the byte selects values of different widths. | The CONNACK property identifier is swept five times, once per value shape; the five accepted sets are the table AND say which identifier is which type. A single sweep would have refused 251 of 256 for the wrong reason. |
| **The ack deserializers take a pointer and a length that an attacker can make disagree**, and that is exactly the input a differential cannot cover — the C would be reading past its own buffer, so its answer depends on memory rather than on the library. | The driver ASSERTS that every case's claim equals its body, so no such line can reach the trace; the over-claim is pinned on the Rust side alone by `a_claim_larger_than_the_buffer_is_refused`. |
| A decision that comes down to a table of byte values looks proven by a handful of cases and is not. | All three of this slice's single-byte tables are swept over 256 values, and the ACCEPTED SET is printed in full rather than hashed — which is how the three specification divergences were found. |
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
| 2026-09-18 | **Where a differential's own input would make the C read out of bounds, the case does not belong in the trace.** `MQTTPacketInfo_t`'s pointer and claimed length can disagree, and that is the attack — but the C's answer for such an input depends on whatever is next in memory, so the line would be a coin toss that happens to be reproducible on one machine, not an oracle. The driver asserts the two are equal and the over-claim is pinned on the Rust side alone. Third instance of the category the fixed header opened. |
| 2026-09-18 | **A one-byte decision table is swept and PRINTED, not hashed.** Twelve accepted values out of 256 is small enough for a reader to check against the specification, and a digest would have said only that the arms agree. Printing the accepted set into the checked-in trace is what turned up three divergences from MQTT 5.0 in `readSubackStatus` and `logAckResponse` — a digest would have concealed every one of them behind a passing test. |
| 2026-09-18 | **A divergence from the SPECIFICATION is transcribed, and asserted from the TRACE.** The Rust arm reproduces all three faithfully, because it is a transcription and its oracle is the C. The test that records them reads the checked-in trace rather than our own code, so it is the pinned oracle changing its mind that fails the suite — which is the event that matters. |
| 2026-09-18 | **An exact-fit check needs a workload that parses CLEANLY and then has one byte too many.** A poison that turned the pub-ack property section's equality into a bound did not fire, because every malformed section in the corpus was already refused by the property walk. The missing shape was a valid section followed by a byte nobody would ever look at — which is precisely the hazard the check exists for, since a broker could carry data inside a packet the client believes it read whole. |
| 2026-09-18 | **A one-byte sweep must vary the SHAPE of what the byte introduces, or it discriminates nothing.** The CONNACK property identifier selects a value of one of five widths, so a body sized for one is malformed for the other four — both arms refuse for the wrong reason and every line still matches. Swept five times, once per shape, the accepted sets are the table and say which identifier is which type. Enumerating an input is not the same as exercising it. |
| 2026-09-18 | **A sweep that confirms conformance is a result, and gets asserted the same way.** The ack deserializers' tables diverged from MQTT 5.0 in three places; the CONNACK's are exactly §3.2.2.2 and §3.2.2.3. That is worth an assertion rather than a sentence, and it is made against the CHECKED-IN TRACE, so the event it reports is the pinned oracle drifting. |
| 2026-09-18 | **Where the C's status carries information, a `Result` must not throw it away.** `MQTTServerRefused` means the packet parsed and the broker said no, and the C fills its out-parameters on it because the Reason String that explains the refusal is in the property section. A refusal is therefore `Ok` here with `refused()` true. Mapping it to `Err` would have looked tidier and discarded the one thing a refused client needs. |
| 2026-09-18 | **A check can be redundant in the ORACLE, not only in the transcription.** Third shape of the family: the size calculators' in-loop check is load-bearing in the C (wrapping arithmetic) and subsumed here (saturating); the ack deserializers' property bound is load-bearing in the C (pointer arithmetic) and subsumed here (a slice); the CONNACK's three-byte minimum is subsumed in BOTH arms, because `decodeVariableLength` refuses a zero-length buffer on its own. Kept because it states the packet's shape in one place; pinned so it stops being free the day it stops being true. |
| 2026-09-18 | **"It fits" is not an invariant; "the parts reconstruct it" is.** The obvious assertion for a PUBLISH's payload — that it is no larger than the buffer — was MEASURED to be vacuous: a mutation that silently emptied the payload passed it. What has teeth is the identity, that two topic-length bytes plus the topic plus the packet id plus the encoded property length plus the properties plus the payload equal the remaining length. Where a parser splits a packet into parts, assert the split, not a bound on one part. |
| 2026-09-18 | **A byte that selects between values of different SHAPES needs one sweep per shape — and so does a byte that changes the shape of the REST of the packet.** The CONNACK needed five property sweeps; the PUBLISH needs six, plus TWO flag sweeps, because QoS decides whether a packet identifier is present and therefore where everything after the topic begins. A single flags sweep would have refused half the nibble for the wrong reason. |
| 2026-09-18 | **Three checks can be one finding.** All three of `checkPublishRemainingLength`'s calls survived poisoning, and for one reason: each is the same predicate as the bounded slice that follows it. The C needs them because it indexes with a length it was handed; this module reads through `bounded`. Recorded as one entry rather than three, because three entries would have suggested three investigations. |
| 2026-09-18 | **A mutation that changes no answer is inert, not undetected — but say which.** Emptying the PUBLISH payload's `?` fallback passed every test, and the reason is that the slice can never fail: `payload_at + payload_length` is the remaining length exactly, and the property section's own slice already required that much buffer. That is now a comment at the call site, because "this `?` is unreachable" is a thing a reader will otherwise re-derive or, worse, quietly rely on. |
