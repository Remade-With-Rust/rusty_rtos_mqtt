# rusty_rtos_mqtt

[![crates.io](https://img.shields.io/crates/v/rusty_rtos_mqtt.svg)](https://crates.io/crates/rusty_rtos_mqtt)
[![docs.rs](https://docs.rs/rusty_rtos_mqtt/badge.svg)](https://docs.rs/rusty_rtos_mqtt)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

A `no_std` MQTT publish state machine, fixed-header codec, MQTT 5 property
primitives, packet-size calculators, **every packet a broker can send** and
**every packet a client can send** — twelve proven slices of the Kairos remake of
coreMQTT. MIT OR Apache-2.0.

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
- **Proven**: the fixed-header writers for every outgoing packet type, byte for
  byte, with an **exhaustive sweep of the CONNECT flags byte** — the one byte
  that packs six independent decisions and where a wrong bit reads like a
  network fault.
- **Proven**: the packet-size calculators that feed those writers, including the
  268,435,455 boundary — reached only by computing the exact values each check
  lands on, because a case that overshoots cannot tell a `>=` from a `>`.
- **Proven**: the acknowledgement deserializers — the first code here whose
  whole input a broker chooses — with all three of its single-byte decision
  tables swept over 256 values. Reading those tables turned up **three
  divergences from MQTT 5.0**, including a conformant UNSUBACK that a stock
  client refuses.
- **Proven**: the CONNACK — the first packet a broker sends and the one that
  sets every limit the session then runs under. Its reason-code and property
  tables are swept over 256 values each and are **exactly** MQTT 5.0's, which
  after the three divergences next door is a result rather than an assumption.
- **Proven**: the incoming PUBLISH — the only packet that carries application
  data, and the only one whose type byte is partly data. Its payload length is a
  four-term subtraction, and an identity test asserts that the parts reconstruct
  the packet rather than merely fitting inside it.
- **Proven**: the DISCONNECT, which MQTT 5 made **bidirectional** — one
  validation table read twice, with different answers, and a property table per
  direction. Comparing the two accepted sets against the specification found a
  server reason code a stock client refuses.
- **Proven**: the CONNECT — eight length-prefixed fields, four optional, and a
  flags byte that has to agree with which ones are there. Sized and serialized
  in one case, byte for byte, with the ORDERING poisons that a length-prefixed
  format hides best.
- **Proven**: the outgoing PUBLISH, across all **three** of coreMQTT's
  serializers — whole packet, header-without-payload, and header-without-topic —
  with the trace showing that the three agree as prefixes, which no
  single-function differential can.
- **Proven**: SUBSCRIBE, UNSUBSCRIBE, the publish acknowledgements and PINGREQ —
  which complete the **outgoing wire codec**. Sweeping the ack reason codes here
  showed that coreMQTT validates them correctly on the way out and incorrectly
  on the way in: the library disagrees with itself.
- **Proven**: the six outgoing property validators — which property may go in
  which packet — swept over all 256 identifiers at six value shapes each. The
  map is printed rather than hashed, and it found three more places where the
  writing side allows what the reading side refuses.
- **Proven**: the connection context, both constructors, the outgoing PUBLISH's
  parameter validator and the **second** fixed-header reader — which completes
  `core_mqtt_serializer.c` but for its logging. Running the two readers side by
  side showed that only one of them can say "not yet", on every packet type a
  client may receive.
- **Proven**: the MQTT 5 property builders, all of `core_mqtt_prop_serializer.c`
  — and this is the slice that found a **buffer overflow**, by being unable to
  reproduce it. `addPropUtf8` forgets to count the property identifier byte, so
  it writes one past a buffer one byte too small. Six public adders go through
  it.
- **Proven**: the MQTT 5 property reader, all of
  `core_mqtt_prop_deserializer.c` — twenty-two getters that each demand one
  identifier, and two tables over one alphabet that, this time, agree.
- **Proven**: topic matching, the first slice of `core_mqtt.c` — swept as a
  printed 39 × 39 **grid** plus 2.4 million digested pairs, which showed a
  wildcard filter silently missing a whole class of topic.
- **Proven**: the client context and the subscription validators — where only
  the last entry in a list turns out to decide whether the list is valid, and a
  topic filter is searched past its length.
- **Proven**: the send plumbing, PINGREQ and DISCONNECT, against a **scripted
  transport** that takes what it likes and a scripted clock — the harness the
  rest of the connection machine runs on.
- **Proven**: SUBSCRIBE, UNSUBSCRIBE and PUBLISH on the wire, built without
  copying the caller's buffers, over both a per-vector transport and a
  **gathered `writev`** — where a packet turns out to be several gathers, and
  the stored copy of a publish differs from the sent one by one bit.
- **Proven**: `MQTT_Connect` end to end — the CONNECT, the wait for a CONNACK
  with both of its timeouts, what the CONNACK does to every limit in the
  context, and what a clean or resumed session owes the retransmit store.
- **Proven**: the receive loop and every handler under it, against a scripted
  **application callback** as well as a scripted transport and clock — the
  reassembly, the acknowledgements in both directions, and the keep alive.
- **Zero allocation**: two caller-supplied arrays, sized independently, exactly
  as the C does it. `forbid(unsafe)`.

**Known gaps: 2 functions, and `core_mqtt.c` is finished.** What is left is
`core_mqtt_serializer.c`'s two logging functions, which need a logger this crate
does not have. Everything else is remade: this crate can build and read every
MQTT packet, off a socket or out of a buffer, and assemble, check and walk back
every property section either end may send. It cannot yet run a connection.


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

**Ten slices built and proven; the rest of coreMQTT is not.** 413 trace lines
agree with `core_mqtt_state.c`, 6,291,456 calls with the fixed-header codec, 104
lines plus a 6,480-call sweep with the property primitives, 51 lines plus a
1,536-call sweep with the outgoing-packet writers, 53 lines with the packet-size
calculators, 57 lines plus three 256-value sweeps with the acknowledgement
deserializers, 54 lines plus six more with the CONNACK, 54 lines plus eight more
with the incoming PUBLISH, 58 lines plus twelve more with the DISCONNECT in both
directions, 30 lines plus a 32-combination whole-packet sweep with the CONNECT,
25 lines across three serializers with the outgoing PUBLISH, 45 lines with
SUBSCRIBE, UNSUBSCRIBE, the acknowledgements and PINGREQ, 20 lines comparing
the transport reader CALL FOR CALL, 94 lines across the six outgoing property
validators — 36 of them sweeps — 56 lines finishing that file, which run the
library's TWO header readers side by side, 73 lines across the MQTT 5 property
builders, 55 across the property reader, 109 across topic matching and 41
across the client context, 29 across the send plumbing, 59 across the outgoing
packets, 39 across opening a connection and 52 across the receive loop — all at
the pinned v5.0.2. **216 of coreMQTT's 218 functions, 99.1 %**, counted by
`oracle/coverage.py` from the pinned source. 199 tests. **This crate reads every packet a broker can send, off a socket or
out of a buffer, and writes every packet a client can send** — the whole wire
codec; what is missing is the connection state machine that drives it.

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

## The fixed-header writers

**51 trace lines plus a 1,536-call CONNECT sweep agree with
`core_mqtt_serializer_private.c`**, byte for byte.

The property primitives are the reading side of the primitive layer. These five
functions are the writing side: the fixed header of every outgoing packet an
MQTT client sends — CONNECT, SUBSCRIBE, UNSUBSCRIBE, DISCONNECT and the publish
acknowledgements. They validate nothing and keep no state, which makes them
exactly the sort of code where a transcription is confidently and quietly wrong.

### One byte does most of the work

`serializeConnectFixedHeader` packs six independent decisions into a single
flags byte: clean session, will present, will QoS, will retain, password
present, username present. Get a bit position wrong and the broker rejects a
packet that looks fine in a hex dump, in a way that reads like a network fault.

So the sweep is **exhaustive over every combination** — 2 × 2 × 3 × 2 × 2 × 2
settings across four keep-alive values and four remaining lengths, 1,536 calls,
compared by an FNV-1a digest, with eight combinations printed in full so a
mismatch has somewhere to start. A standing test asserts every non-reserved bit
is reachable, and a unit test asserts bit 0 — reserved — is never set.

**Poison-proven on eight behaviours, seven caught:** swapping the username and
password flags, will QoS 2 setting the QoS 1 bit, clean session on the reserved
bit, a little-endian keep alive, a DISCONNECT that always writes a reason code,
UNSUBSCRIBE written with the SUBSCRIBE type byte, and a protocol name without
its length prefix.

### A poison that found a gap in the gate

Making `serialize_disconnect_fixed` always write a reason code — so it writes
one byte **beyond the length it returns** — passed every test here at first,
*including* the byte-for-byte differential. The differential only ever looks at
`buffer[..n]`, so a byte written past `n` is invisible to it.

A caller that packed something after the header would have had it silently
clobbered. **"Wrote the right bytes" and "wrote only those bytes" are two
claims, and a trace of the output can only make the first.** A test now fills
the buffer and asserts nothing past the reported length was touched, for all
five writers at seven remaining lengths; it catches that poison.

### A poison that was an equivalence

Writing the will QoS as a shifted number rather than two independent flags
changes nothing — the two bits are *adjacent* and the legal QoS values are 0, 1
and 2, so `qos << WILL_QOS1` and the two-flag form produce identical bytes. The
C's two-flag form is kept because it says what the specification says, and a
unit test pins the adjacency, since moving either constant would part the two
forms with nothing else failing.

## The packet-size calculators

**53 trace lines agree with `core_mqtt_serializer.c`.**

The writers lay down a fixed header for a remaining length the caller has
already worked out. This is where that number comes from — and it is the only
place in the library that does **arithmetic on sizes the application does not
entirely choose**. A subscription list is the application's, but its topic
filters often come from a configuration file, a provisioning payload or a
cloud-side policy, and MQTT 5's limit of 268,435,455 is one a long enough list
reaches.

**Poison-proven on nine behaviours, seven caught:** the ack's property-length
term, the ack's two fixed bytes, the subscribe options byte, the two-byte length
prefix on each topic filter, the packet id, the filter-length upper bound, and
the final maximum-packet-size check.

### The boundaries had to be computed, not guessed

Three did not fire at first, and the reason was the same for all three: nothing
in the workload could get near the limit. Eight topic filters of 65,535 bytes
come to 524,280, and the limit is 268,435,455 — three orders of magnitude away.
The property length is the only input that can bridge that gap, and the C takes
it through an `MQTTPropBuilder_t` whose `currentIndex` the caller sets.

Adding property cases *near* the limit was not enough either. Each check lands
on an **exact** value, and a case that overshoots cannot tell a `>=` from a `>`.
The two boundary cases are computed: one makes the in-loop check see exactly
268,435,456, the other makes the final check see exactly 268,435,455.

### One was a workload gap; the other two are properties

The **final limit check** is now caught — it was a real gap, and the cases that
close it are in the trace. The **in-loop** one still is not, and that turns out
to be a genuine difference between the two arms:

* in the C the check is **load-bearing**. Its accumulator is a `uint32_t` and
  its additions wrap, so a long enough list would wrap past zero and come out
  *under* the limit. Stopping the loop early is what prevents that.
* here the additions **saturate**, so an overflowing list ends at `u32::MAX`
  and the final check refuses it anyway.

The check is kept for fidelity, and a test records that removing it would be
safe *here* and unsafe *there* — a distinction that would otherwise be lost the
next time someone tidies the function.

The early zero-maximum check is the same shape: the smallest packet these
calculators can produce is four bytes, so a maximum of zero is refused by the
final check regardless. Kept because the C has it; pinned so the floor cannot
drift below four without something failing.

### A refusal hands the caller nothing

The C writes its out-parameters **before** its final maximum-packet-size check,
so a call that fails on it has still updated them — a caller who ignored the
status would serialize with a length the library had just refused. A `Result`
has no error-path value to hand back, so the misuse has no Rust equivalent.

That is the fourth member of a family this package keeps finding: **a
differential bounds what the C answers, never what it touches, and never what it
leaves behind.** The trace therefore prints values only on success, and the
divergence is recorded rather than papered over.

### The two slices are only useful together

A calculator that agreed with the C and a writer that agreed with the C could
still disagree with **each other** about the same packet, and neither
differential would notice. A standing test feeds each calculator's answer
straight into the matching writer and checks the bytes add up.

## The acknowledgement deserializers

**57 trace lines agree with `core_mqtt_serializer.c`**, including three sweeps
over all 256 values of a single byte.

The five slices above are all outgoing: a state machine, a header codec,
property reads out of a buffer the caller already trusted, writers and the
sizes that feed them. **This is the first one whose whole input is chosen by the
other end of the socket.** `MQTT_DeserializeAck` is what a client runs on every
PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK, UNSUBACK and PINGRESP that arrives,
before anything above it has looked at a byte.

### Three sweeps, and each one prints the table it found

Three of this function's decisions come down to a switch on one byte: which
packet type routes where, which reason codes a publish acknowledgement may
carry, and which a SUBACK or UNSUBACK may. A byte is enumerable, so each is
swept over all 256 values — and the trace prints the **accepted set in full**,
not a count and a digest:

```
suback-status-sweep accepted=00,01,02,80,83,87,8f,91,97,9e,a1,a2 n=12 refused=244
ack-reason-sweep puback accepted=00,10,80,83,87,90,91,92,97,99 n=10 refused=246
ack-reason-sweep pubrel accepted=00,10,80,83,87,90,91,92,97,99 n=10 refused=246
type-sweep accepted=40,50,62,70,90,b0 n=6 badparam=1 badresponse=249
```

Twelve values out of 256, ten out of 256, six out of 256. A table that small is
worth reading rather than hashing, and reading it is how the next section
happened.

### Three divergences from MQTT 5.0, found by reading the trace

1. **SUBACK and UNSUBACK share one reason-code table, and it is the SUBACK
   one.** It accepts `0x01` and `0x02` — granted QoS values an UNSUBACK cannot
   grant — and refuses **`0x11`, "No subscription existed"**, which MQTT 5.0
   §3.11.3 lists as legal. A client that unsubscribes from a filter it is not
   subscribed to gets `MQTTBadResponse`, its callback never fires, and it
   cannot tell the case from a corrupt packet.
2. **A SUBACK with no reason codes at all is accepted.** The count is derived by
   subtraction and never checked against zero, so a packet whose property
   section fills the body exactly reports success with nothing in it.
3. **`0x92`, "Packet identifier not found", is accepted in a PUBACK**, where
   MQTT 5.0 lists it only for PUBREL and PUBCOMP. Too permissive rather than
   too strict, so nothing conformant is lost.

All three are transcribed exactly, because the C is the oracle — and a test
asserts them **from the checked-in trace**, so the day the pinned oracle changes
its mind, the suite says so. They are written up in `kairos-upstream/drafts/`
for filing.

### A case that cannot be a differential

`MQTTPacketInfo_t` carries a pointer and a claimed length, and every
deserializer indexes the first using the second. Making them disagree is the
attack — and it is also the one case the differential cannot cover, because the
C would then be reading past its own buffer and its answer would depend on
whatever is next in memory. **That is not an oracle, it is a coin toss that
happens to be reproducible on one machine.**

So the driver *asserts* that every case's claim equals its body, and the
over-claim is pinned on the Rust side alone: a claim of 4 to 63 bytes over a
three-byte buffer must be refused, every time. Third instance of the category,
after the fixed header's byte count and the property reader's budget.

### Poison-proven on eleven behaviours, ten caught

Accepting `0x11`; dropping the granted-QoS arm; reading the reason code one
byte early; routing a PUBREL by its `0x60` nibble rather than the `0x62` byte;
ignoring `requestProblemInfo`; allowing a zero packet id; forgetting the type
byte in the packet size; allowing a repeated reason string; refusing a CONNACK
as a bad packet rather than a bad call; and treating the pub-ack property
section as a bound rather than an exact fit.

That last one needed a case adding. A property section that parses cleanly and
is followed by **one extra byte** was the shape the workload was missing: without
the exact-fit check the section reads fine and the trailing byte is simply never
looked at, so a broker could carry data inside a packet the client believes it
has read whole.

The eleventh is a genuine property. The SUB/UNSUBACK property bound is the same
predicate as the slice that follows it — the C needs it precisely because it has
no slice, only pointer arithmetic — so loosening it changes no answer here. It
is kept for fidelity and the equivalence is pinned, which is the second time a
check load-bearing in the C has turned out to be subsumed in the transcription.

## The CONNACK

**54 trace lines agree with `core_mqtt_serializer.c`**, including six sweeps
over all 256 values of a single byte.

`MQTT_DeserializeAck` refuses a CONNACK and points the caller at its own
function, and the split is not arbitrary. A CONNACK is the **first** thing a
broker sends and the only packet that sets connection-wide state: the largest
packet the server will accept, how many messages it will take in flight, whether
retain and wildcards and shared subscriptions work at all, and what keep-alive
the client must now use.

Every later size check runs on numbers that arrive in this packet. Getting it
wrong is not one malformed message — it is the whole session running under
limits somebody else chose.

### Five property sweeps, because one could not have worked

The property identifier is one byte, so it is enumerable. But a **single** sweep
cannot do it: each identifier introduces a value of a different *width*, and a
body sized for one is malformed for the others — both arms would refuse for the
wrong reason and the sweep would discriminate nothing.

So it is swept five times, once per value shape, and each prints its own
accepted set:

```
property-sweep one-byte      accepted=24,25,28,29,2a      n=5 rejected=251
property-sweep two-byte      accepted=13,21,22            n=3 rejected=253
property-sweep four-byte     accepted=11,27               n=2 rejected=254
property-sweep string        accepted=12,15,16,1a,1c,1f   n=6 rejected=250
property-sweep user-property accepted=26                  n=1 rejected=255
```

Seventeen identifiers, and the five sets together say **which one is which
type** — the thing a reader actually needs to check MQTT 5.0 §3.2.2.3.

### This time the tables were right, and that is the result

The [acknowledgement deserializers](#the-acknowledgement-deserializers) were
swept the same way and turned up three divergences from MQTT 5.0. These two are
exactly the specification: 22 reason codes (§3.2.2.2), 17 properties
(§3.2.2.3), each of the right width. A sweep that confirms conformance is a
result, and it is asserted from the checked-in trace so that a drift under the
pin fails the suite.

### A refused connection still parses, and that is deliberate

The C has a **third** status here, `MQTTServerRefused`, and it goes on to read
the property section anyway. That is right: the Reason String that says *why*
the broker refused is in those properties, and it is the one thing a refused
client needs.

So a refusal is `Ok` here, carrying the reason code and everything the packet
said; `ConnAck::refused()` is the C's third status. Making it an `Err` would
have looked tidier and thrown away the explanation.

### Absent and zero are different, and the bitmap says which

`fields_present` is the C's `fieldSet`, and it is not decoration. A Maximum QoS
of **0** means the server supports QoS 0 only; an **absent** Maximum QoS means
it supports QoS 2. The value alone cannot tell you, so the bitmap is part of
the answer and the differential compares it.

### Poison-proven on thirteen behaviours, twelve caught

Any flags byte accepted; a resumed session with a refusal; a zero Receive
Maximum; a zero Maximum Packet Size; a non-boolean flag treated as true; the
property section bounded rather than exactly fitted; Response Information
ungated; two properties read at the wrong width; a reason string allowed to
repeat; a field recorded under the wrong bit; and one reason code dropped from
the table.

The thirteenth is a genuine property, and a **third** shape of one this package
keeps meeting. Lowering the three-byte minimum to two changes no answer: two
bytes is what the flags and reason code need, and the third is what the
property-length decoder needs — and that decoder refuses a zero-length buffer
by itself, **in both arms**. The size calculators' in-loop check is load-bearing
in the C and subsumed here; the ack deserializers' property bound likewise;
this one is redundant in the C too. Kept because it states the packet's shape in
one place, and pinned so that stops being free the day it stops being true.

## The incoming PUBLISH

**54 trace lines agree with `core_mqtt_serializer.c`**, with two flag sweeps
and six property sweeps.

This is the last packet a broker can send, and the only one that carries
**application data**. The acknowledgements and the CONNACK are protocol
bookkeeping a library consumes; a PUBLISH is handed onward. Its topic name, its
payload length and its property section are the numbers somebody else's code
will index with, so a wrong length here is not a dropped packet — it is a buffer
overrun one layer up, in code that trusted this one.

It is also the only packet whose **type byte is partly data**: the low nibble
carries DUP, QoS and RETAIN, so the first byte off the socket is four more
inputs rather than a constant.

### QoS moves the body, so the sweeps had to be doubled

QoS decides whether a packet identifier sits between the topic and the
properties — and everything downstream of it moves by two bytes. So the flags
nibble cannot be swept with one body: one carrying a packet id is malformed at
QoS 0, one without is malformed at QoS 1, and a single sweep would refuse half
the nibble for the wrong reason.

```
flags-sweep no-packet-id   accepted=0,1,8,9                 n=4  rejected=12
flags-sweep with-packet-id accepted=0,1,2,3,4,5,8,9,a,b,c,d n=12 rejected=4
```

The four missing from the second are the QoS 3 nibbles, which is the only
combination MQTT forbids. The same rule gives the properties six sweeps — one
per value shape, including the **variable-length integer** that no other packet
carries.

### The payload length is a four-term subtraction

Remaining length, less the topic and its two length bytes, less the property
section and the bytes that encode its length, less the packet identifier when
there is one. The C does that on a `uint32_t`; a wrap would hand the application
a payload length near four billion pointing into a packet of a few bytes, which
is the worst failure this module could have.

Three growing remaining-length checks are what make it unreachable — and a
standing test asserts something stronger than "the payload is not too big",
because a payload silently emptied satisfies that. It asserts an **identity**:
the two topic-length bytes, the topic, the packet id, the encoded property
length, the properties and the payload add up to the remaining length, over a
space that includes claimed lengths larger than the buffer.

### Two protocol errors MQTT 5.0 names and coreMQTT does not enforce

1. **A zero-length topic name with no Topic Alias.** §3.3.2.3.4 calls it a
   protocol error; the C never links the two, so the application is handed a
   message with no topic and no alias with which to resolve one.
2. **A Subscription Identifier of zero.** §3.3.2.3.8 calls it a protocol error;
   the property's value is never checked, though its two neighbours' are.

Both are transcribed exactly — the C is the oracle, not the specification — and
both are asserted from the checked-in trace so a drift under the pin fails the
suite. They are written up in `kairos-upstream/drafts/` for filing.

The second shows itself in the sweep, which is the argument for printing
accepted sets rather than hashing them: `property-sweep one-byte
accepted=01,0b` lists `0x0B` because the one-byte value swept is `0x00` — the
line says in passing that a zero Subscription Identifier is accepted.

### Poison-proven on fifteen behaviours, twelve caught

QoS 3 accepted; DUP and RETAIN swapped; a packet id read at QoS 0; a zero packet
id; two of the payload subtraction's four terms dropped; a Payload Format
Indicator above 1; a zero Topic Alias; the Topic Alias maximum ignored; a
repeated content type; an exact property fit demanded where a floor is right;
and the PUBLISH type matched as a whole byte rather than a nibble.

**The three that did not fire are one finding.** They are the three
`checkPublishRemainingLength` calls, and each is the same predicate as the slice
that follows it — the C needs them because it indexes with a length it was
handed, and every read here goes through a bounded slice instead. Kept for
fidelity, and the equivalence is pinned. Fourth appearance of a family this
package keeps meeting, and the first where three checks collapse into one
reason.

## The DISCONNECT, both directions

**58 trace lines agree with `core_mqtt_serializer.c`**, with two reason-code
sweeps and ten property sweeps.

### This slice exists because the last one's claim was wrong

After the PUBLISH slice this README said the crate could read every packet a
broker can send. **It could not.** MQTT 5 made the DISCONNECT bidirectional — a
broker sends one to say why it is closing the socket — and
`MQTT_DeserializeDisconnect` was not covered. The claim is recorded here rather
than quietly corrected, because a README that overstates once will be read
sceptically forever.

### One table, two directions, different answers

`validateDisconnectResponse` takes an `incoming` flag, and the same byte means
different things depending on it:

```
reason-sweep outgoing accepted=00,04,80,81,82,83,90,93,94,95,96,97,98,99  n=14
reason-sweep incoming accepted=00,80,81,82,83,87,89,8b,8c,8d,8e,8f,90,93,
                               94,95,96,97,98,99,9a,9b,9c,9d,9e,a0,a1,a2  n=28
```

`0x04` — "disconnecting, send my Will" — is a client's to send and is refused
coming in. Fifteen server codes are refused going out. Neither set contains the
other, so the reason code is swept 256 times in **each** direction.

The property tables differ too: an incoming DISCONNECT may carry a **server
reference**, an outgoing one a **session expiry interval**, and neither may
carry the other's.

### A divergence from MQTT 5.0, found by comparing the two sets

The outgoing set is exactly §3.14.2.1's client column. The incoming set is the
server column **minus `0x9F`, "connection rate exceeded"** — which the
specification lists and coreMQTT's switch does not. A client that receives it
answers `MQTTBadResponse`, treating a conformant disconnection as a malformed
packet.

Transcribed exactly, asserted from the checked-in trace, and written up in
`kairos-upstream/drafts/` for filing. It is the second reason-code table in this
library to be one entry short of the specification; the
[UNSUBACK's](#the-acknowledgement-deserializers) was the first.

### A bare DISCONNECT is right by coincidence

With no reason code and no properties the C still charges a byte for the encoded
property length, so it emits `E0 01 00` where §3.14.2.1 allows `E0 00`. Both are
legal — but the byte the writer intends as a *property length* is read by a
broker as the *reason code*, and they agree only because a property length may
be non-zero only when a reason code is present, which forces it to `0x00`.

Two sides agreeing on the bytes for different reasons is the fragile kind of
agreement, so it has its own test.

### Poison-proven on fifteen behaviours, fourteen caught

The direction flag ignored; server codes accepted both ways; **the missing
`0x9F` added**; an empty body read as a reason code; properties decoded from a
one-byte body; the exact property fit loosened; a session expiry accepted from a
server; a repeated reason string allowed; a server reference accepted on the way
out; the session-expiry rule ignored; properties without a reason code; the
reason-code byte and the property-length bytes dropped from the size; and the
property length written before the reason code.

Two needed cases adding, and both for the same reason: the case meant to catch
them was malformed a **second** way, so it was refused before the check under
test could matter. The fifteenth is a genuine property — the up-front buffer
check is the same predicate as the three slices that follow it, because the C
has pointer writes where this has slices. Fifth appearance of that family, and
the first on the writing side.

## The CONNECT

**30 trace lines agree with `core_mqtt_serializer.c`**, byte for byte, plus a
32-combination sweep of the optional fields.

This is the packet that **starts** a session, and the largest thing a client
assembles: a ten-byte variable header, a property section, a client identifier,
optionally a will (its own property section, a topic and a payload) and
optionally a user name and a password. Eight length-prefixed fields, four of
them optional.

### The flags byte and the payload are one claim in two places

Three bits of the CONNECT's flags byte say whether a will, a user name and a
password are present; the payload must then carry exactly those, in that order.
[The writers](#the-fixed-header-writers) swept that flags byte exhaustively and
proved every bit — and could prove nothing about whether the payload then
matches it.

So the two are proven **together**: a 32-combination sweep over the four
optional inputs, compared by an FNV-1a digest of the **whole serialized
packet**. A field written when its bit is clear moves the bytes, and a digest
over the whole packet is what sees it.

The ordering poisons are the point. Every field is length-prefixed, so swapping
the will topic with the will payload, or the user name with the password, still
*parses* — it just publishes the will to the wrong topic and sends the password
as the user name. Nothing but a byte-for-byte comparison catches that, and both
swaps are in the poison set.

### Absent, empty and present are three states

The C distinguishes a NULL `pUserName` from a non-NULL one of length zero: the
first clears a flag bit and writes nothing, the second sets the bit and writes
two zero bytes. `Option<&[u8]>` models exactly that, and it is the reason
`ConnectInfo` uses it. The trace prints `-` for absent and `.` for
present-and-empty, because a trace that showed both as "nothing" could not tell
them apart.

The *property* sections are different again — the C treats a NULL builder and an
empty one identically — so they are plain slices, and nothing is lost.

### A client identifier may not begin with NUL

The C has one expression meant to catch a length/pointer mismatch:

```c
( pConnectInfo->clientIdentifierLength == 0U ) !=
    ( ( pConnectInfo->pClientIdentifier == NULL ) ||
      ( *( pConnectInfo->pClientIdentifier ) == '\0' ) )
```

Its NULL half cannot happen here — a `&[u8]` carries its own length. Its other
half can, and it means an identifier whose **first** byte is zero is refused
while one with a zero anywhere else is accepted. MQTT 5.0 §1.5.4 forbids U+0000
anywhere in a UTF-8 string, so refusing is defensible; the C refuses only the
first byte, and incidentally. Transcribed, and pinned.

### Poison-proven on thirteen behaviours, twelve caught

The NUL rule; a nine-byte header; a field's two length bytes; each property
section's encoded length; the will counted when absent; **four different
orderings**; an absent user name written as empty; and the three 16-bit field
limits.

Those last three needed cases built for them. Every field in the table is a
handful of bytes and the limit is 65,535, so **all five 16-bit checks were
unreachable** — a poison on any of them passed. Seven cases now put one field at
a time at 65,535 and at 65,536, and print the status and the sizes rather than
131 KB of hex.

The thirteenth is the buffer check, subsumed by the bounded writes below it —
the sixth appearance of that family and the second on the writing side.

### One check the differential structurally cannot reach

`MQTT_GetConnectPacketSize` refuses a total past 268,435,455, and no combination
of fields gets within four orders of magnitude. The property section can reach
it in the C — the function reads the builder's `currentIndex` and never touches
its buffer, so a caller can claim 268 million bytes while pointing at eight, and
the driver did exactly that before the cases were withdrawn.

**A `&[u8]` cannot make that claim.** Its length is its data, so the entire class
of input that makes the check load-bearing does not exist on this side. What is
left is a caller genuinely holding 268 MB of property bytes — a 64-bit host's
problem, not a microcontroller's. The check stays, and a test pins its operator,
which is where it differs from the DISCONNECT's calculator one function away.

## The outgoing PUBLISH

**25 trace lines agree with `core_mqtt_serializer.c`**, byte for byte, across
**three** serializers.

coreMQTT gives an outgoing PUBLISH three entry points rather than one, because a
payload can be large and a microcontroller would rather not copy it:

* `serialize_publish` writes the whole packet, payload copied in;
* `serialize_publish_header` writes everything **but** the payload and reports
  how far it got, so the caller sends the payload from wherever it already
  lives;
* `serialize_publish_header_without_topic` writes less still — the type byte,
  the remaining length and the topic's two **length** bytes.

### The three must agree as prefixes, and that is in the trace

Each case runs all three and prints all three results, so the short one being a
prefix of the middle one, and the middle of the long one, is something the
checked-in trace shows rather than something asserted on our side alone. A
caller on the vectored path is relying on exactly that, and three separate
differentials could each pass while the relationship broke.

### They validate differently, and that is the C's choice

| | topic required | packet id at QoS > 0 | DUP at QoS 0 |
|---|---|---|---|
| `serialize_publish` | yes | yes | refused |
| `serialize_publish_header` | yes | yes | refused |
| `serialize_publish_header_without_topic` | **no** | **no** | **allowed** |

The loose one validates almost nothing, and it is the one reached for when
performance matters. Transcribed, not tidied — and one of the poisons is making
it strict.

### The reading and writing halves disagree about an empty topic

`publish` accepts a zero-length topic name with no Topic Alias, which is
[a divergence from MQTT 5.0](#the-incoming-publish) the C has. This half refuses
the same packet outright. One library, two directions, two answers — the C's,
and now recorded by a test that fails if either side changes its mind.

### A `debug_assert` of mine was wrong, and a case refuted it

`MQTT_SerializePublishHeader` reports the size it **computed** from the remaining
length it was handed, not the bytes it wrote. I wrote that down and added
`debug_assert!(written <= header_size)` alongside it, reasoning that an inflated
remaining length would only ever over-report.

It differs in *both* directions:

```
remaining length 16 for an 11-byte packet -> reports 13, wrote 8
remaining length  8 for an 11-byte packet -> reports  5, wrote 8
```

No case broke the C's stated API contract — "call the size function first" — so
nothing had ever exercised it. Four cases now do, and they refuted the
assumption on the first run. The assertion is gone; nothing is claimed about the
relationship, because the C claims nothing.

### Poison-proven on sixteen behaviours, all caught

The QoS bits swapped; DUP and RETAIN swapped; a packet id written at QoS 0; a
little-endian packet id; the properties written before the packet id; the
payload copied in the header-only serializer; three terms dropped from the size;
the three strict validations removed; the loose serializer made strict; the
header size reported as the bytes written; and the DUP patch on the wrong bit
and on any byte.

Four needed the broken-contract cases before they would fire.

## SUBSCRIBE, UNSUBSCRIBE, the acknowledgements and PINGREQ

**45 trace lines agree with `core_mqtt_serializer.c`**, byte for byte. With the
CONNECT, the outgoing PUBLISH and the DISCONNECT, this completes the outgoing
wire codec: **every packet an MQTT client can put on a socket**.

### The library validates ack reason codes twice, and disagrees with itself

`validateReasonCodeForAck` checks an **outgoing** acknowledgement's reason code
**per packet type**:

```
ack-reason-sweep puback  accepted=00,10,80,83,87,90,91,97,99  n=9
ack-reason-sweep pubrec  accepted=00,10,80,83,87,90,91,97,99  n=9
ack-reason-sweep pubrel  accepted=00,92                       n=2
ack-reason-sweep pubcomp accepted=00,92                       n=2
```

That is exactly MQTT 5.0 §3.4.2.1, §3.5.2.1, §3.6.2.1 and §3.7.2.1. The
[reading side](#the-acknowledgement-deserializers) checks all four against **one
shared table of ten**, so coreMQTT refuses to *send* a PUBACK carrying `0x92`
and accepts one on the way in.

This is the sharpest evidence for the upstream report already filed on that
reading-side table: **the library contains the correct table, three thousand
lines from the incorrect one.** Both are transcribed, and a test asserts the
disagreement from both directions.

### The subscription options byte: five decisions, six bits

SUBSCRIBE carries one per topic filter, packing QoS (two bits), no-local,
retain-as-published and retain handling (two more, three legal values). One
wrong bit subscribes at the wrong QoS, or asks for retained messages that never
come, and the packet looks perfectly well formed.

All 36 combinations are swept and every byte printed. A test asserts they are
**distinct** — two combinations producing one byte would mean a decision is
being lost — and that no reserved bit is ever set.

### A too-small buffer gets two different statuses

`MQTT_SerializeAck` answers `MQTTNoMemory` for a buffer under four bytes, and
`serializeAckBody` answers **`MQTTBadParameter`** for one that is four bytes but
cannot hold a reason code. Two statuses for one condition, a hundred lines
apart. Transcribed as each has it, with a case for each.

### Poison-proven on sixteen behaviours, all caught

The QoS bits swapped; no-local and retain-as-published swapped; the two retain
handling values swapped; the options byte written before its filter; an options
byte added to UNSUBSCRIBE; an empty list, a zero packet id and an empty filter
allowed; the two checks reordered; **the ack tables merged into one**; a PUBACK
allowed to carry `0x92`; the property-length byte dropped from a bare-reason
ack; a wrong remaining length; the buffer statuses unified; a zero ack packet
id; and a PINGREQ with the wrong type byte.

## The transport reader

**20 trace lines agree with `core_mqtt_serializer.c`** — and they compare the
**call sequence**, not just the answer.

Every other function in this crate is handed a buffer. This one is handed a
**callback** and pulls the type byte and then the variable-byte remaining length
off a socket, one byte at a time. So what it asked for, and how many times, is as
much of the behaviour as what it returned — the shape `rusty_rtos_sntp`'s client
differential established.

```
case 1  puback         script=40,02      -> Success calls=2 read=40,02 type=40 rl=2
case 7  type-connect   script=10,00      -> BadResponse calls=1 read=10
case 12 nothing-at-the-type-byte script=none -> NoDataAvailable calls=1 read=none
case 14 nothing-at-the-length-byte script=30,none -> BadResponse calls=2 read=30,none
```

### Two orderings that only the call count can show

**The type is checked before the length is read.** A packet type a client may
not receive is refused after **one** call rather than after the whole header has
been drained. The status is `BadResponse` either way, so nothing but the count
distinguishes a reader that stops at the first byte of a malformed stream from
one that keeps pulling.

**The multiplier guard runs before the read, not after.** A fifth continuation
byte is refused *without asking the transport for it*. Again invisible in the
status, and again one more byte handed to a hostile peer on every malformed
packet if it were the other way round.

Both are poisons, and both are caught by the log.

### Two statuses the library uses nowhere else

A transport can answer with a byte, with nothing yet, or with an error, and the C
distinguishes the last two — `MQTTNoDataAvailable` and `MQTTRecvFailed` — **only
at the type byte**. One byte in, both become `MQTTBadResponse`, because a header
that started must finish. A distinction the C draws once and then drops, with
cases for each side of it.

### Poison-proven on seven behaviours, six caught

The type checked after the length; the two failures distinguished everywhere;
the multiplier guard moved after the read; the non-minimal check dropped; the
type byte dropped from the header length; and the two statuses swapped.

The seventh is the out-of-range check, which cannot fire — the multiplier guard
bounds the value to exactly one less than the constant it tests against. Third
instance of that same arithmetic in this package, and pinned the same way.

### One number the C does not produce

`MQTT_GetIncomingPacketTypeAndLength` sets `remainingLength` and leaves
`headerLength` exactly as the caller left it, so a C caller has to count the
bytes itself. We counted them anyway, so `IncomingHeader::header_length` is free
— an **addition**, not a transcription. It is not in the trace, because the C
has nothing to compare it against, and a unit test pins it instead.

## The outgoing property validators

**94 trace lines agree with `core_mqtt_serializer.c`** — 56 named cases and
**36 sweeps**, and the sweeps between them are the whole map of which property
may go in which outgoing packet.

MQTT 5 lets almost every packet carry properties, and a different set for each.
coreMQTT enforces that with six hand-written tables, each a switch on one byte.
Six tables is what this package has learned to sweep, so each is swept over all
256 identifiers at each of six value shapes and the accepted set is **printed**:

```
sweep connect one-byte accepted=17,19   two-byte accepted=21,22  four-byte accepted=11,27
sweep will    one-byte accepted=01      four-byte accepted=02,18 string accepted=03,08,09
sweep publish one-byte accepted=01      two-byte accepted=23     four-byte accepted=02
sweep puback  string   accepted=1f      user-property accepted=26
sweep unsubscribe user-property accepted=26      (n=0 at every other shape)
```

Printing beats hashing wherever the accepted set is small: a hash tells you a
table changed, and a printed set tells you which identifier moved.

### Three places the writing side is laxer than the reading side

All three are in the PUBLISH table, and in each one the **same library** refuses
the same thing coming in:

1. **A Topic Alias of zero** passes, where the incoming PUBLISH deserializer
   refuses it and §3.3.2.3.4 forbids sending one.
2. **A Payload Format Indicator above 1** passes, where the will validator —
   which carries five of the same seven identifiers — refuses it, and so does
   the incoming deserializer (§3.3.2.3.2).
3. **It deduplicates nothing but the Topic Alias.** Its `used` flag is declared
   *inside* the property loop, so the flag is false at every property and no
   repeat is ever seen. The Topic Alias escapes because its own flag is declared
   *outside* the loop, and the acknowledgement validator — the same loop, one
   brace apart — dedupes correctly.

Transcribed as they stand, pinned from both directions, and written up in
`docs/upstream/`.

### A sweep says what a table ACCEPTS; only reading the C says what it REMEMBERS

The driver was written from the sweeps and produced 49 cases, all of which
passed. Transcribing the C afterwards added seven more, and **three of the nine
poisons are caught only by those seven** — measured by rerunning them against
the 49-case trace, not argued:

| poison | 49 cases | 56 cases |
|---|---|---|
| a malformed subscription id given the table's own status | missed | **caught** |
| the topic alias bound made inclusive | missed | **caught** |
| the acknowledgement table stops deduplicating | missed | **caught** |

Each is about something that spans two properties or two checks — a flag that
survives an iteration, a boundary, a status passed through from a shared
decoder. A sweep sends **one property at a time at one value**, so it can see
none of them. The sweep is still what found the three defects above; the two
instruments answer different questions.

### One identifier no sweep can see

The CONNECT table accepts nine properties and its sweeps report eight.
Authentication data (`0x16`) is refused at every shape, because [MQTT-3.1.2-32]
makes it a protocol error without an authentication method — and the C checks
that **after** the property walk, so either order is fine and neither is visible
one property at a time. Three paired cases carry it.

### Poison-proven on nine behaviours, nine caught

The publish table made to dedupe, the topic alias written after its bound is
checked, Request Problem Information left at the caller's value, a malformed
subscription id given the table's status instead of the decoder's, the alias
bound made inclusive, the zero subscription id accepted, [MQTT-3.1.2-32] checked
in the arm instead of after the walk, the acknowledgement table's flag moved
inside its loop, and a zero Receive Maximum accepted.

## The connection context, and the last of the serializer

**56 trace lines agree with `core_mqtt_serializer.c`** — and with this slice
**every function in that file but its two logging ones** is remade.

What was left was the part that is not a codec: the two constructors, the helper
that fills a connection context from a CONNECT's properties, and the parameter
validator an outgoing PUBLISH goes through — 230 lines. The differential also
drives the file's **second reader of the fixed header**, which the fixed-header
slice had already remade, because the interesting thing about it is how it
compares with the callback-driven one.

### The instrument: one job, done twice

Two of the four are second copies of something already diffed against the C, so
the differential runs **both arms over the same input and prints both answers**:

```
dual 1 puback         avail=3 bytes=400200 -> process=Success type=40 rl=2 hl=2 | get=Success type=40 rl=2 calls=2
dual 6 type-byte-only avail=1 bytes=30     -> process=NeedMoreBytes            | get=BadResponse calls=2
dual-sweep      ... n=168 refused=88 differ=0
truncated-sweep differ=168
```

The first sweep says the two readers agree on every whole header. The second
says they disagree on **every truncated one** — all 168 packet types a client
may receive. A correct arm is the best instrument for finding a wrong one, and
here it found three things:

1. **Only the buffered reader can say "not yet".** Given a type byte and no
   length behind it, it answers `MQTTNeedMoreBytes`; the callback-driven one
   answers `MQTTBadResponse` — and the doxygen for that one shows a
   **non-blocking** loop ending in `assert( status == MQTTSuccess )`. A TCP
   segment boundary between byte 1 and byte 2 of a header is ordinary, and it
   closes a connection carrying a well-formed packet. Running the two side by
   side is also what showed that this slice had written a **second Rust copy**
   of the buffered reader, which slice 2 had already remade; it was deleted and
   the line count corrected.
2. **`updateContextWithConnectProps` stores what the validator refuses** — a
   Receive Maximum of zero, a Maximum Packet Size of zero, a Request Problem
   Information of 2, authentication data with no method. The second of those
   makes nine functions in the library answer `MQTTBadParameter` for ever,
   including three deserializers, so the session is inert in **both**
   directions. It is public, documented with a worked example, and callable
   without the validator.
3. **`MQTT_ValidatePublishParams` compares QoS against zero, not against the
   maximum**, so a broker that announced Maximum QoS 1 is sent QoS 2.

All three are drafted in `docs/upstream/`.

### What a slice does when a refusal cannot cross the language boundary

Three of the C's refusals have no reachable equivalent here, and the honest
thing is to say which rather than to fake them:

- `MQTTPropertyBuilder_Init` takes a **pointer and a length** and never checks
  them against each other, so an eight-byte buffer with a length of a million is
  accepted. `PropertyBuilder::new` takes one slice. Its null-buffer refusal is
  unreachable, and its length bound needs a 256 MB buffer to reach — so neither
  is in the trace, and the bound is pinned by a unit test on the arithmetic.
- `MQTT_ValidatePublishParams` refuses a null topic name with a non-zero length,
  which is the same two-numbers-must-agree shape, and the same resolution.

**A trace should ask only what both arms can answer.** The alternative — printing
a sentinel in both columns — is a constant compared with itself.

### Poison-proven on thirteen behaviours, thirteen caught

The buffered reader made unable to say "not yet", its two empty-buffer statuses
swapped, its type checked after its length, its header length missing the type
byte, its non-minimal check dropped; the context filler made to validate, made
to deduplicate all nine, and given the wrong status for a foreign identifier; a
fresh context zeroed; the builder made to accept an empty buffer; and the three
checks in the parameter validator, one of them corrected to what the
specification says.

## The MQTT 5 property builders

**73 trace lines agree with `core_mqtt_prop_serializer.c` — except for three,
and those three are the finding.**

This is where an application assembles a property section one property at a
time: five primitives, eighteen adders over them, and a 199-line table saying
which property may go in which packet.

### A buffer overflow, found by being unable to reproduce it

Every slice before this one transcribed the C exactly, divergences from MQTT 5.0
included, because the C is the oracle. **This one cannot.**

`addPropUint8`, `addPropUint16` and `addPropUint32` each check for the
identifier byte plus the value. `addPropUtf8` checks for the two length bytes
plus the body and **forgets the identifier**, then writes it. Given a buffer
exactly one byte too small it answers `MQTTSuccess` and writes one byte past the
end:

```
add 38 utf8-in-four-bytes cap=4 | content-type(-,-)->Success index=5 ... OVERFLOW
```

Six public adders route through it — authentication method and data, response
topic, correlation data, content type and reason string. This crate writes
through a `&mut [u8]` under `forbid(unsafe)`, so it answers `NoMemory` and
writes nothing. The differential's rule becomes: **every line matches, except
that every line the C marked `OVERFLOW` must be one where we refused, and there
may be no other difference** — an exception that is itself bounded by a test.

Drafted in `docs/upstream/`.

### Two poisons that could not fire, and the property they proved

Sizing a four-byte property as 4 instead of 5, and sizing a string property the
way `addPropUtf8` does, **changed no answer at all**. They cannot: the size
check is advisory here and the slice bound is the guarantee. A C builder has one
bound, and one byte of arithmetic error in it is a one-byte out-of-bounds write.

That property is now pinned by a sweep of every adder against every buffer size
from 1 to 23, asserting the two things that hold whatever the arithmetic says:
the cursor never passes the buffer, and a refusal writes nothing.

### A fourth copy of which property may go in which packet

`isValidPropertyInPacketType` is `static`, so it can only be asked through the
eighteen adders — which is what the differential does, for all 256 packet-type
bytes, printing the accepted set per type:

```
allowed publish     type=30 props=payload-format,message-expiry,topic-alias,response-topic,
                                  correlation-data,content-type,subscription-id,user-prop
allowed subscribe   type=82 props=subscription-id,user-prop
allowed unsubscribe type=a2 props=user-prop
allowed pingreq     type=c0 props=-
```

It disagrees with the validators' tables in one place: **a PUBLISH is allowed a
Subscription Identifier**, which [MQTT-3.3.4-6] forbids a client to send and
which `validate_publish_properties` refuses. The C's own comment beside that arm
says "only in server-to-client PUBLISH" and the next line sets the bit.

### And the cross-slice check that matters most

A section this crate **builds** is a section this crate **validates**: the bytes
go straight from the builder into the validator that decides whether they may be
sent. Two arms that each agree with the C can still disagree with each other.

### Poison-proven on fourteen behaviours, twelve caught

The two that could not fire are the pair above, and they became a test.

## The MQTT 5 property reader

**55 trace lines agree with `core_mqtt_prop_deserializer.c`** — the other half
of the builder, and the file that walks a property section back.

A reader has a problem a writer does not: it does not know what is in there. So
the shape is a **cursor** — ask what is next, take it if you want it, skip it if
you do not, stop when the section runs out. Every one of the twenty-two getters
demands **one** identifier and refuses every other, which makes a getter an
assertion about what is under the cursor rather than a search.

```
getter max-qos      accepted=24 n=1
getter receive-max  accepted=21 n=1
getter reason-string accepted=1f n=1
```

Twenty-two getters against 256 identifiers: each accepts exactly one. That is a
result, and `the_two_tables_agree_and_the_trace_records_it` asserts it.

### Two tables over one alphabet — and this time they agree

`MQTT_GetNextPropertyType` says whether a byte is a property identifier at all.
`MQTT_SkipNextProperty` maps the same byte to a **width**. They are written
separately in the C, and a byte the first accepts and the second cannot skip
would strand a reader half way through a section.

```
known     accepted=01,02,03,08,09,0b,11,12,13,15,16,17,18,19,1a,1c,1f,21,22,23,24,25,26,27,28,29,2a n=27
skippable accepted=01,02,03,08,09,0b,11,12,13,15,16,17,18,19,1a,1c,1f,21,22,23,24,25,26,27,28,29,2a n=27
tables differ=0
```

Identical. After five tables in this library that disagreed with a sibling, a
differential that only ever reported disagreement would be one nobody believed
when it reported agreement — so the agreement is asserted too. In this crate one
table serves both callers, so they cannot drift.

### The cursor is the answer

Every call advances a caller-owned index, so a getter that returned the right
value and left the cursor one byte out would pass a value-only comparison and
desynchronise everything after it. Every line prints the index, and the cases
that read **twice** are what make that index load-bearing — the second read
demands a particular identifier and finds one only if the first landed exactly
on it.

### Poison-proven on ten behaviours, eight caught

The two that could not fire are the same property as the builder's, third
instance in the package: **the budget is advisory, and the slice is the
guarantee.** A budget one byte too generous, and a cursor allowed to sit exactly
on the end, both change no answer, because the read that follows is bounded by
the slice as well as by the arithmetic. Pinned by a sweep of every getter
against every truncation of a section, from every starting position.

## Topic matching

**109 trace lines agree with `core_mqtt.c`** — the first slice of the connection
machine, and the part of it that needs no connection.

A client that subscribes to `sport/+` and then receives a PUBLISH for
`sport/tennis` has to decide which callback the message belongs to. That
decision is a **string** algorithm, so the sweep is the usual one a dimension
up: every string over `{a, /, +}` to length three — 39 of them — as a 39 × 39
grid of which filter matches which topic, printed into the trace.

```
grid-strings a / + aa a/ a+ /a // /+ +a +/ ++ aaa aa/ aa+ a/a a// a/+ ...
grid-row a/+ ....1..........1.1.....................
grid-row +/+ ......1.1.1....1.1...1.1.........1.1...
```

Then `{a, b, /, +, #, $}` to length four: 2,414,916 pairs, digested.

### A divergence from MQTT 5.0, visible as a column

Row `a/+` has a `1` in the `a/` column. Row `+/+` has a `.` in it.

**A filter whose last level is `+` stops matching a topic whose last level is
empty, once an earlier `+` has been used.** The specification's own example —
`sport/+` against `sport/` — works, which is what makes this a defect rather
than a design:

```
a/    against  a/+     matches
a/    against  +/+     does NOT match
/     against  +/+     does NOT match
a//   against  a/+/+   does NOT match
```

A client subscribed to `+/+` silently never receives messages published to `a/`.
No error is raised anywhere. A grid is to a string algorithm what a printed
accepted set is to a table — this was a **pattern**, not a case anyone had to
think to write. Drafted in `docs/upstream/`, along with the missing
`MQTTEndOfProperties` entry in `MQTT_Status_strerror`.

### One thing the sweep caught in our own arm

`MQTT_GetPacketTypeString` matches **PUBLISH on its nibble** — the low four bits
carry QoS, DUP and RETAIN — and everything else on the **whole byte**, because a
PUBREL's reserved bit must be set and `0x60` is a malformed packet rather than a
PUBREL. The first transcription used the whole byte for all of them, which is
the tidier-looking rule, and the 256-value digest caught it immediately.

### Poison-proven on thirteen behaviours, twelve caught

The one that could not fire is the `strncmp` fast path: deleting it changes no
answer, because the general walk matches every string against itself unaided.
Pinned over all 780 strings up to length four.

## The client context

**41 trace lines agree with `core_mqtt.c`, except one** — the constructor, the
packet-identifier allocator, and the validators an outgoing SUBSCRIBE,
UNSUBSCRIBE or PUBLISH goes through. `MQTT_Subscribe` validates *before* it
looks at the connection status, so an unconnected context separates the two
answers cleanly: a malformed list gives `BadParameter` and a well-formed one
gives `StatusNotConnected`.

### Two defects, and both are about a list

**Only the last entry in a subscription list decides whether the list is
valid.** `validateSubscribeUnsubscribeParams` ends in a loop that **assigns**
its status and never breaks, so every earlier entry's verdict is written over:

```
sub bad-then-good shapes=empty:0:0:0,plain:0:0:0 -> StatusNotConnected
sub good-then-bad shapes=plain:0:0:0,empty:0:0:0 -> BadParameter
sub empty-filter  shapes=empty:0:0:0             -> BadParameter
```

The same two entries, in both orders, with opposite answers. The loop three
lines above it, over the same list, *does* break — which is what makes this a
slip rather than a decision. Everything the per-entry validator checks is lost
this way, including every shared-subscription rule, so the client goes on to
build and send a SUBSCRIBE carrying `$share//a/b`.

**And a topic filter is searched past its length.**
`checkWildcardSubscriptions` reaches for the filter with `strchr`, which runs to
a NUL — while `MQTTSubscribeInfo_t` carries a pointer *and* a length, and every
other function that touches a filter uses the length. A filter of `"abc"` with
length 3, inside a buffer reading `"abc#"`, is refused as containing a wildcard.

That is the second thing in this library the Rust arm **cannot reproduce**: a
`&[u8]` has no bytes past its length. One trace line is a bounded exception,
checked by a test, exactly as in the property builder.

### What a trace should not ask

Five of the C's refusals here have no Rust counterpart, and they are not in the
trace rather than failing in it:

- `MQTT_Init`'s five null-pointer checks — the transport, clock and callback are
  supplied where they are used.
- `MQTT_InitStatefulQoS`'s four pointer-versus-count checks — slices carry both.
- a QoS of 3 and a retain-handling option of 3, which `MQTTQoS_t` can hold and
  `QoS` cannot. **This is the type refusing before any validator runs**, which
  is the same family one level up.

### Poison-proven on fifteen behaviours, fifteen caught

Two needed cases before they would fire, and both gaps had real behaviour behind
them: an UNSUBSCRIBE ignores every subscription option (so a shared subscription
a SUBSCRIBE refuses goes through unexamined), and a filter of exactly `$share/`
is **not** a shared subscription, because the C tests `length > 7` before
comparing seven bytes.

## The send plumbing

**28 trace lines agree with `core_mqtt.c`** — and this slice builds the
instrument the rest of that file needs: a **scripted transport** whose every
call is logged, and a **scripted clock**.

A transport may take all the bytes it is offered, some of them, none, or fail,
and each answer puts the sender round its loop again with a different offset. So
what it is **offered on every call** is as much of the behaviour as how many
bytes arrive:

```
log=2:1,1:1     two bytes in two calls, the second offer correctly advanced
log=2:1,2:1     the same two bytes, with the first one sent twice
```

Both put two bytes on the wire. Only one is right, and only the log tells them
apart. Same shape as the transport reader, pointed the other way.

### A wrong clock does not answer wrongly — it never answers

`calculateElapsedTime` is `later - start` on two `uint32_t`, and it is right
across the 32-bit wrap **because** both sides are unsigned. Replacing that with
a saturating subtraction — which looks like care — makes a client that has been
up for 49.7 days report zero elapsed for ever, so a send against a busy
transport never returns.

That poison is caught in the strongest possible sense and the differential
cannot see it, because a hang is not a failing assertion. It is pinned by a unit
test on the arithmetic instead, and the trace drives the clock from zero, from
one step below the wrap, and from the wrap itself, and requires all three
answers to be identical.

### Poison-proven on twelve behaviours, twelve caught — two by hanging

Three needed cases before they would fire, and all three gaps had real behaviour
behind them: a clock that actually moves (or the recorded transmit time is
always zero), a step that lands elapsed time **exactly** on the timeout (or
`>=` and `>` agree), and a context whose transport has already failed — because
`MQTT_Disconnect` refuses only `NotConnected`, and lets a dying connection try
to send the one packet it might still manage.

## The outgoing packets

**59 trace lines agree with `core_mqtt.c`** — SUBSCRIBE, UNSUBSCRIBE and
PUBLISH, built **without copying** the caller's topic filters or payload, and
handed to the transport as a scatter list.

### One packet, several gathers, and the asymmetry nobody would guess

A list of filters is sent out of a fixed array of four vectors. A SUBSCRIBE
spends three of them on a topic — length, filter, options byte — and an
UNSUBSCRIBE spends two, and the guard is `used <= 4 - per_topic` with the
header's own vectors already counted. So:

```
sub   one-filter   v2/5:5, v3/6:6      the header alone, THEN the filter
unsub one-filter   v4/10:10            the header AND the filter, one gather
```

Same packet either way. The gathers are split, not the stream — and with a
transport that takes one vector at a time, the two are **indistinguishable**.
The only instrument that can see a gather boundary is `writev`, which is handed
the outstanding vectors *and their count*. That is why this slice implements it:
three deliberate breakages of the gather arithmetic changed no line of the trace
until `writev` was there to watch, and the fourth is dead at the shipped
`MQTT_SUB_UNSUB_MAX_VECTORS` of four for a reason a unit test now states.

### The copy that is stored is not the copy that is sent

A QoS 1 or 2 PUBLISH is handed to the retransmit store **before** it goes out,
and the header it is handed has the DUP flag **raised** — because what comes
back out of a store is a list of `const` pointers that cannot be patched later.
The C raises the bit, calls the store, and lowers it again:

```
pub qos1-stored   bytes=320d…   stored=3a0d…
```

One bit apart, on purpose, and a test reads both out of the same line and checks
the relationship rather than two recorded strings.

### Two defects in the send plumbing, found by reading it properly

`sendMessageVector` and `sendBuffer` are thirty lines apart and do not agree:
the first compares elapsed time with `>` and reads the clock only when it will
loop again, the second compares with `>=` and reads it every turn including
after a failure. This crate had given both the second shape. Two new trace cases
— one that lands elapsed time exactly on the timeout in the *vector* sender, and
a `reads=` column — now separate them, and four poisons hold them apart.

## Opening a connection

**39 trace lines agree with `core_mqtt.c`** — `MQTT_Connect`, the CONNACK it
waits for, and what a resumed session owes the broker. This is the first
function in the library that both **sends and receives**, so both directions are
scripted and both are logged.

### Five bytes the application never asked for

A CONNECT with no properties of its own does not go out with an empty property
section. coreMQTT builds one containing a single Maximum Packet Size set to the
**size of the network buffer**, on the grounds that otherwise a server could
send a packet the library cannot read:

```
101400044d515454 05 02 003c  05 27 00000100  0002 6331
                             ^^^^^^^^^^^^^^  the caller supplied none of this
```

### Two timeouts, and only one of them resets

The CONNACK's header is read one byte at a time by a loop that gives up either
on a **clock** or on a **retry count** — whichever, depends on whether the
caller passed a non-zero timeout. The body is read by `recvExact`, whose own
ten-millisecond timeout **restarts every time a byte arrives**, so it bounds the
gap between bytes and not the packet. `dribbled-body` delivers one byte every
nine milliseconds and never times out; `dribbled-on-the-boundary` moves the
clock one millisecond more and fails on the first gap.

### A clean session leaks every stored PUBREL

`handleCleanSession` clears the stored PUBLISHes, zeroes the outgoing record
array, and *then* asks `MQTT_PubrelToResend` — which reads that array — what
PUBRELs to clear. The answer is always none.
`clean-pubrel-only-with-store` has a store, a PUBREL in flight and `clear=0`;
the resumed case beneath it re-sends exactly that record, which is what makes
the first a defect rather than an empty array. Reproduced, and drafted for
upstream.

## The receive loop

**52 trace lines agree with `core_mqtt.c`** — `MQTT_ProcessLoop`,
`MQTT_ReceiveLoop`, and every handler under them. This is the last of
`core_mqtt.c`, and the only part of the library that **reassembles**: a read can
deliver half a packet, two packets, or a packet and a half.

### Three inputs, not two

A process loop is driven by the transport, the clock, **and the application
callback** — which decides whether the packet was accepted, what reason code
the acknowledgement carries, and whether it carries properties. So the callback
is scripted too, and every packet it is handed is logged.

### An acknowledgement with properties and no reason code is never sent

The C's sentinel for "the application set no reason code" is `0xFF`, and the
reason-code validator has no case for it. Adding one Reason String to a PUBACK
is enough to reach that validator, so a diagnostic property silently costs the
acknowledgement:

```
qos1-with-property   -> BadParameter   nothing sent
qos1-with-reason     -> Success        40 04 00 05 00 00
```

The broker never hears about the publish and the handshake stalls. Reproduced,
and drafted for upstream.

### And a field that is different every run is not a value

The callback log's last column is how many reason codes the application was
handed. For a PUBLISH and a PINGRESP it reads `?`, because two of the four
places that build a `MQTTDeserializedInfo_t` leave `pReasonCode` uninitialised —
this harness printed four different large numbers before that was noticed. Also
drafted.

### Keep alive is checked only when nothing arrived

It hangs off "the read returned zero bytes", not off the clock, so a busy
connection never pings however stale its transmit time is —
`keepalive-rx-timeout` sends a PINGREQ and
`keepalive-not-checked-when-busy`, with the same stale time and one packet to
read, does not.

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
