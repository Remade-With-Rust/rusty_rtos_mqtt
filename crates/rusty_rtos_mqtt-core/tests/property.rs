//! The property primitives against `core_mqtt_serializer_private.c`.
//!
//! The C arm is `oracle/property_driver.c` driving the pinned v5.0.2 helpers
//! (`04845c6a`) verbatim, and its trace is checked in.
//!
//! # What is compared
//!
//! The status, the value — and **where the cursor and the property budget were
//! left**. That third part is the one that matters: a failed `decodeUtf8` has
//! already consumed its two length bytes before it discovers the body does not
//! fit, so it leaves the cursor moved and the budget smaller. A transcription
//! that tidied that up would look more correct and would disagree with the C on
//! every malformed packet.
//!
//! # Two variable-length decoders
//!
//! The sweep here is over `decodeVariableLength`, which is **not** the fixed
//! header's decoder. They start at different offsets, are bounded by different
//! things, and treat an out-of-range value differently. Both are transcribed
//! and both have their own exhaustive sweep, because reusing one for the other
//! is the mistake a single-function test cannot see.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::property::{PropertyReader, decode_variable_length, encode_string};

const TRACE: &str = include_str!("../../../oracle/property.trace");

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn unhex(text: &str) -> Vec<u8> {
    text.as_bytes()
        .chunks(2)
        .map(|p| u8::from_str_radix(core::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}

/// The driver's buffer, so a string's offset is comparable.
const MAX_BUF: usize = 64;

/// The driver's variable-length alphabet.
const TAILS: [u8; 6] = [0x00, 0x01, 0x7F, 0x80, 0x81, 0xFF];

struct Fnv(u64);

impl Fnv {
    const fn new() -> Self {
        Self(1_469_598_103_934_665_603)
    }
    fn eat(&mut self, s: &str) {
        for b in s.as_bytes() {
            self.0 ^= u64::from(*b);
            self.0 = self.0.wrapping_mul(1_099_511_628_211);
        }
    }
}

/// Replay one scenario and append our lines to `out`.
fn run_scenario(out: &mut String, buffer: &[u8], property_length: u32, ops: &[(char, u8)]) {
    // The driver copies the scenario into a 64-byte buffer, so offsets are
    // relative to that and reads past the scenario's own bytes are possible.
    let mut padded = [0u8; MAX_BUF];
    padded[..buffer.len()].copy_from_slice(buffer);

    let mut reader = PropertyReader::new(&padded, property_length);

    for (i, (kind, used_in)) in ops.iter().enumerate() {
        let mut used = *used_in != 0;
        let _ = write!(out, "op {i} {kind} used={used_in} -> ");

        match kind {
            '1' => match reader.u8(&mut used) {
                Ok(v) => {
                    let _ = write!(out, "Success {v:02x}");
                }
                Err(_) => {
                    let _ = write!(out, "BadResponse");
                }
            },
            '2' => match reader.u16(&mut used) {
                Ok(v) => {
                    let _ = write!(out, "Success {v:04x}");
                }
                Err(_) => {
                    let _ = write!(out, "BadResponse");
                }
            },
            '4' => match reader.u32(&mut used) {
                Ok(v) => {
                    let _ = write!(out, "Success {v:08x}");
                }
                Err(_) => {
                    let _ = write!(out, "BadResponse");
                }
            },
            's' => {
                // The offset the C reports is where the body starts, which is
                // the cursor BEFORE the body was taken.
                let before = reader.position();
                match reader.utf8(&mut used) {
                    Ok(body) => {
                        let _ = write!(out, "Success {} {}", before + 2, body.len());
                    }
                    Err(_) => {
                        let _ = write!(out, "BadResponse");
                    }
                }
            }
            'u' => {
                let before = reader.position();
                match reader.user_property() {
                    Ok((key, value)) => {
                        let key_at = before + 2;
                        let value_at = key_at + key.len() + 2;
                        let _ = write!(
                            out,
                            "Success {key_at} {} {value_at} {}",
                            key.len(),
                            value.len()
                        );
                    }
                    Err(_) => {
                        let _ = write!(out, "BadResponse");
                    }
                }
            }
            other => panic!("unknown operation {other:?}"),
        }

        // Where the cursor and the budget were left is as much of the answer as
        // the status.
        let _ = writeln!(
            out,
            " at={} left={} used={}",
            reader.position(),
            reader.remaining(),
            u8::from(used)
        );
    }
}

fn our_sweep() -> String {
    let mut fnv = Fnv::new();
    let (mut ok, mut bad, mut total) = (0u64, 0u64, 0u64);

    for a in TAILS {
        for b in TAILS {
            for c in TAILS {
                for d in TAILS {
                    let buffer = [a, b, c, d];

                    for len in 0..=4usize {
                        let line = match decode_variable_length(&buffer[..len]) {
                            Ok(v) => {
                                ok += 1;
                                format!("Success {v}")
                            }
                            Err(_) => {
                                bad += 1;
                                "BadResponse".to_owned()
                            }
                        };
                        fnv.eat(&line);
                        total += 1;
                    }
                }
            }
        }
    }

    let mut out = String::new();
    let _ = writeln!(out, "varlen total {total}");
    let _ = writeln!(out, "varlen Success {ok}");
    let _ = writeln!(out, "varlen refused {bad}");
    let _ = writeln!(out, "varlen digest {:016x}", fnv.0);
    out
}

fn our_encode() -> String {
    const SAMPLES: [&[u8]; 4] = [b"", b"a", b"topic/one", b"0123456789abcdef"];
    let mut out = String::new();

    for (i, sample) in SAMPLES.iter().enumerate() {
        let mut buffer = [0xAAu8; 32];
        let written = encode_string(&mut buffer, Some(sample), sample.len() as u16);
        let _ = writeln!(
            out,
            "encode {i} {} -> {written} {}",
            sample.len(),
            hex(&buffer[..written])
        );
    }

    // A `None` source reserves the body without writing it, which is what the
    // C's NULL pointer does; the fill shows through.
    let mut buffer = [0xAAu8; 32];
    let written = encode_string(&mut buffer, None, 4);
    let _ = writeln!(
        out,
        "encode null 4 -> {written} {}",
        hex(&buffer[..written])
    );

    out
}

fn our_trace() -> String {
    let mut out = String::new();
    let mut lines = TRACE.lines();

    let geometry = lines.next().expect("a geometry line");
    let _ = writeln!(out, "{geometry}");
    assert!(
        geometry.contains(&format!("maxbuf={MAX_BUF}"))
            && geometry.contains(&format!("tails={}", TAILS.len())),
        "the two arms disagree about the geometry: {geometry:?}"
    );

    let mut buffer: Vec<u8> = Vec::new();
    let mut property_length = 0u32;

    for line in lines {
        let f: Vec<&str> = line.split_whitespace().collect();

        match f.first().copied() {
            Some("scenario") => {
                let _ = writeln!(out, "{line}");
            }
            Some("cfg") if f[1] == "buffer" => {
                let _ = writeln!(out, "{line}");
                buffer = if f.len() > 3 { unhex(f[3]) } else { Vec::new() };
            }
            Some("cfg") if f[1] == "proplen" => {
                let _ = writeln!(out, "{line}");
                property_length = f[2].parse().unwrap();
            }
            Some("cfg") if f[1] == "ops" => {
                let _ = writeln!(out, "{line}");
                let ops: Vec<(char, u8)> = f[3..]
                    .iter()
                    .map(|e| {
                        let (kind, used) = e.split_once(':').unwrap();
                        (kind.chars().next().unwrap(), used.parse().unwrap())
                    })
                    .collect();
                run_scenario(&mut out, &buffer, property_length, &ops);
            }
            Some("varlen") | Some("encode") => {}
            Some("end") if f.len() > 1 => {
                let _ = writeln!(out, "{line}");
            }
            Some("end") => {
                let _ = writeln!(out, "{}{}end", our_sweep(), our_encode());
            }
            _ => {}
        }
    }

    out
}

#[test]
fn our_property_primitives_match_the_c_call_for_call() {
    let ours = our_trace();
    let mut theirs = TRACE.lines();
    let mut our_lines = ours.lines();
    let mut n = 0usize;

    loop {
        match (theirs.next(), our_lines.next()) {
            (None, None) => break,
            (Some(t), Some(o)) => assert_eq!(o, t, "line {n} diverged\n  the C: {t}\n  ours : {o}"),
            (Some(t), None) => panic!("our trace ran out at line {n}; the C still has {t:?}"),
            (None, Some(o)) => panic!("the C trace ran out at line {n}; we still have {o:?}"),
        }
        n += 1;
    }

    assert_eq!(n, 104, "the trace should be 104 lines, not {n}");
}

/// The guard: the scenarios must refuse as often as they accept.
///
/// A primitive layer that only ever succeeds is not enforcing a budget. The
/// tenth shape of the guard `heap_4` started.
#[test]
fn the_scenarios_refuse_as_well_as_accept() {
    // `op <i> <kind> used=<n> -> <status> ...`, so the status is field 5.
    let ops: Vec<&str> = TRACE
        .lines()
        .filter(|l| l.starts_with("op "))
        .filter_map(|l| l.split_whitespace().nth(5))
        .collect();

    let refused = ops.iter().filter(|s| **s == "BadResponse").count();

    assert!(refused >= 5, "only {refused} operations are refused");
    assert!(
        ops.len() - refused >= 5,
        "only {} operations succeed",
        ops.len() - refused
    );

    // And the sweep must do the same.
    let value = |name: &str| -> u64 {
        TRACE
            .lines()
            .find(|l| l.starts_with(&format!("varlen {name} ")))
            .and_then(|l| l.split_whitespace().last())
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| panic!("no varlen line for {name}"))
    };
    assert!(value("Success") > 0 && value("refused") > 0);
}

/// A refused string leaves the cursor moved, and that is deliberate.
///
/// The C consumes the two length bytes and charges them to the budget before it
/// discovers the body does not fit. A transcription that tidied that up would
/// look more correct and would disagree with the C on every malformed packet —
/// so it is pinned here rather than left to the trace.
#[test]
fn a_refused_string_still_consumed_its_length_bytes() {
    // A three-byte budget, and a string claiming five bytes of body.
    let bytes = [0x00u8, 0x05, b'a', b'b', b'c'];
    let mut reader = PropertyReader::new(&bytes, 5);
    let mut used = false;

    assert!(reader.utf8(&mut used).is_err());
    assert_eq!(reader.position(), 2, "the length bytes were not consumed");
    assert_eq!(reader.remaining(), 3, "the length bytes were not charged");
    assert!(!used, "a refused read must not mark the property as seen");
}

/// The buffer is a second bound the C does not have.
///
/// `decodeUtf8` checks the length against the property BUDGET and then indexes.
/// If the packet claims a budget larger than the bytes actually received — which
/// an attacker chooses freely — the C reads past the buffer. Here the budget is
/// checked first, exactly as the C does, and then the slice is taken with
/// `get`, so an over-claiming budget produces a refusal instead.
#[test]
fn a_budget_larger_than_the_buffer_cannot_read_past_it() {
    // Four bytes present, but the packet claims a 64-byte property.
    let bytes = [0x00u8, 0x20, b'a', b'b'];
    let mut reader = PropertyReader::new(&bytes, 64);
    let mut used = false;

    assert!(
        reader.utf8(&mut used).is_err(),
        "a 32-byte string was read out of a 4-byte buffer"
    );
}

/// Both variable-length decoders exist, and they are not the same function.
#[test]
fn the_two_variable_length_decoders_differ() {
    use rusty_rtos_mqtt_core::header::process_incoming_packet_type_and_length;

    // The property decoder starts at index 0.
    assert_eq!(decode_variable_length(&[0x7F]), Ok(127));

    // The header decoder starts at index 1, after the type byte, so the same
    // bytes mean something different to it.
    let header = process_incoming_packet_type_and_length(&[0xD0, 0x7F], 2)
        .expect("a PINGRESP with a 127-byte body");
    assert_eq!(header.remaining_length, 127);

    // Both refuse a non-minimal encoding, and both refuse a fifth byte.
    assert!(decode_variable_length(&[0x80, 0x00]).is_err());
    assert!(decode_variable_length(&[0xFF, 0xFF, 0xFF, 0xFF]).is_err());
}
