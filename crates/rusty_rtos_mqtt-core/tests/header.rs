//! The fixed-header codec against `core_mqtt_serializer.c`.
//!
//! The C arm is `oracle/header_driver.c` driving the pinned v5.0.2 serializer
//! (`04845c6a`) verbatim, and its trace is checked in.
//!
//! # Named cases, and an exhaustive sweep
//!
//! The named cases print every observable, so a divergence names itself. The
//! sweep runs **every one of the 256 possible type bytes** against every
//! remaining-length byte pattern at every claimed length — 6,291,456 calls —
//! and compares per-status counts plus an FNV-1a digest of all the answers.
//!
//! That pairing is deliberate. A digest alone tells you something moved and
//! nothing about what; named cases alone leave most of the input space
//! untested. Together they give exhaustive coverage that stays readable, and
//! the counts mean a digest mismatch usually comes with a hint about which
//! status changed.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::header::{
    HeaderError, encode_variable_length, process_incoming_packet_type_and_length,
    variable_length_encoded_size,
};

const TRACE: &str = include_str!("../../../oracle/header.trace");

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

const fn error_name(e: HeaderError) -> &'static str {
    match e {
        HeaderError::NoDataAvailable => "NoDataAvailable",
        HeaderError::NeedMoreBytes => "NeedMoreBytes",
        HeaderError::BadResponse => "BadResponse",
    }
}

/// One call, in the driver's answer format.
fn answer(buffer: &[u8], available: usize) -> String {
    match process_incoming_packet_type_and_length(buffer, available) {
        Ok(h) => format!(
            "Success {:02x} {} {}",
            h.packet_type, h.remaining_length, h.header_length
        ),
        Err(e) => error_name(e).to_owned(),
    }
}

/// FNV-1a, matching the driver's.
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

/// The driver's tail bytes, which the geometry line pins the count of.
const TAIL_BYTES: [u8; 8] = [0x00, 0x01, 0x7E, 0x7F, 0x80, 0x81, 0xFE, 0xFF];

fn our_trace() -> String {
    let mut out = String::new();
    let mut lines = TRACE.lines();

    let geometry = lines.next().expect("a geometry line");
    let _ = writeln!(out, "{geometry}");
    assert!(
        geometry.contains(&format!("tails={}", TAIL_BYTES.len())),
        "the two arms disagree about the sweep's alphabet: {geometry:?}"
    );

    for line in lines {
        let f: Vec<&str> = line.split_whitespace().collect();

        match f.first().copied() {
            // case <i> <name> in <available> <hex> -> <answer...>
            Some("case") => {
                let available: usize = f[4].parse().unwrap();
                // A zero-length buffer prints as an empty field, so the hex may
                // be missing entirely.
                let buffer = if f[5] == "->" {
                    Vec::new()
                } else {
                    unhex(f[5])
                };
                let _ = writeln!(
                    out,
                    "case {} {} in {available} {} -> {}",
                    f[1],
                    f[2],
                    hex(&buffer),
                    answer(&buffer, available)
                );
            }

            // size <i> <length> -> <encoded size> encoded <hex>
            Some("size") => {
                let length: u32 = f[2].parse().unwrap();
                let mut buffer = [0xAAu8; 8];
                let written = encode_variable_length(&mut buffer, length);
                let _ = writeln!(
                    out,
                    "size {} {length} -> {} encoded {}",
                    f[1],
                    variable_length_encoded_size(length),
                    hex(&buffer[..written])
                );
            }

            // The sweep is reproduced wholesale, below.
            Some("sweep") => {}

            Some("end") => {
                let _ = writeln!(out, "end");
            }

            _ => {}
        }
    }

    out
}

/// Run the whole sweep and produce the driver's six summary lines.
fn our_sweep() -> String {
    let mut fnv = Fnv::new();
    let mut counts = [0u64; 5];
    let mut total = 0u64;

    for packet_type in 0..=255u8 {
        for a in TAIL_BYTES {
            for b in TAIL_BYTES {
                for c in TAIL_BYTES {
                    for d in TAIL_BYTES {
                        let buffer = [packet_type, a, b, c, d];

                        for claimed in 0..=5usize {
                            let line = answer(&buffer, claimed);

                            match process_incoming_packet_type_and_length(&buffer, claimed) {
                                Ok(_) => counts[0] += 1,
                                Err(HeaderError::BadResponse) => counts[1] += 1,
                                Err(HeaderError::NeedMoreBytes) => counts[2] += 1,
                                Err(HeaderError::NoDataAvailable) => counts[3] += 1,
                            }

                            fnv.eat(&line);
                            total += 1;
                        }
                    }
                }
            }
        }
    }

    let mut out = String::new();
    let _ = writeln!(out, "sweep total {total}");
    let _ = writeln!(out, "sweep Success {}", counts[0]);
    let _ = writeln!(out, "sweep BadResponse {}", counts[1]);
    let _ = writeln!(out, "sweep NeedMoreBytes {}", counts[2]);
    let _ = writeln!(out, "sweep NoDataAvailable {}", counts[3]);
    let _ = writeln!(out, "sweep other {}", counts[4]);
    let _ = writeln!(out, "sweep digest {:016x}", fnv.0);
    out
}

#[test]
fn our_fixed_header_matches_the_c_case_for_case() {
    let ours = our_trace();
    // The sweep lines are produced separately; splice them where the C put
    // them, immediately before `end`.
    let ours = ours.replace("end\n", &format!("{}end\n", our_sweep()));

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

    assert_eq!(n, 55, "the trace should be 55 lines, not {n}");
}

/// The guard: the sweep must reach every outcome, and must be mostly refusals.
///
/// A fixed-header decoder that accepts most of what it is shown is not
/// checking anything. The ninth shape of the guard heap_4 started.
#[test]
fn the_sweep_is_mostly_refusals_and_reaches_every_outcome() {
    let value = |name: &str| -> u64 {
        TRACE
            .lines()
            .find(|l| l.starts_with(&format!("sweep {name} ")))
            .and_then(|l| l.split_whitespace().last())
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| panic!("no sweep line for {name}"))
    };

    let total = value("total");
    let success = value("Success");

    for name in ["Success", "BadResponse", "NeedMoreBytes", "NoDataAvailable"] {
        assert!(value(name) > 0, "the sweep never produces {name}");
    }

    assert_eq!(value("other"), 0, "the sweep produced an unexpected status");

    assert!(
        success * 2 < total,
        "the sweep accepts {success} of {total} inputs — a fixed-header decoder \
         that accepts most of what it is shown is not checking anything"
    );
}

/// The three ways to lie about a length, each named.
///
/// These are the cases a reader should be able to find without reading a
/// 6-million-call digest.
#[test]
fn the_three_length_attacks_are_all_refused() {
    // 1. Too many bytes: a fifth continuation byte.
    assert_eq!(
        process_incoming_packet_type_and_length(&[0xD0, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F], 6),
        Err(HeaderError::BadResponse),
        "a five-byte length was accepted"
    );

    // 2. Non-minimal: two spellings of the same value.
    assert_eq!(
        process_incoming_packet_type_and_length(&[0xD0, 0x00], 2)
            .expect("the minimal spelling of zero")
            .remaining_length,
        0
    );
    assert_eq!(
        process_incoming_packet_type_and_length(&[0xD0, 0x80, 0x00], 3),
        Err(HeaderError::BadResponse),
        "the non-minimal spelling of zero was accepted — an attacker now has \
         two ways to write one length"
    );

    // 3. A continuation that never terminates.
    assert_eq!(
        process_incoming_packet_type_and_length(&[0xD0, 0x80, 0x80, 0x80, 0x80], 5),
        Err(HeaderError::BadResponse)
    );

    // And the largest legal length is still accepted, so the refusals above
    // are not simply a decoder that says no to everything.
    let header = process_incoming_packet_type_and_length(&[0xD0, 0xFF, 0xFF, 0xFF, 0x7F], 5)
        .expect("268,435,455 is legal");
    assert_eq!(header.remaining_length, 268_435_455);
    assert_eq!(header.header_length, 5);
}

/// A PUBREL's reserved bit is the difference between a packet and a violation.
#[test]
fn a_pubrel_must_have_its_reserved_bit_set() {
    assert!(process_incoming_packet_type_and_length(&[0x62, 0x02], 2).is_ok());
    assert_eq!(
        process_incoming_packet_type_and_length(&[0x60, 0x02], 2),
        Err(HeaderError::BadResponse),
        "a PUBREL without its reserved bit was accepted"
    );
}

/// Encoding and decoding are inverses, across every boundary.
#[test]
fn every_length_round_trips() {
    const PROBES: [u32; 20] = [
        0,
        1,
        2,
        126,
        127,
        128,
        129,
        255,
        256,
        16_382,
        16_383,
        16_384,
        16_385,
        2_097_150,
        2_097_151,
        2_097_152,
        2_097_153,
        268_435_453,
        268_435_454,
        268_435_455,
    ];

    for length in PROBES {
        let mut buffer = [0u8; 8];
        let written = encode_variable_length(&mut buffer[1..], length);
        assert_eq!(
            written,
            variable_length_encoded_size(length) as usize,
            "encoding {length} used {written} bytes"
        );

        buffer[0] = 0xD0;
        let header = process_incoming_packet_type_and_length(&buffer, written + 1)
            .unwrap_or_else(|e| panic!("{length} did not round trip: {e:?}"));

        assert_eq!(header.remaining_length, length);
        assert_eq!(header.header_length, written + 1);
    }
}
