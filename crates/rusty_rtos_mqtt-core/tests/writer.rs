//! The fixed-header writers against `core_mqtt_serializer_private.c`.
//!
//! The C arm is `oracle/writer_driver.c` driving the pinned v5.0.2 helpers
//! (`04845c6a`) verbatim, and its trace is checked in.
//!
//! # One byte does most of the work
//!
//! `serializeConnectFixedHeader` packs six independent decisions into a single
//! flags byte — clean session, will present, will QoS, will retain, password
//! present, username present. A wrong bit position produces a packet the broker
//! rejects in a way that reads like a network fault, so the sweep is
//! **exhaustive over every combination**: 2 × 2 × 3 × 2 × 2 × 2 settings across
//! four keep-alive values and four remaining lengths, 1,536 calls, compared by
//! an FNV-1a digest — with eight combinations printed in full so a mismatch has
//! somewhere to start.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::fmt::Write as _;

use rusty_rtos_mqtt_core::state::QoS;
use rusty_rtos_mqtt_core::writer::{
    ConnectInfo, WillInfo, serialize_ack_fixed, serialize_connect_fixed_header,
    serialize_disconnect_fixed, serialize_subscribe_header, serialize_unsubscribe_header,
};

const TRACE: &str = include_str!("../../../oracle/writer.trace");

/// The driver's output buffer.
const OUT_SIZE: usize = 32;

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

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

const fn qos_of(n: u32) -> QoS {
    match n {
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}

/// The CONNECT sweep, in the driver's summary form.
fn our_connect_sweep() -> String {
    const KEEP_ALIVES: [u16; 4] = [0, 1, 60, 0xFFFF];
    const LENGTHS: [u32; 4] = [10, 127, 128, 16_384];

    let mut fnv = Fnv::new();
    let mut total = 0u64;

    for clean in 0..2u32 {
        for will in 0..2u32 {
            for will_qos in 0..3u32 {
                for will_retain in 0..2u32 {
                    for user in 0..2u32 {
                        for pass in 0..2u32 {
                            for keep_alive in KEEP_ALIVES {
                                for length in LENGTHS {
                                    let connect = ConnectInfo {
                                        clean_session: clean != 0,
                                        keep_alive_seconds: keep_alive,
                                        username: if user != 0 { Some(b"u") } else { None },
                                        password: if pass != 0 { Some(b"p") } else { None },
                                    };
                                    let will_info = WillInfo {
                                        qos: qos_of(will_qos),
                                        retain: will_retain != 0,
                                    };

                                    let mut out = [0xAAu8; OUT_SIZE];
                                    let n = serialize_connect_fixed_header(
                                        &mut out,
                                        &connect,
                                        if will != 0 { Some(&will_info) } else { None },
                                        length,
                                    );

                                    // The flags byte sits three back from the
                                    // end, before the two keep-alive bytes.
                                    fnv.eat(&format!("{n} {:02x}", out[n - 3]));
                                    total += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let mut s = String::new();
    let _ = writeln!(s, "connect total {total}");
    let _ = writeln!(s, "connect digest {:016x}", fnv.0);
    s
}

fn our_trace() -> String {
    let mut out = String::new();
    let mut lines = TRACE.lines();

    let geometry = lines.next().expect("a geometry line");
    let _ = writeln!(out, "{geometry}");

    for line in lines {
        let f: Vec<&str> = line.split_whitespace().collect();

        match f.first().copied() {
            // ack <i> <type> <id> <remaining> <reason> -> <n> <hex>
            Some("ack") => {
                let packet_type = u8::from_str_radix(f[2], 16).unwrap();
                let packet_id = u16::from_str_radix(f[3], 16).unwrap();
                let remaining: u32 = f[4].parse().unwrap();
                let reason = u8::from_str_radix(f[5], 16).unwrap();

                let mut buffer = [0xAAu8; OUT_SIZE];
                let n = serialize_ack_fixed(&mut buffer, packet_type, packet_id, remaining, reason);
                let _ = writeln!(
                    out,
                    "ack {} {} {} {} {} -> {n} {}",
                    f[1],
                    f[2],
                    f[3],
                    f[4],
                    f[5],
                    hex(&buffer[..n])
                );
            }

            // sub / unsub <i> <remaining> <id> -> <n> <hex>
            Some(kind @ ("sub" | "unsub")) => {
                let remaining: u32 = f[2].parse().unwrap();
                let packet_id = u16::from_str_radix(f[3], 16).unwrap();

                let mut buffer = [0xAAu8; OUT_SIZE];
                let n = if kind == "sub" {
                    serialize_subscribe_header(&mut buffer, remaining, packet_id)
                } else {
                    serialize_unsubscribe_header(&mut buffer, remaining, packet_id)
                };
                let _ = writeln!(
                    out,
                    "{kind} {} {} {} -> {n} {}",
                    f[1],
                    f[2],
                    f[3],
                    hex(&buffer[..n])
                );
            }

            // disc <i> <j> <remaining> <reason-or-256> -> <n> <hex>
            Some("disc") => {
                let remaining: u32 = f[3].parse().unwrap();
                let raw: u32 = f[4].parse().unwrap();
                // 256 is the driver's "no reason code", which is the C's NULL.
                let reason = if raw > 0xFF { None } else { Some(raw as u8) };

                let mut buffer = [0xAAu8; OUT_SIZE];
                let n = serialize_disconnect_fixed(&mut buffer, reason, remaining);
                let _ = writeln!(
                    out,
                    "disc {} {} {} {} -> {n} {}",
                    f[1],
                    f[2],
                    f[3],
                    f[4],
                    hex(&buffer[..n])
                );
            }

            // conn <i> <six digits> -> <n> <hex>
            Some("conn") => {
                let bits: Vec<u32> = f[2].chars().map(|c| c.to_digit(10).unwrap()).collect();
                let connect = ConnectInfo {
                    clean_session: bits[0] != 0,
                    keep_alive_seconds: 60,
                    username: if bits[4] != 0 { Some(b"u") } else { None },
                    password: if bits[5] != 0 { Some(b"p") } else { None },
                };
                let will_info = WillInfo {
                    qos: qos_of(bits[2]),
                    retain: bits[3] != 0,
                };

                let mut buffer = [0xAAu8; OUT_SIZE];
                let n = serialize_connect_fixed_header(
                    &mut buffer,
                    &connect,
                    if bits[1] != 0 { Some(&will_info) } else { None },
                    10,
                );
                let _ = writeln!(out, "conn {} {} -> {n} {}", f[1], f[2], hex(&buffer[..n]));
            }

            // The C prints the sweep summary, then the named cases. Emit it
            // on the FIRST of its two lines so the comparison stays ordered --
            // a sorted comparison would hide a transposition, which is exactly
            // the kind of defect a byte-for-byte writer differential is for.
            Some("connect") if f[1] == "total" => {
                let _ = write!(out, "{}", our_connect_sweep());
            }
            Some("connect") => {}

            Some("end") => {
                let _ = writeln!(out, "end");
            }

            _ => {}
        }
    }

    out
}

#[test]
fn our_writers_match_the_c_byte_for_byte() {
    let ours = our_trace();
    let mut theirs = TRACE.lines();
    let mut our_lines = ours.lines();
    let mut n = 0usize;

    loop {
        match (theirs.next(), our_lines.next()) {
            (None, None) => break,
            (Some(t), Some(o)) => {
                assert_eq!(o, t, "line {n} diverged\n  the C: {t}\n  ours : {o}");
            }
            (Some(t), None) => panic!("our trace ran out at line {n}; the C still has {t:?}"),
            (None, Some(o)) => panic!("the C trace ran out at line {n}; we still have {o:?}"),
        }
        n += 1;
    }

    assert_eq!(n, 51, "the trace should be 51 lines, not {n}");
}

/// The guard: the CONNECT sweep must actually vary the flags byte.
///
/// A sweep whose output never changes is a sweep of one case. The eleventh
/// shape of the guard `heap_4` started.
#[test]
fn the_connect_sweep_reaches_every_flag() {
    let mut seen = 0u8;

    for bits in 0..64u32 {
        let connect = ConnectInfo {
            clean_session: bits & 1 != 0,
            keep_alive_seconds: 60,
            username: if bits & 16 != 0 { Some(b"u") } else { None },
            password: if bits & 32 != 0 { Some(b"p") } else { None },
        };
        let will_info = WillInfo {
            qos: qos_of((bits >> 2) & 3),
            retain: bits & 8 != 0,
        };

        let mut out = [0u8; OUT_SIZE];
        let n = serialize_connect_fixed_header(
            &mut out,
            &connect,
            if bits & 2 != 0 {
                Some(&will_info)
            } else {
                None
            },
            10,
        );
        seen |= out[n - 3];
    }

    // Every bit but the reserved one must have been set by something.
    assert_eq!(
        seen, 0xFE,
        "the sweep only ever sets flags {seen:08b}; bit 0 is reserved and must \
         stay clear, every other bit must be reachable"
    );
}

/// Every writer refuses a destination it cannot fill, rather than writing part.
#[test]
fn a_short_destination_is_refused_whole() {
    for size in 0..16usize {
        let mut buffer = vec![0xAAu8; size];

        let n = serialize_ack_fixed(&mut buffer, 0x40, 1, 3, 0);
        assert!(n == 0 || n <= size);
        if n == 0 {
            assert!(
                buffer.iter().all(|b| *b == 0xAA),
                "a refused ack write left bytes behind at size {size}"
            );
        }

        let mut buffer = vec![0xAAu8; size];
        let n = serialize_disconnect_fixed(&mut buffer, Some(0), 1);
        if n == 0 {
            assert!(
                buffer.iter().all(|b| *b == 0xAA),
                "a refused disconnect write left bytes behind at size {size}"
            );
        }
    }
}

/// A DISCONNECT with no reason code is two bytes, not three.
#[test]
fn a_disconnect_may_carry_no_reason_at_all() {
    let mut buffer = [0xAAu8; OUT_SIZE];

    let n = serialize_disconnect_fixed(&mut buffer, None, 0);
    assert_eq!(
        &buffer[..n],
        &[0xE0, 0x00],
        "a bare DISCONNECT is two bytes"
    );

    let n = serialize_disconnect_fixed(&mut buffer, Some(0x04), 1);
    assert_eq!(
        &buffer[..n],
        &[0xE0, 0x01, 0x04],
        "a DISCONNECT with a reason is three"
    );
}

/// A writer must not touch a byte past the length it reports.
///
/// This test exists because a poison found the gap. Making
/// `serialize_disconnect_fixed` always write a reason code — even when there is
/// none, so it writes one byte beyond what it returns — passed every other test
/// here, including the byte-for-byte differential, because the differential
/// only ever looks at `buffer[..n]`.
///
/// A caller that packed something after the header would have it silently
/// clobbered. "Wrote the right bytes" and "wrote only those bytes" are two
/// claims, and a trace of the output can only make the first.
#[test]
fn a_writer_never_writes_past_the_length_it_reports() {
    const FILL: u8 = 0x5A;

    let check = |label: &str, buffer: &[u8], n: usize| {
        assert!(
            buffer[n..].iter().all(|b| *b == FILL),
            "{label} reported {n} bytes and touched at least one beyond that"
        );
    };

    for remaining in [0u32, 1, 2, 127, 128, 16_384, 2_097_152] {
        let mut buffer = [FILL; OUT_SIZE];
        let n = serialize_ack_fixed(&mut buffer, 0x40, 0x1234, remaining, 0x10);
        check("serialize_ack_fixed", &buffer, n);

        let mut buffer = [FILL; OUT_SIZE];
        let n = serialize_subscribe_header(&mut buffer, remaining, 7);
        check("serialize_subscribe_header", &buffer, n);

        let mut buffer = [FILL; OUT_SIZE];
        let n = serialize_unsubscribe_header(&mut buffer, remaining, 7);
        check("serialize_unsubscribe_header", &buffer, n);

        // Both shapes of DISCONNECT, which is where the gap was.
        for reason in [None, Some(0x04u8)] {
            let mut buffer = [FILL; OUT_SIZE];
            let n = serialize_disconnect_fixed(&mut buffer, reason, remaining);
            check("serialize_disconnect_fixed", &buffer, n);
        }

        let connect = ConnectInfo {
            clean_session: true,
            keep_alive_seconds: 60,
            username: Some(b"u"),
            password: Some(b"p"),
        };
        let will = WillInfo {
            qos: QoS::ExactlyOnce,
            retain: true,
        };
        let mut buffer = [FILL; OUT_SIZE];
        let n = serialize_connect_fixed_header(&mut buffer, &connect, Some(&will), remaining);
        check("serialize_connect_fixed_header", &buffer, n);
    }
}
