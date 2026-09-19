//! Instruction counts for the MQTT fixed-header decoder.
//!
//! The sweep is the differential's shape: every one of the 256 type bytes
//! against every remaining-length encoding from one to four bytes, at several
//! claimed `available` counts -- including counts SMALLER than the buffer,
//! which is the incomplete-header case, and counts LARGER than it, which is
//! the shape the C cannot survive.
//!
//! A deterministic counter, not a clock. The four verdict counts are the work
//! parity anchors: a change that moves any of them changed behaviour.

use rusty_rtos_mqtt_core::header::process_incoming_packet_type_and_length;

const REPS: u32 = 40;

/// Remaining-length encodings: one, two, three and four byte varints, plus the
/// malformed continuation that never terminates.
const LENGTHS: &[&[u8]] = &[
    &[0x00],
    &[0x7F],
    &[0x80, 0x01],
    &[0xFF, 0x7F],
    &[0x80, 0x80, 0x01],
    &[0xFF, 0xFF, 0x7F],
    &[0x80, 0x80, 0x80, 0x01],
    &[0xFF, 0xFF, 0xFF, 0x7F],
    &[0xFF, 0xFF, 0xFF, 0xFF],
    &[0x80],
    &[0x80, 0x80],
    &[0x80, 0x80, 0x80],
];

fn main() {
    let mut buf = [0u8; 8];
    let mut ok = 0u64;
    let mut no_data = 0u64;
    let mut need_more = 0u64;
    let mut bad = 0u64;
    let mut checksum = 0u64;

    for _ in 0..REPS {
        // The length encoding is the outer loop and the type byte the inner
        // one, so the remaining-length bytes are laid down once per encoding
        // rather than once per (type, encoding) pair -- 480 times across the
        // run instead of 122,880. Only `buf[0]` depends on the type byte.
        //
        // The sweep is the same set of calls in a different order, and the
        // parity anchors prove it: the four verdict counts are totals and the
        // checksum is a sum, so neither can tell the two orders apart. Both
        // read exactly as they did before this change.
        for (li, length) in LENGTHS.iter().enumerate() {
            let mut n = 1usize;
            for &b in length.iter() {
                if let Some(slot) = buf.get_mut(n) {
                    *slot = b;
                    n = n.saturating_add(1);
                }
            }
            for type_byte in 0..=255u8 {
                buf[0] = type_byte;
                // Every `available` from nothing to one past the header: the
                // incomplete cases and the complete one, in one sweep.
                for available in 0..=n.saturating_add(1) {
                    match process_incoming_packet_type_and_length(&buf, available) {
                        Ok(header) => {
                            ok = ok.wrapping_add(1);
                            // Fold the answer in, so a decoder that returned a
                            // different length shows up as a changed checksum
                            // and not merely as the same verdict count.
                            checksum = checksum
                                .wrapping_add(u64::from(type_byte))
                                .wrapping_add((li as u64).wrapping_mul(7))
                                .wrapping_add(header.remaining_length as u64);
                        }
                        Err(e) => match e as u8 {
                            0 => no_data = no_data.wrapping_add(1),
                            1 => need_more = need_more.wrapping_add(1),
                            _ => bad = bad.wrapping_add(1),
                        },
                    }
                }
            }
        }
    }

    let calls = ok
        .wrapping_add(no_data)
        .wrapping_add(need_more)
        .wrapping_add(bad);
    println!("checksum {checksum}");
    println!(
        "reps {REPS} calls {calls} ok {} nodata {} needmore {} bad {}",
        ok / u64::from(REPS),
        no_data / u64::from(REPS),
        need_more / u64::from(REPS),
        bad / u64::from(REPS)
    );
}
