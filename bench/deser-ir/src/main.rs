//! Instruction counts for the MQTT packet deserializers.
//!
//! PUBLISH and DISCONNECT are the two whose whole input a broker chooses, and
//! both walk an MQTT 5 property section, so this sweep exercises the property
//! reader as well as the two packet bodies.
//!
//! The corpus is deliberately mixed: well-formed packets, packets whose claimed
//! remaining length exceeds what arrived, truncations of a valid packet, and
//! property sections that run past their own budget -- which is the shape an
//! attacker sends.
//!
//! A deterministic counter, not a clock. The verdict counts are the work parity
//! anchors.

use rusty_rtos_mqtt_core::disconnect::deserialize_disconnect;
use rusty_rtos_mqtt_core::publish::deserialize_publish;
use rusty_rtos_mqtt_core::ack::PacketInfo;

const REPS: u32 = 60;

/// Packet bodies, valid and not.
const BODIES: &[&[u8]] = &[
    // A minimal PUBLISH body: topic "a/b", no properties.
    &[0x00, 0x03, b'a', b'/', b'b', 0x00],
    // Topic, packet id, no properties.
    &[0x00, 0x03, b'a', b'/', b'b', 0x12, 0x34, 0x00],
    // Topic, a one-byte property section holding a payload-format indicator.
    &[0x00, 0x03, b'a', b'/', b'b', 0x02, 0x01, 0x01],
    // A property section that claims more than it has.
    &[0x00, 0x03, b'a', b'/', b'b', 0x7F, 0x01, 0x01],
    // A topic length past the end.
    &[0x00, 0x7F, b'a', b'/', b'b', 0x00],
    // Empty.
    &[],
    // One byte.
    &[0x00],
    // A DISCONNECT body: reason code then an empty property section.
    &[0x00, 0x00],
    // Reason code with a reason-string property.
    &[0x00, 0x06, 0x1F, 0x00, 0x03, b'b', b'a', b'd'],
    // A user property pair.
    &[0x00, 0x09, 0x26, 0x00, 0x01, b'k', 0x00, 0x02, b'v', b'x'],
    // A property identifier that is not legal here.
    &[0x00, 0x02, 0x0B, 0x01],
    // A variable-length property length that never terminates.
    &[0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0x01],
];

fn main() {
    let mut ok = 0u64;
    let mut refused = 0u64;
    let mut checksum = 0u64;

    for _ in 0..REPS {
        for (bi, body) in BODIES.iter().enumerate() {
            // Sweep the type nibble so the PUBLISH flag decoding (DUP, QoS,
            // RETAIN) and the wrong-type refusals are both covered.
            for type_byte in [0x30u8, 0x31, 0x32, 0x34, 0x36, 0x3D, 0xE0, 0x20] {
                // A claimed length that matches, and one that does not.
                for claimed in [body.len() as u32, body.len() as u32 + 4] {
                    let packet = PacketInfo {
                        packet_type: type_byte,
                        remaining_length: claimed,
                        remaining_data: body,
                    };

                    match deserialize_publish(&packet, 268_435_455, 16) {
                        Ok(info) => {
                            ok = ok.wrapping_add(1);
                            checksum = checksum
                                .wrapping_add(bi as u64)
                                .wrapping_add(info.topic_name.len() as u64)
                                .wrapping_add(u64::from(info.packet_id.unwrap_or(0)));
                        }
                        Err(_) => refused = refused.wrapping_add(1),
                    }

                    match deserialize_disconnect(&packet, 268_435_455) {
                        Ok(d) => {
                            ok = ok.wrapping_add(1);
                            checksum = checksum
                                .wrapping_add(bi as u64)
                                .wrapping_add(u64::from(d.reason_code.unwrap_or(0)));
                        }
                        Err(_) => refused = refused.wrapping_add(1),
                    }
                }
            }
        }
    }

    println!("checksum {checksum}");
    println!(
        "reps {REPS} calls {} ok {} refused {}",
        ok.wrapping_add(refused),
        ok / u64::from(REPS),
        refused / u64::from(REPS)
    );
}
