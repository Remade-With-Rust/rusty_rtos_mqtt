//! Instruction counts for MQTT topic matching.
//!
//! The workload is a cross product of topics against filters, in the shape the
//! differential's printed grid uses: every topic tried against every filter, so
//! the exact-match path, the `+` walk, the `#` tail and the `$SYS` rule are all
//! exercised in one sweep rather than one at a time.
//!
//! A deterministic counter, not a clock. The three verdict counts are the work
//! parity anchors: a change that moves any of them changed behaviour, and a
//! compiler that removed the work moves the checksum.

use rusty_rtos_mqtt_core::topic::matches;

/// Enough repetitions that process startup is noise in the total.
const REPS: u32 = 400;

const TOPICS: &[&str] = &[
    "sport/tennis/player1",
    "sport/tennis/player1/ranking",
    "sport/tennis/player1/score/wimbledon",
    "sport/tennis",
    "sport",
    "sport/",
    "/finance",
    "finance",
    "$SYS/broker/load/messages/received",
    "$SYS",
    "a/b/c/d/e/f/g",
    "home/kitchen/sensor/temperature",
    "home/kitchen/sensor/humidity",
    "home//sensor",
    "a+b/c",
    "a/#/b",
    "one",
    "one/two",
    "one/two/three",
    "\u{00e9}t\u{00e9}/chaud",
];

const FILTERS: &[&str] = &[
    "sport/tennis/player1/#",
    "sport/tennis/+",
    "sport/+",
    "sport/#",
    "sport",
    "#",
    "+",
    "+/+",
    "+/tennis/#",
    "/finance",
    "+/finance",
    "$SYS/#",
    "$SYS/broker/+/messages/+",
    "home/+/sensor/+",
    "home/#",
    "a+b/c",
    "a/#",
    "one/+/three",
    "one/two/#",
    "+/+/+/+/+/+/+",
];

fn main() {
    let topics: Vec<&[u8]> = TOPICS.iter().map(|s| s.as_bytes()).collect();
    let filters: Vec<&[u8]> = FILTERS.iter().map(|s| s.as_bytes()).collect();

    let mut hit = 0u64;
    let mut miss = 0u64;
    let mut refused = 0u64;
    let mut checksum = 0u64;

    for _ in 0..REPS {
        for (t, topic) in topics.iter().enumerate() {
            for (f, filter) in filters.iter().enumerate() {
                // The index weighting makes a reordering visible, not just a
                // change in how many matched.
                match matches(topic, filter) {
                    Ok(true) => {
                        hit = hit.wrapping_add(1);
                        checksum = checksum.wrapping_add((t * filters.len() + f) as u64);
                    }
                    Ok(false) => miss = miss.wrapping_add(1),
                    Err(_) => refused = refused.wrapping_add(1),
                }
            }
        }
    }

    let pairs = (topics.len() * filters.len()) as u64;
    println!("checksum {checksum}");
    println!(
        "pairs {pairs} reps {REPS} calls {} hit {} miss {} refused {}",
        pairs * u64::from(REPS),
        hit / u64::from(REPS),
        miss / u64::from(REPS),
        refused / u64::from(REPS)
    );
}
