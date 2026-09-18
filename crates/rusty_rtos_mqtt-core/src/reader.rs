//! Pulling a packet's header off a transport, one byte at a time.
//!
//! Every other function in this crate is handed a buffer. This one is handed a
//! **callback** and reads the type byte and then the variable-byte remaining
//! length, stopping when the length is complete — so the thing that matters is
//! not only the answer but **how many times it called**, and what it did when
//! the transport gave it something other than a byte.
//!
//! That is the shape `rusty_rtos_sntp`'s client differential established:
//! script the callback, log every call, and compare the call sequence.
//!
//! # The type is checked BEFORE the length is read
//!
//! A packet type a client may not receive is refused after **one** call, not
//! after the whole header has been consumed. That is the right order — it
//! stops a malformed stream from being drained — and it is invisible in the
//! status, which is `MQTTBadResponse` either way. The differential compares the
//! call count, so it is visible there.
//!
//! # Two statuses nothing else in the library uses
//!
//! A transport can answer with a byte, with nothing yet, or with an error, and
//! the C has a separate status for each of the last two:
//! [`ReadError::NoDataAvailable`] and [`ReadError::RecvFailed`]. They are only
//! distinguished at the **type** byte; once the type has been read, both
//! become `MQTTBadResponse` — a distinction the C draws once and then drops,
//! which the differential has cases for.

use crate::header::{
    REMAINING_LENGTH_INVALID, incoming_packet_valid, variable_length_encoded_size,
};

/// What a transport gave back when asked for one byte.
///
/// The C's `readFunc` returns an `int32_t`: 1 for a byte, 0 for nothing yet,
/// and anything else for a failure. Those are the only three the reader
/// distinguishes, so they are the three here.
///
/// The C's callback also takes a **length**, and this reader always passes 1 —
/// so "the transport was asked for more than one byte" has no equivalent in
/// this trait. It is baked in rather than checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Received {
    /// One byte arrived.
    Byte(u8),
    /// Nothing yet. Only at the type byte does this mean "try again later".
    Nothing,
    /// The transport failed.
    Failed,
}

/// A source of bytes — a socket, a test script, a ring buffer.
pub trait Transport {
    /// Ask for exactly one byte.
    fn recv_one(&mut self) -> Received;
}

/// A packet's fixed header, as far as the transport has read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IncomingHeader {
    /// The first byte, whole.
    pub packet_type: u8,
    /// How many bytes of body the packet claims.
    pub remaining_length: u32,
    /// The type byte plus the bytes that encoded the length.
    ///
    /// **This is an addition, not a transcription.**
    /// `MQTT_GetIncomingPacketTypeAndLength` sets `remainingLength` and leaves
    /// `pIncomingPacket->headerLength` exactly as the caller left it, so a C
    /// caller has to count the bytes itself. We counted them anyway, so the
    /// number is free; it is not in the trace, because the C has nothing to
    /// compare it against, and
    /// `the_header_length_counts_the_type_byte` pins it instead.
    pub header_length: usize,
}

/// Why a header could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadError {
    /// `MQTTNoDataAvailable`: nothing on the transport yet. **Only** reported
    /// at the type byte — a gap part-way through the length is a bad response,
    /// because a header that started must finish.
    NoDataAvailable,
    /// `MQTTRecvFailed`: the transport failed at the type byte.
    RecvFailed,
    /// `MQTTBadResponse`: a packet type a client may not receive, a remaining
    /// length that is too long, non-minimal or truncated, or a transport that
    /// stopped part-way through the length.
    BadResponse,
}

/// `MQTT_GetIncomingPacketTypeAndLength`.
///
/// # Errors
///
/// [`ReadError::NoDataAvailable`] or [`ReadError::RecvFailed`] if the transport
/// gives nothing or fails **at the first byte**. [`ReadError::BadResponse`] for
/// a type a client may not receive, a length in more than four bytes, a
/// non-minimally encoded one, or a transport that stops part-way through it.
pub fn read_header<T: Transport + ?Sized>(transport: &mut T) -> Result<IncomingHeader, ReadError> {
    let packet_type = match transport.recv_one() {
        Received::Byte(byte) => byte,
        // These two are distinguished ONLY here.
        Received::Nothing => return Err(ReadError::NoDataAvailable),
        Received::Failed => return Err(ReadError::RecvFailed),
    };

    // Before any more is read: a type a client may not receive ends it after
    // one call. The order is invisible in the status and visible in the count.
    if !incoming_packet_valid(packet_type) {
        return Err(ReadError::BadResponse);
    }

    let (remaining_length, bytes_decoded) = read_remaining_length(transport)?;

    Ok(IncomingHeader {
        packet_type,
        remaining_length,
        header_length: bytes_decoded.saturating_add(1),
    })
}

/// `getRemainingLength`: the variable-byte integer, a byte at a time.
///
/// The C signals every failure by returning `MQTT_REMAINING_LENGTH_INVALID` and
/// lets the caller turn that into a status, so a transport that stopped and a
/// length that was too long are indistinguishable by the time the caller sees
/// them. Reproduced: both are [`ReadError::BadResponse`].
fn read_remaining_length<T: Transport + ?Sized>(
    transport: &mut T,
) -> Result<(u32, usize), ReadError> {
    let mut remaining_length = 0u32;
    let mut multiplier = 1u32;
    let mut bytes_decoded = 0usize;

    loop {
        // The multiplier guard, checked BEFORE the read -- so a fifth
        // continuation byte is refused without asking the transport for it.
        if multiplier > 2_097_152 {
            return Err(ReadError::BadResponse);
        }

        let byte = match transport.recv_one() {
            Received::Byte(byte) => byte,
            // Past the type byte the two failures are one. The C reaches this
            // by returning its invalid sentinel from both paths.
            Received::Nothing | Received::Failed => return Err(ReadError::BadResponse),
        };

        remaining_length =
            remaining_length.saturating_add(u32::from(byte & 0x7F).saturating_mul(multiplier));
        multiplier = multiplier.saturating_mul(128);
        bytes_decoded = bytes_decoded.saturating_add(1);

        if (byte & 0x80) == 0 {
            break;
        }
    }

    // UNREACHABLE, and kept because the C keeps it: the multiplier guard above
    // stops the loop after four bytes, and four bytes of seven bits reach
    // exactly 268,435,455 -- one less than this. Third instance in the package
    // of the same arithmetic, after the property length decoder's and the
    // fixed header's. A poison removing it changes no answer.
    if remaining_length >= REMAINING_LENGTH_INVALID {
        return Err(ReadError::BadResponse);
    }

    // The same non-minimal check the buffered decoders make: two spellings of
    // one length is how a length check one layer up gets bypassed.
    if bytes_decoded != variable_length_encoded_size(remaining_length) as usize {
        return Err(ReadError::BadResponse);
    }

    Ok((remaining_length, bytes_decoded))
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]
mod tests {
    use super::*;

    /// A transport that answers from a script and counts what it was asked.
    struct Script {
        steps: Vec<Received>,
        at: usize,
        calls: usize,
    }

    impl Script {
        fn new(steps: &[Received]) -> Self {
            Self {
                steps: steps.to_vec(),
                at: 0,
                calls: 0,
            }
        }
    }

    impl Transport for Script {
        fn recv_one(&mut self) -> Received {
            self.calls += 1;
            let step = self.steps.get(self.at).copied();
            self.at += 1;
            // Running off the end is a failure, not a hang: a reader that asks
            // for more than the script describes should be caught, not fed.
            step.unwrap_or(Received::Failed)
        }
    }

    fn bytes(values: &[u8]) -> Vec<Received> {
        values.iter().map(|&b| Received::Byte(b)).collect()
    }

    /// The type is checked before the length is read, and the COUNT says so.
    ///
    /// Both a good and a bad type give the same status shape from a caller's
    /// point of view — one succeeds, one is `BadResponse` — but a reader that
    /// checked the type *after* consuming the length would drain a malformed
    /// stream instead of stopping at its first byte. Only the call count shows
    /// it, which is why this differential compares the count.
    #[test]
    fn a_bad_type_stops_after_one_call() {
        let mut good = Script::new(&bytes(&[0x40, 0x02]));
        assert!(read_header(&mut good).is_ok());
        assert_eq!(good.calls, 2);

        // 0x10 is CONNECT: a client sends it and never receives it.
        let mut bad = Script::new(&bytes(&[0x10, 0x02]));
        assert_eq!(read_header(&mut bad), Err(ReadError::BadResponse));
        assert_eq!(
            bad.calls, 1,
            "the length was read before the type was checked"
        );
    }

    /// The two transport failures are distinguished at the type byte and
    /// nowhere else.
    #[test]
    fn nothing_and_failed_are_one_status_past_the_first_byte() {
        let mut nothing = Script::new(&[Received::Nothing]);
        assert_eq!(read_header(&mut nothing), Err(ReadError::NoDataAvailable));

        let mut failed = Script::new(&[Received::Failed]);
        assert_eq!(read_header(&mut failed), Err(ReadError::RecvFailed));

        // One byte in, the distinction is gone.
        let mut late_nothing = Script::new(&[Received::Byte(0x30), Received::Nothing]);
        assert_eq!(read_header(&mut late_nothing), Err(ReadError::BadResponse));

        let mut late_failed = Script::new(&[Received::Byte(0x30), Received::Failed]);
        assert_eq!(read_header(&mut late_failed), Err(ReadError::BadResponse));
    }

    /// A fifth length byte is refused WITHOUT asking the transport for it.
    ///
    /// The multiplier guard runs before the read, so the reader stops at four
    /// length bytes rather than consuming a fifth and then complaining. A
    /// transcription that checked afterwards would read one byte more from a
    /// hostile stream on every malformed packet.
    #[test]
    fn a_fifth_length_byte_is_never_asked_for() {
        let mut script = Script::new(&bytes(&[0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]));
        assert_eq!(read_header(&mut script), Err(ReadError::BadResponse));
        assert_eq!(
            script.calls, 5,
            "the reader asked for a fifth length byte before giving up"
        );
    }

    /// The out-of-range check cannot fire, for the third time in this package.
    ///
    /// A poison that did not fire. The multiplier guard stops the loop after
    /// four bytes, and four bytes of seven bits reach exactly 268,435,455 --
    /// one less than the value the check tests against. Kept because the C
    /// keeps it and a differential arm does not tidy its oracle; pinned here
    /// because the bound is a relationship between two constants either of
    /// which could move.
    #[test]
    fn the_out_of_range_check_cannot_fire() {
        // Four bytes of seven bits, all set: the largest a four-byte
        // variable-byte integer can be.
        let largest = 0x7Fu32 + (0x7F << 7) + (0x7F << 14) + (0x7F << 21);
        assert_eq!(largest, 268_435_455);
        assert_eq!(largest, REMAINING_LENGTH_INVALID - 1);

        // And the reader accepts exactly that, in four bytes.
        let mut script = Script::new(&bytes(&[0x30, 0xFF, 0xFF, 0xFF, 0x7F]));
        let header = read_header(&mut script).expect("the largest legal length");
        assert_eq!(header.remaining_length, largest);
        assert_eq!(script.calls, 5);
    }

    /// The header length is the type byte plus the length bytes.
    #[test]
    fn the_header_length_counts_the_type_byte() {
        for (script, expected_length, expected_header) in [
            (bytes(&[0x30, 0x00]), 0u32, 2usize),
            (bytes(&[0x30, 0x7F]), 127, 2),
            (bytes(&[0x30, 0x80, 0x01]), 128, 3),
            (bytes(&[0x30, 0xFF, 0xFF, 0x7F]), 2_097_151, 4),
            (bytes(&[0x30, 0xFF, 0xFF, 0xFF, 0x7F]), 268_435_455, 5),
        ] {
            let mut transport = Script::new(&script);
            let header = read_header(&mut transport).expect("a valid header");

            assert_eq!(header.remaining_length, expected_length);
            assert_eq!(header.header_length, expected_header);
            assert_eq!(transport.calls, expected_header);
        }
    }
}
