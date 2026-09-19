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

//! # And a second reader, over a buffer
//!
//! coreMQTT reads this same header **twice**, in two functions. This one pulls
//! it off a callback;
//! [`process_incoming_packet_type_and_length`](crate::header::process_incoming_packet_type_and_length)
//! takes it out of a buffer the caller has already filled, and was remade with
//! the fixed-header codec. Both are driven over the same bytes in
//! `oracle/context.trace`, and they part company in one place that matters:
//!
//! **only the buffered one can say "not yet".** Given a type byte and no length
//! behind it, it answers
//! [`HeaderError::NeedMoreBytes`](crate::header::HeaderError::NeedMoreBytes)
//! and [`read_header`] answers [`ReadError::BadResponse`] — for all 168 packet
//! types a client may receive, which the trace's `truncated-sweep` counts. So a
//! non-blocking transport that hands over a header one byte at a time gets
//! "malformed" from one reader and "call me again" from the other.
//!
//! That is not a difference this crate invented; it is transcribed, and it is
//! written up in `docs/upstream/`.

use crate::header::{
    REMAINING_LENGTH_INVALID, incoming_packet_valid, variable_length_encoded_size,
};

/// What a transport gave back when asked for one byte.
///
/// The C's `readFunc` returns an `int32_t`: 1 for a byte, 0 for nothing yet,
/// and anything else for a failure. Those are the only three the header reader
/// distinguishes, so they are the three here.
///
/// This is what [`Transport::recv_one`] answers, and `recv_one` is a **provided
/// method** over [`Transport::recv`] that asks for exactly one byte — because
/// that is what the C is. There is one `recv` pointer; the header reader calls
/// it with a length of 1 and `recvExact` calls it with the rest of the packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Received {
    /// One byte arrived.
    Byte(u8),
    /// Nothing yet. Only at the type byte does this mean "try again later".
    Nothing,
    /// The transport failed.
    Failed,
}

/// What a transport did with a request for several bytes.
///
/// `recvExact` asks for the whole remainder of a packet and takes what it gets,
/// so unlike the header reader it needs a **count** rather than a byte. Zero is
/// "nothing yet, and nothing is wrong", which is the answer that starts its
/// polling timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recv {
    /// This many bytes arrived, at the front of the buffer. May be fewer than
    /// asked for.
    Bytes(usize),
    /// None arrived, and nothing is wrong.
    Nothing,
    /// The transport failed.
    Failed,
}

/// What a transport did with the bytes it was offered.
///
/// The C's `send` returns an `int32_t`: a positive count is that many bytes
/// accepted, zero is "not now", and a negative is a failure. Those are the
/// three the senders distinguish, so they are the three here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sent {
    /// This many bytes were accepted. May be fewer than offered, and the
    /// sender must offer the rest from the right offset.
    Bytes(usize),
    /// None were accepted, and nothing is wrong. Try again.
    Nothing,
    /// The transport failed.
    Failed,
}

/// A socket, a test script, a ring buffer: something bytes go to and come from.
///
/// The C's `TransportInterface_t` carries `recv`, `send`, an optional `writev`
/// and an implementation-defined context. The context is whatever implements
/// this trait, and `writev` is a PROVIDED METHOD whose default is the fallback
/// the C reaches for when the pointer is null.
pub trait Transport {
    /// Ask for up to `into.len()` bytes, and say how many arrived.
    ///
    /// A transport may deliver fewer than it is asked for; delivering MORE is a
    /// bug in the transport, and the C asserts on it.
    fn recv(&mut self, into: &mut [u8]) -> Recv;

    /// Ask for exactly one byte.
    ///
    /// The C has **one** receive pointer and the header reader calls it with a
    /// length of 1, so this is not a second operation — it is [`recv`](Self::recv)
    /// with a one-byte buffer, and the default says exactly that. A transport
    /// that wants to answer a single byte specially may override it, and the
    /// two must then agree.
    fn recv_one(&mut self) -> Received {
        let mut byte = [0u8; 1];

        match self.recv(&mut byte) {
            Recv::Bytes(0) | Recv::Nothing => Received::Nothing,
            Recv::Bytes(_) => match byte.first() {
                Some(value) => Received::Byte(*value),
                None => Received::Nothing,
            },
            Recv::Failed => Received::Failed,
        }
    }

    /// Offer `bytes`, and say how many were taken.
    ///
    /// A transport may take fewer than it is offered; taking MORE than it is
    /// offered is a bug in the transport, and the C asserts on it.
    fn send(&mut self, bytes: &[u8]) -> Sent;

    /// `TransportInterface_t.writev`: offer SEVERAL buffers in one call.
    ///
    /// This is the one part of the transport the C allows to be **absent**, and
    /// its absence is a null pointer that `sendMessageVector` tests on every
    /// turn of its loop:
    ///
    /// ```c
    /// if( pContext->transportInterface.writev != NULL ) { writev( ..., iterator, vectorsToBeSent ); }
    /// else                                              { send( ..., iterator->iov_base, iterator->iov_len ); }
    /// ```
    ///
    /// So the absent case is not a refusal, it is a **fallback** — and a
    /// fallback is what a default method is. Implement this and a gather goes
    /// out in one call; leave it and the library offers one vector at a time,
    /// which is exactly what the C does with a null pointer there.
    ///
    /// `parts` is the vectors that are **still outstanding**, with the first
    /// one already advanced past whatever a previous call took.
    ///
    /// # Why this is worth having at all
    ///
    /// It is the only thing that makes a GATHER BOUNDARY observable. With the
    /// per-vector fallback, a packet split into two gathers and the same packet
    /// split into three produce the identical sequence of calls, because the
    /// vectors are offered one at a time in the same order either way.
    fn writev(&mut self, parts: &[&[u8]]) -> Sent {
        match parts.first() {
            Some(first) => self.send(first),
            None => Sent::Nothing,
        }
    }
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
    /// number is free. Its buffered twin
    /// `MQTT_ProcessIncomingPacketTypeAndLength` **does** set it — see
    /// [`PacketHeader::header_length`](crate::header::PacketHeader) — and the
    /// `dual` lines of `oracle/context.trace` compare it there. It stays out
    /// of this arm's trace, where the C has nothing to compare it against, and
    /// `the_header_length_counts_the_type_byte` pins it.
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
        /// These unit tests only read. A script asked to send is a test
        /// driving the wrong function, so it says so.
        fn send(&mut self, _bytes: &[u8]) -> Sent {
            panic!("a reader unit test asked the transport to send");
        }

        fn recv(&mut self, into: &mut [u8]) -> Recv {
            self.calls += 1;
            let step = self.steps.get(self.at).copied();
            self.at += 1;

            // Running off the end is a failure, not a hang: a reader that asks
            // for more than the script describes should be caught, not fed.
            match step.unwrap_or(Received::Failed) {
                Received::Byte(value) => match into.first_mut() {
                    Some(slot) => {
                        *slot = value;
                        Recv::Bytes(1)
                    }
                    None => Recv::Bytes(0),
                },
                Received::Nothing => Recv::Nothing,
                Received::Failed => Recv::Failed,
            }
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
