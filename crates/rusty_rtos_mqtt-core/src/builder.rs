//! The buffer an outgoing property section is written into.
//!
//! Everything this crate writes takes a `&mut [u8]` and returns how many bytes
//! it used. A property section is the exception: MQTT 5 lets a caller add
//! properties one at a time, in any order, so the section needs somewhere to
//! accumulate and something to remember what has already been added.
//!
//! That is all `MQTTPropBuilder_t` is — a buffer, a cursor and a bitfield —
//! and `MQTTPropertyBuilder_Init` is the constructor. The writers that fill it
//! are `core_mqtt_prop_serializer.c`, which is the next slice.
//!
//! # Two numbers that must agree
//!
//! The C's constructor takes a **pointer and a length** and never checks them
//! against each other, so an eight-byte buffer with a length of a million is
//! accepted and the first write past the eighth byte is somebody else's memory.
//! [`PropertyBuilder::new`] takes one slice, which carries both, and the
//! question cannot be asked. Two of the C's three refusals therefore have no
//! reachable equivalent here and are absent from the differential rather than
//! failing in it — see [`BuilderError`].
//!
//! # The one place this crate cannot follow the C
//!
//! Every slice of this package so far has transcribed coreMQTT exactly,
//! divergences included, because the C is the oracle. **This one cannot**, and
//! the reason is the whole argument for the project.
//!
//! `addPropUint8`, `addPropUint16` and `addPropUint32` check for `1 + width`
//! bytes, counting the identifier byte they are about to write. `addPropUtf8`
//! checks for `propertyLength + 2` — the two length bytes and the body — and
//! **forgets the identifier**. Given a buffer exactly one byte too small it
//! reports `MQTTSuccess` and writes one byte past the end:
//!
//! ```text
//! add 38 utf8-in-four-bytes cap=4 | content-type()->Success index=5 ... OVERFLOW
//! ```
//!
//! Six public adders route through it — authentication method and data,
//! response topic, correlation data, content type and reason string. This arm
//! counts the identifier, so it answers [`BuilderError::NoMemory`] and writes
//! nothing; `forbid(unsafe)` and a `&mut [u8]` leave it no other option. Three
//! lines of `oracle/propbuild.trace` are therefore marked `OVERFLOW` and are
//! the only lines in the whole package where the two arms disagree on purpose.
//!
//! The differential did not find this by looking for it. It found it because a
//! transcription that could not reproduce the C had to explain why.
//!
//! # A fourth copy of which property may go in which packet
//!
//! [`allowed_properties`] is the table `isValidPropertyInPacketType` holds, and
//! this crate now has four answers to that question — this, the six outgoing
//! validators, the CONNECT context filler, and the incoming deserializers.
//! They do not all agree: see [`allowed_properties`].

use crate::connack::field;
use crate::header::{
    MAX_REMAINING_LENGTH, REMAINING_LENGTH_INVALID, encode_variable_length,
    variable_length_encoded_size,
};
use crate::property::encode_string;
use crate::validate::id;

/// Why a property builder could not be created.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuilderError {
    /// `MQTTBadParameter`: the buffer is empty.
    ///
    /// The C answers this for three conditions: a null builder, a null buffer,
    /// and a zero length. Only the last is reachable through a `&mut [u8]`.
    Empty,
    /// `MQTTBadParameter`: the buffer is at or above
    /// [`REMAINING_LENGTH_INVALID`], so a section filling it could not be
    /// length-prefixed.
    ///
    /// Reachable only with a buffer of 256 MB or more, which is why no case in
    /// `oracle/context.trace` reaches it and
    /// `the_bound_is_the_first_illegal_remaining_length` pins the arithmetic
    /// instead.
    TooLong,
    /// `MQTTBadParameter`: this property is already in the section.
    ///
    /// Every property but the user property may appear once, which the field
    /// bitmask tracks.
    AlreadySet,
    /// `MQTTBadParameter`: this property may not go in that packet type.
    ///
    /// Only raised when the caller says which packet the section is for; the
    /// packet type is optional in the C and optional here.
    NotAllowedInPacket,
    /// `MQTTBadParameter`: the value itself is refused.
    ///
    /// A zero Receive Maximum, Maximum Packet Size, Topic Alias or Subscription
    /// Identifier; a subscription identifier above 268,435,455; an empty
    /// string; a response topic containing a wildcard; or authentication data
    /// with no method yet in the section.
    BadValue,
    /// `MQTTNoMemory`: the buffer has no room for this property.
    NoMemory,
}

/// `MQTTPropBuilder_t`: a section under construction.
#[derive(Debug)]
pub struct PropertyBuilder<'a> {
    buffer: &'a mut [u8],
    at: usize,
    fields: u32,
}

impl<'a> PropertyBuilder<'a> {
    /// `MQTTPropertyBuilder_Init`.
    ///
    /// # Errors
    ///
    /// [`BuilderError::Empty`] for an empty buffer, [`BuilderError::TooLong`]
    /// for one at or above [`REMAINING_LENGTH_INVALID`].
    pub fn new(buffer: &'a mut [u8]) -> Result<Self, BuilderError> {
        if buffer.is_empty() {
            return Err(BuilderError::Empty);
        }

        let Ok(length) = u32::try_from(buffer.len()) else {
            return Err(BuilderError::TooLong);
        };

        if length >= REMAINING_LENGTH_INVALID {
            return Err(BuilderError::TooLong);
        }

        Ok(Self {
            buffer,
            at: 0,
            fields: 0,
        })
    }

    /// How many bytes have been written: the C's `currentIndex`.
    ///
    /// This doubles as the section's **property length**, which is what every
    /// validator and serializer in the crate is handed.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.at
    }

    /// Whether anything has been written yet.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.at == 0
    }

    /// How much room there is: the C's `bufferLength`.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.buffer.len()
    }

    /// Which properties have been added, by bit: the C's `fieldSet`.
    ///
    /// The bit positions are the ones [`connack::field`](crate::connack::field)
    /// already carries — one table, two directions.
    #[must_use]
    pub const fn fields(&self) -> u32 {
        self.fields
    }

    /// The section so far, ready to hand to a serializer.
    #[must_use]
    pub fn section(&self) -> &[u8] {
        self.buffer.get(..self.at).unwrap_or(&[])
    }
}

/// Which properties a packet type may carry, as a mask of [`field`] bits.
///
/// `isValidPropertyInPacketType`, which is a 199-line switch in the C and is
/// `static`, so nothing outside that file can ask it anything except through
/// the adders. It is public here because it is a **fourth** answer to the
/// question three other parts of this crate also answer, and a table nobody can
/// read is a table nobody can check.
///
/// # Where it disagrees with the validators
///
/// PUBLISH is allowed a **Subscription Identifier** here.
/// [`validate_publish_properties`](crate::validate::validate_publish_properties)
/// refuses one, and [MQTT-3.3.4-6] says a client must not send one — the C's
/// own comment beside this arm says "only in server-to-client PUBLISH" and
/// then sets the bit anyway.
///
/// CONNECT carries the **will's** properties as well as its own, because one
/// CONNECT packet holds both sections and one mask serves both.
#[must_use]
pub const fn allowed_properties(packet_type: u8) -> u32 {
    use crate::connack::field as f;
    use crate::header::packet;

    const fn bit(position: u32) -> u32 {
        1u32 << position
    }

    match packet_type {
        packet::CONNECT => {
            bit(f::SESSION_EXPIRY_INTERVAL)
                | bit(f::RECEIVE_MAXIMUM)
                | bit(f::MAX_PACKET_SIZE)
                | bit(f::TOPIC_ALIAS_MAX)
                | bit(f::REQUEST_RESPONSE_INFO)
                | bit(f::REQUEST_PROBLEM_INFO)
                | bit(f::USER_PROP)
                | bit(f::AUTHENTICATION_METHOD)
                | bit(f::AUTHENTICATION_DATA)
                // The will's properties travel in the same packet.
                | bit(f::WILL_DELAY)
                | bit(f::PAYLOAD_FORMAT_INDICATOR)
                | bit(f::MESSAGE_EXPIRY_INTERVAL)
                | bit(f::CONTENT_TYPE)
                | bit(f::RESPONSE_TOPIC)
                | bit(f::CORRELATION_DATA)
        }

        packet::CONNACK => {
            bit(f::SESSION_EXPIRY_INTERVAL)
                | bit(f::RECEIVE_MAXIMUM)
                | bit(f::MAX_QOS)
                | bit(f::RETAIN_AVAILABLE)
                | bit(f::MAX_PACKET_SIZE)
                | bit(f::ASSIGNED_CLIENT_ID)
                | bit(f::TOPIC_ALIAS_MAX)
                | bit(f::REASON_STRING)
                | bit(f::USER_PROP)
                | bit(f::WILDCARD_SUBSCRIPTION_AVAILABLE)
                | bit(f::SUBSCRIPTION_ID_AVAILABLE)
                | bit(f::SHARED_SUBSCRIPTION_AVAILABLE)
                | bit(f::SERVER_KEEP_ALIVE)
                | bit(f::RESPONSE_INFORMATION)
                | bit(f::SERVER_REFERENCE)
                | bit(f::AUTHENTICATION_METHOD)
                | bit(f::AUTHENTICATION_DATA)
        }

        packet::PUBLISH => {
            bit(f::PAYLOAD_FORMAT_INDICATOR)
                | bit(f::MESSAGE_EXPIRY_INTERVAL)
                | bit(f::TOPIC_ALIAS)
                | bit(f::RESPONSE_TOPIC)
                | bit(f::CORRELATION_DATA)
                | bit(f::USER_PROP)
                // [MQTT-3.3.4-6] forbids a CLIENT sending this, and the
                // validator refuses it. Transcribed; see the note above.
                | bit(f::SUBSCRIPTION_ID)
                | bit(f::CONTENT_TYPE)
        }

        packet::PUBACK | packet::PUBREC | packet::PUBREL | packet::PUBCOMP => {
            bit(f::REASON_STRING) | bit(f::USER_PROP)
        }

        packet::SUBSCRIBE => bit(f::SUBSCRIPTION_ID) | bit(f::USER_PROP),

        packet::SUBACK | packet::UNSUBACK => bit(f::REASON_STRING) | bit(f::USER_PROP),

        packet::UNSUBSCRIBE => bit(f::USER_PROP),

        packet::DISCONNECT => {
            bit(f::SESSION_EXPIRY_INTERVAL)
                | bit(f::REASON_STRING)
                | bit(f::USER_PROP)
                | bit(f::SERVER_REFERENCE)
        }

        packet::AUTH => {
            bit(f::AUTHENTICATION_METHOD)
                | bit(f::AUTHENTICATION_DATA)
                | bit(f::REASON_STRING)
                | bit(f::USER_PROP)
        }

        // PINGREQ and PINGRESP have no property section, and everything else
        // falls to the C's `default`, which allows nothing.
        _ => 0,
    }
}

impl<'a> PropertyBuilder<'a> {
    /// The checks every adder makes before it writes anything.
    ///
    /// `needed` is the WHOLE property: the identifier byte and the value.
    ///
    /// **This is where the C is one byte out.** `addPropUint8`, `addPropUint16`
    /// and `addPropUint32` size themselves as `1 + width` and are right;
    /// `addPropUtf8` sizes itself as `propertyLength + 2` — the two length bytes
    /// and the body — and **forgets the identifier**, so it writes one byte past
    /// a buffer that is exactly one byte too small. Six public adders route
    /// through it. See the module note; this arm counts the identifier, which
    /// is why three lines of `oracle/propbuild.trace` are marked `OVERFLOW` and
    /// do not match.
    fn room_for(
        &self,
        needed: usize,
        field: u32,
        packet_type: Option<u8>,
    ) -> Result<(), BuilderError> {
        if self.fields & (1u32 << field) != 0 {
            return Err(BuilderError::AlreadySet);
        }

        if let Some(kind) = packet_type {
            if allowed_properties(kind) & (1u32 << field) == 0 {
                return Err(BuilderError::NotAllowedInPacket);
            }
        }

        // `capacity - at` cannot underflow: `at` only ever advances by an
        // amount this check has already allowed.
        if self.capacity().saturating_sub(self.at) < needed {
            return Err(BuilderError::NoMemory);
        }

        // The C's second bound, on the largest a property section may be. It
        // is a different limit from the buffer's and it is checked separately.
        let Ok(end) = u32::try_from(self.at.saturating_add(needed)) else {
            return Err(BuilderError::TooLong);
        };

        if end > REMAINING_LENGTH_INVALID {
            return Err(BuilderError::TooLong);
        }

        Ok(())
    }

    /// Write `bytes` at the cursor and mark `field` as set.
    fn put(&mut self, id: u8, bytes: &[u8], field: u32) -> Result<(), BuilderError> {
        let end = self.at.saturating_add(1).saturating_add(bytes.len());
        let Some(slot) = self.buffer.get_mut(self.at..end) else {
            return Err(BuilderError::NoMemory);
        };
        let Some((head, body)) = slot.split_first_mut() else {
            return Err(BuilderError::NoMemory);
        };

        *head = id;
        body.copy_from_slice(bytes);

        self.at = end;
        self.fields |= 1u32 << field;
        Ok(())
    }

    /// `addPropUint8`, which the C asserts is only ever called with 0 or 1.
    fn add_u8(
        &mut self,
        value: bool,
        id: u8,
        field: u32,
        packet_type: Option<u8>,
    ) -> Result<(), BuilderError> {
        self.room_for(2, field, packet_type)?;
        self.put(id, &[u8::from(value)], field)
    }

    /// `addPropUint16`.
    fn add_u16(
        &mut self,
        value: u16,
        id: u8,
        field: u32,
        packet_type: Option<u8>,
    ) -> Result<(), BuilderError> {
        self.room_for(3, field, packet_type)?;
        self.put(id, &value.to_be_bytes(), field)
    }

    /// `addPropUint32`.
    fn add_u32(
        &mut self,
        value: u32,
        id: u8,
        field: u32,
        packet_type: Option<u8>,
    ) -> Result<(), BuilderError> {
        self.room_for(5, field, packet_type)?;
        self.put(id, &value.to_be_bytes(), field)
    }

    /// `addPropUtf8`: a two-byte length and the bytes.
    ///
    /// The empty string is refused, which is the C's rule and not MQTT's.
    fn add_utf8(
        &mut self,
        value: &[u8],
        id: u8,
        field: u32,
        packet_type: Option<u8>,
    ) -> Result<(), BuilderError> {
        if value.is_empty() {
            return Err(BuilderError::BadValue);
        }

        let Ok(length) = u16::try_from(value.len()) else {
            return Err(BuilderError::BadValue);
        };

        // 1 identifier + 2 length + the body. The C's is one less.
        self.room_for(usize::from(length).saturating_add(3), field, packet_type)?;

        let start = self.at;
        let end = start.saturating_add(3).saturating_add(usize::from(length));
        let Some(slot) = self.buffer.get_mut(start..end) else {
            return Err(BuilderError::NoMemory);
        };
        let Some((head, rest)) = slot.split_first_mut() else {
            return Err(BuilderError::NoMemory);
        };
        let Some((prefix, body)) = rest.split_at_mut_checked(2) else {
            return Err(BuilderError::NoMemory);
        };

        *head = id;
        prefix.copy_from_slice(&length.to_be_bytes());
        body.copy_from_slice(value);

        self.at = end;
        self.fields |= 1u32 << field;
        Ok(())
    }

    /// `MQTTPropAdd_SessionExpiry`.
    ///
    /// # Errors
    ///
    /// See [`BuilderError`].
    pub fn session_expiry(
        &mut self,
        seconds: u32,
        packet_type: Option<u8>,
    ) -> Result<(), BuilderError> {
        self.add_u32(
            seconds,
            id::SESSION_EXPIRY,
            field::SESSION_EXPIRY_INTERVAL,
            packet_type,
        )
    }

    /// `MQTTPropAdd_ReceiveMax`. Zero is a protocol error.
    ///
    /// # Errors
    ///
    /// [`BuilderError::BadValue`] for zero, else see [`BuilderError`].
    pub fn receive_max(&mut self, value: u16, packet_type: Option<u8>) -> Result<(), BuilderError> {
        if value == 0 {
            return Err(BuilderError::BadValue);
        }

        self.add_u16(value, id::RECEIVE_MAX, field::RECEIVE_MAXIMUM, packet_type)
    }

    /// `MQTTPropAdd_MaxPacketSize`. Zero is a protocol error.
    ///
    /// # Errors
    ///
    /// [`BuilderError::BadValue`] for zero, else see [`BuilderError`].
    pub fn max_packet_size(
        &mut self,
        value: u32,
        packet_type: Option<u8>,
    ) -> Result<(), BuilderError> {
        if value == 0 {
            return Err(BuilderError::BadValue);
        }

        self.add_u32(
            value,
            id::MAX_PACKET_SIZE,
            field::MAX_PACKET_SIZE,
            packet_type,
        )
    }

    /// `MQTTPropAdd_TopicAliasMax`. Zero is legal here — it means "send me no
    /// aliases" — where a Topic Alias of zero is not.
    ///
    /// # Errors
    ///
    /// See [`BuilderError`].
    pub fn topic_alias_max(
        &mut self,
        value: u16,
        packet_type: Option<u8>,
    ) -> Result<(), BuilderError> {
        self.add_u16(
            value,
            id::TOPIC_ALIAS_MAX,
            field::TOPIC_ALIAS_MAX,
            packet_type,
        )
    }

    /// `MQTTPropAdd_RequestRespInfo`.
    ///
    /// # Errors
    ///
    /// See [`BuilderError`].
    pub fn request_response_info(
        &mut self,
        value: bool,
        packet_type: Option<u8>,
    ) -> Result<(), BuilderError> {
        self.add_u8(
            value,
            id::REQUEST_RESPONSE,
            field::REQUEST_RESPONSE_INFO,
            packet_type,
        )
    }

    /// `MQTTPropAdd_RequestProbInfo`.
    ///
    /// # Errors
    ///
    /// See [`BuilderError`].
    pub fn request_problem_info(
        &mut self,
        value: bool,
        packet_type: Option<u8>,
    ) -> Result<(), BuilderError> {
        self.add_u8(
            value,
            id::REQUEST_PROBLEM,
            field::REQUEST_PROBLEM_INFO,
            packet_type,
        )
    }

    /// `MQTTPropAdd_AuthMethod`.
    ///
    /// # Errors
    ///
    /// See [`BuilderError`].
    pub fn auth_method(
        &mut self,
        method: &[u8],
        packet_type: Option<u8>,
    ) -> Result<(), BuilderError> {
        self.add_utf8(
            method,
            id::AUTH_METHOD,
            field::AUTHENTICATION_METHOD,
            packet_type,
        )
    }

    /// `MQTTPropAdd_AuthData`, which refuses to go in without a method.
    ///
    /// The C's comment is explicit that this is its own rule rather than the
    /// specification's — [MQTT-3.1.2-32] makes it an error in the finished
    /// packet, and coreMQTT enforces it a property early. Where the outgoing
    /// CONNECT **validator** checks the same rule after its whole walk, so
    /// either order passes there, this refuses the data outright.
    ///
    /// # Errors
    ///
    /// [`BuilderError::BadValue`] if no authentication method is in the section
    /// yet, else see [`BuilderError`].
    pub fn auth_data(&mut self, data: &[u8], packet_type: Option<u8>) -> Result<(), BuilderError> {
        if self.fields & (1u32 << field::AUTHENTICATION_METHOD) == 0 {
            return Err(BuilderError::BadValue);
        }

        self.add_utf8(data, id::AUTH_DATA, field::AUTHENTICATION_DATA, packet_type)
    }

    /// `MQTTPropAdd_PayloadFormat`.
    ///
    /// A `bool`, so the value cannot be the 2 that
    /// [`validate_publish_properties`](crate::validate::validate_publish_properties)
    /// lets through — the type makes the divergence unreachable from this side.
    ///
    /// # Errors
    ///
    /// See [`BuilderError`].
    pub fn payload_format(
        &mut self,
        utf8: bool,
        packet_type: Option<u8>,
    ) -> Result<(), BuilderError> {
        self.add_u8(
            utf8,
            id::PAYLOAD_FORMAT,
            field::PAYLOAD_FORMAT_INDICATOR,
            packet_type,
        )
    }

    /// `MQTTPropAdd_MessageExpiry`.
    ///
    /// # Errors
    ///
    /// See [`BuilderError`].
    pub fn message_expiry(
        &mut self,
        seconds: u32,
        packet_type: Option<u8>,
    ) -> Result<(), BuilderError> {
        self.add_u32(
            seconds,
            id::MESSAGE_EXPIRY,
            field::MESSAGE_EXPIRY_INTERVAL,
            packet_type,
        )
    }

    /// `MQTTPropAdd_WillDelayInterval`.
    ///
    /// # Errors
    ///
    /// See [`BuilderError`].
    pub fn will_delay(
        &mut self,
        seconds: u32,
        packet_type: Option<u8>,
    ) -> Result<(), BuilderError> {
        self.add_u32(seconds, id::WILL_DELAY, field::WILL_DELAY, packet_type)
    }

    /// `MQTTPropAdd_TopicAlias`. Zero is a protocol error.
    ///
    /// **This is the rule the outgoing PUBLISH validator is missing**: the
    /// builder refuses a zero alias and
    /// [`validate_publish_properties`](crate::validate::validate_publish_properties)
    /// accepts one. A section built here can never carry it; a section built
    /// by hand can.
    ///
    /// # Errors
    ///
    /// [`BuilderError::BadValue`] for zero, else see [`BuilderError`].
    pub fn topic_alias(&mut self, alias: u16, packet_type: Option<u8>) -> Result<(), BuilderError> {
        if alias == 0 {
            return Err(BuilderError::BadValue);
        }

        self.add_u16(alias, id::TOPIC_ALIAS, field::TOPIC_ALIAS, packet_type)
    }

    /// `MQTTPropAdd_ResponseTopic`, which refuses wildcards.
    ///
    /// §4.7: a response topic is a topic NAME, not a filter, so `#` and `+`
    /// have no business in it. The only adder that inspects its string.
    ///
    /// # Errors
    ///
    /// [`BuilderError::BadValue`] for an empty topic or one containing `#` or
    /// `+`, else see [`BuilderError`].
    pub fn response_topic(
        &mut self,
        topic: &[u8],
        packet_type: Option<u8>,
    ) -> Result<(), BuilderError> {
        if topic.contains(&b'#') || topic.contains(&b'+') {
            return Err(BuilderError::BadValue);
        }

        self.add_utf8(
            topic,
            id::RESPONSE_TOPIC,
            field::RESPONSE_TOPIC,
            packet_type,
        )
    }

    /// `MQTTPropAdd_CorrelationData`: binary, in a string's wire shape.
    ///
    /// # Errors
    ///
    /// See [`BuilderError`].
    pub fn correlation_data(
        &mut self,
        data: &[u8],
        packet_type: Option<u8>,
    ) -> Result<(), BuilderError> {
        self.add_utf8(
            data,
            id::CORRELATION_DATA,
            field::CORRELATION_DATA,
            packet_type,
        )
    }

    /// `MQTTPropAdd_ContentType`.
    ///
    /// # Errors
    ///
    /// See [`BuilderError`].
    pub fn content_type(
        &mut self,
        content_type: &[u8],
        packet_type: Option<u8>,
    ) -> Result<(), BuilderError> {
        self.add_utf8(
            content_type,
            id::CONTENT_TYPE,
            field::CONTENT_TYPE,
            packet_type,
        )
    }

    /// `MQTTPropAdd_ReasonString`.
    ///
    /// # Errors
    ///
    /// See [`BuilderError`].
    pub fn reason_string(
        &mut self,
        reason: &[u8],
        packet_type: Option<u8>,
    ) -> Result<(), BuilderError> {
        self.add_utf8(reason, id::REASON_STRING, field::REASON_STRING, packet_type)
    }

    /// `MQTTPropAdd_SubscriptionId`: the one variable-length property.
    ///
    /// # Errors
    ///
    /// [`BuilderError::BadValue`] for zero or for a value above
    /// 268,435,455, else see [`BuilderError`].
    pub fn subscription_id(
        &mut self,
        id: u32,
        packet_type: Option<u8>,
    ) -> Result<(), BuilderError> {
        // §3.8.2.1.2, and the adder checks BOTH ends where the incoming
        // deserializer checks neither.
        if id == 0 || id > MAX_REMAINING_LENGTH {
            return Err(BuilderError::BadValue);
        }

        let width = variable_length_encoded_size(id) as usize;
        self.room_for(width.saturating_add(1), field::SUBSCRIPTION_ID, packet_type)?;

        let start = self.at;
        let end = start.saturating_add(1).saturating_add(width);
        let Some(slot) = self.buffer.get_mut(start..end) else {
            return Err(BuilderError::NoMemory);
        };
        let Some((head, body)) = slot.split_first_mut() else {
            return Err(BuilderError::NoMemory);
        };

        *head = crate::validate::id::SUBSCRIPTION_ID;
        let written = encode_variable_length(body, id);

        if written != width {
            return Err(BuilderError::NoMemory);
        }

        self.at = end;
        self.fields |= 1u32 << field::SUBSCRIPTION_ID;
        Ok(())
    }

    /// `MQTTPropAdd_UserProp`: a key and a value, and the one property that may
    /// repeat.
    ///
    /// It sets no field bit, which is what makes the repeat legal — and means
    /// the only thing stopping a section filling with user properties is the
    /// buffer.
    ///
    /// # Errors
    ///
    /// [`BuilderError::BadValue`] if either half is empty or longer than
    /// 65,535 bytes, else see [`BuilderError`].
    pub fn user_property(
        &mut self,
        key: &[u8],
        value: &[u8],
        packet_type: Option<u8>,
    ) -> Result<(), BuilderError> {
        if key.is_empty() || value.is_empty() {
            return Err(BuilderError::BadValue);
        }

        let (Ok(key_length), Ok(value_length)) =
            (u16::try_from(key.len()), u16::try_from(value.len()))
        else {
            return Err(BuilderError::BadValue);
        };

        if let Some(kind) = packet_type {
            if allowed_properties(kind) & (1u32 << field::USER_PROP) == 0 {
                return Err(BuilderError::NotAllowedInPacket);
            }
        }

        // 1 identifier + 2 + key + 2 + value. This one the C counts correctly.
        let needed = usize::from(key_length)
            .saturating_add(usize::from(value_length))
            .saturating_add(5);

        if self.capacity().saturating_sub(self.at) < needed {
            return Err(BuilderError::NoMemory);
        }

        let Ok(end) = u32::try_from(self.at.saturating_add(needed)) else {
            return Err(BuilderError::TooLong);
        };

        if end > REMAINING_LENGTH_INVALID {
            return Err(BuilderError::TooLong);
        }

        let start = self.at;
        let finish = start.saturating_add(needed);
        let Some(slot) = self.buffer.get_mut(start..finish) else {
            return Err(BuilderError::NoMemory);
        };
        let Some((head, rest)) = slot.split_first_mut() else {
            return Err(BuilderError::NoMemory);
        };

        *head = crate::validate::id::USER_PROPERTY;

        let written = encode_string(rest, Some(key), key_length);

        if written == 0 {
            return Err(BuilderError::NoMemory);
        }

        let Some(tail) = rest.get_mut(written..) else {
            return Err(BuilderError::NoMemory);
        };

        if encode_string(tail, Some(value), value_length) == 0 {
            return Err(BuilderError::NoMemory);
        }

        // No field bit: a user property may appear as often as it likes.
        self.at = finish;
        Ok(())
    }
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

    /// **The size check is advisory; the slice is the guarantee.**
    ///
    /// This began as two poisons that did not fire. Sizing a four-byte property
    /// as 4 instead of 5, and a string property as `length + 2` instead of
    /// `length + 3` — which is *exactly* the arithmetic that makes
    /// `addPropUtf8` write past its buffer — changed no answer here. Both still
    /// refuse; they just refuse from `get_mut` rather than from the size check.
    ///
    /// So the property is worth stating: this crate has **two** independent
    /// bounds on every write, and the second one is not code that can be got
    /// wrong. A C builder has one, and a one-byte slip in it is a one-byte
    /// out-of-bounds write.
    ///
    /// Proven by sweeping every adder against every buffer size it could
    /// plausibly meet, and asserting the two things that must hold whatever the
    /// arithmetic says: the cursor never passes the buffer, and a refusal
    /// writes nothing.
    #[test]
    fn no_arithmetic_error_can_write_past_the_slice() {
        for capacity in 1..24usize {
            for which in 0..18usize {
                let mut bytes = vec![0xAAu8; capacity];
                let before = bytes.clone();
                let mut builder = PropertyBuilder::new(&mut bytes).unwrap();

                let long = [b'x'; 12];
                let result = match which {
                    0 => builder.session_expiry(30, None),
                    1 => builder.receive_max(10, None),
                    2 => builder.max_packet_size(1024, None),
                    3 => builder.topic_alias_max(5, None),
                    4 => builder.request_response_info(true, None),
                    5 => builder.request_problem_info(true, None),
                    6 => builder.auth_method(&long, None),
                    7 => builder.payload_format(true, None),
                    8 => builder.message_expiry(60, None),
                    9 => builder.will_delay(15, None),
                    10 => builder.topic_alias(7, None),
                    11 => builder.response_topic(&long, None),
                    12 => builder.correlation_data(&long, None),
                    13 => builder.content_type(&long, None),
                    14 => builder.reason_string(&long, None),
                    15 => builder.subscription_id(70_000, None),
                    16 => builder.user_property(b"key", b"value", None),
                    _ => builder.auth_method(b"a", None),
                };

                let written = builder.len();
                let capacity_now = builder.capacity();

                assert!(
                    written <= capacity_now,
                    "adder {which} wrote {written} bytes into {capacity_now}"
                );

                if result.is_err() {
                    assert_eq!(written, 0, "adder {which} refused and still moved on");
                    assert_eq!(
                        bytes, before,
                        "adder {which} refused and still changed the buffer"
                    );
                }
            }
        }
    }

    /// The bound is the first illegal remaining length, and nothing below it.
    ///
    /// The C checks `length >= MQTT_REMAINING_LENGTH_INVALID` on a length
    /// ARGUMENT, which a caller can set to anything. Here it is the buffer's
    /// own length, so reaching it needs a 256 MB buffer and no differential
    /// case can. The arithmetic is pinned instead — the same treatment the
    /// package gives every bound it cannot reach with a plausible input.
    #[test]
    fn the_bound_is_the_first_illegal_remaining_length() {
        assert_eq!(REMAINING_LENGTH_INVALID, 268_435_456);
        assert_eq!(
            REMAINING_LENGTH_INVALID - 1,
            268_435_455,
            "the largest legal remaining length is no longer one below the bound"
        );

        // The bound is LIVE code, not dead: a `usize` holds it on every target
        // this crate builds for, so a big enough buffer really would reach it.
        // What makes it untestable is the buffer -- 256 MB, which no part this
        // crate targets has and no test should allocate. So it gets the same
        // treatment as every other bound the package cannot reach with a
        // plausible input: the arithmetic is pinned and the differential
        // stays quiet about it.
        assert!(usize::try_from(REMAINING_LENGTH_INVALID).is_ok());
    }

    /// A builder starts empty, and `len` is the property length.
    #[test]
    fn a_new_builder_is_empty_and_its_length_is_the_section_length() {
        let mut bytes = [0u8; 8];
        let builder = PropertyBuilder::new(&mut bytes).unwrap();

        assert_eq!(builder.len(), 0);
        assert!(builder.is_empty());
        assert_eq!(builder.capacity(), 8);
        assert_eq!(builder.fields(), 0);
        assert_eq!(builder.section(), &[] as &[u8]);
    }

    /// An empty buffer is refused, because a section needs somewhere to go.
    #[test]
    fn an_empty_buffer_is_refused() {
        let mut nothing: [u8; 0] = [];
        assert_eq!(
            PropertyBuilder::new(&mut nothing).map(|b| b.capacity()),
            Err(BuilderError::Empty)
        );
    }
}
