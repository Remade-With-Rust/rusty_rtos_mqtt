//! Walking a property section back, one property at a time.
//!
//! [`builder`](crate::builder) assembles a section; this reads one. Between
//! them they are the two halves of coreMQTT's `MQTTPropBuilder_t`: the same
//! buffer, written by one and walked by the other.
//!
//! A reader of a property section has a problem the writer does not. It does
//! not know what is in there. MQTT 5 lets a broker send any property that
//! packet type allows, in any order, and an application usually wants two or
//! three of them. So the shape is a **cursor**: ask what is next, take it if
//! you want it, [`skip`](PropertyCursor::skip) it if you do not, and stop when
//! the section runs out.
//!
//! # Every getter demands one identifier
//!
//! [`PropertyCursor::receive_max`] reads a Receive Maximum and refuses
//! everything else — it does not search for one. That makes a getter a
//! **assertion about what is under the cursor**, and a section read in the
//! wrong order fails rather than returning a number from the wrong property.
//! The differential sweeps all twenty-two getters over all 256 identifiers:
//! each accepts exactly one, which is a result and is asserted as one.
//!
//! # Two tables over one alphabet, and this time they agree
//!
//! [`next_type`](PropertyCursor::next_type) answers whether a byte is a
//! property identifier at all. [`skip`](PropertyCursor::skip) maps the same
//! byte to a **width**, so it can step past it. They are written separately in
//! the C — twenty-seven `case` labels in one, five groups in the other — and a
//! byte the first accepts and the second cannot skip would strand a reader half
//! way through a section.
//!
//! Both are swept over all 256 bytes and both accepted sets are printed. They
//! are identical. After five tables in this library that disagreed with a
//! sibling, that is worth saying out loud, and
//! `the_two_tables_accept_the_same_alphabet` says it.
//!
//! # The cursor is the answer
//!
//! Every one of these advances a caller-owned index, so what matters is not
//! only the value but **where the cursor ends**. A getter that returned the
//! right number and left the cursor one byte out would pass a value-only
//! comparison and desynchronise every call after it. The differential prints
//! the index after every call.

use crate::connack::property as server;
use crate::header::variable_length_encoded_size;
use crate::property::{PropertyError, PropertyReader, decode_variable_length};
use crate::validate::id as client;

/// Why a property could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorError {
    /// `MQTTBadParameter`: the property under the cursor is not the one asked
    /// for, or its identifier is not one MQTT 5 defines.
    BadParameter,
    /// `MQTTBadResponse`: the identifier is right and the value is malformed —
    /// truncated, or a non-minimally encoded variable-length integer.
    BadResponse,
    /// `MQTTEndOfProperties`: the cursor is at or past the end of the section.
    ///
    /// **Not an error**, and the C is careful about that: it logs at debug
    /// level where every other refusal logs an error. It is how a walk ends.
    EndOfProperties,
}

impl From<PropertyError> for CursorError {
    fn from(_: PropertyError) -> Self {
        Self::BadResponse
    }
}

/// How wide a property's value is, once its identifier has been read.
///
/// The C spells this as five groups of `case` labels inside
/// `MQTT_SkipNextProperty`; naming it makes the same table usable by
/// [`PropertyCursor::next_type`] and by the skipper, which is what makes them
/// impossible to disagree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Width {
    /// One byte.
    Byte,
    /// Two bytes, big-endian.
    TwoBytes,
    /// Four bytes, big-endian.
    FourBytes,
    /// A two-byte length and that many bytes.
    String,
    /// Two strings: a key and a value.
    Pair,
    /// A variable-length integer, one to four bytes.
    Variable,
}

/// The width of the property `id` introduces, or `None` if it introduces none.
///
/// **One table, two callers.** The C has two — the identifier list in
/// `MQTT_GetNextPropertyType` and the width groups in
/// `MQTT_SkipNextProperty` — and they happen to agree. Here they cannot
/// disagree, because `next_type` refuses exactly what this returns `None` for.
#[must_use]
pub const fn width_of(id: u8) -> Option<Width> {
    match id {
        client::SESSION_EXPIRY
        | client::MAX_PACKET_SIZE
        | client::WILL_DELAY
        | client::MESSAGE_EXPIRY => Some(Width::FourBytes),

        client::RECEIVE_MAX
        | client::TOPIC_ALIAS_MAX
        | client::TOPIC_ALIAS
        | server::SERVER_KEEP_ALIVE => Some(Width::TwoBytes),

        client::REQUEST_PROBLEM
        | client::REQUEST_RESPONSE
        | client::PAYLOAD_FORMAT
        | server::MAX_QOS
        | server::RETAIN_AVAILABLE
        | server::WILDCARD_AVAILABLE
        | server::SUBSCRIPTION_ID_AVAILABLE
        | server::SHARED_SUB_AVAILABLE => Some(Width::Byte),

        client::AUTH_METHOD
        | client::CONTENT_TYPE
        | client::RESPONSE_TOPIC
        | server::ASSIGNED_CLIENT_ID
        | client::REASON_STRING
        | server::RESPONSE_INFO
        | server::SERVER_REFERENCE
        | client::AUTH_DATA
        | client::CORRELATION_DATA => Some(Width::String),

        client::USER_PROPERTY => Some(Width::Pair),

        client::SUBSCRIPTION_ID => Some(Width::Variable),

        _ => None,
    }
}

/// A cursor over a property section.
///
/// The C keeps the section in an `MQTTPropBuilder_t` and the cursor in a
/// `size_t` the caller owns; the two are joined here, because a cursor into a
/// section it is not part of is the same hazard as a pointer and a length that
/// must agree.
#[derive(Debug, Clone)]
pub struct PropertyCursor<'a> {
    section: &'a [u8],
    at: usize,
}

impl<'a> PropertyCursor<'a> {
    /// A cursor at the start of `section`.
    #[must_use]
    pub const fn new(section: &'a [u8]) -> Self {
        Self { section, at: 0 }
    }

    /// Where the cursor is.
    #[must_use]
    pub const fn position(&self) -> usize {
        self.at
    }

    /// Move the cursor, as a C caller does by assigning to its `size_t`.
    ///
    /// A position past the end is not refused here — it is refused by the next
    /// read, with [`CursorError::EndOfProperties`], which is what the C does.
    pub const fn seek(&mut self, at: usize) {
        self.at = at;
    }

    /// Whether anything is left.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.at >= self.section.len()
    }

    /// `checkPropBuilderParams`, and the identifier byte under the cursor.
    fn identifier(&self) -> Result<u8, CursorError> {
        if self.at >= self.section.len() {
            return Err(CursorError::EndOfProperties);
        }

        self.section
            .get(self.at)
            .copied()
            .ok_or(CursorError::EndOfProperties)
    }

    /// A reader over everything after the identifier byte, with the budget the
    /// C computes: `section length - cursor - 1`.
    fn body(&self) -> Result<PropertyReader<'a>, CursorError> {
        let start = self.at.saturating_add(1);
        let Some(rest) = self.section.get(start..) else {
            return Err(CursorError::EndOfProperties);
        };
        let Ok(budget) = u32::try_from(rest.len()) else {
            return Err(CursorError::BadResponse);
        };

        Ok(PropertyReader::new(rest, budget))
    }

    /// `MQTT_GetNextPropertyType`: what is under the cursor, without moving it.
    ///
    /// # Errors
    ///
    /// [`CursorError::EndOfProperties`] at the end of the section, and
    /// [`CursorError::BadParameter`] for a byte that is not a property
    /// identifier.
    pub fn next_type(&self) -> Result<u8, CursorError> {
        let id = self.identifier()?;

        if width_of(id).is_none() {
            return Err(CursorError::BadParameter);
        }

        Ok(id)
    }

    /// `MQTT_SkipNextProperty`: step past whatever is under the cursor.
    ///
    /// The only way to reach the property after one you do not want.
    ///
    /// # Errors
    ///
    /// [`CursorError::EndOfProperties`] at the end of the section,
    /// [`CursorError::BadParameter`] for an unknown identifier, and
    /// [`CursorError::BadResponse`] for a malformed value.
    pub fn skip(&mut self) -> Result<(), CursorError> {
        let id = self.identifier()?;
        let Some(width) = width_of(id) else {
            return Err(CursorError::BadParameter);
        };

        let mut reader = self.body()?;
        let mut used = false;

        match width {
            Width::Byte => {
                let _ = reader.u8(&mut used)?;
            }
            Width::TwoBytes => {
                let _ = reader.u16(&mut used)?;
            }
            Width::FourBytes => {
                let _ = reader.u32(&mut used)?;
            }
            Width::String => {
                let _ = reader.utf8(&mut used)?;
            }
            Width::Pair => {
                let _ = reader.user_property()?;
            }
            Width::Variable => {
                let _ = self.variable()?;
                return Ok(());
            }
        }

        self.at = self.at.saturating_add(1).saturating_add(reader.position());
        Ok(())
    }

    /// The subscription identifier's variable-length integer, which the C
    /// decodes inline in three places rather than through a primitive.
    fn variable(&mut self) -> Result<u32, CursorError> {
        let start = self.at.saturating_add(1);
        let Some(rest) = self.section.get(start..) else {
            return Err(CursorError::EndOfProperties);
        };

        let value = decode_variable_length(rest)?;
        let width = variable_length_encoded_size(value) as usize;

        self.at = start.saturating_add(width);
        Ok(value)
    }

    /// Read one property of a known width, demanding `expected`.
    fn one<T>(
        &mut self,
        expected: u8,
        read: impl FnOnce(&mut PropertyReader<'a>, &mut bool) -> Result<T, PropertyError>,
    ) -> Result<T, CursorError> {
        if self.identifier()? != expected {
            return Err(CursorError::BadParameter);
        }

        let mut reader = self.body()?;
        let mut used = false;
        let value = read(&mut reader, &mut used)?;

        self.at = self.at.saturating_add(1).saturating_add(reader.position());
        Ok(value)
    }

    /// `MQTTPropGet_UserProp`: the key and the value.
    ///
    /// # Errors
    ///
    /// See [`CursorError`].
    pub fn user_property(&mut self) -> Result<(&'a [u8], &'a [u8]), CursorError> {
        if self.identifier()? != client::USER_PROPERTY {
            return Err(CursorError::BadParameter);
        }

        let mut reader = self.body()?;
        let pair = reader.user_property()?;

        self.at = self.at.saturating_add(1).saturating_add(reader.position());
        Ok(pair)
    }

    /// `MQTTPropGet_SubscriptionId`.
    ///
    /// # Errors
    ///
    /// See [`CursorError`].
    pub fn subscription_id(&mut self) -> Result<u32, CursorError> {
        if self.identifier()? != client::SUBSCRIPTION_ID {
            return Err(CursorError::BadParameter);
        }

        self.variable()
    }
}

/// Generate the twenty getters that are one line each in the C.
macro_rules! getters {
    ($( $(#[$note:meta])* $name:ident, $id:expr, $kind:ident -> $ty:ty; )*) => {
        impl<'a> PropertyCursor<'a> {
            $(
                $(#[$note])*
                ///
                /// # Errors
                ///
                /// See [`CursorError`].
                pub fn $name(&mut self) -> Result<$ty, CursorError> {
                    self.one($id, |reader, used| reader.$kind(used))
                }
            )*
        }
    };
}

getters! {
    /// `MQTTPropGet_SessionExpiry`.
    session_expiry, client::SESSION_EXPIRY, u32 -> u32;
    /// `MQTTPropGet_ReceiveMax`.
    receive_max, client::RECEIVE_MAX, u16 -> u16;
    /// `MQTTPropGet_MaxQos`: the highest QoS the broker supports.
    max_qos, server::MAX_QOS, u8 -> u8;
    /// `MQTTPropGet_RetainAvailable`.
    retain_available, server::RETAIN_AVAILABLE, u8 -> u8;
    /// `MQTTPropGet_MaxPacketSize`.
    max_packet_size, client::MAX_PACKET_SIZE, u32 -> u32;
    /// `MQTTPropGet_AssignedClientId`, which a broker sends when the client
    /// connected with an empty identifier.
    assigned_client_id, server::ASSIGNED_CLIENT_ID, utf8 -> &'a [u8];
    /// `MQTTPropGet_TopicAliasMax`.
    topic_alias_max, client::TOPIC_ALIAS_MAX, u16 -> u16;
    /// `MQTTPropGet_ReasonString`.
    reason_string, client::REASON_STRING, utf8 -> &'a [u8];
    /// `MQTTPropGet_WildcardId`: whether wildcard subscriptions work.
    wildcard_available, server::WILDCARD_AVAILABLE, u8 -> u8;
    /// `MQTTPropGet_SubsIdAvailable`.
    subscription_id_available, server::SUBSCRIPTION_ID_AVAILABLE, u8 -> u8;
    /// `MQTTPropGet_SharedSubAvailable`.
    shared_sub_available, server::SHARED_SUB_AVAILABLE, u8 -> u8;
    /// `MQTTPropGet_ServerKeepAlive`, which overrides the client's.
    server_keep_alive, server::SERVER_KEEP_ALIVE, u16 -> u16;
    /// `MQTTPropGet_ResponseInfo`.
    response_info, server::RESPONSE_INFO, utf8 -> &'a [u8];
    /// `MQTTPropGet_ServerRef`: where the broker would rather you connected.
    server_reference, server::SERVER_REFERENCE, utf8 -> &'a [u8];
    /// `MQTTPropGet_AuthMethod`.
    auth_method, client::AUTH_METHOD, utf8 -> &'a [u8];
    /// `MQTTPropGet_AuthData`.
    auth_data, client::AUTH_DATA, utf8 -> &'a [u8];
    /// `MQTTPropGet_PayloadFormatIndicator`.
    payload_format, client::PAYLOAD_FORMAT, u8 -> u8;
    /// `MQTTPropGet_MessageExpiryInterval`.
    message_expiry, client::MESSAGE_EXPIRY, u32 -> u32;
    /// `MQTTPropGet_TopicAlias`.
    topic_alias, client::TOPIC_ALIAS, u16 -> u16;
    /// `MQTTPropGet_ResponseTopic`.
    response_topic, client::RESPONSE_TOPIC, utf8 -> &'a [u8];
    /// `MQTTPropGet_CorrelationData`.
    correlation_data, client::CORRELATION_DATA, utf8 -> &'a [u8];
    /// `MQTTPropGet_ContentType`.
    content_type, client::CONTENT_TYPE, utf8 -> &'a [u8];
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

    /// **The budget is advisory; the slice is the guarantee.** Again.
    ///
    /// Two poisons did not fire here, and they are the same shape as the two
    /// that did not fire in [`builder`](crate::builder): handing the property
    /// reader a budget one byte too generous, and letting the cursor sit
    /// exactly on the end of the section. Neither changes an answer, because
    /// the read that follows is bounded by the slice as well as by the
    /// arithmetic.
    ///
    /// Third instance in the package of one property, so it is worth naming
    /// rather than noting: **an off-by-one in a length calculation is a bug in
    /// C and a redundancy here.** That is not an argument for sloppy
    /// arithmetic — the budget is transcribed exactly, and the differential
    /// would catch a budget that was too SMALL — it is the reason the same slip
    /// that overflows `addPropUtf8` cannot overflow anything in this crate.
    ///
    /// Proven the way the builder's is: every getter against every truncation
    /// of every section, asserting the cursor never leaves the section and a
    /// refusal never moves it.
    #[test]
    fn no_budget_error_can_read_past_the_section() {
        let full = [
            client::SESSION_EXPIRY,
            0x00,
            0x00,
            0x0E,
            0x10,
            client::REASON_STRING,
            0x00,
            0x02,
            b'h',
            b'i',
            client::USER_PROPERTY,
            0x00,
            0x01,
            b'k',
            0x00,
            0x01,
            b'v',
            client::SUBSCRIPTION_ID,
            0x81,
            0x01,
            server::MAX_QOS,
            0x01,
        ];

        for cut in 0..=full.len() {
            let section = &full[..cut];

            for start in 0..=full.len() {
                let mut cursor = PropertyCursor::new(section);
                cursor.seek(start);

                for _ in 0..4 {
                    let before = cursor.position();

                    // One of each shape, so every read path is exercised at
                    // every truncation.
                    let outcomes = [
                        cursor.clone().session_expiry().map(|_| ()),
                        cursor.clone().reason_string().map(|_| ()),
                        cursor.clone().user_property().map(|_| ()),
                        cursor.clone().subscription_id().map(|_| ()),
                        cursor.clone().max_qos().map(|_| ()),
                        cursor.next_type().map(|_| ()),
                    ];

                    for outcome in outcomes {
                        let _ = outcome;
                    }

                    assert_eq!(
                        cursor.position(),
                        before,
                        "a peek or a clone moved the cursor"
                    );

                    let stepped = cursor.skip();

                    assert!(
                        cursor.position() <= section.len().max(start),
                        "the cursor reached {} in a {}-byte section",
                        cursor.position(),
                        section.len()
                    );

                    if stepped.is_err() {
                        assert_eq!(cursor.position(), before, "a refused skip moved the cursor");
                        break;
                    }

                    assert!(
                        cursor.position() > before,
                        "a successful skip did not advance"
                    );
                }
            }
        }
    }

    /// The two tables accept the same alphabet, and here they cannot do
    /// otherwise.
    ///
    /// coreMQTT writes them separately — an identifier list in
    /// `MQTT_GetNextPropertyType`, width groups in `MQTT_SkipNextProperty` —
    /// and they agree, which after five tables in this library that disagreed
    /// with a sibling is worth asserting rather than assuming. Here one table
    /// serves both, so the agreement is structural.
    #[test]
    fn the_two_tables_accept_the_same_alphabet() {
        let mut known = 0usize;

        for id in 0..=255u8 {
            let section = [id, 0x00, 0x02, b'a', b'b', 0x00, 0x02, b'c', b'd'];
            let cursor = PropertyCursor::new(&section);
            let mut skipper = PropertyCursor::new(&section);

            let accepted = cursor.next_type().is_ok();
            let skippable = skipper.skip().is_ok();

            assert_eq!(
                accepted, skippable,
                "the two tables disagree about {id:#04x}"
            );
            assert_eq!(accepted, width_of(id).is_some());

            if accepted {
                known += 1;
            }
        }

        assert_eq!(known, 27, "MQTT 5 defines 27 property identifiers");
    }

    /// Every getter demands exactly one identifier.
    ///
    /// A getter is an assertion about what is under the cursor, not a search,
    /// so a section read in the wrong order fails rather than handing back a
    /// number from the wrong property. Twenty-two getters against 256
    /// identifiers is the whole of that rule.
    #[test]
    fn every_getter_accepts_exactly_one_identifier() {
        for id in 0..=255u8 {
            let section = [id, 0x00, 0x02, b'a', b'b', 0x00, 0x02, b'c', b'd'];

            let mut hits = 0usize;
            let mut cursor = PropertyCursor::new(&section);

            macro_rules! count {
                ($($name:ident),*) => {$(
                    cursor.seek(0);
                    if cursor.$name().is_ok() {
                        hits += 1;
                    }
                )*};
            }

            count!(
                session_expiry,
                receive_max,
                max_qos,
                retain_available,
                max_packet_size,
                assigned_client_id,
                topic_alias_max,
                reason_string,
                wildcard_available,
                subscription_id_available,
                shared_sub_available,
                server_keep_alive,
                response_info,
                server_reference,
                auth_method,
                auth_data,
                payload_format,
                message_expiry,
                topic_alias,
                response_topic,
                correlation_data,
                content_type
            );

            cursor.seek(0);
            if cursor.user_property().is_ok() {
                hits += 1;
            }

            cursor.seek(0);
            if cursor.subscription_id().is_ok() {
                hits += 1;
            }

            assert!(
                hits <= 1,
                "{hits} getters accept {id:#04x}, so two are wired to one constant"
            );
        }
    }

    /// What the builder writes, the cursor reads back.
    ///
    /// The strongest check the two halves admit, and one neither arm can pass
    /// alone: build a section, walk it, and get the same values out. Two arms
    /// that each agree with the C can still disagree with each other.
    #[test]
    fn what_the_builder_writes_the_cursor_reads_back() {
        use crate::builder::PropertyBuilder;

        let mut bytes = [0u8; 64];
        let mut builder = PropertyBuilder::new(&mut bytes).unwrap();

        builder.session_expiry(3600, None).unwrap();
        builder.receive_max(20, None).unwrap();
        builder.reason_string(b"hi", None).unwrap();
        builder.user_property(b"k", b"v", None).unwrap();
        builder.subscription_id(300, None).unwrap();

        let section = builder.section().to_vec();
        let mut cursor = PropertyCursor::new(&section);

        assert_eq!(cursor.session_expiry(), Ok(3600));
        assert_eq!(cursor.receive_max(), Ok(20));
        assert_eq!(cursor.reason_string(), Ok(&b"hi"[..]));
        assert_eq!(cursor.user_property(), Ok((&b"k"[..], &b"v"[..])));
        assert_eq!(cursor.subscription_id(), Ok(300));

        assert!(cursor.is_empty(), "the walk should end exactly at the end");
        assert_eq!(cursor.next_type(), Err(CursorError::EndOfProperties));
    }

    /// Skipping reaches the property after the one you did not want.
    ///
    /// Every width, because the skip table is five groups and a width computed
    /// wrong lands the cursor inside a value, where the next identifier byte is
    /// somebody's data.
    #[test]
    fn a_skip_lands_on_the_next_identifier_whatever_the_width() {
        use crate::builder::PropertyBuilder;

        let mut bytes = [0u8; 96];
        let mut builder = PropertyBuilder::new(&mut bytes).unwrap();

        // One of every width, then a marker.
        builder.payload_format(true, None).unwrap();
        builder.topic_alias(7, None).unwrap();
        builder.session_expiry(30, None).unwrap();
        builder.content_type(b"text", None).unwrap();
        builder.user_property(b"k", b"v", None).unwrap();
        builder.subscription_id(300, None).unwrap();
        builder.reason_string(b"end", None).unwrap();

        let section = builder.section().to_vec();
        let mut cursor = PropertyCursor::new(&section);

        for _ in 0..6 {
            // Every position reached by skipping must be a legal identifier.
            assert!(cursor.next_type().is_ok());
            assert_eq!(cursor.skip(), Ok(()));
        }

        assert_eq!(cursor.reason_string(), Ok(&b"end"[..]));
        assert!(cursor.is_empty());
    }

    /// The end of a section is not an error.
    #[test]
    fn running_out_is_its_own_answer() {
        let section = [server::MAX_QOS, 0x01];
        let mut cursor = PropertyCursor::new(&section);

        assert_eq!(cursor.max_qos(), Ok(1));
        assert_eq!(cursor.max_qos(), Err(CursorError::EndOfProperties));
        assert_eq!(cursor.skip(), Err(CursorError::EndOfProperties));
        assert_eq!(cursor.next_type(), Err(CursorError::EndOfProperties));

        // And a cursor moved past the end says the same thing rather than
        // reading whatever is there.
        cursor.seek(99);
        assert_eq!(cursor.max_qos(), Err(CursorError::EndOfProperties));
        assert_eq!(cursor.position(), 99, "a refusal must not move the cursor");
    }

    /// A refused read leaves the cursor where it was.
    ///
    /// The C returns without assigning its out-parameter on every failure path,
    /// so a caller that retried would retry the same property. Reproduced, and
    /// it matters: the alternative is a cursor inside a value.
    #[test]
    fn a_refusal_does_not_move_the_cursor() {
        let section = [client::RECEIVE_MAX, 0x00, 0x14];
        let mut cursor = PropertyCursor::new(&section);

        assert_eq!(cursor.session_expiry(), Err(CursorError::BadParameter));
        assert_eq!(cursor.position(), 0);

        let truncated = [client::SESSION_EXPIRY, 0x00, 0x00];
        let mut cursor = PropertyCursor::new(&truncated);

        assert_eq!(cursor.session_expiry(), Err(CursorError::BadResponse));
        assert_eq!(cursor.position(), 0);
    }
}
