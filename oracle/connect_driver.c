/* The C arm of K7's coreMQTT CONNECT differential: size, then serialize.
 *
 * `core_mqtt_serializer.c` is compiled VERBATIM out of the pinned checkout;
 * nothing here copies or edits it.
 *
 * # Why this slice
 *
 * The package can read every packet a broker sends and can end a session. This
 * is the packet that STARTS one, and it is the largest thing a client
 * assembles: a ten-byte fixed variable header, a property section, a client
 * identifier, optionally a will (its own property section, a topic and a
 * payload) and optionally a user name and a password. Eight length-prefixed
 * fields, four of them optional, and the fixed header's flags byte has to agree
 * with which ones are present.
 *
 * # ABSENT and EMPTY are different, and the trace has to say which
 *
 * The C distinguishes a NULL `pUserName` from a non-NULL one of length zero:
 * the first clears a flag bit and writes nothing, the second sets the bit and
 * writes two zero bytes. Same for the password and the will. A trace that
 * printed both as "nothing" could not tell the two apart, so this one prints
 * `-` for absent and `.` for present-and-empty.
 *
 * # Size and serialize in one case
 *
 * `MQTT_GetConnectPacketSize` and then `MQTT_SerializeConnect` on its answer,
 * with the bytes printed. The C's own comment says the second function does not
 * re-check the remaining length because calling the first is "part of the API
 * contract" -- so running them apart would test a contract nobody keeps.
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core_mqtt_serializer.h"

#define MAX_FIELD    32
#define OUT_SIZE     192

static void put_hex( const uint8_t *p, size_t n )
{
    size_t i;

    for( i = 0; i < n; i++ )
    {
        printf( "%02x", p[ i ] );
    }
}

/* `-` absent, `.` present and empty, hex otherwise. */
static void put_opt( bool present, const uint8_t *p, size_t n )
{
    if( !present )
    {
        printf( "-" );
    }
    else if( n == 0U )
    {
        printf( "." );
    }
    else
    {
        put_hex( p, n );
    }
}

static const char * status_name( MQTTStatus_t status )
{
    switch( status )
    {
        case MQTTSuccess:      return "Success";
        case MQTTBadParameter: return "BadParameter";
        case MQTTNoMemory:     return "NoMemory";
        default:               return "OTHER";
    }
}

typedef struct
{
    const char *name;

    bool        cleanSession;
    uint16_t    keepAlive;

    size_t      clientIdLength;
    uint8_t     clientId[ MAX_FIELD ];
    /* A client identifier the C is handed as a NULL pointer, which is not the
     * same as one of length zero. */
    bool        clientIdPresent;

    bool        hasUserName;
    size_t      userNameLength;
    uint8_t     userName[ MAX_FIELD ];

    bool        hasPassword;
    size_t      passwordLength;
    uint8_t     password[ MAX_FIELD ];

    size_t      propertyLength;
    uint8_t     properties[ MAX_FIELD ];

    bool        hasWill;
    uint8_t     willQos;
    bool        willRetain;
    size_t      willTopicLength;
    uint8_t     willTopic[ MAX_FIELD ];
    size_t      willPayloadLength;
    uint8_t     willPayload[ MAX_FIELD ];
    size_t      willPropertyLength;
    uint8_t     willProperties[ MAX_FIELD ];

    size_t      bufferSize;
} ConnectCase_t;

static const ConnectCase_t CASES[] = {
    /* --- the smallest things that work --- */
    { "minimal", true, 60,
      3, { 'a', 'b', 'c' }, true,
      false, 0, { 0 }, false, 0, { 0 },
      0, { 0 },
      false, 0, false, 0, { 0 }, 0, { 0 }, 0, { 0 },
      OUT_SIZE },
    { "persistent-session", false, 0,
      3, { 'a', 'b', 'c' }, true,
      false, 0, { 0 }, false, 0, { 0 },
      0, { 0 },
      false, 0, false, 0, { 0 }, 0, { 0 }, 0, { 0 },
      OUT_SIZE },
    /* MQTT 5.0 s3.1.3.1 allows a zero-length client id: the server assigns one
     * and returns it in the CONNACK. */
    { "empty-client-id", true, 60,
      0, { 0 }, true,
      false, 0, { 0 }, false, 0, { 0 },
      0, { 0 },
      false, 0, false, 0, { 0 }, 0, { 0 }, 0, { 0 },
      OUT_SIZE },
    /* A NULL `pClientIdentifier` with a non-zero length is the other half of
     * the C's mismatch check, and it is NOT a case here: a `&[u8]` carries its
     * own length, so the two cannot disagree and the refusal has no Rust
     * equivalent. Sixth member of the family the ack deserializers listed.
     * What IS reachable is the check's other effect, below. */
    /* A client id whose FIRST byte is NUL. The C's check reads `*p == '\0'`,
     * so this is refused -- a side effect of a check meant to catch a
     * length/pointer mismatch. */
    { "client-id-starting-with-nul", true, 60,
      3, { 0x00, 'b', 'c' }, true,
      false, 0, { 0 }, false, 0, { 0 },
      0, { 0 },
      false, 0, false, 0, { 0 }, 0, { 0 }, 0, { 0 },
      OUT_SIZE },
    /* ...and one with a NUL anywhere else, which is accepted. */
    { "client-id-with-an-inner-nul", true, 60,
      3, { 'a', 0x00, 'c' }, true,
      false, 0, { 0 }, false, 0, { 0 },
      0, { 0 },
      false, 0, false, 0, { 0 }, 0, { 0 }, 0, { 0 },
      OUT_SIZE },

    /* --- credentials: absent, empty and present are three states --- */
    { "username", true, 60,
      3, { 'a', 'b', 'c' }, true,
      true, 4, { 'u', 's', 'e', 'r' }, false, 0, { 0 },
      0, { 0 },
      false, 0, false, 0, { 0 }, 0, { 0 }, 0, { 0 },
      OUT_SIZE },
    { "empty-username", true, 60,
      3, { 'a', 'b', 'c' }, true,
      true, 0, { 0 }, false, 0, { 0 },
      0, { 0 },
      false, 0, false, 0, { 0 }, 0, { 0 }, 0, { 0 },
      OUT_SIZE },
    { "username-and-password", true, 60,
      3, { 'a', 'b', 'c' }, true,
      true, 4, { 'u', 's', 'e', 'r' }, true, 2, { 'p', 'w' },
      0, { 0 },
      false, 0, false, 0, { 0 }, 0, { 0 }, 0, { 0 },
      OUT_SIZE },
    /* MQTT 5.0 s3.1.2.9 allows a password with no user name, unlike 3.1.1. */
    { "password-without-username", true, 60,
      3, { 'a', 'b', 'c' }, true,
      false, 0, { 0 }, true, 2, { 'p', 'w' },
      0, { 0 },
      false, 0, false, 0, { 0 }, 0, { 0 }, 0, { 0 },
      OUT_SIZE },

    /* --- connect properties --- */
    { "session-expiry-property", true, 60,
      3, { 'a', 'b', 'c' }, true,
      false, 0, { 0 }, false, 0, { 0 },
      5, { 0x11, 0x00, 0x00, 0x0E, 0x10 },
      false, 0, false, 0, { 0 }, 0, { 0 }, 0, { 0 },
      OUT_SIZE },
    { "two-properties", true, 60,
      3, { 'a', 'b', 'c' }, true,
      false, 0, { 0 }, false, 0, { 0 },
      8, { 0x11, 0x00, 0x00, 0x0E, 0x10, 0x21, 0x00, 0x14 },
      false, 0, false, 0, { 0 }, 0, { 0 }, 0, { 0 },
      OUT_SIZE },

    /* --- the will, which is four more fields and three flag bits --- */
    { "will-qos0", true, 60,
      3, { 'a', 'b', 'c' }, true,
      false, 0, { 0 }, false, 0, { 0 },
      0, { 0 },
      true, 0, false, 4, { 'l', 'a', 's', 't' }, 3, { 'b', 'y', 'e' }, 0, { 0 },
      OUT_SIZE },
    { "will-qos1-retained", true, 60,
      3, { 'a', 'b', 'c' }, true,
      false, 0, { 0 }, false, 0, { 0 },
      0, { 0 },
      true, 1, true, 4, { 'l', 'a', 's', 't' }, 3, { 'b', 'y', 'e' }, 0, { 0 },
      OUT_SIZE },
    { "will-qos2", true, 60,
      3, { 'a', 'b', 'c' }, true,
      false, 0, { 0 }, false, 0, { 0 },
      0, { 0 },
      true, 2, false, 4, { 'l', 'a', 's', 't' }, 3, { 'b', 'y', 'e' }, 0, { 0 },
      OUT_SIZE },
    { "will-with-an-empty-payload", true, 60,
      3, { 'a', 'b', 'c' }, true,
      false, 0, { 0 }, false, 0, { 0 },
      0, { 0 },
      true, 0, false, 4, { 'l', 'a', 's', 't' }, 0, { 0 }, 0, { 0 },
      OUT_SIZE },
    { "will-with-properties", true, 60,
      3, { 'a', 'b', 'c' }, true,
      false, 0, { 0 }, false, 0, { 0 },
      0, { 0 },
      true, 1, false, 4, { 'l', 'a', 's', 't' }, 3, { 'b', 'y', 'e' },
      5, { 0x18, 0x00, 0x00, 0x00, 0x0A },
      OUT_SIZE },
    /* Everything at once, which is where a field written in the wrong ORDER
     * shows up: each one is length-prefixed, so a swap still parses. */
    { "everything", false, 30,
      3, { 'a', 'b', 'c' }, true,
      true, 4, { 'u', 's', 'e', 'r' }, true, 2, { 'p', 'w' },
      5, { 0x11, 0x00, 0x00, 0x0E, 0x10 },
      true, 2, true, 4, { 'l', 'a', 's', 't' }, 3, { 'b', 'y', 'e' },
      5, { 0x18, 0x00, 0x00, 0x00, 0x0A },
      OUT_SIZE },

    /* --- the buffer, which is the caller's --- */
    { "buffer-exactly-big-enough", true, 60,
      3, { 'a', 'b', 'c' }, true,
      false, 0, { 0 }, false, 0, { 0 },
      0, { 0 },
      false, 0, false, 0, { 0 }, 0, { 0 }, 0, { 0 },
      18 },
    { "buffer-one-byte-short", true, 60,
      3, { 'a', 'b', 'c' }, true,
      false, 0, { 0 }, false, 0, { 0 },
      0, { 0 },
      false, 0, false, 0, { 0 }, 0, { 0 }, 0, { 0 },
      17 },
    { "buffer-empty", true, 60,
      3, { 'a', 'b', 'c' }, true,
      false, 0, { 0 }, false, 0, { 0 },
      0, { 0 },
      false, 0, false, 0, { 0 }, 0, { 0 }, 0, { 0 },
      0 },
};

#define N_CASES    ( sizeof( CASES ) / sizeof( CASES[ 0 ] ) )

static void run_case( size_t i )
{
    const ConnectCase_t *c = &CASES[ i ];
    uint8_t clientId[ MAX_FIELD ], userName[ MAX_FIELD ], password[ MAX_FIELD ];
    uint8_t properties[ MAX_FIELD ], willTopic[ MAX_FIELD ];
    uint8_t willPayload[ MAX_FIELD ], willProperties[ MAX_FIELD ];
    uint8_t out[ OUT_SIZE ];

    MQTTConnectInfo_t info;
    MQTTPublishInfo_t will;
    MQTTPropBuilder_t props, willProps;
    MQTTFixedBuffer_t fixed;
    const MQTTPublishInfo_t *pWill = NULL;
    const MQTTPropBuilder_t *pProps = NULL;
    const MQTTPropBuilder_t *pWillProps = NULL;
    uint32_t remaining = 0xAAAAAAAAU;
    uint32_t size = 0xAAAAAAAAU;
    MQTTStatus_t status;

    memcpy( clientId, c->clientId, sizeof clientId );
    memcpy( userName, c->userName, sizeof userName );
    memcpy( password, c->password, sizeof password );
    memcpy( properties, c->properties, sizeof properties );
    memcpy( willTopic, c->willTopic, sizeof willTopic );
    memcpy( willPayload, c->willPayload, sizeof willPayload );
    memcpy( willProperties, c->willProperties, sizeof willProperties );

    memset( &info, 0, sizeof info );
    info.cleanSession = c->cleanSession;
    info.keepAliveSeconds = c->keepAlive;
    info.pClientIdentifier = c->clientIdPresent ? ( const char * ) clientId : NULL;
    info.clientIdentifierLength = c->clientIdLength;

    if( c->hasUserName )
    {
        info.pUserName = ( const char * ) userName;
        info.userNameLength = c->userNameLength;
    }

    if( c->hasPassword )
    {
        info.pPassword = ( const char * ) password;
        info.passwordLength = c->passwordLength;
    }

    if( c->propertyLength != 0U )
    {
        memset( &props, 0, sizeof props );
        props.pBuffer = properties;
        props.bufferLength = sizeof properties;
        props.currentIndex = c->propertyLength;
        pProps = &props;
    }

    if( c->hasWill )
    {
        memset( &will, 0, sizeof will );
        will.qos = ( MQTTQoS_t ) c->willQos;
        will.retain = c->willRetain;
        will.pTopicName = ( const char * ) willTopic;
        will.topicNameLength = c->willTopicLength;
        will.pPayload = willPayload;
        will.payloadLength = c->willPayloadLength;
        pWill = &will;

        if( c->willPropertyLength != 0U )
        {
            memset( &willProps, 0, sizeof willProps );
            willProps.pBuffer = willProperties;
            willProps.bufferLength = sizeof willProperties;
            willProps.currentIndex = c->willPropertyLength;
            pWillProps = &willProps;
        }
    }

    printf( "case %u %s clean=%u ka=%u id=", ( unsigned ) i, c->name,
            c->cleanSession ? 1U : 0U, ( unsigned ) c->keepAlive );
    put_opt( c->clientIdPresent, c->clientId, c->clientIdLength );
    printf( " user=" );
    put_opt( c->hasUserName, c->userName, c->userNameLength );
    printf( " pass=" );
    put_opt( c->hasPassword, c->password, c->passwordLength );
    printf( " props=" );
    put_opt( c->propertyLength != 0U, c->properties, c->propertyLength );
    printf( " will=" );

    if( c->hasWill )
    {
        printf( "%u,%u,", ( unsigned ) c->willQos, c->willRetain ? 1U : 0U );
        put_opt( true, c->willTopic, c->willTopicLength );
        printf( "," );
        put_opt( true, c->willPayload, c->willPayloadLength );
        printf( "," );
        put_opt( c->willPropertyLength != 0U, c->willProperties, c->willPropertyLength );
    }
    else
    {
        printf( "-" );
    }

    printf( " buf=%u", ( unsigned ) c->bufferSize );

    status = MQTT_GetConnectPacketSize( &info, pWill, pProps, pWillProps,
                                        &remaining, &size );

    printf( " -> size %s", status_name( status ) );

    if( status == MQTTSuccess )
    {
        printf( " remaining=%u packet=%u", ( unsigned ) remaining,
                ( unsigned ) size );

        memset( out, 0xCC, sizeof out );
        memset( &fixed, 0, sizeof fixed );
        fixed.pBuffer = out;
        fixed.size = c->bufferSize;

        status = MQTT_SerializeConnect( &info, pWill, pProps, pWillProps,
                                        remaining, &fixed );

        printf( " serialize %s", status_name( status ) );

        if( status == MQTTSuccess )
        {
            printf( " bytes=" );
            put_hex( out, ( size_t ) size );
        }
    }

    printf( "\n" );
}

/* ---- the 16-bit boundary -------------------------------------------------- */

/* Five of this function's checks refuse a field that will not fit its two-byte
 * length prefix, and NO case above goes near 65,535 -- every field in the table
 * is a handful of bytes, so all five checks are unreachable and a poison on any
 * of them passes.
 *
 * These cases reach them, one field at a time. They print the STATUS and the two
 * sizes and not the bytes: a 65 KB packet's hex would be 131 KB on one line of a
 * checked-in trace, and the thing under test is the refusal, not the content.
 */
typedef struct
{
    const char *name;
    int         which;     /* 0 client id, 1 user name, 2 password, 3 will topic, 4 will payload */
    size_t      length;
} BigCase_t;

static const BigCase_t BIG_CASES[] = {
    { "client-id-at-the-16-bit-max",   0, 65535U },
    { "client-id-one-past-16-bits",    0, 65536U },
    { "username-at-the-16-bit-max",    1, 65535U },
    { "username-one-past-16-bits",     1, 65536U },
    { "password-one-past-16-bits",     2, 65536U },
    { "will-topic-one-past-16-bits",   3, 65536U },
    { "will-payload-one-past-16-bits", 4, 65536U },
};

#define N_BIG    ( sizeof( BIG_CASES ) / sizeof( BIG_CASES[ 0 ] ) )

static uint8_t g_big[ 65536U + 4U ];

static void run_big_case( size_t i )
{
    const BigCase_t *c = &BIG_CASES[ i ];
    static const uint8_t SHORT_ID[] = { 'i', 'd' };
    static const uint8_t SHORT_FIELD[] = { 'x' };

    MQTTConnectInfo_t info;
    MQTTPublishInfo_t will;
    const MQTTPublishInfo_t *pWill = NULL;
    uint32_t remaining = 0xAAAAAAAAU;
    uint32_t size = 0xAAAAAAAAU;
    MQTTStatus_t status;

    memset( g_big, 'x', sizeof g_big );

    memset( &info, 0, sizeof info );
    info.cleanSession = true;
    info.keepAliveSeconds = 60U;
    info.pClientIdentifier = ( const char * ) SHORT_ID;
    info.clientIdentifierLength = sizeof SHORT_ID;

    memset( &will, 0, sizeof will );
    will.qos = MQTTQoS0;
    will.pTopicName = ( const char * ) SHORT_FIELD;
    will.topicNameLength = sizeof SHORT_FIELD;
    will.pPayload = SHORT_FIELD;
    will.payloadLength = sizeof SHORT_FIELD;

    switch( c->which )
    {
        case 0:
            info.pClientIdentifier = ( const char * ) g_big;
            info.clientIdentifierLength = c->length;
            break;

        case 1:
            info.pUserName = ( const char * ) g_big;
            info.userNameLength = c->length;
            break;

        case 2:
            info.pPassword = ( const char * ) g_big;
            info.passwordLength = c->length;
            break;

        case 3:
            will.pTopicName = ( const char * ) g_big;
            will.topicNameLength = c->length;
            pWill = &will;
            break;

        default:
            will.pPayload = g_big;
            will.payloadLength = c->length;
            pWill = &will;
            break;
    }

    printf( "big %u %s which=%d len=%u", ( unsigned ) i, c->name, c->which,
            ( unsigned ) c->length );

    status = MQTT_GetConnectPacketSize( &info, pWill, NULL, NULL,
                                        &remaining, &size );

    printf( " -> size %s", status_name( status ) );

    if( status == MQTTSuccess )
    {
        printf( " remaining=%u packet=%u", ( unsigned ) remaining,
                ( unsigned ) size );
    }

    printf( "\n" );
}

/* ---- the remaining-length limit, and why it is NOT here -------------------- */

/* `MQTT_GetConnectPacketSize` refuses a total past 268,435,455, and the largest
 * field is 65,535 -- four orders of magnitude short, so no combination of
 * fields can reach it.
 *
 * The property section can, and this driver DID reach it: the function reads
 * `pConnectProperties->currentIndex` and never touches `pBuffer`, so a builder
 * may claim 268 million bytes while pointing at eight, and
 * `currentIndex = 268435437` puts the total on exactly 268,435,455 while
 * 268435438 puts it one past.
 *
 * The cases are not here because the Rust arm cannot replay them. Its property
 * section is a `&[u8]`, whose length IS its data -- a slice cannot claim a size
 * it does not have, so the whole class of input that makes this check
 * load-bearing does not exist on that side. The check is kept there and its
 * arithmetic is pinned by a unit test; the property is recorded in the package
 * plan. Reaching it honestly would need a caller genuinely holding 268 MB of
 * property bytes, which is a 64-bit host's problem and not a microcontroller's.
 */

/* ---- the sweep ----------------------------------------------------------- */

/* Every combination of the four OPTIONAL fields, at two will QoS values.
 *
 * A CONNECT's flags byte says which of the will, the user name and the password
 * are present, and its payload must then carry exactly those and in that order.
 * The writers' slice swept the flags byte alone; this sweeps the whole packet,
 * so a field written when its bit is clear -- or in the wrong place -- moves the
 * bytes rather than just one nibble.
 *
 * The digest is over the WHOLE serialized packet, which is what a byte in the
 * wrong order changes and a per-field comparison would not. */
static void sweep_optional_fields( void )
{
    unsigned combination;
    uint64_t fnv = 1469598103934665603ULL;
    unsigned accepted = 0U;
    unsigned refused = 0U;
    unsigned shortest = 0xFFFFFFFFU;
    unsigned longest = 0U;

    for( combination = 0U; combination < 32U; combination++ )
    {
        static const uint8_t CLIENT_ID[] = { 'i', 'd' };
        static const uint8_t USER[] = { 'u' };
        static const uint8_t PASS[] = { 'p', 'w' };
        static const uint8_t TOPIC[] = { 'w', '/', 't' };
        static const uint8_t PAYLOAD[] = { 'b', 'y', 'e' };
        static const uint8_t PROPS[] = { 0x11, 0x00, 0x00, 0x00, 0x01 };

        uint8_t out[ OUT_SIZE ];
        MQTTConnectInfo_t info;
        MQTTPublishInfo_t will;
        MQTTPropBuilder_t props, willProps;
        MQTTFixedBuffer_t fixed;
        const MQTTPublishInfo_t *pWill = NULL;
        const MQTTPropBuilder_t *pProps = NULL;
        const MQTTPropBuilder_t *pWillProps = NULL;
        uint32_t remaining = 0U, size = 0U;
        MQTTStatus_t status;
        size_t k;

        bool hasUser = ( combination & 1U ) != 0U;
        bool hasPass = ( combination & 2U ) != 0U;
        bool hasWill = ( combination & 4U ) != 0U;
        bool hasProps = ( combination & 8U ) != 0U;
        bool willQos1 = ( combination & 16U ) != 0U;

        memset( &info, 0, sizeof info );
        info.cleanSession = true;
        info.keepAliveSeconds = 60U;
        info.pClientIdentifier = ( const char * ) CLIENT_ID;
        info.clientIdentifierLength = sizeof CLIENT_ID;

        if( hasUser )
        {
            info.pUserName = ( const char * ) USER;
            info.userNameLength = sizeof USER;
        }

        if( hasPass )
        {
            info.pPassword = ( const char * ) PASS;
            info.passwordLength = sizeof PASS;
        }

        if( hasProps )
        {
            memset( &props, 0, sizeof props );
            props.pBuffer = ( uint8_t * ) PROPS;
            props.bufferLength = sizeof PROPS;
            props.currentIndex = sizeof PROPS;
            pProps = &props;
        }

        if( hasWill )
        {
            memset( &will, 0, sizeof will );
            will.qos = willQos1 ? MQTTQoS1 : MQTTQoS0;
            will.retain = willQos1;
            will.pTopicName = ( const char * ) TOPIC;
            will.topicNameLength = sizeof TOPIC;
            will.pPayload = PAYLOAD;
            will.payloadLength = sizeof PAYLOAD;
            pWill = &will;

            if( hasProps )
            {
                memset( &willProps, 0, sizeof willProps );
                willProps.pBuffer = ( uint8_t * ) PROPS;
                willProps.bufferLength = sizeof PROPS;
                willProps.currentIndex = sizeof PROPS;
                pWillProps = &willProps;
            }
        }

        status = MQTT_GetConnectPacketSize( &info, pWill, pProps, pWillProps,
                                            &remaining, &size );

        if( status != MQTTSuccess )
        {
            refused++;
            continue;
        }

        memset( out, 0xCC, sizeof out );
        memset( &fixed, 0, sizeof fixed );
        fixed.pBuffer = out;
        fixed.size = sizeof out;

        status = MQTT_SerializeConnect( &info, pWill, pProps, pWillProps,
                                        remaining, &fixed );

        if( status != MQTTSuccess )
        {
            refused++;
            continue;
        }

        accepted++;

        if( size < shortest )
        {
            shortest = ( unsigned ) size;
        }

        if( size > longest )
        {
            longest = ( unsigned ) size;
        }

        for( k = 0; k < ( size_t ) size; k++ )
        {
            fnv ^= ( uint64_t ) out[ k ];
            fnv *= 1099511628211ULL;
        }
    }

    printf( "optional-field-sweep n=32 accepted=%u refused=%u shortest=%u "
            "longest=%u digest=%016llx\n",
            accepted, refused, shortest, longest,
            ( unsigned long long ) fnv );
}

int main( void )
{
    size_t i;

    printf( "geometry cases=%u big=%u\n", ( unsigned ) N_CASES,
            ( unsigned ) N_BIG );

    for( i = 0; i < N_CASES; i++ )
    {
        run_case( i );
    }

    for( i = 0; i < N_BIG; i++ )
    {
        run_big_case( i );
    }

    sweep_optional_fields();

    printf( "end\n" );
    return 0;
}
