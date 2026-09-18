/* The C arm of K7's coreMQTT CONNACK differential.
 *
 * `core_mqtt_serializer.c` is compiled VERBATIM out of the pinned checkout;
 * nothing here copies or edits it.
 *
 * # Why this slice
 *
 * `MQTT_DeserializeAck` refuses a CONNACK and points the caller at
 * `MQTT_DeserializeConnAck`, and the split is not arbitrary. A CONNACK is the
 * FIRST thing a broker sends and the only packet that sets connection-wide
 * state: the largest packet the server will accept, how many messages it will
 * take in flight, whether retain and wildcards and shared subscriptions work at
 * all, and what keep-alive the client must now use. Every later size check runs
 * on numbers that arrive here.
 *
 * # Five property sweeps, not one
 *
 * The property identifier is one byte, so it is enumerable -- but a single
 * fixed body cannot sweep it, because each identifier introduces a value of a
 * different WIDTH and a body sized for one is malformed for the others. Both
 * arms would answer MQTTBadResponse for the wrong reason and the sweep would
 * discriminate nothing.
 *
 * So the identifier is swept five times, once per value shape: a one-byte
 * value, a two-byte value, a four-byte value, a string, and a user property
 * (two strings). Each prints its own accepted set, so the five sets together
 * are the property table AND say which identifier is which type -- which is the
 * thing a reader wants to check against MQTT 5.0 §3.2.2.3.
 *
 * # The status is three-valued here
 *
 * A non-zero reason code makes the C return `MQTTServerRefused` and parse the
 * property section anyway, because the Reason String that says WHY lives in it.
 * That is a third status this package has not seen before, and the out
 * parameters are filled on it, so the trace prints them for both Success and
 * ServerRefused.
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core_mqtt_serializer.h"

#define MAX_BODY    64

static void put_hex( const uint8_t *p, size_t n )
{
    size_t i;

    if( n == 0U )
    {
        printf( "-" );
        return;
    }

    for( i = 0; i < n; i++ )
    {
        printf( "%02x", p[ i ] );
    }
}

static const char * status_name( MQTTStatus_t status )
{
    switch( status )
    {
        case MQTTSuccess:       return "Success";
        case MQTTBadParameter:  return "BadParameter";
        case MQTTBadResponse:   return "BadResponse";
        case MQTTServerRefused: return "ServerRefused";
        default:                return "OTHER";
    }
}

/* ---- the named cases ----------------------------------------------------- */

typedef struct
{
    const char *name;
    uint8_t     type;             /* 0x20 for every case but the routing ones */
    uint32_t    remainingLength;  /* always equal to bodyLength; see below */
    size_t      bodyLength;
    uint8_t     body[ MAX_BODY ];
    uint32_t    maxPacketSize;
    bool        requestResponseInfo;
} ConnackCase_t;

/* The same rule the ack driver enforces: a case whose claim exceeds its body
 * would have the C indexing past `body`, so its answer would depend on whatever
 * is next in memory rather than on the library. That is not an oracle. The
 * over-claim is pinned on the Rust side alone. */
static const ConnackCase_t CASES[] = {
    /* --- the shape of the thing --- */
    { "minimum",                  0x20U, 3U, 3U, { 0x00, 0x00, 0x00 },             1024U, true },
    { "session-present",          0x20U, 3U, 3U, { 0x01, 0x00, 0x00 },             1024U, true },
    { "refused-not-authorized",   0x20U, 3U, 3U, { 0x00, 0x87, 0x00 },             1024U, true },
    /* MQTT-3.2.2-4: a non-zero reason code MUST come with session present clear. */
    { "resumed-and-refused",      0x20U, 3U, 3U, { 0x01, 0x87, 0x00 },             1024U, true },
    { "reserved-flag-bit-1",      0x20U, 3U, 3U, { 0x02, 0x00, 0x00 },             1024U, true },
    { "reserved-flag-bit-7",      0x20U, 3U, 3U, { 0x80, 0x00, 0x00 },             1024U, true },
    { "unknown-reason-code",      0x20U, 3U, 3U, { 0x00, 0x7F, 0x00 },             1024U, true },
    { "two-byte-body",            0x20U, 2U, 2U, { 0x00, 0x00 },                   1024U, true },
    { "empty-body",               0x20U, 0U, 0U, { 0 },                            1024U, true },

    /* --- one property at a time, each with a real value --- */
    { "session-expiry",           0x20U, 8U, 8U,
      { 0x00, 0x00, 0x05, 0x11, 0x00, 0x00, 0x0E, 0x10 },                          1024U, true },
    { "receive-maximum",          0x20U, 6U, 6U,
      { 0x00, 0x00, 0x03, 0x21, 0x00, 0x14 },                                      1024U, true },
    { "receive-maximum-zero",     0x20U, 6U, 6U,
      { 0x00, 0x00, 0x03, 0x21, 0x00, 0x00 },                                      1024U, true },
    { "maximum-qos-zero",         0x20U, 5U, 5U, { 0x00, 0x00, 0x02, 0x24, 0x00 }, 1024U, true },
    { "maximum-qos-one",          0x20U, 5U, 5U, { 0x00, 0x00, 0x02, 0x24, 0x01 }, 1024U, true },
    /* QoS 2 is spelled by OMITTING the property; saying "2" is malformed. */
    { "maximum-qos-two",          0x20U, 5U, 5U, { 0x00, 0x00, 0x02, 0x24, 0x02 }, 1024U, true },
    { "retain-unavailable",       0x20U, 5U, 5U, { 0x00, 0x00, 0x02, 0x25, 0x00 }, 1024U, true },
    { "retain-not-a-boolean",     0x20U, 5U, 5U, { 0x00, 0x00, 0x02, 0x25, 0xFF }, 1024U, true },
    { "maximum-packet-size",      0x20U, 8U, 8U,
      { 0x00, 0x00, 0x05, 0x27, 0x00, 0x01, 0x00, 0x00 },                          1024U, true },
    { "maximum-packet-size-zero", 0x20U, 8U, 8U,
      { 0x00, 0x00, 0x05, 0x27, 0x00, 0x00, 0x00, 0x00 },                          1024U, true },
    { "assigned-client-id",       0x20U, 9U, 9U,
      { 0x00, 0x00, 0x06, 0x12, 0x00, 0x03, 'a', 'b', 'c' },                       1024U, true },
    { "topic-alias-maximum",      0x20U, 6U, 6U,
      { 0x00, 0x00, 0x03, 0x22, 0x00, 0x0A },                                      1024U, true },
    { "reason-string",            0x20U, 9U, 9U,
      { 0x00, 0x00, 0x06, 0x1F, 0x00, 0x03, 'w', 'h', 'y' },                       1024U, true },
    { "user-property",            0x20U, 10U, 10U,
      { 0x00, 0x00, 0x07, 0x26, 0x00, 0x01, 'k', 0x00, 0x01, 'v' },                1024U, true },
    /* A user property MAY repeat where everything else may not. */
    { "two-user-properties",      0x20U, 17U, 17U,
      { 0x00, 0x00, 0x0E,
        0x26, 0x00, 0x01, 'a', 0x00, 0x01, 'b',
        0x26, 0x00, 0x01, 'c', 0x00, 0x01, 'd' },                                  1024U, true },
    { "wildcard-unavailable",     0x20U, 5U, 5U, { 0x00, 0x00, 0x02, 0x28, 0x00 }, 1024U, true },
    { "subscription-id-unavailable", 0x20U, 5U, 5U, { 0x00, 0x00, 0x02, 0x29, 0x00 }, 1024U, true },
    { "shared-sub-unavailable",   0x20U, 5U, 5U, { 0x00, 0x00, 0x02, 0x2A, 0x00 }, 1024U, true },
    { "server-keep-alive",        0x20U, 6U, 6U,
      { 0x00, 0x00, 0x03, 0x13, 0x00, 0x3C },                                      1024U, true },
    { "server-reference",         0x20U, 9U, 9U,
      { 0x00, 0x00, 0x06, 0x1C, 0x00, 0x03, 'a', '.', 'b' },                       1024U, true },
    { "auth-method",              0x20U, 9U, 9U,
      { 0x00, 0x00, 0x06, 0x15, 0x00, 0x03, 'S', 'C', 'R' },                       1024U, true },
    /* Binary data per the specification, read with decodeUtf8 -- identical wire
     * format, and decodeUtf8 never validated UTF-8 anyway. */
    { "auth-data",                0x20U, 8U, 8U,
      { 0x00, 0x00, 0x05, 0x16, 0x00, 0x02, 0xFF, 0xFE },                          1024U, true },

    /* --- Response Information, which the client must have asked for --- */
    { "response-info-requested",  0x20U, 9U, 9U,
      { 0x00, 0x00, 0x06, 0x1A, 0x00, 0x03, 'r', 'e', 's' },                       1024U, true },
    { "response-info-unrequested", 0x20U, 9U, 9U,
      { 0x00, 0x00, 0x06, 0x1A, 0x00, 0x03, 'r', 'e', 's' },                       1024U, false },
    /* Reason String and User Property are NOT gated on request-problem-info in
     * a CONNACK -- [MQTT-3.1.2-29] allows them -- and the C agrees. */
    { "reason-string-unrequested", 0x20U, 9U, 9U,
      { 0x00, 0x00, 0x06, 0x1F, 0x00, 0x03, 'w', 'h', 'y' },                       1024U, false },

    /* --- malformed property sections --- */
    { "repeated-reason-string",   0x20U, 13U, 13U,
      { 0x00, 0x00, 0x0A, 0x1F, 0x00, 0x02, 'h', 'i', 0x1F, 0x00, 0x02, 'y', 'o' }, 1024U, true },
    { "repeated-receive-maximum", 0x20U, 9U, 9U,
      { 0x00, 0x00, 0x06, 0x21, 0x00, 0x14, 0x21, 0x00, 0x15 },                    1024U, true },
    { "unknown-property",         0x20U, 5U, 5U, { 0x00, 0x00, 0x02, 0x7F, 0x00 }, 1024U, true },
    /* The property length says more than the body has. */
    { "property-length-too-long", 0x20U, 5U, 5U, { 0x00, 0x00, 0x09, 0x24, 0x00 }, 1024U, true },
    /* ...and less: a CONNACK's properties are last, so the fit is EXACT. */
    { "property-length-too-short", 0x20U, 6U, 6U,
      { 0x00, 0x00, 0x02, 0x24, 0x00, 0x00 },                                      1024U, true },
    { "property-length-non-minimal", 0x20U, 6U, 6U,
      { 0x00, 0x00, 0x80, 0x00, 0x24, 0x00 },                                      1024U, true },
    /* A property whose value runs off the end of the section. */
    { "string-runs-past-the-section", 0x20U, 7U, 7U,
      { 0x00, 0x00, 0x04, 0x1F, 0x00, 0x09, 'x' },                                 1024U, true },

    /* --- several at once, which is what a real broker sends --- */
    { "a-realistic-connack",      0x20U, 20U, 20U,
      { 0x00, 0x00, 0x11,
        0x21, 0x00, 0x14,                   /* receive maximum 20 */
        0x24, 0x01,                         /* maximum QoS 1 */
        0x25, 0x01,                         /* retain available */
        0x27, 0x00, 0x01, 0x00, 0x00,       /* maximum packet size 65536 */
        0x2A, 0x00,                         /* shared subs unavailable */
        0x13, 0x00, 0x3C },                 /* server keep alive 60 */
      1024U, true },

    /* --- routing and limits --- */
    { "puback-routed-here",       0x40U, 3U, 3U, { 0x00, 0x00, 0x00 },             1024U, true },
    { "zero-max-packet-size",     0x20U, 3U, 3U, { 0x00, 0x00, 0x00 },             0U,    true },
    { "exactly-at-the-maximum",   0x20U, 3U, 3U, { 0x00, 0x00, 0x00 },             5U,    true },
    { "one-byte-over-the-maximum", 0x20U, 3U, 3U, { 0x00, 0x00, 0x00 },            4U,    true },
};

#define N_CASES    ( sizeof( CASES ) / sizeof( CASES[ 0 ] ) )

/* Run one CONNACK and print everything the C hands back. */
static MQTTStatus_t run_one( uint8_t type,
                             uint32_t remainingLength,
                             const uint8_t *bodyBytes,
                             size_t bodyLength,
                             uint32_t maxPacketSize,
                             bool requestResponseInfo,
                             bool print )
{
    uint8_t body[ MAX_BODY ];
    MQTTPacketInfo_t packet;
    MQTTConnectionProperties_t props;
    MQTTPropBuilder_t propBuffer;
    bool sessionPresent = false;
    MQTTStatus_t status;

    memset( body, 0, sizeof body );
    memcpy( body, bodyBytes, bodyLength );

    memset( &packet, 0, sizeof packet );
    packet.type = type;
    packet.remainingLength = remainingLength;
    packet.pRemainingData = body;

    memset( &props, 0, sizeof props );
    props.maxPacketSize = maxPacketSize;
    props.requestResponseInfo = requestResponseInfo;

    memset( &propBuffer, 0, sizeof propBuffer );

    status = MQTT_DeserializeConnAck( &packet, &sessionPresent, &propBuffer, &props );

    if( print )
    {
        printf( " -> %s", status_name( status ) );

        /* The out-parameters are filled on BOTH Success and ServerRefused,
         * because a refused connection still parses its properties. */
        if( ( status == MQTTSuccess ) || ( status == MQTTServerRefused ) )
        {
            printf( " sp=%u fields=%08x exp=%u rmax=%u qos=%u ret=%u mps=%u"
                    " alias=%u wild=%u subid=%u shared=%u ka=%u props=",
                    sessionPresent ? 1U : 0U,
                    ( unsigned ) propBuffer.fieldSet,
                    ( unsigned ) props.sessionExpiry,
                    ( unsigned ) props.serverReceiveMax,
                    ( unsigned ) props.serverMaxQos,
                    ( unsigned ) props.retainAvailable,
                    ( unsigned ) props.serverMaxPacketSize,
                    ( unsigned ) props.serverTopicAliasMax,
                    ( unsigned ) props.isWildcardAvailable,
                    ( unsigned ) props.isSubscriptionIdAvailable,
                    ( unsigned ) props.isSharedAvailable,
                    ( unsigned ) props.serverKeepAlive );
            put_hex( propBuffer.pBuffer, propBuffer.bufferLength );
        }
    }

    return status;
}

static void run_case( size_t i )
{
    const ConnackCase_t *c = &CASES[ i ];

    if( ( size_t ) c->remainingLength != c->bodyLength )
    {
        fprintf( stderr, "case %s claims %u bytes and carries %u\n",
                 c->name, ( unsigned ) c->remainingLength,
                 ( unsigned ) c->bodyLength );
        exit( 1 );
    }

    printf( "case %u %s type=%02x rl=%u max=%u rri=%u in=",
            ( unsigned ) i, c->name, ( unsigned ) c->type,
            ( unsigned ) c->remainingLength, ( unsigned ) c->maxPacketSize,
            c->requestResponseInfo ? 1U : 0U );
    put_hex( c->body, c->bodyLength );

    ( void ) run_one( c->type, c->remainingLength, c->body, c->bodyLength,
                      c->maxPacketSize, c->requestResponseInfo, true );

    printf( "\n" );
}

/* ---- the sweeps ---------------------------------------------------------- */

/* Every value of the reason-code byte, through `isValidConnackReasonCode`.
 * Zero is Success and the other legal codes are ServerRefused, so the two are
 * counted separately -- a table that answered Success for everything would be
 * no table at all. */
static void sweep_reason_code( void )
{
    unsigned value;
    unsigned success = 0U;
    unsigned refused = 0U;
    unsigned rejected = 0U;
    int first = 1;

    printf( "reason-code-sweep accepted=" );

    for( value = 0U; value < 256U; value++ )
    {
        uint8_t body[ 3 ] = { 0x00, 0x00, 0x00 };
        MQTTStatus_t status;

        body[ 1 ] = ( uint8_t ) value;
        status = run_one( 0x20U, 3U, body, 3U, 1024U, true, false );

        if( ( status == MQTTSuccess ) || ( status == MQTTServerRefused ) )
        {
            printf( "%s%02x", first ? "" : ",", value );
            first = 0;

            if( status == MQTTSuccess )
            {
                success++;
            }
            else
            {
                refused++;
            }
        }
        else
        {
            rejected++;
        }
    }

    printf( " n=%u success=%u refused=%u rejected=%u\n",
            success + refused, success, refused, rejected );
}

/* Every value of the property-identifier byte, at one value SHAPE.
 *
 * `pValue` is the bytes that follow the identifier; the property section is the
 * identifier plus those, and the body is the flags, the reason code, the
 * property length and the section. */
static void sweep_property( const char *label,
                            const uint8_t *pValue,
                            size_t valueLength )
{
    unsigned id;
    unsigned accepted = 0U;
    unsigned rejected = 0U;
    int first = 1;
    size_t sectionLength = valueLength + 1U;

    printf( "property-sweep %s accepted=", label );

    for( id = 0U; id < 256U; id++ )
    {
        uint8_t body[ MAX_BODY ];
        MQTTStatus_t status;
        size_t bodyLength = sectionLength + 3U;

        body[ 0 ] = 0x00;
        body[ 1 ] = 0x00;
        body[ 2 ] = ( uint8_t ) sectionLength;
        body[ 3 ] = ( uint8_t ) id;
        memcpy( &body[ 4 ], pValue, valueLength );

        status = run_one( 0x20U, ( uint32_t ) bodyLength, body, bodyLength,
                          1024U, true, false );

        if( status == MQTTSuccess )
        {
            printf( "%s%02x", first ? "" : ",", id );
            first = 0;
            accepted++;
        }
        else
        {
            rejected++;
        }
    }

    printf( " n=%u rejected=%u\n", accepted, rejected );
}

int main( void )
{
    size_t i;

    /* One value of each shape a CONNACK property can take. The two-byte and
     * four-byte values are non-zero, because Receive Maximum and Maximum Packet
     * Size refuse zero and the sweep would then confuse "unknown identifier"
     * with "known identifier, illegal value". */
    static const uint8_t ONE_BYTE[] = { 0x00 };
    static const uint8_t TWO_BYTE[] = { 0x00, 0x01 };
    static const uint8_t FOUR_BYTE[] = { 0x00, 0x00, 0x00, 0x01 };
    static const uint8_t STRING[] = { 0x00, 0x01, 'x' };
    static const uint8_t USER_PROP[] = { 0x00, 0x01, 'k', 0x00, 0x01, 'v' };

    printf( "geometry cases=%u\n", ( unsigned ) N_CASES );

    for( i = 0; i < N_CASES; i++ )
    {
        run_case( i );
    }

    sweep_reason_code();

    sweep_property( "one-byte", ONE_BYTE, sizeof ONE_BYTE );
    sweep_property( "two-byte", TWO_BYTE, sizeof TWO_BYTE );
    sweep_property( "four-byte", FOUR_BYTE, sizeof FOUR_BYTE );
    sweep_property( "string", STRING, sizeof STRING );
    sweep_property( "user-property", USER_PROP, sizeof USER_PROP );

    printf( "end\n" );
    return 0;
}
