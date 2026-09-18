/* The C arm of K7's coreMQTT CONNECTION-CONTEXT differential -- and the last of
 * `core_mqtt_serializer.c`.
 *
 * `core_mqtt_serializer.c` is compiled VERBATIM out of the pinned checkout;
 * nothing here copies or edits it.
 *
 * # What is left of the file, and why it belongs together
 *
 * Thirteen slices have taken every codec out of this file. What remains is the
 * part that is not a codec: the two constructors, the helper that fills a
 * connection context from the properties a CONNECT carried, the parameter
 * validator an outgoing PUBLISH goes through, and a SECOND reader of the fixed
 * header -- the buffered twin of the callback-driven one already proven.
 *
 * # The instrument: one job, done twice
 *
 * Two of these four are second copies of something this package has already
 * diffed against the C, which is the strongest instrument the house has:
 *
 *   `MQTT_ProcessIncomingPacketTypeAndLength` reads a fixed header out of a
 *   BUFFER; `MQTT_GetIncomingPacketTypeAndLength` reads the same header off a
 *   CALLBACK. Both are driven here over the same bytes and their answers are
 *   printed side by side, so a line where they disagree is a line about the
 *   library rather than about us.
 *
 *   `updateContextWithConnectProps` walks the CONNECT property section, which
 *   `MQTT_ValidateConnectProperties` has just walked. Both accept the same nine
 *   identifiers; whether they accept the same VALUES is what the cases ask.
 *
 * A correct arm is the best instrument for finding a wrong one.
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core_mqtt_serializer.h"

#define MAX_BYTES    12
#define MAX_SECTION  24

/* The out-parameter sentinels: values no arm can produce, so "left alone" is
 * visible in the trace. */
#define NO_REMAINING_LENGTH    0xAAAAAAAAU
#define NO_HEADER_LENGTH       0xAAU
#define NO_TYPE                0xAAU

static const char * status_name( MQTTStatus_t status )
{
    switch( status )
    {
        case MQTTSuccess:         return "Success";
        case MQTTBadParameter:    return "BadParameter";
        case MQTTBadResponse:     return "BadResponse";
        case MQTTNoDataAvailable: return "NoDataAvailable";
        case MQTTRecvFailed:      return "RecvFailed";
        case MQTTNeedMoreBytes:   return "NeedMoreBytes";
        default:                  return "OTHER";
    }
}

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

/* The rolling digest every driver in this package uses: FNV-1a's multiplier,
 * and a seed that is one digit short of FNV-1a's offset basis and has been
 * since the first differential. That costs nothing -- a digest only has to be
 * deterministic and shared by both arms -- and it is recorded here so nobody
 * "fixes" one arm of a pair. */
static uint64_t fnv( uint64_t state, uint64_t value )
{
    return ( state ^ value ) * 1099511628211ULL;
}

#define FNV_SEED    1469598103934665603ULL

/* ---- the two readers, over the same bytes --------------------------------- */

/* The scripted transport hands out the case's bytes and then reports "nothing
 * available", which is what a socket holding N bytes does. */
static const uint8_t *g_bytes;
static size_t g_available;
static size_t g_at;
static size_t g_calls;

static int32_t scripted_recv( NetworkContext_t *pContext,
                              void *pBuffer,
                              size_t bytesToRecv )
{
    ( void ) pContext;
    g_calls++;

    if( bytesToRecv != 1U )
    {
        return -1;
    }

    if( g_at >= g_available )
    {
        /* Nothing more has arrived yet. */
        return 0;
    }

    *( ( uint8_t * ) pBuffer ) = g_bytes[ g_at ];
    g_at++;
    return 1;
}

typedef struct
{
    const char *name;
    size_t      available;
    size_t      length;
    uint8_t     bytes[ MAX_BYTES ];
} ReadCase_t;

static const ReadCase_t READ_CASES[] = {
    /* --- ordinary packets, with exactly the bytes the header needs --- */
    { "pingresp",           2, 2, { 0xD0, 0x00 } },
    { "puback",             3, 3, { 0x40, 0x02, 0x00 } },
    { "two-byte-length",    3, 3, { 0x30, 0x80, 0x01 } },
    { "three-byte-length",  4, 4, { 0x30, 0xFF, 0xFF, 0x7F } },
    { "four-byte-length",   5, 5, { 0x30, 0xFF, 0xFF, 0xFF, 0x7F } },

    /* --- nothing, and the type byte alone --- */
    { "nothing-available",  0, 0, { 0 } },
    /* One byte in: the type is known and the length has not started. The two
     * readers part company here -- see the `dual` lines. */
    { "type-byte-only",     1, 1, { 0x30 } },
    { "two-of-a-three-byte-length", 2, 3, { 0x30, 0x80, 0x01 } },

    /* --- the type byte --- */
    { "type-connect-is-client-only", 2, 2, { 0x10, 0x00 } },
    { "type-zero",          2, 2, { 0x00, 0x00 } },

    /* --- what the length can get wrong --- */
    { "five-length-bytes",  6, 6, { 0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F } },
    { "non-minimal-length", 3, 3, { 0x30, 0x80, 0x00 } },

    /* --- more bytes than the header needs, which is the NORMAL case: a real
     * receive buffer holds the header AND the packet behind it --- */
    { "header-and-body",    9, 9, { 0x30, 0x07, 1, 2, 3, 4, 5, 6, 7 } },
};

#define N_READ_CASES    ( sizeof( READ_CASES ) / sizeof( READ_CASES[ 0 ] ) )

/* Run the buffered reader, print what it answered and what it wrote. */
static MQTTStatus_t run_process( const uint8_t *bytes, size_t available, bool print )
{
    MQTTPacketInfo_t packet;
    MQTTStatus_t status;

    packet.type = NO_TYPE;
    packet.pRemainingData = NULL;
    packet.remainingLength = NO_REMAINING_LENGTH;
    packet.headerLength = NO_HEADER_LENGTH;

    status = MQTT_ProcessIncomingPacketTypeAndLength( bytes, &available, &packet );

    if( print )
    {
        printf( "process=%s", status_name( status ) );

        /* Only on success. On a refusal the C leaves `remainingLength` and
         * `headerLength` as the caller left them and has already written
         * `type` -- a sentinel printed in both arms is a constant compared
         * with itself, not a comparison. The refusals are the status. */
        if( status == MQTTSuccess )
        {
            printf( " type=%02x rl=%u hl=%u", ( unsigned ) packet.type,
                    ( unsigned ) packet.remainingLength,
                    ( unsigned ) packet.headerLength );
        }
    }

    return status;
}

/* Run the callback-driven reader over the same bytes. */
static MQTTStatus_t run_get( const uint8_t *bytes, size_t available, bool print )
{
    MQTTPacketInfo_t packet;
    MQTTStatus_t status;

    packet.type = NO_TYPE;
    packet.pRemainingData = NULL;
    packet.remainingLength = NO_REMAINING_LENGTH;
    packet.headerLength = NO_HEADER_LENGTH;

    g_bytes = bytes;
    g_available = available;
    g_at = 0;
    g_calls = 0;

    status = MQTT_GetIncomingPacketTypeAndLength( scripted_recv, NULL, &packet );

    if( print )
    {
        printf( "get=%s", status_name( status ) );

        if( status == MQTTSuccess )
        {
            /* No `hl`: this reader never sets headerLength, which slice 13
             * found and pinned. The buffered twin does. */
            printf( " type=%02x rl=%u", ( unsigned ) packet.type,
                    ( unsigned ) packet.remainingLength );
        }

        printf( " calls=%u", ( unsigned ) g_calls );
    }

    return status;
}

static void run_read_case( size_t i )
{
    const ReadCase_t *c = &READ_CASES[ i ];

    printf( "dual %u %s avail=%u bytes=", ( unsigned ) i, c->name,
            ( unsigned ) c->available );
    put_hex( c->bytes, c->length );
    printf( " -> " );
    ( void ) run_process( c->bytes, c->available, true );
    printf( " | " );
    ( void ) run_get( c->bytes, c->available, true );
    printf( "\n" );
}

/* Every type byte through BOTH readers, with the accepted sets printed and the
 * disagreements counted. */
static void read_sweep( void )
{
    unsigned id;
    unsigned accepted = 0U;
    unsigned refused = 0U;
    unsigned differ = 0U;
    int first = 1;
    uint64_t digest = FNV_SEED;

    printf( "dual-sweep accepted=" );

    for( id = 0U; id < 256U; id++ )
    {
        uint8_t bytes[ 2 ];
        MQTTStatus_t a, b;

        bytes[ 0 ] = ( uint8_t ) id;
        bytes[ 1 ] = 0x00;

        a = run_process( bytes, sizeof bytes, false );
        b = run_get( bytes, sizeof bytes, false );

        digest = fnv( digest, ( uint64_t ) a );
        digest = fnv( digest, ( uint64_t ) b );

        if( a != b )
        {
            differ++;
        }

        if( a == MQTTSuccess )
        {
            printf( "%s%02x", first ? "" : ",", id );
            first = 0;
            accepted++;
        }
        else
        {
            refused++;
        }
    }

    printf( " n=%u refused=%u differ=%u digest=%016llx\n", accepted, refused,
            differ, ( unsigned long long ) digest );
}

/* The same sweep one byte short, where the two readers are known to part. */
static void truncated_sweep( void )
{
    unsigned id;
    unsigned differ = 0U;
    uint64_t digest = FNV_SEED;

    for( id = 0U; id < 256U; id++ )
    {
        uint8_t bytes[ 2 ];
        MQTTStatus_t a, b;

        bytes[ 0 ] = ( uint8_t ) id;
        bytes[ 1 ] = 0x00;

        /* One byte available, so the length has not arrived. */
        a = run_process( bytes, 1U, false );
        b = run_get( bytes, 1U, false );

        digest = fnv( digest, ( uint64_t ) a );
        digest = fnv( digest, ( uint64_t ) b );

        if( a != b )
        {
            differ++;
        }
    }

    printf( "truncated-sweep differ=%u digest=%016llx\n", differ,
            ( unsigned long long ) digest );
}

/* ---- the two constructors -------------------------------------------------- */

static void print_init( void )
{
    MQTTConnectionProperties_t properties;
    MQTTStatus_t status;

    memset( &properties, 0xAA, sizeof properties );
    status = MQTT_InitConnect( &properties );

    printf( "init %s sessionexp=%u recvmax=%u maxpkt=%u aliasmax=%u rri=%u rpi=%u "
            "srecvmax=%u smaxqos=%u retain=%u smaxpkt=%u saliasmax=%u wildcard=%u "
            "subid=%u shared=%u keepalive=%u\n",
            status_name( status ),
            ( unsigned ) properties.sessionExpiry,
            ( unsigned ) properties.receiveMax,
            ( unsigned ) properties.maxPacketSize,
            ( unsigned ) properties.topicAliasMax,
            properties.requestResponseInfo ? 1U : 0U,
            properties.requestProblemInfo ? 1U : 0U,
            ( unsigned ) properties.serverReceiveMax,
            ( unsigned ) properties.serverMaxQos,
            ( unsigned ) properties.retainAvailable,
            ( unsigned ) properties.serverMaxPacketSize,
            ( unsigned ) properties.serverTopicAliasMax,
            ( unsigned ) properties.isWildcardAvailable,
            ( unsigned ) properties.isSubscriptionIdAvailable,
            ( unsigned ) properties.isSharedAvailable,
            ( unsigned ) properties.serverKeepAlive );
}

/* `MQTTPropertyBuilder_Init` takes a POINTER and a LENGTH, which is to say two
 * numbers that must agree and are not checked against each other: a caller may
 * hand it an eight-byte buffer and a length of a million, and it will say
 * MQTTSuccess. So the cases here always pass the real length of a real buffer,
 * because that is the whole of what the Rust arm -- which takes one slice -- can
 * be handed.
 *
 * Two of the C's refusals are therefore absent from this trace rather than
 * failing in it: a NULL buffer, and a length at or above
 * MQTT_REMAINING_LENGTH_INVALID. The first is the null-pointer family again;
 * the second needs a 256 MB buffer to reach through a slice, and is pinned by a
 * unit test on the constant instead. */
static void builder_case( const char *name, size_t length )
{
    uint8_t buffer[ 32 ];
    MQTTPropBuilder_t builder;
    MQTTStatus_t status;

    memset( &builder, 0xAA, sizeof builder );
    status = MQTTPropertyBuilder_Init( &builder, buffer, length );

    printf( "builder %s length=%u -> %s", name, ( unsigned ) length,
            status_name( status ) );

    if( status == MQTTSuccess )
    {
        printf( " index=%u buflen=%u fieldset=%u",
                ( unsigned ) builder.currentIndex,
                ( unsigned ) builder.bufferLength,
                ( unsigned ) builder.fieldSet );
    }

    printf( "\n" );
}

/* ---- the third copy of the CONNECT property table -------------------------- */

typedef struct
{
    const char *name;
    size_t      length;
    uint8_t     section[ MAX_SECTION ];
} ContextCase_t;

static const ContextCase_t CONTEXT_CASES[] = {
    { "empty",                  0, { 0 } },
    { "session-expiry",         5, { 0x11, 0x00, 0x00, 0x0E, 0x10 } },
    { "receive-max",            3, { 0x21, 0x00, 0x14 } },
    { "max-packet-size",        5, { 0x27, 0x00, 0x01, 0x00, 0x00 } },
    { "topic-alias-max",        3, { 0x22, 0x00, 0x0A } },
    { "all-four",              16, { 0x11, 0x00, 0x00, 0x0E, 0x10,
                                     0x21, 0x00, 0x14,
                                     0x27, 0x00, 0x01, 0x00, 0x00,
                                     0x22, 0x00, 0x0A } },

    /* --- the four values MQTT_ValidateConnectProperties refuses. This walker
     * has just been handed the same section; does it agree? --- */
    { "receive-max-zero",       3, { 0x21, 0x00, 0x00 } },
    { "max-packet-zero",        5, { 0x27, 0x00, 0x00, 0x00, 0x00 } },
    { "request-problem-two",    2, { 0x17, 0x02 } },
    { "auth-data-alone",        4, { 0x16, 0x00, 0x01, 'd' } },

    /* --- repeats: which of the nine does THIS one deduplicate? --- */
    { "repeated-session-expiry", 10, { 0x11, 0x00, 0x00, 0x0E, 0x10,
                                       0x11, 0x00, 0x00, 0x0E, 0x10 } },
    { "repeated-receive-max",    6, { 0x21, 0x00, 0x14, 0x21, 0x00, 0x15 } },
    { "repeated-request-problem", 4, { 0x17, 0x00, 0x17, 0x01 } },
    { "repeated-auth-method",    8, { 0x15, 0x00, 0x01, 'x', 0x15, 0x00, 0x01, 'y' } },
    { "two-user-properties",    14, { 0x26, 0x00, 0x01, 'a', 0x00, 0x01, 'b',
                                      0x26, 0x00, 0x01, 'c', 0x00, 0x01, 'd' } },

    /* --- and a malformed value --- */
    { "truncated-string",        4, { 0x15, 0x00, 0x09, 'x' } },
    { "will-property-here",      5, { 0x18, 0x00, 0x00, 0x00, 0x0A } },
};

#define N_CONTEXT_CASES    ( sizeof( CONTEXT_CASES ) / sizeof( CONTEXT_CASES[ 0 ] ) )

static MQTTStatus_t run_context( const uint8_t *section, size_t length, bool print )
{
    uint8_t bytes[ MAX_SECTION ];
    MQTTPropBuilder_t builder;
    MQTTConnectionProperties_t properties;
    MQTTStatus_t status;

    memset( bytes, 0, sizeof bytes );
    memcpy( bytes, section, length );

    memset( &builder, 0, sizeof builder );
    builder.pBuffer = bytes;
    builder.bufferLength = sizeof bytes;
    builder.currentIndex = length;

    /* Started from the library's own defaults, so a field this walker does not
     * touch is visibly the default rather than rubbish. */
    ( void ) MQTT_InitConnect( &properties );

    status = updateContextWithConnectProps( &builder, &properties );

    if( print )
    {
        printf( " -> %s sessionexp=%u recvmax=%u maxpkt=%u aliasmax=%u",
                status_name( status ),
                ( unsigned ) properties.sessionExpiry,
                ( unsigned ) properties.receiveMax,
                ( unsigned ) properties.maxPacketSize,
                ( unsigned ) properties.topicAliasMax );
    }

    return status;
}

static void context_sweep( const char *shapeName, const uint8_t *value, size_t valueLength )
{
    unsigned id;
    unsigned accepted = 0U;
    unsigned refused = 0U;
    int first = 1;

    printf( "ctx-sweep %s accepted=", shapeName );

    for( id = 0U; id < 256U; id++ )
    {
        uint8_t section[ MAX_SECTION ];

        memset( section, 0, sizeof section );
        section[ 0 ] = ( uint8_t ) id;
        memcpy( &section[ 1 ], value, valueLength );

        if( run_context( section, valueLength + 1U, false ) == MQTTSuccess )
        {
            printf( "%s%02x", first ? "" : ",", id );
            first = 0;
            accepted++;
        }
        else
        {
            refused++;
        }
    }

    printf( " n=%u refused=%u\n", accepted, refused );
}

/* ---- the outgoing PUBLISH's parameter validator ---------------------------- */

/* As above, `pTopicName` and `topicNameLength` are two numbers that must agree,
 * and the C checks only one direction of it (NULL with a non-zero length). A
 * `&[u8]` carries both, so that refusal is unreachable here -- another member
 * of the null-pointer family -- and no case in this trace asks for it. */
static void params_case( const char *name,
                         bool retain,
                         uint8_t retainAvailable,
                         MQTTQoS_t qos,
                         uint8_t maxQos,
                         uint16_t topicAlias,
                         uint16_t topicNameLength,
                         uint32_t maxPacketSize )
{
    MQTTPublishInfo_t info;
    MQTTStatus_t status;
    static const char TOPIC[] = "abcdefgh";

    memset( &info, 0, sizeof info );
    info.retain = retain;
    info.qos = qos;
    info.pTopicName = TOPIC;
    info.topicNameLength = topicNameLength;

    status = MQTT_ValidatePublishParams( &info, retainAvailable, maxQos, topicAlias,
                                         maxPacketSize );

    printf( "params %s retain=%u avail=%u qos=%u maxqos=%u alias=%u topiclen=%u "
            "maxpkt=%u -> %s\n",
            name, retain ? 1U : 0U, ( unsigned ) retainAvailable, ( unsigned ) qos,
            ( unsigned ) maxQos, ( unsigned ) topicAlias, ( unsigned ) topicNameLength,
            ( unsigned ) maxPacketSize, status_name( status ) );
}

/* Every combination of the six things this validator looks at.
 *
 * 2 x 2 x 3 x 3 x 2 x 3 x 2 = 432 calls, digested rather than printed -- unlike
 * an identifier sweep, the accepted set here is most of the space and a printed
 * list would say nothing. */
static void params_sweep( void )
{
    static const MQTTQoS_t QOS[] = { MQTTQoS0, MQTTQoS1, MQTTQoS2 };
    static const uint8_t MAX_QOS[] = { 0U, 1U, 2U };
    static const uint16_t TOPIC_LENGTH[] = { 0U, 1U, 8U };
    static const uint32_t MAX_PACKET[] = { 0U, 1024U };
    static const uint16_t ALIAS[] = { 0U, 1U };

    static const char TOPIC[] = "abcdefgh";

    unsigned accepted = 0U;
    unsigned refused = 0U;
    uint64_t digest = FNV_SEED;
    size_t a, b, c, d, e, f, g;

    for( a = 0; a < 2U; a++ )                              /* retain */
    {
        for( b = 0; b < 2U; b++ )                          /* retainAvailable */
        {
            for( c = 0; c < 3U; c++ )                      /* qos */
            {
                for( d = 0; d < 3U; d++ )                  /* maxQos */
                {
                    for( e = 0; e < 2U; e++ )              /* topicAlias */
                    {
                        for( f = 0; f < 3U; f++ )          /* topicNameLength */
                        {
                            for( g = 0; g < 2U; g++ )      /* maxPacketSize */
                            {
                                MQTTPublishInfo_t info;
                                MQTTStatus_t status;

                                memset( &info, 0, sizeof info );
                                info.retain = ( a == 1U );
                                info.qos = QOS[ c ];
                                info.pTopicName = TOPIC;
                                info.topicNameLength = TOPIC_LENGTH[ f ];

                                status = MQTT_ValidatePublishParams( &info,
                                                                     ( uint8_t ) b,
                                                                     MAX_QOS[ d ],
                                                                     ALIAS[ e ],
                                                                     MAX_PACKET[ g ] );

                                digest = fnv( digest, ( uint64_t ) status );

                                if( status == MQTTSuccess )
                                {
                                    accepted++;
                                }
                                else
                                {
                                    refused++;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    printf( "params-sweep n=%u refused=%u digest=%016llx\n", accepted, refused,
            ( unsigned long long ) digest );
}

/* ---- main ------------------------------------------------------------------ */

int main( void )
{
    size_t i;

    static const uint8_t ONE_BYTE[] = { 0x01 };
    static const uint8_t TWO_BYTE[] = { 0x00, 0x01 };
    static const uint8_t FOUR_BYTE[] = { 0x00, 0x00, 0x00, 0x01 };
    static const uint8_t STRING[] = { 0x00, 0x01, 'x' };
    static const uint8_t VARINT[] = { 0x07 };
    static const uint8_t USER_PROP[] = { 0x00, 0x01, 'k', 0x00, 0x01, 'v' };

    printf( "geometry reads=%u contexts=%u\n", ( unsigned ) N_READ_CASES,
            ( unsigned ) N_CONTEXT_CASES );

    for( i = 0; i < N_READ_CASES; i++ )
    {
        run_read_case( i );
    }

    read_sweep();
    truncated_sweep();

    print_init();

    builder_case( "ordinary", 8U );
    builder_case( "one-byte", 1U );
    builder_case( "zero-length", 0U );
    builder_case( "a-whole-section", 32U );

    for( i = 0; i < N_CONTEXT_CASES; i++ )
    {
        const ContextCase_t *c = &CONTEXT_CASES[ i ];

        printf( "ctx %u %s props=", ( unsigned ) i, c->name );
        put_hex( c->section, c->length );
        ( void ) run_context( c->section, c->length, true );
        printf( "\n" );
    }

    context_sweep( "one-byte", ONE_BYTE, sizeof ONE_BYTE );
    context_sweep( "two-byte", TWO_BYTE, sizeof TWO_BYTE );
    context_sweep( "four-byte", FOUR_BYTE, sizeof FOUR_BYTE );
    context_sweep( "string", STRING, sizeof STRING );
    context_sweep( "varint", VARINT, sizeof VARINT );
    context_sweep( "user-property", USER_PROP, sizeof USER_PROP );

    params_case( "ordinary",              false, 1U, MQTTQoS0, 2U, 0U, 8U, 1024U );
    params_case( "retain-unavailable",     true, 0U, MQTTQoS0, 2U, 0U, 8U, 1024U );
    params_case( "retain-available",       true, 1U, MQTTQoS0, 2U, 0U, 8U, 1024U );
    params_case( "qos1-when-max-is-zero", false, 1U, MQTTQoS1, 0U, 0U, 8U, 1024U );
    params_case( "qos2-when-max-is-one",  false, 1U, MQTTQoS2, 1U, 0U, 8U, 1024U );
    params_case( "qos0-when-max-is-zero", false, 1U, MQTTQoS0, 0U, 0U, 8U, 1024U );
    /* [MQTT-3.3.2-8] on the way OUT: a zero-length topic name needs an alias,
     * and the INCOMING path does not check the same rule. */
    params_case( "empty-topic-no-alias",  false, 1U, MQTTQoS0, 2U, 0U, 0U, 1024U );
    params_case( "empty-topic-with-alias", false, 1U, MQTTQoS0, 2U, 1U, 0U, 1024U );
    params_case( "max-packet-zero",       false, 1U, MQTTQoS0, 2U, 0U, 8U, 0U );
    params_case( "max-packet-one",        false, 1U, MQTTQoS0, 2U, 0U, 8U, 1U );

    params_sweep();

    printf( "end\n" );
    return 0;
}
