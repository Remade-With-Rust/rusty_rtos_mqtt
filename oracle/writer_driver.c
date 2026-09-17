/* The C arm of K7's coreMQTT FIXED-HEADER WRITER differential.
 *
 * `core_mqtt_serializer_private.c` is compiled VERBATIM out of the pinned
 * checkout; nothing here copies or edits it.
 *
 * # Why this slice
 *
 * The property differential next door is the READING side of the primitive
 * layer. These five functions are the WRITING side: the fixed header of every
 * outgoing packet type an MQTT client sends. They are pure byte-writers with no
 * validation and no state, which makes them exactly the sort of code where a
 * transcription is confidently and quietly wrong.
 *
 * The interesting one is `serializeConnectFixedHeader`, which packs seven
 * boolean-ish inputs into ONE flags byte. Getting a bit position wrong there
 * produces a packet a broker will reject in a way that looks like a network
 * problem, so the sweep below is exhaustive over every combination rather than
 * a handful of samples: two clean-session values x three will QoS values x two
 * will-retain x two username x two password x will-present, at several keep
 * alive values.
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "core_mqtt_serializer.h"

extern uint8_t * serializeAckFixed( uint8_t * pIndex, uint8_t packetType,
                                    uint16_t packetId, uint32_t remainingLength,
                                    MQTTSuccessFailReasonCode_t reasonCode );
extern uint8_t * serializeConnectFixedHeader( uint8_t * pIndex,
                                              const MQTTConnectInfo_t * pConnectInfo,
                                              const MQTTPublishInfo_t * pWillInfo,
                                              uint32_t remainingLength );
extern uint8_t * serializeSubscribeHeader( uint32_t remainingLength, uint8_t * pIndex,
                                           uint16_t packetId );
extern uint8_t * serializeUnsubscribeHeader( uint32_t remainingLength, uint8_t * pIndex,
                                             uint16_t packetId );
extern uint8_t * serializeDisconnectFixed( uint8_t * pIndex,
                                           const MQTTSuccessFailReasonCode_t * pReasonCode,
                                           uint32_t remainingLength );

#define OUT_SIZE    32

static void put_hex( const uint8_t *p, size_t n )
{
    size_t i;
    for( i = 0; i < n; i++ )
    {
        printf( "%02x", p[ i ] );
    }
}

static uint64_t fnv = 1469598103934665603ULL;

static void fnv_str( const char *s )
{
    while( *s != '\0' )
    {
        fnv ^= ( uint64_t ) ( unsigned char ) *s;
        fnv *= 1099511628211ULL;
        s++;
    }
}

/* ---- the ack writer ------------------------------------------------------ */

/* packetType, packetId, remainingLength, reasonCode */
static const uint32_t ACK_CASES[][ 4 ] = {
    { 0x40U, 1U,     2U, 0x00U },   /* a PUBACK with no reason code beyond the id */
    { 0x40U, 0xFFFFU, 3U, 0x00U },  /* the largest packet id */
    { 0x50U, 0x1234U, 3U, 0x10U },  /* PUBREC, "no matching subscribers" */
    { 0x62U, 0x1234U, 3U, 0x92U },  /* PUBREL, "packet identifier not found" */
    { 0x70U, 0x0001U, 3U, 0x00U },  /* PUBCOMP */
    { 0x40U, 0x8000U, 130U, 0x87U },/* a two-byte remaining length */
    { 0x40U, 0x0100U, 16384U, 0x00U }, /* a three-byte remaining length */
};

#define N_ACK ( sizeof( ACK_CASES ) / sizeof( ACK_CASES[ 0 ] ) )

static void run_acks( void )
{
    size_t i;

    for( i = 0; i < N_ACK; i++ )
    {
        uint8_t out[ OUT_SIZE ];
        uint8_t *end;

        memset( out, 0xAA, sizeof out );
        end = serializeAckFixed( out, ( uint8_t ) ACK_CASES[ i ][ 0 ],
                                 ( uint16_t ) ACK_CASES[ i ][ 1 ],
                                 ACK_CASES[ i ][ 2 ],
                                 ( MQTTSuccessFailReasonCode_t ) ACK_CASES[ i ][ 3 ] );

        printf( "ack %u %02x %04x %u %02x -> %u ", ( unsigned ) i,
                ACK_CASES[ i ][ 0 ], ACK_CASES[ i ][ 1 ], ACK_CASES[ i ][ 2 ],
                ACK_CASES[ i ][ 3 ], ( unsigned ) ( end - out ) );
        put_hex( out, ( size_t ) ( end - out ) );
        printf( "\n" );
    }
}

/* ---- the subscribe and unsubscribe writers ------------------------------- */

static const uint32_t SUB_CASES[][ 2 ] = {
    { 2U, 1U }, { 127U, 0xFFFFU }, { 128U, 0x1234U }, { 16384U, 1U },
    { 2097152U, 42U }, { 268435455U, 0x8000U }
};

#define N_SUB ( sizeof( SUB_CASES ) / sizeof( SUB_CASES[ 0 ] ) )

static void run_subs( void )
{
    size_t i;

    for( i = 0; i < N_SUB; i++ )
    {
        uint8_t out[ OUT_SIZE ];
        uint8_t *end;

        memset( out, 0xAA, sizeof out );
        end = serializeSubscribeHeader( SUB_CASES[ i ][ 0 ], out,
                                        ( uint16_t ) SUB_CASES[ i ][ 1 ] );
        printf( "sub %u %u %04x -> %u ", ( unsigned ) i, SUB_CASES[ i ][ 0 ],
                SUB_CASES[ i ][ 1 ], ( unsigned ) ( end - out ) );
        put_hex( out, ( size_t ) ( end - out ) );
        printf( "\n" );

        memset( out, 0xAA, sizeof out );
        end = serializeUnsubscribeHeader( SUB_CASES[ i ][ 0 ], out,
                                          ( uint16_t ) SUB_CASES[ i ][ 1 ] );
        printf( "unsub %u %u %04x -> %u ", ( unsigned ) i, SUB_CASES[ i ][ 0 ],
                SUB_CASES[ i ][ 1 ], ( unsigned ) ( end - out ) );
        put_hex( out, ( size_t ) ( end - out ) );
        printf( "\n" );
    }
}

/* ---- the disconnect writer ----------------------------------------------- */

static void run_disconnects( void )
{
    static const uint32_t LENGTHS[] = { 0U, 1U, 2U, 130U };
    static const uint8_t REASONS[] = { 0x00U, 0x04U, 0x81U, 0x8EU };
    size_t i, j;

    for( i = 0; i < 4U; i++ )
    {
        for( j = 0; j < 5U; j++ )
        {
            uint8_t out[ OUT_SIZE ];
            uint8_t *end;
            MQTTSuccessFailReasonCode_t reason;
            /* j == 4 means "no reason code at all", which is the C's NULL. */
            const MQTTSuccessFailReasonCode_t *pReason = NULL;

            if( j < 4U )
            {
                reason = ( MQTTSuccessFailReasonCode_t ) REASONS[ j ];
                pReason = &reason;
            }

            memset( out, 0xAA, sizeof out );
            end = serializeDisconnectFixed( out, pReason, LENGTHS[ i ] );

            printf( "disc %u %u %u %u -> %u ", ( unsigned ) i, ( unsigned ) j,
                    LENGTHS[ i ], ( j < 4U ) ? REASONS[ j ] : 0x100U,
                    ( unsigned ) ( end - out ) );
            put_hex( out, ( size_t ) ( end - out ) );
            printf( "\n" );
        }
    }
}

/* ---- the CONNECT flags byte, exhaustively -------------------------------- */

static void run_connect( void )
{
    static const uint16_t KEEP_ALIVES[] = { 0U, 1U, 60U, 0xFFFFU };
    static const uint32_t LENGTHS[] = { 10U, 127U, 128U, 16384U };
    unsigned clean, will, willQos, willRetain, user, pass;
    size_t ka, len;
    unsigned long total = 0;

    for( clean = 0; clean < 2U; clean++ )
    {
        for( will = 0; will < 2U; will++ )
        {
            for( willQos = 0; willQos < 3U; willQos++ )
            {
                for( willRetain = 0; willRetain < 2U; willRetain++ )
                {
                    for( user = 0; user < 2U; user++ )
                    {
                        for( pass = 0; pass < 2U; pass++ )
                        {
                            for( ka = 0; ka < 4U; ka++ )
                            {
                                for( len = 0; len < 4U; len++ )
                                {
                                    uint8_t out[ OUT_SIZE ];
                                    uint8_t *end;
                                    MQTTConnectInfo_t connect;
                                    MQTTPublishInfo_t willInfo;
                                    char line[ 96 ];
                                    size_t n;

                                    memset( &connect, 0, sizeof connect );
                                    memset( &willInfo, 0, sizeof willInfo );

                                    connect.cleanSession = ( clean != 0U );
                                    connect.keepAliveSeconds = KEEP_ALIVES[ ka ];
                                    connect.pUserName = ( user != 0U ) ? "u" : NULL;
                                    connect.pPassword = ( pass != 0U ) ? "p" : NULL;

                                    willInfo.qos = ( MQTTQoS_t ) willQos;
                                    willInfo.retain = ( willRetain != 0U );

                                    memset( out, 0xAA, sizeof out );
                                    end = serializeConnectFixedHeader(
                                        out, &connect,
                                        ( will != 0U ) ? &willInfo : NULL,
                                        LENGTHS[ len ] );

                                    n = ( size_t ) ( end - out );

                                    /* The flags byte is what this sweep is for;
                                     * print it alongside the digest so a
                                     * mismatch has somewhere to start. */
                                    ( void ) snprintf( line, sizeof line,
                                                       "%u %02x", ( unsigned ) n,
                                                       out[ n - 3U ] );
                                    fnv_str( line );
                                    total++;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    printf( "connect total %lu\n", total );
    printf( "connect digest %016llx\n", ( unsigned long long ) fnv );

    /* And a handful printed in full, so a digest mismatch is localisable. */
    {
        static const unsigned NAMED[][ 6 ] = {
            /* clean, will, willQos, willRetain, user, pass */
            { 0, 0, 0, 0, 0, 0 },
            { 1, 0, 0, 0, 0, 0 },
            { 1, 1, 0, 0, 0, 0 },
            { 1, 1, 1, 0, 0, 0 },
            { 1, 1, 2, 1, 0, 0 },
            { 1, 0, 0, 0, 1, 1 },
            { 1, 1, 2, 1, 1, 1 },
            { 0, 1, 1, 1, 1, 0 },
        };
        size_t i;

        for( i = 0; i < 8U; i++ )
        {
            uint8_t out[ OUT_SIZE ];
            uint8_t *end;
            MQTTConnectInfo_t connect;
            MQTTPublishInfo_t willInfo;

            memset( &connect, 0, sizeof connect );
            memset( &willInfo, 0, sizeof willInfo );

            connect.cleanSession = ( NAMED[ i ][ 0 ] != 0U );
            connect.keepAliveSeconds = 60U;
            connect.pUserName = ( NAMED[ i ][ 4 ] != 0U ) ? "u" : NULL;
            connect.pPassword = ( NAMED[ i ][ 5 ] != 0U ) ? "p" : NULL;
            willInfo.qos = ( MQTTQoS_t ) NAMED[ i ][ 2 ];
            willInfo.retain = ( NAMED[ i ][ 3 ] != 0U );

            memset( out, 0xAA, sizeof out );
            end = serializeConnectFixedHeader( out, &connect,
                                               ( NAMED[ i ][ 1 ] != 0U ) ? &willInfo : NULL,
                                               10U );

            printf( "conn %u %u%u%u%u%u%u -> %u ", ( unsigned ) i,
                    NAMED[ i ][ 0 ], NAMED[ i ][ 1 ], NAMED[ i ][ 2 ],
                    NAMED[ i ][ 3 ], NAMED[ i ][ 4 ], NAMED[ i ][ 5 ],
                    ( unsigned ) ( end - out ) );
            put_hex( out, ( size_t ) ( end - out ) );
            printf( "\n" );
        }
    }
}

int main( void )
{
    printf( "geometry acks=%u subs=%u\n", ( unsigned ) N_ACK, ( unsigned ) N_SUB );

    run_acks();
    run_subs();
    run_disconnects();
    run_connect();

    printf( "end\n" );
    return 0;
}
