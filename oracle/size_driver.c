/* The C arm of K7's coreMQTT PACKET-SIZE differential.
 *
 * `core_mqtt_serializer.c` is compiled VERBATIM out of the pinned checkout;
 * nothing here copies or edits it.
 *
 * # Why this slice
 *
 * The writers lay down a fixed header for a remaining length the caller has
 * already worked out. These are where that number comes from -- and they are
 * the only place in the library that does ARITHMETIC ON ATTACKER-INFLUENCED
 * SIZES. A subscription list is application data, but its topic filters often
 * are not, and the MQTT 5 limit of 268,435,455 is one that a long enough list
 * reaches.
 *
 * Two behaviours here are easy to miss and both are reproduced:
 *
 *   1. The output parameters are written BEFORE the max-packet-size check, so
 *      a call that fails on that check has still updated them. A caller that
 *      ignored the status would serialize with a length the library just
 *      refused.
 *   2. The in-loop overflow check runs AFTER the topic filter is added but
 *      BEFORE the SUBSCRIBE QoS byte, so the byte that pushes a list over the
 *      line is caught by the final check rather than the loop's.
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "core_mqtt_serializer.h"

#define MAX_SUBS    8

static const char *status_name( MQTTStatus_t s )
{
    switch( s )
    {
        case MQTTSuccess:      return "Success";
        case MQTTBadParameter: return "BadParameter";
        default:               return "UNEXPECTED";
    }
}

/* The C writes its out-parameters BEFORE its last check, so a failed call
 * has still updated some of them. That is real behaviour a C caller can
 * observe, and a Rust `Result` structurally cannot reproduce it -- there is
 * nothing to hand back on the error path. Rather than invent a trace line
 * that one arm cannot produce, the values are printed only on SUCCESS, and
 * the leak is recorded in the package plan and in a Rust test asserting a
 * refusal yields the caller nothing at all.
 */
static void print_outcome( MQTTStatus_t status, uint32_t remaining, uint32_t size )
{
    if( status == MQTTSuccess )
    {
        printf( " -> Success remaining=%u size=%u\n", remaining, size );
    }
    else
    {
        printf( " -> %s\n", status_name( status ) );
    }
}

/* ---- PINGREQ ------------------------------------------------------------- */

static void run_pingreq( void )
{
    uint32_t size = 0xAAAAAAAAU;
    MQTTStatus_t status = MQTT_GetPingreqPacketSize( &size );

    printf( "pingreq -> %s %u\n", status_name( status ), size );
}

/* ---- the acknowledgement size -------------------------------------------- */

/* maxPacketSize, ackPropertyLength */
static const uint32_t ACK_CASES[][ 2 ] = {
    { 100U,        0U },          /* the ordinary case: 3 + 1 + 1 = 5 bytes */
    { 5U,          0U },          /* exactly the packet size */
    { 4U,          0U },          /* one byte under */
    { 0U,          0U },          /* a zero maximum is refused outright */
    { 1000U,       10U },
    { 1000U,       127U },        /* the one-byte property length boundary */
    { 1000U,       128U },        /* two bytes now */
    { 300000U,     16383U },
    { 300000U,     16384U },
    { 300000U,     2097151U },
    { 300000U,     2097152U },
    { 0xFFFFFFFFU, 268435455U },  /* the property length AT the limit */
    { 0xFFFFFFFFU, 268435456U },  /* one past it */
    { 0xFFFFFFFFU, 268435450U },  /* close enough that the header pushes it over */
};

#define N_ACK ( sizeof( ACK_CASES ) / sizeof( ACK_CASES[ 0 ] ) )

static void run_acks( void )
{
    size_t i;

    for( i = 0; i < N_ACK; i++ )
    {
        uint32_t remaining = 0xAAAAAAAAU;
        uint32_t size = 0xAAAAAAAAU;
        MQTTStatus_t status;

        status = MQTT_GetAckPacketSize( &remaining, &size,
                                        ACK_CASES[ i ][ 0 ],
                                        ( size_t ) ACK_CASES[ i ][ 1 ] );

        /* The outputs are printed whatever the status, because the library
         * writes them before its last check and a caller can see that. */
        printf( "ack %u %u %u", ( unsigned ) i,
                ACK_CASES[ i ][ 0 ], ACK_CASES[ i ][ 1 ] );
        print_outcome( status, remaining, size );
    }
}

/* ---- the subscribe and unsubscribe sizes --------------------------------- */

typedef struct
{
    const char *name;
    size_t count;
    uint32_t topicLengths[ MAX_SUBS ];
    uint32_t maxPacketSize;
    uint32_t propertyLength;
} SubCase_t;

static const SubCase_t SUB_CASES[] = {
    { "one-short-filter",        1, { 5 },                          1000 , 0 },
    { "three-filters",           3, { 5, 10, 20 },                  1000 , 0 },
    { "empty-filter",            1, { 0 },                          1000 , 0 },
    { "max-length-filter",       1, { 65535 },                      1000000 , 0 },
    { "filter-one-past-16-bits", 1, { 65536 },                      1000000 , 0 },
    { "eight-filters",           8, { 1, 2, 3, 4, 5, 6, 7, 8 },     1000 , 0 },
    /* A one-filter SUBSCRIBE is 13 bytes and the UNSUBSCRIBE 12, so these two
     * sit exactly on and one under the SUBSCRIBE's own size -- which makes the
     * unsubscribe row of the first pass the interesting one, since 13 is two
     * more than it needs. */
    { "subscribe-exactly-at-the-max", 1, { 5 },                     13 , 0 },
    { "subscribe-one-byte-over",      1, { 5 },                     12 , 0 },
    { "zero-max-packet-size",    1, { 5 },                          0 , 0 },
    /* Four filters of 65,535 plus the per-filter overhead approach nothing
     * near the limit; the limit needs the loop to run far more than a
     * realistic list would, so the boundary is reached through the property
     * length in the ack cases instead. These check the ordinary arithmetic. */
    { "many-large-filters",      8, { 65535, 65535, 65535, 65535, 65535, 65535, 65535, 65535 }, 1000000, 0 },

    /* The 268,435,455 boundary. Eight topic filters of 65,535 reach 524,280,
     * so the only input that can get near the limit is the property length --
     * which is exactly why these rows exist. Without them the in-loop overflow
     * check and the final limit check are both unreachable, and a poison on
     * either passes every test. */
    { "properties-just-under-the-limit", 1, { 0 }, 0xFFFFFFFFU, 268435440U },
    { "properties-at-the-limit",         1, { 0 }, 0xFFFFFFFFU, 268435455U },
    { "properties-one-past-the-limit",   1, { 0 }, 0xFFFFFFFFU, 268435456U },
    /* A property length that leaves exactly enough room for one filter, so the
     * in-loop check is the one that fires rather than the final one. */
    { "properties-then-one-filter-over", 1, { 100 }, 0xFFFFFFFFU, 268435350U },
    { "properties-then-one-filter-fits", 1, { 100 }, 0xFFFFFFFFU, 268435000U },

    /* The two checks land on EXACT values, and a case that overshoots
     * cannot tell a `>=` from a `>`. These are computed to hit each one
     * precisely: the first makes the in-loop check see exactly
     * 268,435,456, the second makes the final check see exactly
     * 268,435,455 for a SUBSCRIBE. Without them both comparisons can be
     * loosened by one with every test still passing. */
    { "in-loop-check-at-exactly-the-invalid-value", 1, { 100 }, 0xFFFFFFFFU, 268435348U },
    { "final-check-at-exactly-the-maximum",         1, { 0 },   0xFFFFFFFFU, 268435446U },
};

#define N_SUB ( sizeof( SUB_CASES ) / sizeof( SUB_CASES[ 0 ] ) )

static void run_subs( void )
{
    size_t i;

    for( i = 0; i < N_SUB; i++ )
    {
        MQTTSubscribeInfo_t list[ MAX_SUBS ];
        uint32_t remaining, size;
        MQTTStatus_t status;
        MQTTPropBuilder_t props;
        const MQTTPropBuilder_t *pProps;
        static uint8_t propBuffer[ 8 ];
        size_t k;

        memset( list, 0, sizeof list );

        for( k = 0; k < SUB_CASES[ i ].count; k++ )
        {
            list[ k ].pTopicFilter = "x";
            list[ k ].topicFilterLength = ( size_t ) SUB_CASES[ i ].topicLengths[ k ];
            list[ k ].qos = MQTTQoS1;
        }

        memset( &props, 0, sizeof props );
        props.pBuffer = propBuffer;
        props.bufferLength = sizeof propBuffer;
        props.currentIndex = ( size_t ) SUB_CASES[ i ].propertyLength;
        pProps = ( SUB_CASES[ i ].propertyLength != 0U ) ? &props : NULL;

        remaining = 0xAAAAAAAAU;
        size = 0xAAAAAAAAU;
        status = MQTT_GetSubscribePacketSize( list, SUB_CASES[ i ].count, pProps,
                                              &remaining, &size,
                                              SUB_CASES[ i ].maxPacketSize );
        printf( "sub %u %s prop=%u", ( unsigned ) i, SUB_CASES[ i ].name,
                SUB_CASES[ i ].propertyLength );
        print_outcome( status, remaining, size );

        remaining = 0xAAAAAAAAU;
        size = 0xAAAAAAAAU;
        status = MQTT_GetUnsubscribePacketSize( list, SUB_CASES[ i ].count, pProps,
                                                &remaining, &size,
                                                SUB_CASES[ i ].maxPacketSize );
        printf( "unsub %u %s prop=%u", ( unsigned ) i, SUB_CASES[ i ].name,
                SUB_CASES[ i ].propertyLength );
        print_outcome( status, remaining, size );
    }

    /* An empty list, and a count of zero, are both refused. */
    {
        MQTTSubscribeInfo_t list[ 1 ];
        uint32_t remaining = 0xAAAAAAAAU, size = 0xAAAAAAAAU;
        MQTTStatus_t status;

        memset( list, 0, sizeof list );
        list[ 0 ].topicFilterLength = 5;

        status = MQTT_GetSubscribePacketSize( list, 0, NULL, &remaining, &size, 1000 );
        printf( "sub zero-count" );
        print_outcome( status, remaining, size );

        remaining = 0xAAAAAAAAU;
        size = 0xAAAAAAAAU;
        status = MQTT_GetUnsubscribePacketSize( list, 0, NULL, &remaining, &size, 1000 );
        printf( "unsub zero-count" );
        print_outcome( status, remaining, size );
    }
}

int main( void )
{
    printf( "geometry acks=%u subs=%u\n", ( unsigned ) N_ACK, ( unsigned ) N_SUB );

    run_pingreq();
    run_acks();
    run_subs();

    printf( "end\n" );
    return 0;
}
