/* The C arm of K7's coreMQTT REMAINING-OUTGOING-PACKETS differential:
 * SUBSCRIBE, UNSUBSCRIBE, the publish acknowledgements and PINGREQ.
 *
 * `core_mqtt_serializer.c` is compiled VERBATIM out of the pinned checkout;
 * nothing here copies or edits it.
 *
 * # Why these four together
 *
 * They are what is left of the outgoing wire codec once CONNECT, PUBLISH and
 * DISCONNECT are done, and they share `validateSubscriptionSerializeParams`
 * and a family of small fixed shapes. Finishing them finishes everything a
 * client can put on the wire.
 *
 * # The subscription options byte
 *
 * SUBSCRIBE carries one options byte per topic filter, packing FIVE decisions
 * into six bits: QoS (two bits), no-local, retain-as-published, and retain
 * handling (two more bits, three legal values). That is the same shape as the
 * CONNECT flags byte the writers' slice swept, and it gets the same treatment:
 * an exhaustive sweep of every combination, digested.
 *
 * # And the ack reason codes, which the library validates TWICE
 *
 * `validateReasonCodeForAck` checks an outgoing acknowledgement's reason code
 * PER PACKET TYPE -- nine values for a PUBACK, two for a PUBREL. The reading
 * side's `logAckResponse` checks the same thing with ONE shared table of ten.
 * So the two halves of coreMQTT disagree about whether a PUBACK may say `0x92`,
 * and this driver sweeps all four types over all 256 values so the trace can
 * say which.
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core_mqtt_serializer.h"

#define MAX_FILTERS    4
#define MAX_FIELD      16
#define OUT_SIZE       160

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
        case MQTTSuccess:      return "Success";
        case MQTTBadParameter: return "BadParameter";
        case MQTTNoMemory:     return "NoMemory";
        default:               return "OTHER";
    }
}

/* ---- SUBSCRIBE and UNSUBSCRIBE ------------------------------------------- */

typedef struct
{
    const char *name;
    bool        subscribe;        /* false for UNSUBSCRIBE */
    uint16_t    packetId;
    size_t      filterCount;
    size_t      filterLength[ MAX_FILTERS ];
    uint8_t     filters[ MAX_FILTERS ][ MAX_FIELD ];
    uint8_t     qos[ MAX_FILTERS ];
    bool        noLocal[ MAX_FILTERS ];
    bool        retainAsPublished[ MAX_FILTERS ];
    uint8_t     retainHandling[ MAX_FILTERS ];
    size_t      propertyLength;
    uint8_t     properties[ MAX_FIELD ];
    uint32_t    remainingLength;
    size_t      bufferSize;
} ListCase_t;

static const ListCase_t LIST_CASES[] = {
    /* remaining length = 2 (packet id) + 1 (property length) + per filter
     * (2 + length + 1 for SUBSCRIBE, 2 + length for UNSUBSCRIBE). */
    { "sub-one-filter", true, 1, 1, { 3 }, { { 'a', '/', 'b' } },
      { 0 }, { false }, { false }, { 0 }, 0, { 0 }, 9, OUT_SIZE },
    { "sub-qos1", true, 1, 1, { 3 }, { { 'a', '/', 'b' } },
      { 1 }, { false }, { false }, { 0 }, 0, { 0 }, 9, OUT_SIZE },
    { "sub-qos2", true, 1, 1, { 3 }, { { 'a', '/', 'b' } },
      { 2 }, { false }, { false }, { 0 }, 0, { 0 }, 9, OUT_SIZE },
    { "sub-no-local", true, 1, 1, { 3 }, { { 'a', '/', 'b' } },
      { 0 }, { true }, { false }, { 0 }, 0, { 0 }, 9, OUT_SIZE },
    { "sub-retain-as-published", true, 1, 1, { 3 }, { { 'a', '/', 'b' } },
      { 0 }, { false }, { true }, { 0 }, 0, { 0 }, 9, OUT_SIZE },
    { "sub-retain-handling-1", true, 1, 1, { 3 }, { { 'a', '/', 'b' } },
      { 0 }, { false }, { false }, { 1 }, 0, { 0 }, 9, OUT_SIZE },
    { "sub-retain-handling-2", true, 1, 1, { 3 }, { { 'a', '/', 'b' } },
      { 0 }, { false }, { false }, { 2 }, 0, { 0 }, 9, OUT_SIZE },
    { "sub-everything-set", true, 65535, 1, { 3 }, { { 'a', '/', 'b' } },
      { 2 }, { true }, { true }, { 1 }, 0, { 0 }, 9, OUT_SIZE },
    { "sub-three-filters", true, 7, 3, { 1, 3, 5 },
      { { 'x' }, { 'a', '/', 'b' }, { 'l', 'o', 'n', 'g', 'r' } },
      { 0, 1, 2 }, { false, true, false }, { false, false, true }, { 0, 1, 2 },
      0, { 0 }, 21, OUT_SIZE },
    { "sub-with-properties", true, 1, 1, { 3 }, { { 'a', '/', 'b' } },
      { 0 }, { false }, { false }, { 0 }, 2, { 0x0B, 0x07 }, 11, OUT_SIZE },

    { "unsub-one-filter", false, 1, 1, { 3 }, { { 'a', '/', 'b' } },
      { 0 }, { false }, { false }, { 0 }, 0, { 0 }, 8, OUT_SIZE },
    { "unsub-three-filters", false, 7, 3, { 1, 3, 5 },
      { { 'x' }, { 'a', '/', 'b' }, { 'l', 'o', 'n', 'g', 'r' } },
      { 0, 0, 0 }, { false }, { false }, { 0 }, 0, { 0 }, 18, OUT_SIZE },
    { "unsub-with-properties", false, 1, 1, { 3 }, { { 'a', '/', 'b' } },
      { 0 }, { false }, { false }, { 0 }, 2, { 0x26, 0x00 }, 10, OUT_SIZE },

    /* --- what the shared validator refuses --- */
    { "zero-filters", true, 1, 0, { 0 }, { { 0 } },
      { 0 }, { false }, { false }, { 0 }, 0, { 0 }, 3, OUT_SIZE },
    { "packet-id-zero", true, 0, 1, { 3 }, { { 'a', '/', 'b' } },
      { 0 }, { false }, { false }, { 0 }, 0, { 0 }, 9, OUT_SIZE },
    { "empty-filter", true, 1, 1, { 0 }, { { 0 } },
      { 0 }, { false }, { false }, { 0 }, 0, { 0 }, 6, OUT_SIZE },
    /* The C takes the remaining length as a parameter and never recomputes it,
     * so a caller who gets it wrong gets a packet whose own length byte
     * disagrees with its contents. Deliberate, and named for it. */
    { "remaining-length-one-short", true, 1, 1, { 3 }, { { 'a', '/', 'b' } },
      { 0 }, { false }, { false }, { 0 }, 0, { 0 }, 8, OUT_SIZE },
    { "buffer-exactly-big-enough", true, 1, 1, { 3 }, { { 'a', '/', 'b' } },
      { 0 }, { false }, { false }, { 0 }, 0, { 0 }, 9, 11 },
    { "buffer-one-byte-short", true, 1, 1, { 3 }, { { 'a', '/', 'b' } },
      { 0 }, { false }, { false }, { 0 }, 0, { 0 }, 9, 10 },
};

#define N_LIST    ( sizeof( LIST_CASES ) / sizeof( LIST_CASES[ 0 ] ) )

static void run_list_case( size_t i )
{
    const ListCase_t *c = &LIST_CASES[ i ];
    uint8_t filters[ MAX_FILTERS ][ MAX_FIELD ];
    uint8_t properties[ MAX_FIELD ];
    uint8_t out[ OUT_SIZE ];
    MQTTSubscribeInfo_t list[ MAX_FILTERS ];
    MQTTPropBuilder_t props;
    const MQTTPropBuilder_t *pProps = NULL;
    MQTTFixedBuffer_t fixed;
    MQTTStatus_t status;
    size_t k;

    memcpy( filters, c->filters, sizeof filters );
    memcpy( properties, c->properties, sizeof properties );

    memset( list, 0, sizeof list );

    for( k = 0; k < c->filterCount; k++ )
    {
        list[ k ].qos = ( MQTTQoS_t ) c->qos[ k ];
        list[ k ].pTopicFilter = ( c->filterLength[ k ] != 0U )
                                 ? ( const char * ) filters[ k ] : NULL;
        list[ k ].topicFilterLength = c->filterLength[ k ];
        list[ k ].noLocalOption = c->noLocal[ k ];
        list[ k ].retainAsPublishedOption = c->retainAsPublished[ k ];
        list[ k ].retainHandlingOption = ( MQTTRetainHandling_t ) c->retainHandling[ k ];
    }

    if( c->propertyLength != 0U )
    {
        memset( &props, 0, sizeof props );
        props.pBuffer = properties;
        props.bufferLength = sizeof properties;
        props.currentIndex = c->propertyLength;
        pProps = &props;
    }

    printf( "%s %u %s id=%u n=%u", c->subscribe ? "sub" : "unsub",
            ( unsigned ) i, c->name, ( unsigned ) c->packetId,
            ( unsigned ) c->filterCount );

    for( k = 0; k < c->filterCount; k++ )
    {
        printf( " f%u=", ( unsigned ) k );
        put_hex( c->filters[ k ], c->filterLength[ k ] );
        printf( ",%u,%u,%u,%u", ( unsigned ) c->qos[ k ],
                c->noLocal[ k ] ? 1U : 0U, c->retainAsPublished[ k ] ? 1U : 0U,
                ( unsigned ) c->retainHandling[ k ] );
    }

    printf( " props=" );
    put_hex( c->properties, c->propertyLength );
    printf( " rl=%u buf=%u", ( unsigned ) c->remainingLength,
            ( unsigned ) c->bufferSize );

    memset( out, 0xCC, sizeof out );
    memset( &fixed, 0, sizeof fixed );
    fixed.pBuffer = out;
    fixed.size = c->bufferSize;

    if( c->subscribe )
    {
        status = MQTT_SerializeSubscribe( list, c->filterCount, pProps,
                                          c->packetId, c->remainingLength,
                                          &fixed );
    }
    else
    {
        status = MQTT_SerializeUnsubscribe( list, c->filterCount, pProps,
                                            c->packetId, c->remainingLength,
                                            &fixed );
    }

    printf( " -> %s", status_name( status ) );

    if( status == MQTTSuccess )
    {
        printf( " bytes=" );
        put_hex( out, ( size_t ) ( 1U + c->remainingLength
                                   + ( ( c->remainingLength < 128U ) ? 1U : 2U ) ) );
    }

    printf( "\n" );
}

/* ---- the publish acknowledgements and PINGREQ ----------------------------- */

typedef struct
{
    const char *name;
    uint8_t     packetType;
    uint16_t    packetId;
    bool        hasReasonCode;
    uint8_t     reasonCode;
    size_t      propertyLength;
    uint8_t     properties[ MAX_FIELD ];
    size_t      bufferSize;
} AckCase_t;

static const AckCase_t ACK_CASES[] = {
    /* The three shapes `serializeAckBody` names: no reason code, reason code
     * only, reason code with properties. */
    { "puback-bare",          0x40U, 42U, false, 0x00, 0U, { 0 }, OUT_SIZE },
    { "puback-reason-only",   0x40U, 42U, true,  0x00, 0U, { 0 }, OUT_SIZE },
    { "puback-with-properties", 0x40U, 42U, true, 0x10, 4U,
      { 0x1F, 0x00, 0x01, 'x' }, OUT_SIZE },
    { "pubrec-reason",        0x50U, 1U,  true,  0x87, 0U, { 0 }, OUT_SIZE },
    { "pubrel-success",       0x62U, 1U,  true,  0x00, 0U, { 0 }, OUT_SIZE },
    { "pubrel-not-found",     0x62U, 1U,  true,  0x92, 0U, { 0 }, OUT_SIZE },
    { "pubcomp-not-found",    0x70U, 1U,  true,  0x92, 0U, { 0 }, OUT_SIZE },
    /* 0x92 is legal in a PUBREL and NOT in a PUBACK -- the writing side knows
     * this and the reading side does not. */
    { "puback-pubrel-code",   0x40U, 1U,  true,  0x92, 0U, { 0 }, OUT_SIZE },
    /* 0x10 "no matching subscribers" is a PUBACK/PUBREC code only. */
    { "pubrel-puback-code",   0x62U, 1U,  true,  0x10, 0U, { 0 }, OUT_SIZE },
    { "unknown-reason-code",  0x40U, 1U,  true,  0x7F, 0U, { 0 }, OUT_SIZE },
    { "not-an-ack-type",      0x90U, 1U,  true,  0x00, 0U, { 0 }, OUT_SIZE },
    { "packet-id-zero",       0x40U, 0U,  true,  0x00, 0U, { 0 }, OUT_SIZE },
    /* Properties with no reason code: refused, as in the DISCONNECT. */
    { "properties-no-reason", 0x40U, 1U,  false, 0x00, 4U,
      { 0x1F, 0x00, 0x01, 'x' }, OUT_SIZE },
    { "buffer-four-bytes",    0x40U, 42U, false, 0x00, 0U, { 0 }, 4U },
    { "buffer-three-bytes",   0x40U, 42U, false, 0x00, 0U, { 0 }, 3U },
    /* A reason code needs six bytes, and the buffer has four. */
    { "reason-in-a-four-byte-buffer", 0x40U, 42U, true, 0x00, 0U, { 0 }, 4U },
};

#define N_ACK    ( sizeof( ACK_CASES ) / sizeof( ACK_CASES[ 0 ] ) )

static void run_ack_case( size_t i )
{
    const AckCase_t *c = &ACK_CASES[ i ];
    uint8_t properties[ MAX_FIELD ];
    uint8_t out[ OUT_SIZE ];
    MQTTPropBuilder_t props;
    const MQTTPropBuilder_t *pProps = NULL;
    MQTTSuccessFailReasonCode_t reason;
    const MQTTSuccessFailReasonCode_t *pReason = NULL;
    MQTTFixedBuffer_t fixed;
    MQTTStatus_t status;

    memcpy( properties, c->properties, sizeof properties );

    if( c->propertyLength != 0U )
    {
        memset( &props, 0, sizeof props );
        props.pBuffer = properties;
        props.bufferLength = sizeof properties;
        props.currentIndex = c->propertyLength;
        pProps = &props;
    }

    if( c->hasReasonCode )
    {
        reason = ( MQTTSuccessFailReasonCode_t ) c->reasonCode;
        pReason = &reason;
    }

    printf( "ack %u %s type=%02x id=%u rc=", ( unsigned ) i, c->name,
            ( unsigned ) c->packetType, ( unsigned ) c->packetId );

    if( c->hasReasonCode )
    {
        printf( "%02x", ( unsigned ) c->reasonCode );
    }
    else
    {
        printf( "-" );
    }

    printf( " props=" );
    put_hex( c->properties, c->propertyLength );
    printf( " buf=%u", ( unsigned ) c->bufferSize );

    memset( out, 0xCC, sizeof out );
    memset( &fixed, 0, sizeof fixed );
    fixed.pBuffer = out;
    fixed.size = c->bufferSize;

    status = MQTT_SerializeAck( &fixed, c->packetType, c->packetId, pProps,
                                pReason );

    printf( " -> %s", status_name( status ) );

    if( status == MQTTSuccess )
    {
        /* The whole packet: type, remaining length, and the body. The C never
         * reports a size, so the trace prints the fixed header plus whatever
         * the remaining-length byte says. */
        size_t total = 2U + ( size_t ) out[ 1 ];
        printf( " bytes=" );
        put_hex( out, total );
    }

    printf( "\n" );
}

static void run_pingreq_case( size_t bufferSize )
{
    uint8_t out[ OUT_SIZE ];
    MQTTFixedBuffer_t fixed;
    MQTTStatus_t status;

    memset( out, 0xCC, sizeof out );
    memset( &fixed, 0, sizeof fixed );
    fixed.pBuffer = out;
    fixed.size = bufferSize;

    printf( "pingreq buf=%u", ( unsigned ) bufferSize );

    status = MQTT_SerializePingreq( &fixed );

    printf( " -> %s", status_name( status ) );

    if( status == MQTTSuccess )
    {
        printf( " bytes=" );
        put_hex( out, 2U );
    }

    printf( "\n" );
}

/* ---- the sweeps ----------------------------------------------------------- */

/* Every combination of the subscription options byte: QoS x no-local x
 * retain-as-published x retain handling. Six bits, five decisions, and one
 * wrong bit subscribes at the wrong QoS or asks for retained messages that
 * never come -- the same hazard the CONNECT flags byte has. */
static void sweep_subscription_options( void )
{
    unsigned combination;
    uint64_t fnv = 1469598103934665603ULL;
    unsigned accepted = 0U;
    int first = 1;

    printf( "options-sweep bytes=" );

    for( combination = 0U; combination < 36U; combination++ )
    {
        static const uint8_t FILTER[] = { 'f' };
        uint8_t out[ OUT_SIZE ];
        MQTTSubscribeInfo_t one;
        MQTTFixedBuffer_t fixed;
        MQTTStatus_t status;

        unsigned qos = combination % 3U;
        unsigned handling = ( combination / 3U ) % 3U;
        bool noLocal = ( ( combination / 9U ) & 1U ) != 0U;
        bool retainAsPublished = ( ( combination / 18U ) & 1U ) != 0U;

        memset( &one, 0, sizeof one );
        one.qos = ( MQTTQoS_t ) qos;
        one.pTopicFilter = ( const char * ) FILTER;
        one.topicFilterLength = sizeof FILTER;
        one.noLocalOption = noLocal;
        one.retainAsPublishedOption = retainAsPublished;
        one.retainHandlingOption = ( MQTTRetainHandling_t ) handling;

        memset( out, 0xCC, sizeof out );
        memset( &fixed, 0, sizeof fixed );
        fixed.pBuffer = out;
        fixed.size = sizeof out;

        /* 2 packet id + 1 property length + 2 + 1 filter + 1 options. */
        status = MQTT_SerializeSubscribe( &one, 1U, NULL, 1U, 7U, &fixed );

        if( status == MQTTSuccess )
        {
            accepted++;
            /* The options byte is the last one of the packet. */
            printf( "%s%02x", first ? "" : ",", ( unsigned ) out[ 8 ] );
            first = 0;
            fnv ^= ( uint64_t ) out[ 8 ];
            fnv *= 1099511628211ULL;
        }
    }

    printf( " n=%u digest=%016llx\n", accepted, ( unsigned long long ) fnv );
}

/* Every reason-code byte, for each of the four publish acknowledgements.
 *
 * The four tables are DIFFERENT -- nine values for a PUBACK and a PUBREC, two
 * for a PUBREL and a PUBCOMP -- where the reading side checks all four against
 * one shared table of ten. The two halves of coreMQTT disagree, and the writing
 * side is the one that matches MQTT 5.0. */
static void sweep_ack_reason_codes( uint8_t packetType, const char *label )
{
    unsigned value;
    unsigned accepted = 0U;
    int first = 1;

    printf( "ack-reason-sweep %s accepted=", label );

    for( value = 0U; value < 256U; value++ )
    {
        uint8_t out[ OUT_SIZE ];
        MQTTFixedBuffer_t fixed;
        MQTTSuccessFailReasonCode_t reason = ( MQTTSuccessFailReasonCode_t ) value;
        MQTTStatus_t status;

        memset( out, 0xCC, sizeof out );
        memset( &fixed, 0, sizeof fixed );
        fixed.pBuffer = out;
        fixed.size = sizeof out;

        status = MQTT_SerializeAck( &fixed, packetType, 1U, NULL, &reason );

        if( status == MQTTSuccess )
        {
            printf( "%s%02x", first ? "" : ",", value );
            first = 0;
            accepted++;
        }
    }

    printf( " n=%u refused=%u\n", accepted, 256U - accepted );
}

int main( void )
{
    size_t i;

    printf( "geometry list=%u ack=%u\n", ( unsigned ) N_LIST,
            ( unsigned ) N_ACK );

    for( i = 0; i < N_LIST; i++ )
    {
        run_list_case( i );
    }

    for( i = 0; i < N_ACK; i++ )
    {
        run_ack_case( i );
    }

    run_pingreq_case( 2U );
    run_pingreq_case( 1U );
    run_pingreq_case( 0U );

    sweep_subscription_options();

    sweep_ack_reason_codes( 0x40U, "puback" );
    sweep_ack_reason_codes( 0x50U, "pubrec" );
    sweep_ack_reason_codes( 0x62U, "pubrel" );
    sweep_ack_reason_codes( 0x70U, "pubcomp" );

    printf( "end\n" );
    return 0;
}
