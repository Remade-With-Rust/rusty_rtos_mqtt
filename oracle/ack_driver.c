/* The C arm of K7's coreMQTT ACKNOWLEDGEMENT-DESERIALIZER differential.
 *
 * `core_mqtt_serializer.c` is compiled VERBATIM out of the pinned checkout;
 * nothing here copies or edits it.
 *
 * # Why this slice
 *
 * The four slices before this one were all outgoing: a state machine, a header
 * codec, property READS out of a buffer the caller already trusted, and
 * writers. This is the first function in the package whose whole input is
 * chosen by the other end of the socket. `MQTT_DeserializeAck` is what a client
 * runs on every PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK, UNSUBACK and PINGRESP
 * that arrives, before anything above it has looked at a byte.
 *
 * # What the trace carries
 *
 * Every case prints its own inputs -- the type byte, the claimed remaining
 * length, the body in hex, the maximum packet size and the request-problem flag
 * -- so the Rust arm REPLAYS the trace rather than keeping a second copy of
 * this table. That convention came out of the sntp differential and every K7
 * slice since has used it.
 *
 * On success it prints the packet id, the reason codes and the property
 * section, because those are the three things the C hands back and a
 * status-only comparison would bless a transcription that returned the right
 * answer pointing at the wrong bytes.
 *
 * # Every case's claimed length EQUALS its body, and that is a rule
 *
 * `remainingLength` is what the packet claims and `pRemainingData` is what
 * arrived. The interesting attack makes them disagree -- and a case that does
 * would have the C indexing past its own buffer, so its answer would depend on
 * whatever is next in memory rather than on the library. Such a line is not an
 * oracle, it is a coin toss that happens to be reproducible on one machine.
 *
 * So the table asserts the two are equal, and the over-claim is tested on the
 * Rust side alone, by `a_claim_larger_than_the_buffer_is_refused`. It is the
 * same boundary the fixed-header slice found: a differential proves we match
 * the C's ANSWERS, and says nothing about inputs that violate the C's own
 * preconditions.
 *
 * # Two sweeps, because two tables decide what a broker is allowed to say
 *
 * `readSubackStatus` and `logAckResponse` are both switch statements over a
 * single byte, and both answer MQTTBadResponse for anything they do not
 * recognise. A byte is enumerable, so both are swept over all 256 values and
 * the ACCEPTED set is printed in full -- which is how the divergence from
 * MQTT 5.0 in each of them becomes visible in a checked-in file rather than
 * something a reader has to take on trust.
 *
 * The packet TYPE byte is swept the same way, over all 256 values, because the
 * routing switch is the first decision this function makes.
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
        case MQTTSuccess:      return "Success";
        case MQTTBadParameter: return "BadParameter";
        case MQTTBadResponse:  return "BadResponse";
        default:               return "OTHER";
    }
}

/* ---- the named cases ----------------------------------------------------- */

typedef struct
{
    const char *name;
    uint8_t     type;
    uint32_t    remainingLength;  /* what the packet CLAIMS */
    size_t      bodyLength;       /* what actually exists */
    uint8_t     body[ MAX_BODY ];
    uint32_t    maxPacketSize;
    bool        requestProblem;
} AckCase_t;

/* The bodies are written out byte by byte so the trace and this table cannot
 * drift apart: whatever is here is printed, and whatever is printed is what the
 * Rust arm replays. */
static const AckCase_t CASES[] = {
    /* --- the publish acknowledgements --- */
    { "puback-bare",              0x40U, 2U, 2U, { 0x00, 0x2A },                   1024U, true },
    { "puback-largest-id",        0x40U, 2U, 2U, { 0xFF, 0xFF },                   1024U, true },
    { "puback-id-zero",           0x40U, 2U, 2U, { 0x00, 0x00 },                   1024U, true },
    { "puback-one-byte-body",     0x40U, 1U, 1U, { 0x00 },                         1024U, true },
    { "puback-empty-body",        0x40U, 0U, 0U, { 0 },                            1024U, true },
    { "puback-success-reason",    0x40U, 3U, 3U, { 0x00, 0x2A, 0x00 },             1024U, true },
    { "puback-no-subscribers",    0x40U, 3U, 3U, { 0x00, 0x2A, 0x10 },             1024U, true },
    { "puback-not-authorized",    0x40U, 3U, 3U, { 0x00, 0x2A, 0x87 },             1024U, true },
    /* 0x92 is a PUBREL/PUBCOMP code; the shared log table takes it anyway. */
    { "puback-pubrel-only-code",  0x40U, 3U, 3U, { 0x00, 0x2A, 0x92 },             1024U, true },
    { "puback-unknown-reason",    0x40U, 3U, 3U, { 0x00, 0x2A, 0x05 },             1024U, true },
    { "pubrec-success",           0x50U, 3U, 3U, { 0x12, 0x34, 0x00 },             1024U, true },
    { "pubrel-not-found",         0x62U, 3U, 3U, { 0x12, 0x34, 0x92 },             1024U, true },
    /* 0x60 is a PUBREL with its reserved bit clear: an unknown type here. */
    { "pubrel-reserved-bit-clear", 0x60U, 3U, 3U, { 0x12, 0x34, 0x00 },            1024U, true },
    { "pubcomp-success",          0x70U, 3U, 3U, { 0x00, 0x01, 0x00 },             1024U, true },

    /* --- publish acknowledgements with a property section --- */
    { "puback-empty-properties",  0x40U, 4U, 4U, { 0x00, 0x2A, 0x00, 0x00 },       1024U, true },
    { "puback-reason-string",     0x40U, 9U, 9U,
      { 0x00, 0x2A, 0x00, 0x05, 0x1F, 0x00, 0x02, 'h', 'i' },                      1024U, true },
    { "puback-user-property",     0x40U, 12U, 12U,
      { 0x00, 0x2A, 0x00, 0x08, 0x26, 0x00, 0x01, 'k', 0x00, 0x02, 'v', 'v' },     1024U, true },
    /* The same, with one byte of slack inside the property section: the walk
     * reads it as a property id and refuses. */
    { "puback-property-slack",    0x40U, 13U, 13U,
      { 0x00, 0x2A, 0x00, 0x09, 0x26, 0x00, 0x01, 'k', 0x00, 0x02, 'v', 'v', 0x00 }, 1024U, true },
    /* A user property MAY repeat, where a reason string may not: 7 bytes each,
     * so a section of 14 and a body of 18. */
    { "puback-two-user-properties", 0x40U, 18U, 18U,
      { 0x00, 0x2A, 0x00, 0x0E,
        0x26, 0x00, 0x01, 'a', 0x00, 0x01, 'b',
        0x26, 0x00, 0x01, 'c', 0x00, 0x01, 'd' },                                  1024U, true },
    { "puback-unknown-property",  0x40U, 6U, 6U,
      { 0x00, 0x2A, 0x00, 0x02, 0x11, 0x00 },                                      1024U, true },
    /* The property length says more than the packet has left. */
    { "puback-property-overrun",  0x40U, 6U, 6U,
      { 0x00, 0x2A, 0x00, 0x09, 0x1F, 0x00 },                                      1024U, true },
    /* And less: the pub-ack decoder demands an EXACT fit. */
    { "puback-property-underrun", 0x40U, 8U, 8U,
      { 0x00, 0x2A, 0x00, 0x02, 0x1F, 0x00, 0x00, 0x00 },                          1024U, true },
    /* A COMPLETE property section followed by a byte that is not part of it.
     * This is the case that makes the exact-fit check load-bearing: without it
     * the section parses cleanly and the trailing byte is simply never looked
     * at, so a broker could carry data inside a packet the client believes it
     * has read whole. Added because a poison on that check did not fire. */
    { "puback-property-trailing-byte", 0x40U, 8U, 8U,
      { 0x00, 0x2A, 0x00, 0x03, 0x1F, 0x00, 0x00, 0xFF },                          1024U, true },
    /* A reason string twice is a protocol error. */
    { "puback-repeated-reason",   0x40U, 12U, 12U,
      { 0x00, 0x2A, 0x00, 0x08, 0x1F, 0x00, 0x01, 'a', 0x1F, 0x00, 0x01, 'b' },    1024U, true },
    /* Properties a client said it did not want. */
    { "puback-props-unrequested", 0x40U, 9U, 9U,
      { 0x00, 0x2A, 0x00, 0x05, 0x1F, 0x00, 0x02, 'h', 'i' },                      1024U, false },
    /* A reason code but no properties, with requestProblem false: still fine. */
    { "puback-reason-unrequested", 0x40U, 3U, 3U, { 0x00, 0x2A, 0x00 },            1024U, false },

    /* --- the subscribe acknowledgements --- */
    { "suback-one-granted",       0x90U, 4U, 4U, { 0x00, 0x2A, 0x00, 0x01 },       1024U, true },
    { "suback-three-granted",     0x90U, 6U, 6U,
      { 0x00, 0x2A, 0x00, 0x00, 0x01, 0x02 },                                      1024U, true },
    { "suback-refused",           0x90U, 4U, 4U, { 0x00, 0x2A, 0x00, 0x80 },       1024U, true },
    { "suback-unknown-status",    0x90U, 4U, 4U, { 0x00, 0x2A, 0x00, 0x7F },       1024U, true },
    { "suback-id-zero",           0x90U, 4U, 4U, { 0x00, 0x00, 0x00, 0x01 },       1024U, true },
    { "suback-three-byte-body",   0x90U, 3U, 3U, { 0x00, 0x2A, 0x00 },             1024U, true },
    /* A one-byte property section, which no property fits in. */
    { "suback-property-too-short", 0x90U, 4U, 4U, { 0x00, 0x2A, 0x01, 0x00 },      1024U, true },
    /* No reason codes AT ALL: an empty reason string fills the body exactly.
     * MQTT 5.0 s3.9 requires one reason code per topic filter, so a SUBACK
     * with none is malformed -- and the C accepts it, because it derives the
     * count by subtraction and never checks that the count is non-zero. */
    { "suback-zero-reason-codes", 0x90U, 6U, 6U,
      { 0x00, 0x2A, 0x03, 0x1F, 0x00, 0x00 },                                      1024U, true },
    { "unsuback-zero-reason-codes", 0xB0U, 6U, 6U,
      { 0x00, 0x2A, 0x03, 0x1F, 0x00, 0x00 },                                      1024U, true },
    { "suback-with-reason-string", 0x90U, 10U, 10U,
      { 0x00, 0x2A, 0x05, 0x1F, 0x00, 0x02, 'h', 'i', 0x00, 0x01 },                1024U, true },
    { "suback-property-overrun",  0x90U, 5U, 5U, { 0x00, 0x2A, 0x7F, 0x00, 0x01 }, 1024U, true },
    { "suback-unknown-property",  0x90U, 6U, 6U,
      { 0x00, 0x2A, 0x02, 0x11, 0x00, 0x01 },                                      1024U, true },
    { "unsuback-success",         0xB0U, 4U, 4U, { 0x00, 0x2A, 0x00, 0x00 },       1024U, true },
    /* 0x11 is "no subscription existed" -- legal per MQTT 5.0 s3.11.3. */
    { "unsuback-no-subscription", 0xB0U, 4U, 4U, { 0x00, 0x2A, 0x00, 0x11 },       1024U, true },
    /* 0x02 is a granted QoS, which an UNSUBACK cannot grant. */
    { "unsuback-granted-qos",     0xB0U, 4U, 4U, { 0x00, 0x2A, 0x00, 0x02 },       1024U, true },
    { "unsuback-two-codes",       0xB0U, 5U, 5U, { 0x00, 0x2A, 0x00, 0x00, 0x8F }, 1024U, true },

    /* --- PINGRESP --- */
    { "pingresp-empty",           0xD0U, 0U, 0U, { 0 },                            1024U, true },
    { "pingresp-with-a-body",     0xD0U, 1U, 1U, { 0x00 },                         1024U, true },

    /* --- routing and limits --- */
    { "connack-routed-here",      0x20U, 4U, 4U, { 0x00, 0x00, 0x00, 0x00 },       1024U, true },
    { "publish-routed-here",      0x30U, 4U, 4U, { 0x00, 0x2A, 0x00, 0x00 },       1024U, true },
    { "zero-max-packet-size",     0x40U, 2U, 2U, { 0x00, 0x2A },                   0U,    true },
    /* A three-byte packet against a maximum of three: exactly at the line. */
    { "exactly-at-the-maximum",   0x40U, 2U, 2U, { 0x00, 0x2A },                   4U,    true },
    { "one-byte-over-the-maximum", 0x40U, 2U, 2U, { 0x00, 0x2A },                  3U,    true },
};

#define N_CASES    ( sizeof( CASES ) / sizeof( CASES[ 0 ] ) )

static void run_case( size_t i )
{
    const AckCase_t *c = &CASES[ i ];
    uint8_t body[ MAX_BODY ];

    /* The rule from the header comment, enforced rather than trusted: a case
     * that claims more than it carries would have the C reading past `body`. */
    if( ( size_t ) c->remainingLength != c->bodyLength )
    {
        fprintf( stderr, "case %s claims %u bytes and carries %u\n",
                 c->name, ( unsigned ) c->remainingLength,
                 ( unsigned ) c->bodyLength );
        exit( 1 );
    }

    MQTTPacketInfo_t packet;
    MQTTConnectionProperties_t props;
    MQTTReasonCodeInfo_t reasonCode;
    MQTTPropBuilder_t propBuffer;
    uint16_t packetId = 0U;
    MQTTStatus_t status;

    memcpy( body, c->body, sizeof body );

    memset( &packet, 0, sizeof packet );
    packet.type = c->type;
    packet.remainingLength = c->remainingLength;
    packet.pRemainingData = body;

    memset( &props, 0, sizeof props );
    props.maxPacketSize = c->maxPacketSize;
    props.requestProblemInfo = c->requestProblem;

    memset( &reasonCode, 0, sizeof reasonCode );
    memset( &propBuffer, 0, sizeof propBuffer );

    printf( "case %u %s type=%02x rl=%u max=%u rp=%u in=",
            ( unsigned ) i, c->name, ( unsigned ) c->type,
            ( unsigned ) c->remainingLength, ( unsigned ) c->maxPacketSize,
            c->requestProblem ? 1U : 0U );
    put_hex( c->body, c->bodyLength );

    status = MQTT_DeserializeAck( &packet, &packetId, &reasonCode,
                                  &propBuffer, &props );

    printf( " -> %s", status_name( status ) );

    /* The out-parameters are printed only on success. The C writes some of them
     * before it fails -- `deserializeSubUnsubAck` fills the packet id before its
     * property section is checked -- and a Rust `Result` has no error-path value
     * to hand back, so there is nothing for the other arm to print. The
     * divergence is recorded in the plan rather than papered over with a line
     * only one arm can produce. */
    if( status == MQTTSuccess )
    {
        if( c->type != 0xD0U )
        {
            printf( " id=%u", ( unsigned ) packetId );
        }
        else
        {
            printf( " id=-" );
        }

        printf( " rc=" );
        put_hex( reasonCode.reasonCode, reasonCode.reasonCodeLength );
        printf( " props=" );
        put_hex( propBuffer.pBuffer, propBuffer.bufferLength );
    }

    printf( "\n" );
}

/* ---- a long packet, generated ------------------------------------------- */

/* Everything above fits on one line of a table, which means everything above
 * has a ONE-byte remaining length. The encoding changes shape at 128, and the
 * maximum-packet-size check adds the bytes that encode it -- so a packet that
 * only just fits is a different arithmetic from one that comfortably does.
 *
 * This builds a PUBACK whose property section is one long reason string,
 * sized so the whole packet is exactly `128 + 2 + 1` bytes, and runs it against
 * a maximum of exactly that and of one less.
 */
static void run_long_case( const char *name, uint32_t maxPacketSize )
{
    uint8_t body[ 160 ];
    MQTTPacketInfo_t packet;
    MQTTConnectionProperties_t props;
    MQTTReasonCodeInfo_t reasonCode;
    MQTTPropBuilder_t propBuffer;
    uint16_t packetId = 0U;
    MQTTStatus_t status;
    size_t i;

    /* packet id, reason code, a one-byte property length of 124, then a reason
     * string of 121 bytes: 2 + 1 + 1 + 124 = 128. */
    memset( body, 0, sizeof body );
    body[ 0 ] = 0x00;
    body[ 1 ] = 0x2A;
    body[ 2 ] = 0x00;
    body[ 3 ] = 124U;
    body[ 4 ] = 0x1FU;
    body[ 5 ] = 0x00;
    body[ 6 ] = 121U;

    for( i = 0; i < 121U; i++ )
    {
        body[ 7U + i ] = ( uint8_t ) ( 'a' + ( i % 26U ) );
    }

    memset( &packet, 0, sizeof packet );
    packet.type = 0x40U;
    packet.remainingLength = 128U;
    packet.pRemainingData = body;

    memset( &props, 0, sizeof props );
    props.maxPacketSize = maxPacketSize;
    props.requestProblemInfo = true;

    memset( &reasonCode, 0, sizeof reasonCode );
    memset( &propBuffer, 0, sizeof propBuffer );

    printf( "long %s type=40 rl=128 max=%u rp=1 in=", name,
            ( unsigned ) maxPacketSize );
    put_hex( body, 128U );

    status = MQTT_DeserializeAck( &packet, &packetId, &reasonCode,
                                  &propBuffer, &props );

    printf( " -> %s", status_name( status ) );

    if( status == MQTTSuccess )
    {
        printf( " id=%u rc=", ( unsigned ) packetId );
        put_hex( reasonCode.reasonCode, reasonCode.reasonCodeLength );
        printf( " props=" );
        put_hex( propBuffer.pBuffer, propBuffer.bufferLength );
    }

    printf( "\n" );
}

/* ---- the sweeps ---------------------------------------------------------- */

/* Every reason code a SUBACK or UNSUBACK may carry, all 256 of them, through
 * the shared `readSubackStatus` table. A one-filter SUBACK whose single status
 * byte is the value under test. */
static void sweep_suback_status( void )
{
    unsigned value;
    unsigned accepted = 0U;
    unsigned refused = 0U;
    int first = 1;

    printf( "suback-status-sweep accepted=" );

    for( value = 0U; value < 256U; value++ )
    {
        uint8_t body[ 4 ] = { 0x00, 0x2A, 0x00, 0x00 };
        MQTTPacketInfo_t packet;
        MQTTConnectionProperties_t props;
        MQTTReasonCodeInfo_t reasonCode;
        MQTTPropBuilder_t propBuffer;
        uint16_t packetId = 0U;
        MQTTStatus_t status;

        body[ 3 ] = ( uint8_t ) value;

        memset( &packet, 0, sizeof packet );
        packet.type = 0x90U;
        packet.remainingLength = 4U;
        packet.pRemainingData = body;

        memset( &props, 0, sizeof props );
        props.maxPacketSize = 1024U;
        props.requestProblemInfo = true;

        memset( &reasonCode, 0, sizeof reasonCode );
        memset( &propBuffer, 0, sizeof propBuffer );

        status = MQTT_DeserializeAck( &packet, &packetId, &reasonCode,
                                      &propBuffer, &props );

        if( status == MQTTSuccess )
        {
            accepted++;
            printf( "%s%02x", first ? "" : ",", value );
            first = 0;
        }
        else
        {
            refused++;
        }
    }

    printf( " n=%u refused=%u\n", accepted, refused );
}

/* The same sweep through `logAckResponse`, which decides what a PUBACK,
 * PUBREC, PUBREL or PUBCOMP may say. */
static void sweep_ack_reason( uint8_t type, const char *label )
{
    unsigned value;
    unsigned accepted = 0U;
    unsigned refused = 0U;
    int first = 1;

    printf( "ack-reason-sweep %s accepted=", label );

    for( value = 0U; value < 256U; value++ )
    {
        uint8_t body[ 3 ] = { 0x00, 0x2A, 0x00 };
        MQTTPacketInfo_t packet;
        MQTTConnectionProperties_t props;
        MQTTReasonCodeInfo_t reasonCode;
        MQTTPropBuilder_t propBuffer;
        uint16_t packetId = 0U;
        MQTTStatus_t status;

        body[ 2 ] = ( uint8_t ) value;

        memset( &packet, 0, sizeof packet );
        packet.type = type;
        packet.remainingLength = 3U;
        packet.pRemainingData = body;

        memset( &props, 0, sizeof props );
        props.maxPacketSize = 1024U;
        props.requestProblemInfo = true;

        memset( &reasonCode, 0, sizeof reasonCode );
        memset( &propBuffer, 0, sizeof propBuffer );

        status = MQTT_DeserializeAck( &packet, &packetId, &reasonCode,
                                      &propBuffer, &props );

        if( status == MQTTSuccess )
        {
            accepted++;
            printf( "%s%02x", first ? "" : ",", value );
            first = 0;
        }
        else
        {
            refused++;
        }
    }

    printf( " n=%u refused=%u\n", accepted, refused );
}

/* Every one of the 256 possible type bytes against one fixed four-byte body.
 * The routing switch is the first decision this function makes, and a
 * transcription that widened it -- matching a nibble where the C matches a
 * byte, say -- would accept packets a broker never sent. */
static void sweep_packet_type( void )
{
    unsigned type;

    printf( "type-sweep accepted=" );

    {
        unsigned accepted = 0U;
        unsigned bad_param = 0U;
        unsigned bad_response = 0U;
        int first = 1;

        for( type = 0U; type < 256U; type++ )
        {
            uint8_t body[ 4 ] = { 0x00, 0x2A, 0x00, 0x00 };
            MQTTPacketInfo_t packet;
            MQTTConnectionProperties_t props;
            MQTTReasonCodeInfo_t reasonCode;
            MQTTPropBuilder_t propBuffer;
            uint16_t packetId = 0U;
            MQTTStatus_t status;

            memset( &packet, 0, sizeof packet );
            packet.type = ( uint8_t ) type;
            packet.remainingLength = 4U;
            packet.pRemainingData = body;

            memset( &props, 0, sizeof props );
            props.maxPacketSize = 1024U;
            props.requestProblemInfo = true;

            memset( &reasonCode, 0, sizeof reasonCode );
            memset( &propBuffer, 0, sizeof propBuffer );

            status = MQTT_DeserializeAck( &packet, &packetId, &reasonCode,
                                          &propBuffer, &props );

            if( status == MQTTSuccess )
            {
                accepted++;
                printf( "%s%02x", first ? "" : ",", type );
                first = 0;
            }
            else if( status == MQTTBadParameter )
            {
                bad_param++;
            }
            else
            {
                bad_response++;
            }
        }

        printf( " n=%u badparam=%u badresponse=%u\n",
                accepted, bad_param, bad_response );
    }
}

int main( void )
{
    size_t i;

    printf( "geometry cases=%u\n", ( unsigned ) N_CASES );

    for( i = 0; i < N_CASES; i++ )
    {
        run_case( i );
    }

    run_long_case( "exactly-at-the-maximum", 131U );
    run_long_case( "one-byte-over-the-maximum", 130U );

    sweep_suback_status();
    sweep_ack_reason( 0x40U, "puback" );
    sweep_ack_reason( 0x62U, "pubrel" );
    sweep_packet_type();

    printf( "end\n" );
    return 0;
}
