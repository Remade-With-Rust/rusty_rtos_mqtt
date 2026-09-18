/* The C arm of K7's coreMQTT incoming-PUBLISH differential.
 *
 * `core_mqtt_serializer.c` is compiled VERBATIM out of the pinned checkout;
 * nothing here copies or edits it.
 *
 * # Why this slice
 *
 * This is the last packet a broker can send that the package could not read,
 * and the only one that carries APPLICATION DATA. Everything above it -- the
 * acks, the CONNACK -- is protocol bookkeeping a library consumes. A PUBLISH is
 * handed to the application, so its topic name, its payload length and its
 * property section are the numbers an application will index with.
 *
 * It is also the only packet whose TYPE BYTE is partly data: the low nibble
 * carries DUP, QoS and RETAIN, so the first byte off the socket is four more
 * inputs rather than a constant.
 *
 * # Two flag sweeps, for the reason the CONNACK needed five property sweeps
 *
 * The flags nibble is sixteen values, which is enumerable. But QoS decides
 * whether a packet identifier is present, so ONE body cannot sweep it: a body
 * with a packet id is malformed at QoS 0 and a body without one is malformed at
 * QoS 1. Sweeping once would refuse half the nibble for the wrong reason.
 *
 * So the nibble is swept twice, once against each body shape, and each prints
 * its own accepted set.
 *
 * # Six property sweeps, same rule
 *
 * A PUBLISH property may be a one-byte value, a two-byte value, a four-byte
 * value, a string, a VARIABLE-LENGTH integer (the subscription identifier, which
 * no other packet carries) or a user property. Six shapes, six sweeps.
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core_mqtt_serializer.h"

#define MAX_BODY    96

static void put_hex( const uint8_t *p, size_t n )
{
    size_t i;

    if( ( n == 0U ) || ( p == NULL ) )
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

/* Run one PUBLISH and print everything the C hands back. */
static MQTTStatus_t run_one( uint8_t type,
                             uint32_t remainingLength,
                             const uint8_t *bodyBytes,
                             size_t bodyLength,
                             uint32_t maxPacketSize,
                             uint16_t topicAliasMax,
                             bool print )
{
    uint8_t body[ MAX_BODY ];
    MQTTPacketInfo_t packet;
    MQTTPublishInfo_t info;
    MQTTPropBuilder_t propBuffer;
    uint16_t packetId = 0U;
    MQTTStatus_t status;

    memset( body, 0, sizeof body );
    memcpy( body, bodyBytes, bodyLength );

    memset( &packet, 0, sizeof packet );
    packet.type = type;
    packet.remainingLength = remainingLength;
    packet.pRemainingData = body;

    memset( &info, 0, sizeof info );
    memset( &propBuffer, 0, sizeof propBuffer );

    status = MQTT_DeserializePublish( &packet, &packetId, &info, &propBuffer,
                                      maxPacketSize, topicAliasMax );

    if( print )
    {
        printf( " -> %s", status_name( status ) );

        if( status == MQTTSuccess )
        {
            printf( " qos=%u dup=%u ret=%u id=%u topic=",
                    ( unsigned ) info.qos,
                    info.dup ? 1U : 0U,
                    info.retain ? 1U : 0U,
                    ( unsigned ) packetId );
            put_hex( ( const uint8_t * ) info.pTopicName, info.topicNameLength );
            printf( " proplen=%u props=", ( unsigned ) info.propertyLength );
            put_hex( propBuffer.pBuffer, propBuffer.bufferLength );
            printf( " payload=" );
            put_hex( ( const uint8_t * ) info.pPayload, info.payloadLength );
        }
    }

    return status;
}

/* ---- the named cases ----------------------------------------------------- */

typedef struct
{
    const char *name;
    uint8_t     type;
    uint32_t    remainingLength;  /* always equal to bodyLength */
    size_t      bodyLength;
    uint8_t     body[ MAX_BODY ];
    uint32_t    maxPacketSize;
    uint16_t    topicAliasMax;
} PublishCase_t;

/* The same rule every driver in this package enforces: a case whose claim
 * exceeds its body would have the C indexing past `body`, so its answer would
 * depend on whatever is next in memory rather than on the library. */
static const PublishCase_t CASES[] = {
    /* --- QoS 0: topic length, topic, property length, payload --- */
    { "qos0-minimal",             0x30U, 4U, 4U,
      { 0x00, 0x01, 'a', 0x00 },                                        1024U, 10U },
    { "qos0-with-payload",        0x30U, 9U, 9U,
      { 0x00, 0x01, 'a', 0x00, 'h', 'e', 'l', 'l', 'o' },               1024U, 10U },
    { "qos0-longer-topic",        0x30U, 11U, 11U,
      { 0x00, 0x05, 's', 'e', 'n', 's', 'e', 0x00, 'x', 'y', 'z' },     1024U, 10U },
    { "qos0-empty-payload",       0x30U, 6U, 6U,
      { 0x00, 0x03, 'a', '/', 'b', 0x00 },                              1024U, 10U },
    /* MQTT 5.0 s3.3.2.1 makes a zero-length topic name a protocol error unless
     * a Topic Alias is present. This case has neither. */
    { "qos0-empty-topic",         0x30U, 4U, 4U,
      { 0x00, 0x00, 0x00, 'p' },                                        1024U, 10U },
    /* ...and the same with one, which IS legal: the alias names the topic. */
    { "qos0-empty-topic-with-alias", 0x30U, 7U, 7U,
      { 0x00, 0x00, 0x03, 0x23, 0x00, 0x05, 'p' },                      1024U, 10U },
    { "qos0-three-byte-body",     0x30U, 3U, 3U, { 0x00, 0x01, 'a' },   1024U, 10U },
    { "qos0-topic-past-the-end",  0x30U, 5U, 5U,
      { 0x00, 0x09, 'a', 0x00, 'p' },                                   1024U, 10U },

    /* --- the flags nibble, named --- */
    { "retain",                   0x31U, 6U, 6U,
      { 0x00, 0x01, 'a', 0x00, 'h', 'i' },                              1024U, 10U },
    { "dup-at-qos0",              0x38U, 6U, 6U,
      { 0x00, 0x01, 'a', 0x00, 'h', 'i' },                              1024U, 10U },
    { "qos1",                     0x32U, 8U, 8U,
      { 0x00, 0x01, 'a', 0x00, 0x2A, 0x00, 'h', 'i' },                  1024U, 10U },
    { "qos2",                     0x34U, 8U, 8U,
      { 0x00, 0x01, 'a', 0x00, 0x2A, 0x00, 'h', 'i' },                  1024U, 10U },
    { "qos1-dup-retain",          0x3BU, 8U, 8U,
      { 0x00, 0x01, 'a', 0x00, 0x2A, 0x00, 'h', 'i' },                  1024U, 10U },
    /* Both QoS bits set is QoS 3, which does not exist. */
    { "qos3",                     0x36U, 8U, 8U,
      { 0x00, 0x01, 'a', 0x00, 0x2A, 0x00, 'h', 'i' },                  1024U, 10U },
    { "qos1-packet-id-zero",      0x32U, 8U, 8U,
      { 0x00, 0x01, 'a', 0x00, 0x00, 0x00, 'h', 'i' },                  1024U, 10U },
    { "qos1-no-room-for-id",      0x32U, 5U, 5U,
      { 0x00, 0x01, 'a', 0x00, 0x2A },                                  1024U, 10U },
    { "qos1-empty-payload",       0x32U, 6U, 6U,
      { 0x00, 0x01, 'a', 0x00, 0x2A, 0x00 },                            1024U, 10U },

    /* --- the properties, one at a time --- */
    { "payload-format-zero",      0x30U, 6U, 6U,
      { 0x00, 0x01, 'a', 0x02, 0x01, 0x00 },                            1024U, 10U },
    { "payload-format-one",       0x30U, 6U, 6U,
      { 0x00, 0x01, 'a', 0x02, 0x01, 0x01 },                            1024U, 10U },
    { "payload-format-two",       0x30U, 6U, 6U,
      { 0x00, 0x01, 'a', 0x02, 0x01, 0x02 },                            1024U, 10U },
    { "message-expiry",           0x30U, 9U, 9U,
      { 0x00, 0x01, 'a', 0x05, 0x02, 0x00, 0x00, 0x0E, 0x10 },          1024U, 10U },
    { "content-type",             0x30U, 9U, 9U,
      { 0x00, 0x01, 'a', 0x05, 0x03, 0x00, 0x02, 'a', 'b' },            1024U, 10U },
    { "response-topic",           0x30U, 9U, 9U,
      { 0x00, 0x01, 'a', 0x05, 0x08, 0x00, 0x02, 'r', 't' },            1024U, 10U },
    { "correlation-data",         0x30U, 9U, 9U,
      { 0x00, 0x01, 'a', 0x05, 0x09, 0x00, 0x02, 0xFF, 0xFE },          1024U, 10U },
    { "topic-alias-in-range",     0x30U, 7U, 7U,
      { 0x00, 0x01, 'a', 0x03, 0x23, 0x00, 0x05 },                      1024U, 10U },
    { "topic-alias-at-the-max",   0x30U, 7U, 7U,
      { 0x00, 0x01, 'a', 0x03, 0x23, 0x00, 0x0A },                      1024U, 10U },
    { "topic-alias-over-the-max", 0x30U, 7U, 7U,
      { 0x00, 0x01, 'a', 0x03, 0x23, 0x00, 0x0B },                      1024U, 10U },
    { "topic-alias-zero",         0x30U, 7U, 7U,
      { 0x00, 0x01, 'a', 0x03, 0x23, 0x00, 0x00 },                      1024U, 10U },
    /* A zero Topic Alias Maximum means the client accepts none at all. */
    { "topic-alias-when-none-allowed", 0x30U, 7U, 7U,
      { 0x00, 0x01, 'a', 0x03, 0x23, 0x00, 0x01 },                      1024U, 0U },
    { "subscription-id",          0x30U, 6U, 6U,
      { 0x00, 0x01, 'a', 0x02, 0x0B, 0x07 },                            1024U, 10U },
    /* MQTT 5.0 s3.3.2.3.8 makes a Subscription Identifier of 0 a protocol
     * error. This case is here to find out whether the C says so. */
    { "subscription-id-zero",     0x30U, 6U, 6U,
      { 0x00, 0x01, 'a', 0x02, 0x0B, 0x00 },                            1024U, 10U },
    /* A PUBLISH MAY carry several, unlike every other property. */
    { "subscription-id-zero-two-byte", 0x30U, 7U, 7U,
      { 0x00, 0x01, 'a', 0x03, 0x0B, 0x80, 0x00 },                      1024U, 10U },
    { "two-subscription-ids",     0x30U, 8U, 8U,
      { 0x00, 0x01, 'a', 0x04, 0x0B, 0x07, 0x0B, 0x08 },                1024U, 10U },
    { "multi-byte-subscription-id", 0x30U, 7U, 7U,
      { 0x00, 0x01, 'a', 0x03, 0x0B, 0x80, 0x01 },                      1024U, 10U },
    { "user-property",            0x30U, 11U, 11U,
      { 0x00, 0x01, 'a', 0x07, 0x26, 0x00, 0x01, 'k', 0x00, 0x01, 'v' }, 1024U, 10U },
    { "unknown-property",         0x30U, 6U, 6U,
      { 0x00, 0x01, 'a', 0x02, 0x7F, 0x00 },                            1024U, 10U },
    { "repeated-content-type",    0x30U, 14U, 14U,
      { 0x00, 0x01, 'a', 0x0A, 0x03, 0x00, 0x02, 'a', 'b',
        0x03, 0x00, 0x02, 'c', 'd' },                                   1024U, 10U },
    { "property-length-too-long", 0x30U, 6U, 6U,
      { 0x00, 0x01, 'a', 0x09, 0x01, 0x00 },                            1024U, 10U },
    /* Properties are NOT last in a PUBLISH -- the payload follows -- so a
     * property section shorter than the body is legal and the rest is payload. */
    { "properties-then-payload",  0x30U, 11U, 11U,
      { 0x00, 0x01, 'a', 0x02, 0x01, 0x01, 'p', 'a', 'y', 'l', 'd' },   1024U, 10U },
    { "qos2-properties-payload",  0x34U, 13U, 13U,
      { 0x00, 0x01, 'a', 0x00, 0x2A, 0x02, 0x01, 0x01,
        'p', 'a', 'y', 'l', 'd' },                                      1024U, 10U },

    /* --- routing and limits --- */
    { "puback-routed-here",       0x40U, 4U, 4U,
      { 0x00, 0x01, 'a', 0x00 },                                        1024U, 10U },
    { "connack-routed-here",      0x20U, 4U, 4U,
      { 0x00, 0x01, 'a', 0x00 },                                        1024U, 10U },
    { "exactly-at-the-maximum",   0x30U, 4U, 4U,
      { 0x00, 0x01, 'a', 0x00 },                                        6U,    10U },
    { "one-byte-over-the-maximum", 0x30U, 4U, 4U,
      { 0x00, 0x01, 'a', 0x00 },                                        5U,    10U },
};

#define N_CASES    ( sizeof( CASES ) / sizeof( CASES[ 0 ] ) )

static void run_case( size_t i )
{
    const PublishCase_t *c = &CASES[ i ];

    if( ( size_t ) c->remainingLength != c->bodyLength )
    {
        fprintf( stderr, "case %s claims %u bytes and carries %u\n",
                 c->name, ( unsigned ) c->remainingLength,
                 ( unsigned ) c->bodyLength );
        exit( 1 );
    }

    printf( "case %u %s type=%02x rl=%u max=%u alias=%u in=",
            ( unsigned ) i, c->name, ( unsigned ) c->type,
            ( unsigned ) c->remainingLength, ( unsigned ) c->maxPacketSize,
            ( unsigned ) c->topicAliasMax );
    put_hex( c->body, c->bodyLength );

    ( void ) run_one( c->type, c->remainingLength, c->body, c->bodyLength,
                      c->maxPacketSize, c->topicAliasMax, true );

    printf( "\n" );
}

/* ---- the sweeps ---------------------------------------------------------- */

/* All sixteen values of the flags nibble, against ONE body shape.
 *
 * QoS decides whether a packet identifier is present, so a single body cannot
 * sweep the nibble: one with an id is malformed at QoS 0 and one without is
 * malformed at QoS 1. Both shapes are swept, and each prints its own set. */
static void sweep_flags( const char *label,
                         const uint8_t *bodyBytes,
                         size_t bodyLength )
{
    unsigned nibble;
    unsigned accepted = 0U;
    unsigned rejected = 0U;
    int first = 1;

    printf( "flags-sweep %s accepted=", label );

    for( nibble = 0U; nibble < 16U; nibble++ )
    {
        MQTTStatus_t status = run_one( ( uint8_t ) ( 0x30U | nibble ),
                                       ( uint32_t ) bodyLength, bodyBytes,
                                       bodyLength, 1024U, 10U, false );

        if( status == MQTTSuccess )
        {
            printf( "%s%x", first ? "" : ",", nibble );
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

/* Every value of the property-identifier byte, at one value SHAPE. */
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
        /* topic length (2) + topic (1) + property length (1) + section */
        size_t bodyLength = 4U + sectionLength;

        memset( body, 0, sizeof body );
        body[ 0 ] = 0x00;
        body[ 1 ] = 0x01;
        body[ 2 ] = 'a';
        body[ 3 ] = ( uint8_t ) sectionLength;
        body[ 4 ] = ( uint8_t ) id;
        memcpy( &body[ 5 ], pValue, valueLength );

        status = run_one( 0x30U, ( uint32_t ) bodyLength, body, bodyLength,
                          1024U, 10U, false );

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

    /* A body with no packet identifier, and one with. */
    static const uint8_t QOS0_BODY[] = { 0x00, 0x01, 'a', 0x00, 'h', 'i' };
    static const uint8_t QOS12_BODY[] = { 0x00, 0x01, 'a', 0x00, 0x2A, 0x00, 'h', 'i' };

    /* One value of each shape a PUBLISH property can take. The topic alias is
     * two bytes and must be non-zero and within the maximum, so 0x0001 serves
     * for the two-byte shape. */
    static const uint8_t ONE_BYTE[] = { 0x00 };
    static const uint8_t TWO_BYTE[] = { 0x00, 0x01 };
    static const uint8_t FOUR_BYTE[] = { 0x00, 0x00, 0x00, 0x01 };
    static const uint8_t STRING[] = { 0x00, 0x01, 'x' };
    static const uint8_t VARINT[] = { 0x07 };
    static const uint8_t USER_PROP[] = { 0x00, 0x01, 'k', 0x00, 0x01, 'v' };

    printf( "geometry cases=%u\n", ( unsigned ) N_CASES );

    for( i = 0; i < N_CASES; i++ )
    {
        run_case( i );
    }

    sweep_flags( "no-packet-id", QOS0_BODY, sizeof QOS0_BODY );
    sweep_flags( "with-packet-id", QOS12_BODY, sizeof QOS12_BODY );

    sweep_property( "one-byte", ONE_BYTE, sizeof ONE_BYTE );
    sweep_property( "two-byte", TWO_BYTE, sizeof TWO_BYTE );
    sweep_property( "four-byte", FOUR_BYTE, sizeof FOUR_BYTE );
    sweep_property( "string", STRING, sizeof STRING );
    sweep_property( "varint", VARINT, sizeof VARINT );
    sweep_property( "user-property", USER_PROP, sizeof USER_PROP );

    printf( "end\n" );
    return 0;
}
