/* The C arm of K7's coreMQTT OUTGOING-PUBLISH differential.
 *
 * `core_mqtt_serializer.c` is compiled VERBATIM out of the pinned checkout;
 * nothing here copies or edits it.
 *
 * # Why this slice
 *
 * The package can open and close a session and read everything inside one. This
 * is the packet that carries the application's own data OUT, and coreMQTT gives
 * it THREE serializers rather than one:
 *
 *   - `MQTT_SerializePublish` writes the whole packet, payload copied in;
 *   - `MQTT_SerializePublishHeader` writes everything BUT the payload and
 *     reports the header size, so a caller can send the payload from its own
 *     buffer without a copy;
 *   - `MQTT_SerializePublishHeaderWithoutTopic` writes even less -- the type
 *     byte, the remaining length and the topic's two LENGTH bytes -- so the
 *     topic name can be sent from wherever it already lives.
 *
 * All three share `serializePublishCommon`, and the interesting question is
 * whether the three prefixes AGREE: the short one must be a prefix of the
 * medium one, and the medium one a prefix of the long one. Each case runs all
 * three into the same shape of buffer and prints all three results, so the trace
 * carries that relationship rather than leaving it to a test on one side.
 *
 * # The dup flag is patched IN PLACE
 *
 * `MQTT_UpdateDuplicatePublishFlag` takes a serialized header byte and sets or
 * clears one bit, which is how a resend happens without rebuilding the packet.
 * It accepts only bytes whose high nibble is `0x30`, so it is swept over all 256
 * values in both directions.
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core_mqtt_serializer.h"

#define MAX_FIELD    32
#define OUT_SIZE     128

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
    uint8_t     qos;
    bool        dup;
    bool        retain;
    uint16_t    packetId;

    bool        hasTopic;
    size_t      topicLength;
    uint8_t     topic[ MAX_FIELD ];

    bool        hasPayload;
    size_t      payloadLength;
    uint8_t     payload[ MAX_FIELD ];

    size_t      propertyLength;
    uint8_t     properties[ MAX_FIELD ];

    uint32_t    maxPacketSize;
    size_t      bufferSize;
} PublishCase_t;

static const PublishCase_t CASES[] = {
    /* --- the ordinary shapes --- */
    { "qos0",                   0, false, false, 0,
      true, 3, { 'a', '/', 'b' }, true, 5, { 'h', 'e', 'l', 'l', 'o' },
      0, { 0 }, 1024U, OUT_SIZE },
    { "qos0-empty-payload",     0, false, false, 0,
      true, 3, { 'a', '/', 'b' }, true, 0, { 0 },
      0, { 0 }, 1024U, OUT_SIZE },
    { "qos0-retained",          0, false, true, 0,
      true, 3, { 'a', '/', 'b' }, true, 5, { 'h', 'e', 'l', 'l', 'o' },
      0, { 0 }, 1024U, OUT_SIZE },
    { "qos1",                   1, false, false, 42,
      true, 3, { 'a', '/', 'b' }, true, 5, { 'h', 'e', 'l', 'l', 'o' },
      0, { 0 }, 1024U, OUT_SIZE },
    { "qos1-duplicate",         1, true, false, 42,
      true, 3, { 'a', '/', 'b' }, true, 5, { 'h', 'e', 'l', 'l', 'o' },
      0, { 0 }, 1024U, OUT_SIZE },
    { "qos2-retained-duplicate", 2, true, true, 65535,
      true, 3, { 'a', '/', 'b' }, true, 5, { 'h', 'e', 'l', 'l', 'o' },
      0, { 0 }, 1024U, OUT_SIZE },
    { "with-properties",        1, false, false, 7,
      true, 3, { 'a', '/', 'b' }, true, 5, { 'h', 'e', 'l', 'l', 'o' },
      5, { 0x01, 0x01, 0x02, 0x00, 0x00 }, 1024U, OUT_SIZE },
    { "properties-no-payload",  0, false, false, 0,
      true, 3, { 'a', '/', 'b' }, true, 0, { 0 },
      2, { 0x01, 0x01 }, 1024U, OUT_SIZE },
    /* A two-byte remaining length, which changes the header's own size. */
    { "long-payload",           0, false, false, 0,
      true, 3, { 'a', '/', 'b' }, true, 30,
      { 'p', 'p', 'p', 'p', 'p', 'p', 'p', 'p', 'p', 'p',
        'p', 'p', 'p', 'p', 'p', 'p', 'p', 'p', 'p', 'p',
        'p', 'p', 'p', 'p', 'p', 'p', 'p', 'p', 'p', 'p' },
      0, { 0 }, 1024U, OUT_SIZE },

    /* --- what each serializer refuses --- */
    { "qos1-packet-id-zero",    1, false, false, 0,
      true, 3, { 'a', '/', 'b' }, true, 5, { 'h', 'e', 'l', 'l', 'o' },
      0, { 0 }, 1024U, OUT_SIZE },
    { "dup-at-qos0",            0, true, false, 0,
      true, 3, { 'a', '/', 'b' }, true, 5, { 'h', 'e', 'l', 'l', 'o' },
      0, { 0 }, 1024U, OUT_SIZE },
    /* A topic name is mandatory on the way OUT, where an empty one is accepted
     * on the way in -- the two directions disagree, and both are transcribed. */
    { "empty-topic",            0, false, false, 0,
      true, 0, { 0 }, true, 5, { 'h', 'e', 'l', 'l', 'o' },
      0, { 0 }, 1024U, OUT_SIZE },
    /* A NULL payload with a NON-ZERO length is the vectored-I/O shape, and the
     * three serializers disagree about it: the full one refuses, the header one
     * accepts (it never copies the payload) and the size one never looks. It is
     * NOT a case here, because a `&[u8]` cannot claim a length it does not
     * have -- the same reason the CONNECT's null client identifier is not one.
     * The Rust API asks for the slice in all three, which costs a caller
     * nothing: it must have the bytes somewhere to send them. */

    /* --- the maximum packet size and the buffer --- */
    { "exactly-at-the-maximum", 0, false, false, 0,
      true, 3, { 'a', '/', 'b' }, true, 5, { 'h', 'e', 'l', 'l', 'o' },
      0, { 0 }, 13U, OUT_SIZE },
    { "one-byte-over-the-maximum", 0, false, false, 0,
      true, 3, { 'a', '/', 'b' }, true, 5, { 'h', 'e', 'l', 'l', 'o' },
      0, { 0 }, 12U, OUT_SIZE },
    { "buffer-exactly-big-enough", 0, false, false, 0,
      true, 3, { 'a', '/', 'b' }, true, 5, { 'h', 'e', 'l', 'l', 'o' },
      0, { 0 }, 1024U, 13U },
    /* Big enough for the header but not the payload: the full serializer
     * refuses and the header one does not, which is the whole point of having
     * two. */
    { "buffer-header-only",     0, false, false, 0,
      true, 3, { 'a', '/', 'b' }, true, 5, { 'h', 'e', 'l', 'l', 'o' },
      0, { 0 }, 1024U, 8U },
    { "buffer-empty",           0, false, false, 0,
      true, 3, { 'a', '/', 'b' }, true, 5, { 'h', 'e', 'l', 'l', 'o' },
      0, { 0 }, 1024U, 0U },
};

#define N_CASES    ( sizeof( CASES ) / sizeof( CASES[ 0 ] ) )

static void run_case( size_t i )
{
    const PublishCase_t *c = &CASES[ i ];
    uint8_t topic[ MAX_FIELD ], payload[ MAX_FIELD ], properties[ MAX_FIELD ];
    uint8_t out[ OUT_SIZE ];

    MQTTPublishInfo_t info;
    MQTTPropBuilder_t props;
    const MQTTPropBuilder_t *pProps = NULL;
    MQTTFixedBuffer_t fixed;
    uint32_t remaining = 0xAAAAAAAAU;
    uint32_t size = 0xAAAAAAAAU;
    size_t headerSize = 0xAAAAU;
    MQTTStatus_t status;

    memcpy( topic, c->topic, sizeof topic );
    memcpy( payload, c->payload, sizeof payload );
    memcpy( properties, c->properties, sizeof properties );

    memset( &info, 0, sizeof info );
    info.qos = ( MQTTQoS_t ) c->qos;
    info.dup = c->dup;
    info.retain = c->retain;
    info.pTopicName = c->hasTopic ? ( const char * ) topic : NULL;
    info.topicNameLength = c->topicLength;
    info.pPayload = c->hasPayload ? payload : NULL;
    info.payloadLength = c->payloadLength;

    if( c->propertyLength != 0U )
    {
        memset( &props, 0, sizeof props );
        props.pBuffer = properties;
        props.bufferLength = sizeof properties;
        props.currentIndex = c->propertyLength;
        pProps = &props;
    }

    printf( "case %u %s qos=%u dup=%u ret=%u id=%u topic=", ( unsigned ) i,
            c->name, ( unsigned ) c->qos, c->dup ? 1U : 0U, c->retain ? 1U : 0U,
            ( unsigned ) c->packetId );
    put_opt( c->hasTopic, c->topic, c->topicLength );
    printf( " payload=" );
    put_opt( c->hasPayload, c->payload, c->payloadLength );
    printf( " props=" );
    put_opt( c->propertyLength != 0U, c->properties, c->propertyLength );
    printf( " max=%u buf=%u", ( unsigned ) c->maxPacketSize,
            ( unsigned ) c->bufferSize );

    status = MQTT_GetPublishPacketSize( &info, pProps, &remaining, &size,
                                        c->maxPacketSize );

    printf( " -> size %s", status_name( status ) );

    if( status != MQTTSuccess )
    {
        printf( "\n" );
        return;
    }

    printf( " remaining=%u packet=%u", ( unsigned ) remaining,
            ( unsigned ) size );

    /* 1. The whole packet, payload copied in. */
    memset( out, 0xCC, sizeof out );
    memset( &fixed, 0, sizeof fixed );
    fixed.pBuffer = out;
    fixed.size = c->bufferSize;
    status = MQTT_SerializePublish( &info, pProps, c->packetId, remaining, &fixed );
    printf( " | full %s", status_name( status ) );

    if( status == MQTTSuccess )
    {
        printf( " bytes=" );
        put_hex( out, ( size_t ) size );
    }

    /* 2. Everything but the payload, with the header size reported. */
    memset( out, 0xCC, sizeof out );
    fixed.size = c->bufferSize;
    headerSize = 0xAAAAU;
    status = MQTT_SerializePublishHeader( &info, pProps, c->packetId, remaining,
                                          &fixed, &headerSize );
    printf( " | hdr %s", status_name( status ) );

    if( status == MQTTSuccess )
    {
        printf( " hsize=%u bytes=", ( unsigned ) headerSize );
        put_hex( out, headerSize );
    }

    /* 3. The type byte, the remaining length and the topic's LENGTH only. */
    memset( out, 0xCC, sizeof out );
    headerSize = 0xAAAAU;
    status = MQTT_SerializePublishHeaderWithoutTopic( &info, remaining, out,
                                                      &headerSize );
    printf( " | notopic %s", status_name( status ) );

    if( status == MQTTSuccess )
    {
        printf( " hsize=%u bytes=", ( unsigned ) headerSize );
        put_hex( out, headerSize );
    }

    printf( "\n" );
}

/* ---- the API contract, broken on purpose ---------------------------------- */

/* `MQTT_SerializePublishHeader` reports the header size it COMPUTED from the
 * remaining length it was handed, not the bytes it wrote. The C's own comment
 * on `MQTT_SerializePublish` calls calling the size function first "part of the
 * API contract", and every case above keeps it -- so the two numbers are always
 * equal and a transcription that returned the wrong one passes.
 *
 * These cases hand the serializers a remaining length that is NOT the one the
 * calculator produced, which is the only way to tell the two apart. */
typedef struct
{
    const char *name;
    uint32_t    remainingLength;
} ContractCase_t;

static const ContractCase_t CONTRACT_CASES[] = {
    /* The honest value for the fixture below is 11. */
    { "remaining-length-as-computed", 11U },
    { "remaining-length-too-large",   16U },
    { "remaining-length-too-small",    8U },
    /* Smaller than the payload, which the header serializer refuses outright. */
    { "remaining-length-under-the-payload", 4U },
};

#define N_CONTRACT    ( sizeof( CONTRACT_CASES ) / sizeof( CONTRACT_CASES[ 0 ] ) )

static void run_contract_case( size_t i )
{
    const ContractCase_t *c = &CONTRACT_CASES[ i ];
    static const uint8_t TOPIC[] = { 'a', '/', 'b' };
    static const uint8_t PAYLOAD[] = { 'h', 'e', 'l', 'l', 'o' };

    uint8_t out[ OUT_SIZE ];
    MQTTPublishInfo_t info;
    MQTTFixedBuffer_t fixed;
    size_t headerSize = 0xAAAAU;
    MQTTStatus_t status;

    memset( &info, 0, sizeof info );
    info.qos = MQTTQoS0;
    info.pTopicName = ( const char * ) TOPIC;
    info.topicNameLength = sizeof TOPIC;
    info.pPayload = PAYLOAD;
    info.payloadLength = sizeof PAYLOAD;

    printf( "contract %u %s rl=%u", ( unsigned ) i, c->name,
            ( unsigned ) c->remainingLength );

    memset( out, 0xCC, sizeof out );
    memset( &fixed, 0, sizeof fixed );
    fixed.pBuffer = out;
    fixed.size = sizeof out;
    status = MQTT_SerializePublish( &info, NULL, 0U, c->remainingLength, &fixed );
    printf( " -> full %s", status_name( status ) );

    memset( out, 0xCC, sizeof out );
    fixed.size = sizeof out;
    headerSize = 0xAAAAU;
    status = MQTT_SerializePublishHeader( &info, NULL, 0U, c->remainingLength,
                                          &fixed, &headerSize );
    printf( " | hdr %s", status_name( status ) );

    if( status == MQTTSuccess )
    {
        printf( " hsize=%u bytes=", ( unsigned ) headerSize );
        put_hex( out, headerSize );
    }

    printf( "\n" );
}

/* ---- the dup-flag sweep --------------------------------------------------- */

/* `MQTT_UpdateDuplicatePublishFlag` patches one bit of an already-serialized
 * header byte, which is how a resend happens without rebuilding the packet. It
 * accepts only bytes whose high nibble is 0x30, so both directions are swept
 * over all 256 values and the RESULTING byte is printed for the accepted ones --
 * a function that accepted the right inputs and set the wrong bit would pass a
 * status-only sweep. */
static void sweep_duplicate_flag( bool set )
{
    unsigned value;
    unsigned accepted = 0U;
    unsigned refused = 0U;
    uint64_t fnv = 1469598103934665603ULL;

    for( value = 0U; value < 256U; value++ )
    {
        uint8_t header = ( uint8_t ) value;
        MQTTStatus_t status = MQTT_UpdateDuplicatePublishFlag( &header, set );

        if( status == MQTTSuccess )
        {
            accepted++;
            fnv ^= ( uint64_t ) header;
            fnv *= 1099511628211ULL;
        }
        else
        {
            refused++;
        }
    }

    printf( "dup-sweep %s accepted=%u refused=%u digest=%016llx\n",
            set ? "set" : "clear", accepted, refused,
            ( unsigned long long ) fnv );
}

int main( void )
{
    size_t i;

    printf( "geometry cases=%u contract=%u\n", ( unsigned ) N_CASES,
            ( unsigned ) N_CONTRACT );

    for( i = 0; i < N_CASES; i++ )
    {
        run_case( i );
    }

    for( i = 0; i < N_CONTRACT; i++ )
    {
        run_contract_case( i );
    }

    sweep_duplicate_flag( true );
    sweep_duplicate_flag( false );

    printf( "end\n" );
    return 0;
}
