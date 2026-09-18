/* The C arm of K7's coreMQTT DISCONNECT differential, BOTH DIRECTIONS.
 *
 * `core_mqtt_serializer.c` is compiled VERBATIM out of the pinned checkout;
 * nothing here copies or edits it.
 *
 * # Why this slice
 *
 * The package claimed after the PUBLISH slice that it could read every packet a
 * broker can send. That was WRONG: MQTT 5 added a server-sent DISCONNECT, and
 * `MQTT_DeserializeDisconnect` is how a client reads it. This slice makes the
 * claim true.
 *
 * It is also the only packet in the library that travels BOTH WAYS through the
 * same validation. `validateDisconnectResponse` takes an `incoming` flag and
 * answers differently for the same byte:
 *
 *   - 0x04 "disconnect with Will message" is legal OUTGOING and refused INCOMING;
 *   - fifteen server-only codes are legal INCOMING and refused OUTGOING;
 *   - thirteen are legal either way.
 *
 * So the reason code is swept 256 times in EACH direction. That is the same
 * rule the CONNACK's five property sweeps and the PUBLISH's two flag sweeps
 * came from, applied to a flag that changes the ANSWER rather than the input's
 * shape.
 *
 * # The outgoing arm sizes and then serializes, in one case
 *
 * `MQTT_GetDisconnectPacketSize` and `MQTT_SerializeDisconnect` are run back to
 * back and the bytes printed, so the trace carries the two-slice agreement
 * directly rather than leaving it to a test on the Rust side alone.
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core_mqtt_serializer.h"

#define MAX_BODY    64
#define OUT_SIZE    64

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
        case MQTTNoMemory:     return "NoMemory";
        default:               return "OTHER";
    }
}

/* ---- incoming: MQTT_DeserializeDisconnect -------------------------------- */

typedef struct
{
    const char *name;
    uint32_t    remainingLength;   /* always equal to bodyLength */
    size_t      bodyLength;
    uint8_t     body[ MAX_BODY ];
    uint32_t    maxPacketSize;
} IncomingCase_t;

static const IncomingCase_t IN_CASES[] = {
    /* MQTT 5.0 s3.14.2.1: the whole variable header may be omitted, which is a
     * normal disconnection with nothing to say. */
    { "empty",                    0U, 0U, { 0 },                          1024U },
    { "normal-disconnection",     1U, 1U, { 0x00 },                       1024U },
    { "server-shutting-down",     1U, 1U, { 0x8B },                       1024U },
    { "session-taken-over",       1U, 1U, { 0x8E },                       1024U },
    /* 0x04 is the CLIENT's "disconnect with Will message" and a server may not
     * send it. The same table refuses it in this direction only. */
    { "will-message-from-server", 1U, 1U, { 0x04 },                       1024U },
    { "unknown-reason-code",      1U, 1U, { 0x7F },                       1024U },
    /* Empty property section: reason code, then a property length of zero. */
    { "no-properties",            2U, 2U, { 0x00, 0x00 },                 1024U },
    { "reason-string",            8U, 8U,
      { 0x8B, 0x06, 0x1F, 0x00, 0x03, 'b', 'y', 'e' },                    1024U },
    { "server-reference",         9U, 9U,
      { 0x9C, 0x07, 0x1C, 0x00, 0x04, 'a', '.', 'b', 'c' },               1024U },
    { "user-property",            9U, 9U,
      { 0x00, 0x07, 0x26, 0x00, 0x01, 'k', 0x00, 0x01, 'v' },             1024U },
    { "two-user-properties",      16U, 16U,
      { 0x00, 0x0E, 0x26, 0x00, 0x01, 'a', 0x00, 0x01, 'b',
        0x26, 0x00, 0x01, 'c', 0x00, 0x01, 'd' },                         1024U },
    /* Session Expiry is legal in an OUTGOING disconnect and not an incoming
     * one; the two directions have different property tables. */
    { "session-expiry-from-server", 8U, 8U,
      { 0x00, 0x06, 0x11, 0x00, 0x00, 0x0E, 0x10, 0x00 },                 1024U },
    { "unknown-property",         4U, 4U, { 0x00, 0x02, 0x7F, 0x00 },     1024U },
    { "repeated-reason-string",   12U, 12U,
      { 0x00, 0x0A, 0x1F, 0x00, 0x02, 'h', 'i', 0x1F, 0x00, 0x02, 'y', 'o' }, 1024U },
    /* The property length must fill the body EXACTLY: nothing follows it. */
    { "property-length-too-long", 4U, 4U, { 0x00, 0x08, 0x1F, 0x00 },     1024U },
    { "property-length-too-short", 5U, 5U,
      { 0x00, 0x02, 0x1F, 0x00, 0x00 },                                   1024U },
    /* A COMPLETE property section -- an empty reason string -- followed by one
     * byte that is not part of it. This is what makes the exact-fit check
     * load-bearing: without it the section reads fine and the trailing byte is
     * never looked at, so a broker could carry data inside a packet the client
     * believes it read whole. The same shape the ack slice needed. */
    { "property-trailing-byte",   6U, 6U,
      { 0x00, 0x03, 0x1F, 0x00, 0x00, 0xFF },                             1024U },
    { "property-length-non-minimal", 5U, 5U,
      { 0x00, 0x80, 0x00, 0x1F, 0x00 },                                   1024U },
    { "zero-max-packet-size",     1U, 1U, { 0x00 },                       0U },
    { "exactly-at-the-maximum",   1U, 1U, { 0x00 },                       3U },
    { "one-byte-over-the-maximum", 1U, 1U, { 0x00 },                      2U },
};

#define N_IN    ( sizeof( IN_CASES ) / sizeof( IN_CASES[ 0 ] ) )

static MQTTStatus_t run_incoming( uint32_t remainingLength,
                                  const uint8_t *bodyBytes,
                                  size_t bodyLength,
                                  uint32_t maxPacketSize,
                                  bool print )
{
    uint8_t body[ MAX_BODY ];
    MQTTPacketInfo_t packet;
    MQTTReasonCodeInfo_t info;
    MQTTPropBuilder_t propBuffer;
    MQTTStatus_t status;

    memset( body, 0, sizeof body );
    memcpy( body, bodyBytes, bodyLength );

    memset( &packet, 0, sizeof packet );
    packet.type = 0xE0U;
    packet.remainingLength = remainingLength;
    packet.pRemainingData = body;

    memset( &info, 0, sizeof info );
    memset( &propBuffer, 0, sizeof propBuffer );

    status = MQTT_DeserializeDisconnect( &packet, maxPacketSize, &info, &propBuffer );

    if( print )
    {
        printf( " -> %s", status_name( status ) );

        if( status == MQTTSuccess )
        {
            printf( " rc=" );
            put_hex( info.reasonCode, info.reasonCodeLength );
            printf( " props=" );
            put_hex( propBuffer.pBuffer, propBuffer.bufferLength );
        }
    }

    return status;
}

/* ---- outgoing: size, then serialize -------------------------------------- */

typedef struct
{
    const char *name;
    bool        hasReasonCode;
    uint8_t     reasonCode;
    size_t      propertyLength;   /* 0 means "no property builder at all" */
    uint8_t     properties[ MAX_BODY ];
    uint32_t    maxPacketSize;
    size_t      bufferSize;
} OutgoingCase_t;

static const OutgoingCase_t OUT_CASES[] = {
    /* No reason code and no properties: the smallest DISCONNECT this library
     * will build. Note what remaining length it chooses. */
    { "bare",                     false, 0x00, 0U, { 0 },                 1024U, OUT_SIZE },
    { "normal-disconnection",     true,  0x00, 0U, { 0 },                 1024U, OUT_SIZE },
    { "with-will-message",        true,  0x04, 0U, { 0 },                 1024U, OUT_SIZE },
    /* 0x8B is a SERVER-only code; a client may not send it. */
    { "server-only-code",         true,  0x8B, 0U, { 0 },                 1024U, OUT_SIZE },
    { "unspecified-error",        true,  0x80, 0U, { 0 },                 1024U, OUT_SIZE },
    { "unknown-reason-code",      true,  0x7F, 0U, { 0 },                 1024U, OUT_SIZE },
    { "session-expiry",           true,  0x00, 5U,
      { 0x11, 0x00, 0x00, 0x0E, 0x10 },                                   1024U, OUT_SIZE },
    { "reason-string",            true,  0x80, 6U,
      { 0x1F, 0x00, 0x03, 'b', 'y', 'e' },                                1024U, OUT_SIZE },
    { "user-property",            true,  0x00, 7U,
      { 0x26, 0x00, 0x01, 'k', 0x00, 0x01, 'v' },                         1024U, OUT_SIZE },
    /* Properties with NO reason code: refused, because the reason code comes
     * first on the wire and cannot be skipped. */
    { "properties-without-a-reason", false, 0x00, 6U,
      { 0x1F, 0x00, 0x03, 'b', 'y', 'e' },                                1024U, OUT_SIZE },
    { "zero-max-packet-size",     true,  0x00, 0U, { 0 },                 0U,    OUT_SIZE },
    { "exactly-at-the-maximum",   true,  0x00, 0U, { 0 },                 4U,    OUT_SIZE },
    { "one-byte-over-the-maximum", true, 0x00, 0U, { 0 },                 3U,    OUT_SIZE },
    /* The fixed buffer is the caller's, and may be too small for a packet the
     * size calculator was happy with. */
    { "buffer-too-small",         true,  0x80, 6U,
      { 0x1F, 0x00, 0x03, 'b', 'y', 'e' },                                1024U, 4U },
};

#define N_OUT    ( sizeof( OUT_CASES ) / sizeof( OUT_CASES[ 0 ] ) )

/* Size then serialize, and print both answers plus the bytes. */
static void run_outgoing( size_t i )
{
    const OutgoingCase_t *c = &OUT_CASES[ i ];
    uint8_t propBytes[ MAX_BODY ];
    uint8_t out[ OUT_SIZE ];
    MQTTPropBuilder_t props;
    const MQTTPropBuilder_t *pProps = NULL;
    MQTTFixedBuffer_t fixed;
    MQTTSuccessFailReasonCode_t reason;
    const MQTTSuccessFailReasonCode_t *pReason = NULL;
    uint32_t remaining = 0xAAAAAAAAU;
    uint32_t size = 0xAAAAAAAAU;
    MQTTStatus_t status;

    memset( propBytes, 0, sizeof propBytes );
    memcpy( propBytes, c->properties, sizeof c->properties );

    if( c->propertyLength != 0U )
    {
        memset( &props, 0, sizeof props );
        props.pBuffer = propBytes;
        props.bufferLength = sizeof propBytes;
        props.currentIndex = c->propertyLength;
        pProps = &props;
    }

    if( c->hasReasonCode )
    {
        /* More details at: the reason code is an enum in the C. */
        reason = ( MQTTSuccessFailReasonCode_t ) c->reasonCode;
        pReason = &reason;
    }

    printf( "out %u %s rc=", ( unsigned ) i, c->name );

    if( c->hasReasonCode )
    {
        printf( "%02x", ( unsigned ) c->reasonCode );
    }
    else
    {
        printf( "-" );
    }

    printf( " proplen=%u max=%u buf=%u props=",
            ( unsigned ) c->propertyLength, ( unsigned ) c->maxPacketSize,
            ( unsigned ) c->bufferSize );
    put_hex( c->properties, c->propertyLength );

    status = MQTT_GetDisconnectPacketSize( pProps, &remaining, &size,
                                           c->maxPacketSize, pReason );

    printf( " -> size %s", status_name( status ) );

    if( status == MQTTSuccess )
    {
        printf( " remaining=%u packet=%u", ( unsigned ) remaining,
                ( unsigned ) size );

        memset( out, 0xCC, sizeof out );
        memset( &fixed, 0, sizeof fixed );
        fixed.pBuffer = out;
        fixed.size = c->bufferSize;

        status = MQTT_SerializeDisconnect( pProps, pReason, remaining, &fixed );

        printf( " serialize %s", status_name( status ) );

        if( status == MQTTSuccess )
        {
            printf( " bytes=" );
            put_hex( out, ( size_t ) size );
        }
    }

    printf( "\n" );
}

/* ---- outgoing property validation ---------------------------------------- */

typedef struct
{
    const char *name;
    uint32_t    connectSessionExpiry;
    size_t      propertyLength;
    uint8_t     properties[ MAX_BODY ];
} ValidateCase_t;

static const ValidateCase_t VAL_CASES[] = {
    { "empty",                 3600U, 0U, { 0 } },
    { "session-expiry",        3600U, 5U, { 0x11, 0x00, 0x00, 0x0E, 0x10 } },
    /* MQTT 5.0 s3.14.2.2.2: a client that connected with a Session Expiry of
     * zero may not set a non-zero one on the way out. */
    { "session-expiry-after-zero", 0U, 5U, { 0x11, 0x00, 0x00, 0x0E, 0x10 } },
    { "zero-expiry-after-zero", 0U, 5U, { 0x11, 0x00, 0x00, 0x00, 0x00 } },
    { "reason-string",         3600U, 6U, { 0x1F, 0x00, 0x03, 'b', 'y', 'e' } },
    { "user-property",         3600U, 7U, { 0x26, 0x00, 0x01, 'k', 0x00, 0x01, 'v' } },
    /* Server Reference is an INCOMING disconnect property and is refused here. */
    { "server-reference",      3600U, 6U, { 0x1C, 0x00, 0x03, 'a', '.', 'b' } },
    { "unknown-property",      3600U, 2U, { 0x7F, 0x00 } },
    { "truncated-string",      3600U, 4U, { 0x1F, 0x00, 0x09, 'x' } },
};

#define N_VAL    ( sizeof( VAL_CASES ) / sizeof( VAL_CASES[ 0 ] ) )

static MQTTStatus_t run_validate( uint32_t connectSessionExpiry,
                                  const uint8_t *properties,
                                  size_t propertyLength,
                                  bool print )
{
    uint8_t propBytes[ MAX_BODY ];
    MQTTPropBuilder_t props;
    MQTTStatus_t status;

    memset( propBytes, 0, sizeof propBytes );
    memcpy( propBytes, properties, propertyLength );

    memset( &props, 0, sizeof props );
    props.pBuffer = propBytes;
    props.bufferLength = sizeof propBytes;
    props.currentIndex = propertyLength;

    status = MQTT_ValidateDisconnectProperties( connectSessionExpiry, &props );

    if( print )
    {
        printf( " -> %s", status_name( status ) );
    }

    return status;
}

/* ---- the sweeps ---------------------------------------------------------- */

/* Every reason-code byte, in each direction. The same table answers
 * differently, which is the whole point of this slice. */
static void sweep_reason_codes( const char *label, bool incoming )
{
    unsigned value;
    unsigned accepted = 0U;
    unsigned rejected = 0U;
    int first = 1;

    printf( "reason-sweep %s accepted=", label );

    for( value = 0U; value < 256U; value++ )
    {
        MQTTStatus_t status;

        if( incoming )
        {
            uint8_t body[ 1 ];
            body[ 0 ] = ( uint8_t ) value;
            status = run_incoming( 1U, body, 1U, 1024U, false );
        }
        else
        {
            MQTTSuccessFailReasonCode_t reason = ( MQTTSuccessFailReasonCode_t ) value;
            uint32_t remaining = 0U;
            uint32_t size = 0U;
            status = MQTT_GetDisconnectPacketSize( NULL, &remaining, &size,
                                                   1024U, &reason );
        }

        if( status == MQTTSuccess )
        {
            printf( "%s%02x", first ? "" : ",", value );
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

/* Every property-identifier byte, at one value shape, in one direction. */
static void sweep_properties( const char *direction,
                              const char *label,
                              const uint8_t *pValue,
                              size_t valueLength,
                              bool incoming )
{
    unsigned id;
    unsigned accepted = 0U;
    unsigned rejected = 0U;
    int first = 1;
    size_t sectionLength = valueLength + 1U;

    printf( "prop-sweep %s %s accepted=", direction, label );

    for( id = 0U; id < 256U; id++ )
    {
        uint8_t section[ MAX_BODY ];
        MQTTStatus_t status;

        memset( section, 0, sizeof section );
        section[ 0 ] = ( uint8_t ) id;
        memcpy( &section[ 1 ], pValue, valueLength );

        if( incoming )
        {
            /* reason code, property length, then the section. */
            uint8_t body[ MAX_BODY ];
            size_t bodyLength = sectionLength + 2U;

            memset( body, 0, sizeof body );
            body[ 0 ] = 0x00;
            body[ 1 ] = ( uint8_t ) sectionLength;
            memcpy( &body[ 2 ], section, sectionLength );

            status = run_incoming( ( uint32_t ) bodyLength, body, bodyLength,
                                   1024U, false );
        }
        else
        {
            status = run_validate( 3600U, section, sectionLength, false );
        }

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

    static const uint8_t ONE_BYTE[] = { 0x00 };
    static const uint8_t TWO_BYTE[] = { 0x00, 0x01 };
    static const uint8_t FOUR_BYTE[] = { 0x00, 0x00, 0x00, 0x01 };
    static const uint8_t STRING[] = { 0x00, 0x01, 'x' };
    static const uint8_t USER_PROP[] = { 0x00, 0x01, 'k', 0x00, 0x01, 'v' };

    printf( "geometry incoming=%u outgoing=%u validate=%u\n",
            ( unsigned ) N_IN, ( unsigned ) N_OUT, ( unsigned ) N_VAL );

    for( i = 0; i < N_IN; i++ )
    {
        const IncomingCase_t *c = &IN_CASES[ i ];

        if( ( size_t ) c->remainingLength != c->bodyLength )
        {
            fprintf( stderr, "case %s claims %u bytes and carries %u\n",
                     c->name, ( unsigned ) c->remainingLength,
                     ( unsigned ) c->bodyLength );
            exit( 1 );
        }

        printf( "in %u %s rl=%u max=%u in=", ( unsigned ) i, c->name,
                ( unsigned ) c->remainingLength, ( unsigned ) c->maxPacketSize );
        put_hex( c->body, c->bodyLength );
        ( void ) run_incoming( c->remainingLength, c->body, c->bodyLength,
                               c->maxPacketSize, true );
        printf( "\n" );
    }

    for( i = 0; i < N_OUT; i++ )
    {
        run_outgoing( i );
    }

    for( i = 0; i < N_VAL; i++ )
    {
        const ValidateCase_t *c = &VAL_CASES[ i ];

        printf( "val %u %s connexp=%u props=", ( unsigned ) i, c->name,
                ( unsigned ) c->connectSessionExpiry );
        put_hex( c->properties, c->propertyLength );
        ( void ) run_validate( c->connectSessionExpiry, c->properties,
                               c->propertyLength, true );
        printf( "\n" );
    }

    sweep_reason_codes( "incoming", true );
    sweep_reason_codes( "outgoing", false );

    sweep_properties( "incoming", "one-byte", ONE_BYTE, sizeof ONE_BYTE, true );
    sweep_properties( "incoming", "two-byte", TWO_BYTE, sizeof TWO_BYTE, true );
    sweep_properties( "incoming", "four-byte", FOUR_BYTE, sizeof FOUR_BYTE, true );
    sweep_properties( "incoming", "string", STRING, sizeof STRING, true );
    sweep_properties( "incoming", "user-property", USER_PROP, sizeof USER_PROP, true );

    sweep_properties( "outgoing", "one-byte", ONE_BYTE, sizeof ONE_BYTE, false );
    sweep_properties( "outgoing", "two-byte", TWO_BYTE, sizeof TWO_BYTE, false );
    sweep_properties( "outgoing", "four-byte", FOUR_BYTE, sizeof FOUR_BYTE, false );
    sweep_properties( "outgoing", "string", STRING, sizeof STRING, false );
    sweep_properties( "outgoing", "user-property", USER_PROP, sizeof USER_PROP, false );

    printf( "end\n" );
    return 0;
}
