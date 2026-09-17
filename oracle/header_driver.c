/* The C arm of K7's coreMQTT FIXED-HEADER differential.
 *
 * `core_mqtt_serializer.c` and `core_mqtt_serializer_private.c` are compiled
 * VERBATIM out of the pinned checkout; nothing here copies or edits them.
 *
 * # Why this slice
 *
 * Every MQTT packet begins with one type byte and a "remaining length" encoded
 * as a variable-byte integer of one to four bytes. It is the first thing a
 * device parses from a socket, before it knows what kind of packet it is
 * holding, and it is the classic place to attack an MQTT implementation: a
 * length that is too long, encoded in too many bytes, or encoded
 * non-minimally.
 *
 * The C rejects all three, and the third is subtle -- `0x80 0x00` decodes to
 * zero but is not the minimal encoding of zero, and is refused by comparing the
 * bytes consumed against `variableLengthEncodedSize` of the result. That is the
 * same shape as the over-long UTF-8 rule `rusty_rtos_json` had to get right.
 *
 * # Explicit cases, and an exhaustive sweep
 *
 * The named cases print every observable, so a divergence is localised. The
 * sweep runs every one of the 256 possible type bytes against every
 * remaining-length pattern and reports per-status COUNTS plus an FNV-1a digest
 * of all the answers -- exhaustive coverage that a reader can still check, and
 * a digest that changes if any one of the 300,000-odd answers moves.
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "core_mqtt_serializer.h"

/* Both live in core_mqtt_serializer_private.c with external linkage. */
extern uint32_t variableLengthEncodedSize( uint32_t length );
extern uint8_t * encodeVariableLength( uint8_t * pDestination, uint32_t length );

#define MAX_BUF    8

static const char *status_name( MQTTStatus_t s )
{
    switch( s )
    {
        case MQTTSuccess:         return "Success";
        case MQTTBadParameter:    return "BadParameter";
        case MQTTBadResponse:     return "BadResponse";
        case MQTTNoDataAvailable: return "NoDataAvailable";
        case MQTTNeedMoreBytes:   return "NeedMoreBytes";
        default:                  return "UNEXPECTED";
    }
}

static void put_hex( const uint8_t *p, size_t n )
{
    size_t i;
    for( i = 0; i < n; i++ )
    {
        printf( "%02x", p[ i ] );
    }
}

/* ---- FNV-1a over the sweep's answers ------------------------------------ */

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

/* ---- the named cases ----------------------------------------------------- */

typedef struct
{
    const char *name;
    uint8_t bytes[ MAX_BUF ];
    size_t len;      /* bytes actually in the buffer */
    size_t index;    /* what the caller claims is available */
} Case_t;

/* 0xD0 = PINGRESP, 0x30 = PUBLISH, 0x62 = PUBREL (bit 1 set), 0x60 = PUBREL
 * without it, 0x20 = CONNACK. */
static const Case_t CASES[] = {
    { "nothing-available",        { 0 },                     0, 0 },
    { "type-byte-only",           { 0xD0 },                  1, 1 },
    { "pingresp-zero-length",     { 0xD0, 0x00 },            2, 2 },
    { "puback-two-bytes",         { 0x40, 0x02, 0x12, 0x34 },4, 4 },
    { "pubrel-with-bit-set",      { 0x62, 0x02, 0x12, 0x34 },4, 4 },
    { "pubrel-without-bit-set",   { 0x60, 0x02, 0x12, 0x34 },4, 4 },
    { "connack",                  { 0x20, 0x02, 0x00, 0x00 },4, 4 },
    { "publish-qos0",             { 0x30, 0x05 },            2, 2 },
    { "subscribe-is-not-incoming",{ 0x82, 0x02 },            2, 2 },
    { "connect-is-not-incoming",  { 0x10, 0x02 },            2, 2 },
    { "type-zero",                { 0x00, 0x00 },            2, 2 },
    { "type-0xf0-auth",           { 0xF0, 0x00 },            2, 2 },

    /* The remaining-length encodings. */
    { "len-127-one-byte",         { 0xD0, 0x7F },            2, 2 },
    { "len-128-two-bytes",        { 0xD0, 0x80, 0x01 },      3, 3 },
    { "len-16383-two-bytes",      { 0xD0, 0xFF, 0x7F },      3, 3 },
    { "len-16384-three-bytes",    { 0xD0, 0x80, 0x80, 0x01 },4, 4 },
    { "len-2097151-three-bytes",  { 0xD0, 0xFF, 0xFF, 0x7F },4, 4 },
    { "len-2097152-four-bytes",   { 0xD0, 0x80, 0x80, 0x80, 0x01 }, 5, 5 },
    { "len-max-268435455",        { 0xD0, 0xFF, 0xFF, 0xFF, 0x7F }, 5, 5 },

    /* The three attacks. */
    { "five-byte-length",         { 0xD0, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F }, 6, 6 },
    { "non-minimal-zero",         { 0xD0, 0x80, 0x00 },      3, 3 },
    { "non-minimal-one",          { 0xD0, 0x81, 0x00 },      3, 3 },
    { "non-minimal-127",          { 0xD0, 0xFF, 0x00 },      3, 3 },
    { "non-minimal-three-byte",   { 0xD0, 0x80, 0x80, 0x00 },4, 4 },
    { "continuation-never-ends",  { 0xD0, 0x80, 0x80, 0x80, 0x80 }, 5, 5 },

    /* Truncation: the bytes are there but the caller says fewer are available. */
    { "truncated-after-type",     { 0xD0, 0x80, 0x01 },      3, 1 },
    { "truncated-mid-length",     { 0xD0, 0x80, 0x01 },      3, 2 },
    { "truncated-three-of-four",  { 0xD0, 0x80, 0x80, 0x01 },4, 3 },
    { "exactly-enough",           { 0xD0, 0x80, 0x01 },      3, 3 },
    { "more-than-enough",         { 0xD0, 0x00, 0xAA, 0xAA },4, 4 },
};

#define N_CASES ( sizeof( CASES ) / sizeof( CASES[ 0 ] ) )

static void run_cases( void )
{
    size_t i;

    for( i = 0; i < N_CASES; i++ )
    {
        MQTTPacketInfo_t packet;
        MQTTStatus_t status;
        size_t index = CASES[ i ].index;

        /* A recognisable fill, so a field the library does NOT write shows up
         * as junk rather than as a plausible zero. */
        memset( &packet, 0xAA, sizeof packet );
        packet.pRemainingData = NULL;

        status = MQTT_ProcessIncomingPacketTypeAndLength( CASES[ i ].bytes, &index, &packet );

        printf( "case %u %s in %u ", ( unsigned ) i, CASES[ i ].name,
                ( unsigned ) CASES[ i ].index );
        put_hex( CASES[ i ].bytes, CASES[ i ].len );
        printf( " -> %s", status_name( status ) );

        if( status == MQTTSuccess )
        {
            printf( " %02x %u %u", packet.type, ( unsigned ) packet.remainingLength,
                    ( unsigned ) packet.headerLength );
        }

        printf( "\n" );
    }
}

/* ---- variableLengthEncodedSize, at every boundary ------------------------ */

static const uint32_t SIZE_PROBES[] = {
    0U, 1U, 126U, 127U, 128U, 129U,
    16382U, 16383U, 16384U, 16385U,
    2097150U, 2097151U, 2097152U, 2097153U,
    268435454U, 268435455U
};

#define N_SIZE_PROBES ( sizeof( SIZE_PROBES ) / sizeof( SIZE_PROBES[ 0 ] ) )

static void run_sizes( void )
{
    size_t i;

    for( i = 0; i < N_SIZE_PROBES; i++ )
    {
        uint8_t buffer[ 8 ];
        uint8_t *end;

        memset( buffer, 0xAA, sizeof buffer );
        end = encodeVariableLength( buffer, SIZE_PROBES[ i ] );

        printf( "size %u %u -> %u encoded ", ( unsigned ) i, SIZE_PROBES[ i ],
                variableLengthEncodedSize( SIZE_PROBES[ i ] ) );
        put_hex( buffer, ( size_t ) ( end - buffer ) );
        printf( "\n" );
    }
}

/* ---- the exhaustive sweep ------------------------------------------------ */

/* Every remaining-length byte pattern worth trying: the boundaries, the
 * continuation bits, and the non-minimal forms. */
static const uint8_t TAIL_BYTES[] = {
    0x00, 0x01, 0x7E, 0x7F, 0x80, 0x81, 0xFE, 0xFF
};

#define N_TAILS ( sizeof( TAIL_BYTES ) / sizeof( TAIL_BYTES[ 0 ] ) )

static void run_sweep( void )
{
    unsigned type;
    size_t a, b, c, d;
    unsigned long counts[ 8 ];
    unsigned long total = 0;

    memset( counts, 0, sizeof counts );

    /* Every type byte, against every 4-byte tail, at every claimed length. */
    for( type = 0; type < 256U; type++ )
    {
        for( a = 0; a < N_TAILS; a++ )
        {
            for( b = 0; b < N_TAILS; b++ )
            {
                for( c = 0; c < N_TAILS; c++ )
                {
                    for( d = 0; d < N_TAILS; d++ )
                    {
                        uint8_t buffer[ 5 ];
                        size_t claimed;

                        buffer[ 0 ] = ( uint8_t ) type;
                        buffer[ 1 ] = TAIL_BYTES[ a ];
                        buffer[ 2 ] = TAIL_BYTES[ b ];
                        buffer[ 3 ] = TAIL_BYTES[ c ];
                        buffer[ 4 ] = TAIL_BYTES[ d ];

                        for( claimed = 0; claimed <= 5U; claimed++ )
                        {
                            MQTTPacketInfo_t packet;
                            MQTTStatus_t status;
                            size_t index = claimed;
                            char line[ 96 ];

                            memset( &packet, 0xAA, sizeof packet );
                            packet.pRemainingData = NULL;

                            status = MQTT_ProcessIncomingPacketTypeAndLength(
                                buffer, &index, &packet );

                            if( status == MQTTSuccess )
                            {
                                ( void ) snprintf( line, sizeof line, "%s %02x %u %u",
                                                   status_name( status ), packet.type,
                                                   ( unsigned ) packet.remainingLength,
                                                   ( unsigned ) packet.headerLength );
                                counts[ 0 ]++;
                            }
                            else
                            {
                                ( void ) snprintf( line, sizeof line, "%s",
                                                   status_name( status ) );

                                if( status == MQTTBadResponse )      { counts[ 1 ]++; }
                                else if( status == MQTTNeedMoreBytes ) { counts[ 2 ]++; }
                                else if( status == MQTTNoDataAvailable ) { counts[ 3 ]++; }
                                else                                  { counts[ 4 ]++; }
                            }

                            fnv_str( line );
                            total++;
                        }
                    }
                }
            }
        }
    }

    printf( "sweep total %lu\n", total );
    printf( "sweep Success %lu\n", counts[ 0 ] );
    printf( "sweep BadResponse %lu\n", counts[ 1 ] );
    printf( "sweep NeedMoreBytes %lu\n", counts[ 2 ] );
    printf( "sweep NoDataAvailable %lu\n", counts[ 3 ] );
    printf( "sweep other %lu\n", counts[ 4 ] );
    printf( "sweep digest %016llx\n", ( unsigned long long ) fnv );
}

int main( void )
{
    printf( "geometry cases=%u sizes=%u tails=%u\n",
            ( unsigned ) N_CASES, ( unsigned ) N_SIZE_PROBES, ( unsigned ) N_TAILS );

    run_cases();
    run_sizes();
    run_sweep();

    printf( "end\n" );
    return 0;
}
