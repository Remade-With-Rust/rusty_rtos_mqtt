/* The C arm of K7's coreMQTT TOPIC-MATCHING differential.
 *
 * `core_mqtt.c` is compiled VERBATIM out of the pinned checkout; nothing here
 * copies or edits it.
 *
 * # The first slice of the connection machine, and the one that needs no
 * connection
 *
 * `core_mqtt.c` is 5,618 lines of state machine, and almost all of it needs a
 * transport. Four things in it do not: the topic matcher, the two functions
 * that pull reason codes out of a SUBACK or an UNSUBACK, and the two tables
 * that turn a status or a packet type into a string.
 *
 * # Matching is a string algorithm, so the sweep is exhaustive
 *
 * Every other sweep in this package walks one byte over its 256 values. This
 * one walks two STRINGS over an alphabet, which is the same idea one dimension
 * up: `{a, /, +}` to length three is 39 strings, and the full 39 x 39 matrix of
 * which filter matches which topic is printed as a grid. A grid is to a string
 * algorithm what a printed accepted set is to a table -- a divergence is
 * legible, not merely detectable.
 *
 * Then the same over `{a, b, /, +, #, $}` to length four: 1,554 strings each
 * way, 2,414,916 calls, digested. The wildcards and the `$` rule are both in
 * that alphabet, so the digest covers every interaction between them at that
 * length.
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core_mqtt.h"

static const char * status_name( MQTTStatus_t status )
{
    switch( status )
    {
        case MQTTSuccess:      return "Success";
        case MQTTBadParameter: return "BadParameter";
        default:               return "OTHER";
    }
}

/* The house digest: FNV-1a's multiplier, and a seed one digit short of its
 * offset basis, as every driver in this package uses. */
static uint64_t fnv( uint64_t state, uint64_t value )
{
    return ( state ^ value ) * 1099511628211ULL;
}

#define FNV_SEED    1469598103934665603ULL

/* ---- the grid -------------------------------------------------------------- */

/* Every string of length 1..maxLength over `alphabet`, in order. Returns how
 * many were written. */
static size_t enumerate( const char *alphabet,
                         size_t symbols,
                         size_t maxLength,
                         char out[][ 8 ],
                         size_t capacity )
{
    size_t count = 0U;
    size_t length;

    for( length = 1U; length <= maxLength; length++ )
    {
        size_t total = 1U;
        size_t i;

        for( i = 0U; i < length; i++ )
        {
            total *= symbols;
        }

        for( i = 0U; i < total; i++ )
        {
            size_t value = i;
            size_t position;

            if( count >= capacity )
            {
                return count;
            }

            for( position = 0U; position < length; position++ )
            {
                out[ count ][ length - 1U - position ] = alphabet[ value % symbols ];
                value /= symbols;
            }

            out[ count ][ length ] = '\0';
            count++;
        }
    }

    return count;
}

static bool matches( const char *topic, const char *filter )
{
    bool result = false;
    MQTTStatus_t status = MQTT_MatchTopic( topic, strlen( topic ),
                                           filter, strlen( filter ), &result );

    return ( status == MQTTSuccess ) && result;
}

/* The full matrix over a small alphabet, printed. One row per FILTER, one
 * column per TOPIC, in the same order both ways. */
static void print_grid( void )
{
    static char strings[ 64 ][ 8 ];
    size_t n = enumerate( "a/+", 3U, 3U, strings, 64U );
    size_t f, t;

    printf( "grid alphabet=a/+ maxlen=3 n=%u\n", ( unsigned ) n );

    /* The strings themselves, so a row can be read without regenerating
     * them -- the trace carries its own inputs. */
    printf( "grid-strings" );

    for( f = 0U; f < n; f++ )
    {
        printf( " %s", strings[ f ] );
    }

    printf( "\n" );

    for( f = 0U; f < n; f++ )
    {
        printf( "grid-row %s ", strings[ f ] );

        for( t = 0U; t < n; t++ )
        {
            printf( "%c", matches( strings[ t ], strings[ f ] ) ? '1' : '.' );
        }

        printf( "\n" );
    }
}

/* The same idea over the whole interesting alphabet, digested. */
static void sweep_all( void )
{
    static char strings[ 1600 ][ 8 ];
    size_t n = enumerate( "ab/+#$", 6U, 4U, strings, 1600U );
    size_t f, t;
    uint64_t digest = FNV_SEED;
    unsigned long long hits = 0U;

    for( f = 0U; f < n; f++ )
    {
        for( t = 0U; t < n; t++ )
        {
            bool hit = matches( strings[ t ], strings[ f ] );

            digest = fnv( digest, hit ? 1U : 0U );

            if( hit )
            {
                hits++;
            }
        }
    }

    printf( "sweep alphabet=ab/+#$ maxlen=4 n=%u calls=%llu matches=%llu digest=%016llx\n",
            ( unsigned ) n, ( unsigned long long ) n * ( unsigned long long ) n,
            hits, ( unsigned long long ) digest );
}

/* ---- the named cases, which are the specification's own examples ----------- */

typedef struct
{
    const char *topic;
    const char *filter;
} MatchCase_t;

static const MatchCase_t CASES[] = {
    /* MQTT 5.0 section 4.7.1.2, the '#' wildcard. */
    { "sport/tennis/player1",        "sport/tennis/player1/#" },
    { "sport/tennis/player1/ranking", "sport/tennis/player1/#" },
    { "sport",                       "sport/#" },
    { "sport/",                      "sport/#" },
    { "sport/tennis",                "#" },
    { "sport",                       "#" },

    /* Section 4.7.1.3, the '+' wildcard. */
    { "sport/tennis/player1",        "sport/tennis/+" },
    { "sport/tennis/player1/ranking", "sport/tennis/+" },
    { "sport/",                      "sport/+" },
    { "sport",                       "sport/+" },
    { "/finance",                    "+/+" },
    { "/finance",                    "/+" },
    { "/finance",                    "+" },

    /* Section 4.7.2: a topic beginning with '$' is not matched by a filter
     * beginning with a wildcard. */
    { "$SYS/broker",                 "#" },
    { "$SYS/broker",                 "+/broker" },
    { "$SYS/broker",                 "$SYS/#" },
    { "$SYS/broker",                 "$SYS/+" },
    { "a$b",                         "#" },

    /* Exact matches, and near misses. */
    { "sport",                       "sport" },
    { "sport",                       "sports" },
    { "sports",                      "sport" },
    { "sport/tennis",                "sport/tennis" },

    /* A wildcard where MQTT does not allow one: the C matches character by
     * character, so these say what it actually does. */
    { "sport+",                      "sport+" },
    { "sportx",                      "sport+" },
    { "sport#",                      "sport#" },
    { "sportx",                      "sport#" },
    { "a/b",                         "a+/b" },
    { "a/b",                         "+a/b" },

    /* Empty levels, which MQTT allows: 4.7.3 says a topic of just "/" is
     * valid, and 4.7.1.3 says '+' matches a single level -- including an empty
     * one. These are the cases that characterise where this matcher stops
     * agreeing with that. */
    { "/",                           "/" },
    { "/",                           "+/+" },
    { "//",                          "+/+/+" },
    { "a//b",                        "a/+/b" },
    { "a//b",                        "a/#" },
    /* The trailing '+' against a topic that ENDS at a separator. */
    { "a/",                          "a/+" },
    { "a/",                          "+/+" },
    { "/a",                          "+/+" },
    { "/",                           "+/" },
    { "/",                           "/+" },
    { "//",                          "+/+/" },
    { "a//",                         "a/+/+" },
    { "/",                           "#" },
    { "/",                           "/#" },

    /* '#' not in the last position. */
    { "a/b",                         "#/b" },
    { "a/b",                         "a/#/b" },

    /* Several wildcards. */
    { "a/b/c",                       "+/+/+" },
    { "a/b/c",                       "+/b/+" },
    { "a/b/c",                       "a/+/#" },
    { "a/b/c/d",                     "a/+/#" },
};

#define N_CASES    ( sizeof( CASES ) / sizeof( CASES[ 0 ] ) )

/* The parameter refusals, which are a different answer from "no match". */
static void print_refusals( void )
{
    bool result = false;
    static const char TOPIC[] = "a/b";

    printf( "refusal empty-topic -> %s\n",
            status_name( MQTT_MatchTopic( TOPIC, 0U, TOPIC, 3U, &result ) ) );
    printf( "refusal empty-filter -> %s\n",
            status_name( MQTT_MatchTopic( TOPIC, 3U, TOPIC, 0U, &result ) ) );
    printf( "refusal huge-topic -> %s\n",
            status_name( MQTT_MatchTopic( TOPIC, 65536U, TOPIC, 3U, &result ) ) );
    printf( "refusal huge-filter -> %s\n",
            status_name( MQTT_MatchTopic( TOPIC, 3U, TOPIC, 65536U, &result ) ) );
}

/* ---- the reason codes in a SUBACK and an UNSUBACK -------------------------- */

static void print_ack_codes( void )
{
    static const struct
    {
        const char *name;
        uint8_t     type;
        size_t      length;
        uint8_t     bytes[ 140 ];
        bool        suback;
    } ACKS[] = {
        { "suback-one-code",   MQTT_PACKET_TYPE_SUBACK, 4, { 0x00, 0x01, 0x00, 0x01 }, true },
        { "suback-three",      MQTT_PACKET_TYPE_SUBACK, 6,
          { 0x00, 0x01, 0x00, 0x00, 0x01, 0x02 }, true },
        { "suback-failure",    MQTT_PACKET_TYPE_SUBACK, 4, { 0x00, 0x01, 0x00, 0x80 }, true },
        { "suback-no-codes",   MQTT_PACKET_TYPE_SUBACK, 3, { 0x00, 0x01, 0x00 }, true },
        { "suback-with-props", MQTT_PACKET_TYPE_SUBACK, 8,
          { 0x00, 0x01, 0x04, 0x1F, 0x00, 0x01, 'x', 0x01 }, true },
        { "unsuback-one-code", MQTT_PACKET_TYPE_UNSUBACK, 4, { 0x00, 0x01, 0x00, 0x00 }, false },
        { "unsuback-no-sub",   MQTT_PACKET_TYPE_UNSUBACK, 4, { 0x00, 0x01, 0x00, 0x11 }, false },
        { "unsuback-two",      MQTT_PACKET_TYPE_UNSUBACK, 5,
          { 0x00, 0x01, 0x00, 0x00, 0x11 }, false },
        /* The wrong packet type for each getter. */
        /* A property section of 130 bytes, so its length needs TWO bytes to
         * encode. Nothing else in this trace varies that width, and a caller
         * that skipped the value without skipping the bytes that encoded it
         * would pass every one-byte case. */
        { "suback-two-byte-property-length", MQTT_PACKET_TYPE_SUBACK, 135,
          { 0x00, 0x01, 0x82, 0x01, 0x1F, 0x00, 0x7F }, true },
        { "suback-getter-on-unsuback", MQTT_PACKET_TYPE_UNSUBACK, 4,
          { 0x00, 0x01, 0x00, 0x00 }, true },
        { "unsuback-getter-on-suback", MQTT_PACKET_TYPE_SUBACK, 4,
          { 0x00, 0x01, 0x00, 0x01 }, false },
    };

    size_t i;

    for( i = 0U; i < sizeof( ACKS ) / sizeof( ACKS[ 0 ] ); i++ )
    {
        uint8_t bytes[ 140 ];
        MQTTPacketInfo_t packet;
        uint8_t *codes = NULL;
        size_t count = 0xAAAAU;
        MQTTStatus_t status;
        size_t k;

        memcpy( bytes, ACKS[ i ].bytes, sizeof bytes );

        memset( &packet, 0, sizeof packet );
        packet.type = ACKS[ i ].type;
        packet.pRemainingData = bytes;
        packet.remainingLength = ( uint32_t ) ACKS[ i ].length;
        packet.headerLength = 2U;

        status = ACKS[ i ].suback
                 ? MQTT_GetSubAckStatusCodes( &packet, &codes, &count )
                 : MQTT_GetUnsubAckStatusCodes( &packet, &codes, &count );

        printf( "ack %s which=%s -> %s", ACKS[ i ].name,
                ACKS[ i ].suback ? "suback" : "unsuback", status_name( status ) );

        if( status == MQTTSuccess )
        {
            printf( " n=%u codes=", ( unsigned ) count );

            for( k = 0U; k < count; k++ )
            {
                printf( "%02x", codes[ k ] );
            }
        }

        printf( "\n" );
    }
}

/* ---- the two tables that name things --------------------------------------- */

static void print_names( void )
{
    unsigned value;
    uint64_t digest = FNV_SEED;

    /* Every status the enumeration defines, and a few past the end. */
    printf( "status" );

    for( value = 0U; value < 20U; value++ )
    {
        printf( " %u=%s", value, MQTT_Status_strerror( ( MQTTStatus_t ) value ) );
    }

    printf( "\n" );

    /* Every packet type byte. The named ones are printed; all 256 are
     * digested, because the function masks and a mask is a table. */
    printf( "types" );

    for( value = 0U; value < 256U; value++ )
    {
        const char *name = MQTT_GetPacketTypeString( ( uint8_t ) value );
        size_t k;

        for( k = 0U; name[ k ] != '\0'; k++ )
        {
            digest = fnv( digest, ( uint64_t ) ( unsigned char ) name[ k ] );
        }

        digest = fnv( digest, 0U );

        /* One representative of each high nibble, plus the three types whose
         * reserved low bits are part of the byte -- PUBREL, SUBSCRIBE and
         * UNSUBSCRIBE -- because printing only the round numbers would say
         * "UNKNOWN" for three packets that have names. */
        if( ( ( value & 0x0FU ) == 0U ) || ( value == 0x62U ) ||
            ( value == 0x82U ) || ( value == 0xA2U ) )
        {
            printf( " %02x=%s", value, name );
        }
    }

    printf( " digest=%016llx\n", ( unsigned long long ) digest );
}

int main( void )
{
    size_t i;

    printf( "geometry cases=%u\n", ( unsigned ) N_CASES );

    print_grid();
    sweep_all();

    for( i = 0U; i < N_CASES; i++ )
    {
        bool result = false;
        MQTTStatus_t status = MQTT_MatchTopic( CASES[ i ].topic,
                                               strlen( CASES[ i ].topic ),
                                               CASES[ i ].filter,
                                               strlen( CASES[ i ].filter ),
                                               &result );

        printf( "match %u topic=%s filter=%s -> %s %s\n", ( unsigned ) i,
                CASES[ i ].topic, CASES[ i ].filter, status_name( status ),
                result ? "yes" : "no" );
    }

    print_refusals();
    print_ack_codes();
    print_names();

    printf( "end\n" );
    return 0;
}
