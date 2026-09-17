/* The C arm of K7's coreMQTT PROPERTY-PRIMITIVE differential.
 *
 * `core_mqtt_serializer_private.c` is compiled VERBATIM out of the pinned
 * checkout; nothing here copies or edits it.
 *
 * # Why this slice
 *
 * MQTT 5 adds properties to almost every packet, and every one of them is
 * decoded through the same five primitives: a one-, two- or four-byte integer,
 * a length-prefixed UTF-8 string, or a user property (two strings). They carry
 * two rules between them, and both are protocol requirements rather than
 * conveniences:
 *
 *   1. a property may appear **once**, and a repeat is a protocol error;
 *   2. every read is bounded by the **property length**, which is itself
 *      decoded from the packet.
 *
 * The second is the length-prefix attack surface: a string that claims more
 * bytes than the property has left must be refused, and the refusal has to
 * happen before the read rather than after it.
 *
 * `decodeVariableLength` is here too, and it is worth having beside
 * `processRemainingLength` from the fixed-header differential: the library has
 * TWO variable-length decoders with different bounds handling, and a
 * transcription that reuses one for the other would be wrong in a way no
 * single-function test would show.
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "core_mqtt_serializer.h"

/* All declared in the library's private header; external linkage. */
extern MQTTStatus_t decodeUint8t( uint8_t * pProperty, uint32_t * pPropertyLength,
                                  bool * pUsed, uint8_t ** pIndex );
extern MQTTStatus_t decodeUint16t( uint16_t * pProperty, uint32_t * pPropertyLength,
                                   bool * pUsed, uint8_t ** pIndex );
extern MQTTStatus_t decodeUint32t( uint32_t * pProperty, uint32_t * pPropertyLength,
                                   bool * pUsed, uint8_t ** pIndex );
extern MQTTStatus_t decodeUtf8( const char ** pProperty, size_t * pLength,
                                uint32_t * pPropertyLength, bool * pUsed,
                                uint8_t ** pIndex );
extern MQTTStatus_t decodeUserProp( const char ** pPropertyKey, size_t * pPropertyKeyLen,
                                    const char ** pPropertyValue, size_t * pPropertyValueLen,
                                    uint32_t * pPropertyLength, uint8_t ** pIndex );
extern MQTTStatus_t decodeVariableLength( const uint8_t * pBuffer, size_t bufferLength,
                                          uint32_t * pLength );
extern uint8_t * encodeString( uint8_t * pDestination, const char * pSource,
                               uint16_t sourceLength );

#define MAX_BUF    64
#define MAX_OPS    12

static const char *status_name( MQTTStatus_t s )
{
    switch( s )
    {
        case MQTTSuccess:      return "Success";
        case MQTTBadResponse:  return "BadResponse";
        case MQTTBadParameter: return "BadParameter";
        default:               return "UNEXPECTED";
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

/* ---- the scripted decode sequences -------------------------------------- */

typedef struct
{
    char kind;     /* 1 2 4 = uintN, s = utf8, u = user property */
    uint8_t used;  /* the caller's "already seen this property" flag */
} Op_t;

typedef struct
{
    const char *name;
    uint8_t buffer[ MAX_BUF ];
    size_t bufferLen;
    uint32_t propertyLength;
    Op_t ops[ MAX_OPS ];
    size_t opCount;
} Scenario_t;

static void print_cfg( const Scenario_t *sc )
{
    size_t i;

    printf( "cfg buffer %u ", ( unsigned ) sc->bufferLen );
    put_hex( sc->buffer, sc->bufferLen );
    printf( "\n" );
    printf( "cfg proplen %u\n", sc->propertyLength );
    printf( "cfg ops %u", ( unsigned ) sc->opCount );

    for( i = 0; i < sc->opCount; i++ )
    {
        printf( " %c:%u", sc->ops[ i ].kind, sc->ops[ i ].used );
    }

    printf( "\n" );
}

static void run_scenario( size_t id, const Scenario_t *sc )
{
    static uint8_t buffer[ MAX_BUF ];
    uint8_t *index;
    uint32_t propertyLength = sc->propertyLength;
    size_t i;

    printf( "scenario %u %s\n", ( unsigned ) id, sc->name );
    print_cfg( sc );

    memcpy( buffer, sc->buffer, MAX_BUF );
    index = buffer;

    for( i = 0; i < sc->opCount; i++ )
    {
        bool used = ( sc->ops[ i ].used != 0U );
        MQTTStatus_t status = MQTTSuccess;
        uint8_t v8 = 0xAAU;
        uint16_t v16 = 0xAAAAU;
        uint32_t v32 = 0xAAAAAAAAU;
        const char *pStr = NULL, *pKey = NULL, *pVal = NULL;
        size_t strLen = 0, keyLen = 0, valLen = 0;

        printf( "op %u %c used=%u -> ", ( unsigned ) i, sc->ops[ i ].kind,
                sc->ops[ i ].used );

        switch( sc->ops[ i ].kind )
        {
            case '1':
                status = decodeUint8t( &v8, &propertyLength, &used, &index );
                printf( "%s", status_name( status ) );
                if( status == MQTTSuccess ) { printf( " %02x", v8 ); }
                break;

            case '2':
                status = decodeUint16t( &v16, &propertyLength, &used, &index );
                printf( "%s", status_name( status ) );
                if( status == MQTTSuccess ) { printf( " %04x", v16 ); }
                break;

            case '4':
                status = decodeUint32t( &v32, &propertyLength, &used, &index );
                printf( "%s", status_name( status ) );
                if( status == MQTTSuccess ) { printf( " %08x", v32 ); }
                break;

            case 's':
                status = decodeUtf8( &pStr, &strLen, &propertyLength, &used, &index );
                printf( "%s", status_name( status ) );
                if( status == MQTTSuccess )
                {
                    printf( " %u %u", ( unsigned ) ( ( const uint8_t * ) pStr - buffer ),
                            ( unsigned ) strLen );
                }
                break;

            case 'u':
                status = decodeUserProp( &pKey, &keyLen, &pVal, &valLen,
                                         &propertyLength, &index );
                printf( "%s", status_name( status ) );
                if( status == MQTTSuccess )
                {
                    printf( " %u %u %u %u",
                            ( unsigned ) ( ( const uint8_t * ) pKey - buffer ),
                            ( unsigned ) keyLen,
                            ( unsigned ) ( ( const uint8_t * ) pVal - buffer ),
                            ( unsigned ) valLen );
                }
                break;

            default:
                printf( "UNEXPECTED" );
                break;
        }

        /* Where the cursor and the property budget were left is as much of the
         * answer as the status: the next decode starts from there. */
        printf( " at=%u left=%u used=%u\n",
                ( unsigned ) ( index - buffer ), propertyLength,
                used ? 1U : 0U );
    }

    printf( "end scenario %u\n", ( unsigned ) id );
}

/* ---- decodeVariableLength, exhaustively --------------------------------- */

static const uint8_t TAILS[] = { 0x00, 0x01, 0x7F, 0x80, 0x81, 0xFF };
#define N_TAILS ( sizeof( TAILS ) / sizeof( TAILS[ 0 ] ) )

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

static void run_varlen( void )
{
    size_t a, b, c, d, len;
    unsigned long ok = 0, bad = 0, total = 0;

    for( a = 0; a < N_TAILS; a++ )
    {
        for( b = 0; b < N_TAILS; b++ )
        {
            for( c = 0; c < N_TAILS; c++ )
            {
                for( d = 0; d < N_TAILS; d++ )
                {
                    uint8_t buf[ 4 ];
                    buf[ 0 ] = TAILS[ a ];
                    buf[ 1 ] = TAILS[ b ];
                    buf[ 2 ] = TAILS[ c ];
                    buf[ 3 ] = TAILS[ d ];

                    for( len = 0; len <= 4U; len++ )
                    {
                        uint32_t value = 0xAAAAAAAAU;
                        MQTTStatus_t status;
                        char line[ 64 ];

                        status = decodeVariableLength( buf, len, &value );

                        if( status == MQTTSuccess )
                        {
                            ( void ) snprintf( line, sizeof line, "Success %u",
                                               ( unsigned ) value );
                            ok++;
                        }
                        else
                        {
                            ( void ) snprintf( line, sizeof line, "%s",
                                               status_name( status ) );
                            bad++;
                        }

                        fnv_str( line );
                        total++;
                    }
                }
            }
        }
    }

    printf( "varlen total %lu\n", total );
    printf( "varlen Success %lu\n", ok );
    printf( "varlen refused %lu\n", bad );
    printf( "varlen digest %016llx\n", ( unsigned long long ) fnv );
}

/* ---- encodeString -------------------------------------------------------- */

static void run_encode( void )
{
    static const char *SAMPLES[] = { "", "a", "topic/one", "0123456789abcdef" };
    size_t i;

    for( i = 0; i < 4U; i++ )
    {
        uint8_t out[ 32 ];
        uint8_t *end;
        uint16_t len = ( uint16_t ) strlen( SAMPLES[ i ] );

        memset( out, 0xAA, sizeof out );
        end = encodeString( out, SAMPLES[ i ], len );

        printf( "encode %u %u -> %u ", ( unsigned ) i, len,
                ( unsigned ) ( end - out ) );
        put_hex( out, ( size_t ) ( end - out ) );
        printf( "\n" );
    }

    /* A NULL source still writes the length and advances, which is how the
     * library reserves room for a payload it copies in separately. */
    {
        uint8_t out[ 32 ];
        uint8_t *end;

        memset( out, 0xAA, sizeof out );
        end = encodeString( out, NULL, 4 );

        printf( "encode null 4 -> %u ", ( unsigned ) ( end - out ) );
        put_hex( out, ( size_t ) ( end - out ) );
        printf( "\n" );
    }
}

/* ---- the scenarios -------------------------------------------------------- */

static Scenario_t sc;

static void reset( const char *name, uint32_t propertyLength )
{
    memset( &sc, 0, sizeof sc );
    sc.name = name;
    sc.propertyLength = propertyLength;
}

static void bytes( const uint8_t *p, size_t n )
{
    memcpy( sc.buffer, p, n );
    sc.bufferLen = n;
}

static void op( char kind, uint8_t used )
{
    sc.ops[ sc.opCount ].kind = kind;
    sc.ops[ sc.opCount ].used = used;
    sc.opCount++;
}

int main( void )
{
    size_t id = 0;

    printf( "geometry maxbuf=%u tails=%u\n", ( unsigned ) MAX_BUF,
            ( unsigned ) N_TAILS );

    /* 1. One of each integer width, in sequence. */
    {
        static const uint8_t B[] = { 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77 };
        reset( "one-of-each-integer", 7 );
        bytes( B, sizeof B );
        op( '1', 0 );
        op( '2', 0 );
        op( '4', 0 );
        run_scenario( id++, &sc );
    }

    /* 2. A property that appears twice is a protocol error. */
    {
        static const uint8_t B[] = { 0x11, 0x22, 0x33, 0x44 };
        reset( "duplicate-property", 4 );
        bytes( B, sizeof B );
        op( '1', 0 );
        op( '1', 1 );      /* the caller says it has seen this one already */
        run_scenario( id++, &sc );
    }

    /* 3. Each integer width against a property budget one byte short. */
    {
        static const uint8_t B[] = { 0x11, 0x22, 0x33, 0x44 };
        reset( "uint32-one-byte-short", 3 );
        bytes( B, sizeof B );
        op( '4', 0 );
        run_scenario( id++, &sc );
    }
    {
        static const uint8_t B[] = { 0x11, 0x22 };
        reset( "uint16-one-byte-short", 1 );
        bytes( B, sizeof B );
        op( '2', 0 );
        run_scenario( id++, &sc );
    }
    {
        static const uint8_t B[] = { 0x11 };
        reset( "uint8-with-no-budget", 0 );
        bytes( B, sizeof B );
        op( '1', 0 );
        run_scenario( id++, &sc );
    }

    /* 4. A string, and the ways its length can lie. */
    {
        static const uint8_t B[] = { 0x00, 0x03, 'a', 'b', 'c', 0x00, 0x00 };
        reset( "utf8-exact", 7 );
        bytes( B, sizeof B );
        op( 's', 0 );
        op( 's', 0 );      /* the empty string that follows */
        run_scenario( id++, &sc );
    }
    {
        static const uint8_t B[] = { 0x00, 0x05, 'a', 'b', 'c' };
        reset( "utf8-claims-more-than-the-budget", 5 );
        bytes( B, sizeof B );
        op( 's', 0 );
        run_scenario( id++, &sc );
    }
    {
        static const uint8_t B[] = { 0x00 };
        reset( "utf8-with-only-one-length-byte", 1 );
        bytes( B, sizeof B );
        op( 's', 0 );
        run_scenario( id++, &sc );
    }
    {
        static const uint8_t B[] = { 0xFF, 0xFF, 'a' };
        reset( "utf8-claims-65535", 3 );
        bytes( B, sizeof B );
        op( 's', 0 );
        run_scenario( id++, &sc );
    }
    {
        static const uint8_t B[] = { 0x00, 0x00, 0x00, 0x00 };
        reset( "utf8-empty-string", 4 );
        bytes( B, sizeof B );
        op( 's', 0 );
        op( 's', 0 );
        run_scenario( id++, &sc );
    }

    /* 5. User properties: two strings, and the failure modes in between. */
    {
        static const uint8_t B[] = { 0x00, 0x01, 'k', 0x00, 0x02, 'v', '1' };
        reset( "user-property", 7 );
        bytes( B, sizeof B );
        op( 'u', 0 );
        run_scenario( id++, &sc );
    }
    {
        static const uint8_t B[] = { 0x00, 0x01, 'k', 0x00, 0x05, 'v' };
        reset( "user-property-value-overruns", 6 );
        bytes( B, sizeof B );
        op( 'u', 0 );
        run_scenario( id++, &sc );
    }
    {
        static const uint8_t B[] = { 0x00, 0x01, 'k' };
        reset( "user-property-with-no-value", 3 );
        bytes( B, sizeof B );
        op( 'u', 0 );
        run_scenario( id++, &sc );
    }

    /* 6. A realistic mixed run, and then the budget running out. */
    {
        static const uint8_t B[] = {
            0x01,                          /* a uint8 */
            0x12, 0x34,                    /* a uint16 */
            0x00, 0x04, 't', 'e', 's', 't',/* a string */
            0xDE, 0xAD, 0xBE, 0xEF,        /* a uint32 */
            0x00                           /* one byte left over */
        };
        reset( "mixed-run-then-exhaustion", 14 );
        bytes( B, sizeof B );
        op( '1', 0 );
        op( '2', 0 );
        op( 's', 0 );
        op( '4', 0 );
        op( '2', 0 );      /* only one byte of budget left */
        run_scenario( id++, &sc );
    }

    run_varlen();
    run_encode();

    printf( "end\n" );
    return 0;
}
