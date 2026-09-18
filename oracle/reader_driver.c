/* The C arm of K7's coreMQTT TRANSPORT-READER differential.
 *
 * `core_mqtt_serializer.c` is compiled VERBATIM out of the pinned checkout;
 * nothing here copies or edits it.
 *
 * # Why this slice, and why it is different
 *
 * Every other function in this package is handed a buffer. This one is handed a
 * CALLBACK, and pulls bytes off a socket one at a time: the type byte, then the
 * variable-byte remaining length, stopping when the length is complete.
 *
 * So the interesting thing is not just the answer but HOW MANY TIMES IT CALLED,
 * and what it did when the transport returned something other than one byte.
 * A transport can answer 1 (a byte), 0 (nothing yet) or a negative number (an
 * error), and `MQTT_GetIncomingPacketTypeAndLength` has a different status for
 * each -- `MQTTNoDataAvailable` and `MQTTRecvFailed` -- which nothing else in
 * the library uses.
 *
 * This is the shape `rusty_rtos_sntp`'s client differential established: script
 * the callback, log every call, and compare the CALL SEQUENCE and not only the
 * return value.
 *
 * # The script
 *
 * Each case is a list of what the transport should return, in order: a byte
 * value, "nothing", or "an error". The driver logs each call and prints the
 * count, so a reader that consumed one byte too many is visible even when the
 * status is right.
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core_mqtt_serializer.h"

#define MAX_SCRIPT    8

/* A scripted transport: each step is a byte, or one of the two failures. */
#define STEP_NOTHING    ( -1 )
#define STEP_ERROR      ( -2 )
#define STEP_EXHAUSTED  ( -3 )

typedef struct
{
    const char *name;
    int         script[ MAX_SCRIPT ];
    size_t      steps;
} ReaderCase_t;

static const ReaderCase_t CASES[] = {
    /* --- ordinary packets --- */
    { "pingresp",            { 0xD0, 0x00 }, 2 },
    { "puback",              { 0x40, 0x02 }, 2 },
    { "publish-qos0",        { 0x30, 0x0B }, 2 },
    { "pubrel",              { 0x62, 0x02 }, 2 },
    /* A two-byte remaining length: 0x80 0x01 is 128. */
    { "two-byte-length",     { 0x30, 0x80, 0x01 }, 3 },
    /* Three and four bytes, at the top of each range. */
    { "three-byte-length",   { 0x30, 0xFF, 0xFF, 0x7F }, 4 },
    { "four-byte-length",    { 0x30, 0xFF, 0xFF, 0xFF, 0x7F }, 5 },

    /* --- the type byte --- */
    { "type-connect-is-client-only", { 0x10, 0x00 }, 2 },
    { "type-zero",           { 0x00, 0x00 }, 2 },
    { "pubrel-without-its-reserved-bit", { 0x60, 0x02 }, 2 },

    /* --- what the length can get wrong --- */
    /* A fifth continuation byte: the multiplier guard stops it. */
    { "five-length-bytes",   { 0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F }, 6 },
    /* Non-minimal: 0x80 0x00 decodes to zero, and zero needs one byte. */
    { "non-minimal-length",  { 0x30, 0x80, 0x00 }, 3 },

    /* --- what the transport can do instead of answering --- */
    { "nothing-at-the-type-byte",   { STEP_NOTHING }, 1 },
    { "error-at-the-type-byte",     { STEP_ERROR }, 1 },
    { "nothing-at-the-length-byte", { 0x30, STEP_NOTHING }, 2 },
    { "error-at-the-length-byte",   { 0x30, STEP_ERROR }, 2 },
    /* The transport stops halfway through a multi-byte length. */
    { "nothing-mid-length",  { 0x30, 0x80, STEP_NOTHING }, 3 },
};

#define N_CASES    ( sizeof( CASES ) / sizeof( CASES[ 0 ] ) )

/* The scripted transport's state, and the log of what it was asked. */
static const ReaderCase_t *g_case;
static size_t g_at;
static size_t g_calls;
static char g_log[ 256 ];
static size_t g_log_len;

static void log_step( const char *text )
{
    int n = snprintf( &g_log[ g_log_len ], sizeof g_log - g_log_len, "%s%s",
                      ( g_log_len == 0U ) ? "" : ",", text );

    if( n > 0 )
    {
        g_log_len += ( size_t ) n;
    }
}

static int32_t scripted_recv( NetworkContext_t *pContext,
                              void *pBuffer,
                              size_t bytesToRecv )
{
    char text[ 16 ];
    int step;

    ( void ) pContext;

    g_calls++;

    /* The reader only ever asks for one byte; if that ever changes, the trace
     * should say so rather than quietly cope. */
    if( bytesToRecv != 1U )
    {
        snprintf( text, sizeof text, "ask%u", ( unsigned ) bytesToRecv );
        log_step( text );
        return -1;
    }

    step = ( g_at < g_case->steps ) ? g_case->script[ g_at ] : STEP_EXHAUSTED;
    g_at++;

    switch( step )
    {
        case STEP_NOTHING:
            log_step( "none" );
            return 0;

        case STEP_ERROR:
            log_step( "err" );
            return -5;

        case STEP_EXHAUSTED:
            /* The script ran out: the reader asked for more than the case
             * described, which is itself a finding. */
            log_step( "OVERRUN" );
            return -1;

        default:
            snprintf( text, sizeof text, "%02x", ( unsigned ) step );
            log_step( text );
            *( ( uint8_t * ) pBuffer ) = ( uint8_t ) step;
            return 1;
    }
}

static const char * status_name( MQTTStatus_t status )
{
    switch( status )
    {
        case MQTTSuccess:         return "Success";
        case MQTTBadParameter:    return "BadParameter";
        case MQTTBadResponse:     return "BadResponse";
        case MQTTNoDataAvailable: return "NoDataAvailable";
        case MQTTRecvFailed:      return "RecvFailed";
        default:                  return "OTHER";
    }
}

static void run_case( size_t i )
{
    const ReaderCase_t *c = &CASES[ i ];
    MQTTPacketInfo_t packet;
    MQTTStatus_t status;
    size_t k;

    g_case = c;
    g_at = 0;
    g_calls = 0;
    g_log_len = 0;
    g_log[ 0 ] = '\0';

    memset( &packet, 0, sizeof packet );

    printf( "case %u %s script=", ( unsigned ) i, c->name );

    for( k = 0; k < c->steps; k++ )
    {
        int step = c->script[ k ];

        if( k != 0U )
        {
            printf( "," );
        }

        if( step == STEP_NOTHING )
        {
            printf( "none" );
        }
        else if( step == STEP_ERROR )
        {
            printf( "err" );
        }
        else
        {
            printf( "%02x", ( unsigned ) step );
        }
    }

    status = MQTT_GetIncomingPacketTypeAndLength( scripted_recv, NULL, &packet );

    printf( " -> %s calls=%u read=%s", status_name( status ),
            ( unsigned ) g_calls, ( g_log_len == 0U ) ? "-" : g_log );

    if( status == MQTTSuccess )
    {
        printf( " type=%02x rl=%u", ( unsigned ) packet.type,
                ( unsigned ) packet.remainingLength );
    }

    printf( "\n" );
}

/* Every one of the 256 type bytes, with a one-byte remaining length of zero.
 *
 * The header slice swept `incomingPacketValid` directly; this sweeps it THROUGH
 * the reader, which is a different question: a reader that checked the type
 * AFTER reading the length would consume different numbers of bytes, and only
 * the call count shows it. */
static void sweep_type_bytes( void )
{
    unsigned value;
    unsigned accepted = 0U;
    unsigned refused = 0U;
    uint64_t fnv = 1469598103934665603ULL;
    int first = 1;

    printf( "type-sweep accepted=" );

    for( value = 0U; value < 256U; value++ )
    {
        static ReaderCase_t one;
        MQTTPacketInfo_t packet;
        MQTTStatus_t status;

        one.name = "sweep";
        one.script[ 0 ] = ( int ) value;
        one.script[ 1 ] = 0x00;
        one.steps = 2;

        g_case = &one;
        g_at = 0;
        g_calls = 0;
        g_log_len = 0;

        memset( &packet, 0, sizeof packet );
        status = MQTT_GetIncomingPacketTypeAndLength( scripted_recv, NULL, &packet );

        /* The call COUNT is digested too: a type check moved after the length
         * read would keep every status and change every count. */
        fnv ^= ( uint64_t ) g_calls;
        fnv *= 1099511628211ULL;

        if( status == MQTTSuccess )
        {
            printf( "%s%02x", first ? "" : ",", value );
            first = 0;
            accepted++;
        }
        else
        {
            refused++;
        }
    }

    printf( " n=%u refused=%u calldigest=%016llx\n", accepted, refused,
            ( unsigned long long ) fnv );
}

int main( void )
{
    size_t i;

    printf( "geometry cases=%u\n", ( unsigned ) N_CASES );

    for( i = 0; i < N_CASES; i++ )
    {
        run_case( i );
    }

    sweep_type_bytes();

    printf( "end\n" );
    return 0;
}
