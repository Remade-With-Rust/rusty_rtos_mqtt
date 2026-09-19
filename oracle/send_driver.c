/* The C arm of K7's coreMQTT SEND-PLUMBING differential.
 *
 * `core_mqtt.c` is compiled VERBATIM out of the pinned checkout; nothing here
 * copies or edits it.
 *
 * # The harness the rest of `core_mqtt.c` needs
 *
 * Everything left in that file talks to a transport, so this slice builds the
 * instrument the rest will use: a SCRIPTED transport whose every call is logged,
 * and a SCRIPTED clock. It then proves the two functions that stand between the
 * library and the network -- `sendBuffer`, which pushes one buffer, and
 * `sendMessageVector`, which pushes an array of them -- plus
 * `calculateElapsedTime`, which decides when they give up.
 *
 * # Why the log is the answer
 *
 * A transport may accept all the bytes it is offered, some of them, none of
 * them, or fail. Each of those puts the sender round its loop again with a
 * different offset, and the only way to see an off-by-one in that arithmetic is
 * to record WHAT IT WAS OFFERED on every call. `2:1,1:1` is a sender that
 * pushed two bytes in two calls with the second offer correctly advanced;
 * `2:1,2:1` is one that re-sent the first byte. Both send two bytes; only one
 * is right.
 *
 * This is the shape `rusty_rtos_sntp`'s client differential established and the
 * transport reader used in slice 13, now pointed the other way.
 *
 * # The clock
 *
 * `sendBuffer` gives up after MQTT_SEND_TIMEOUT_MS (20,000 by default). The
 * scripted clock advances by a fixed step per call, so a step of 10,001 crosses
 * the timeout on the second check and a step of 0 never does. `getTime` is
 * called once at the start, once after every accepted byte, and once per
 * timeout check, so the number of clock reads is itself part of the behaviour
 * and is logged.
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core_mqtt.h"

#define MAX_SCRIPT     8
#define MAX_SENT      64
#define MAX_LOG      256

static const char * status_name( MQTTStatus_t status )
{
    switch( status )
    {
        case MQTTSuccess:                 return "Success";
        case MQTTBadParameter:            return "BadParameter";
        case MQTTNoMemory:                return "NoMemory";
        case MQTTSendFailed:              return "SendFailed";
        case MQTTStatusConnected:         return "StatusConnected";
        case MQTTStatusNotConnected:      return "StatusNotConnected";
        case MQTTStatusDisconnectPending: return "StatusDisconnectPending";
        default:                          return "OTHER";
    }
}

/* ---- the scripted transport ------------------------------------------------ */

/* Each step says what the next `send` returns: a positive count is that many
 * bytes accepted, 0 is "not now", and a negative is a failure. The script is
 * held at its last step once exhausted, so a loop that will not terminate
 * shows up as a long log rather than as a hang. */
static int32_t g_script[ MAX_SCRIPT ];
static size_t g_steps;
static size_t g_at;
static size_t g_calls;

static uint8_t g_sent[ MAX_SENT ];
static size_t g_sent_len;

static char g_log[ MAX_LOG ];
static size_t g_log_len;

/* The clock. */
static uint32_t g_now;
static uint32_t g_step;
static size_t g_clock_reads;

static void log_call( size_t offered, int32_t answered )
{
    int n;

    /* A sender that will not terminate must show up as a truncated log rather
     * than as a buffer overrun in the harness. */
    if( g_log_len + 24U >= sizeof g_log )
    {
        return;
    }

    n = snprintf( &g_log[ g_log_len ], sizeof g_log - g_log_len, "%s%u:%ld",
                  ( g_log_len == 0U ) ? "" : ",", ( unsigned ) offered,
                  ( long ) answered );

    if( ( n > 0 ) && ( ( size_t ) n < ( sizeof g_log - g_log_len ) ) )
    {
        g_log_len += ( size_t ) n;
    }
}

static int32_t scripted_send( NetworkContext_t *context, const void *buffer, size_t bytes )
{
    int32_t answer;
    size_t take;

    ( void ) context;

    g_calls++;

    answer = ( g_at < g_steps ) ? g_script[ g_at ] : g_script[ g_steps - 1U ];

    if( g_at < g_steps )
    {
        g_at++;
    }

    /* A transport never accepts more than it was offered; a script that says
     * so is a bug in the script, and the trace should show it rather than let
     * the library's assert decide. */
    if( answer > ( int32_t ) bytes )
    {
        answer = ( int32_t ) bytes;
    }

    log_call( bytes, answer );

    if( answer > 0 )
    {
        take = ( size_t ) answer;

        if( ( g_sent_len + take ) <= sizeof g_sent )
        {
            memcpy( &g_sent[ g_sent_len ], buffer, take );
            g_sent_len += take;
        }
    }

    return answer;
}

static int32_t scripted_recv( NetworkContext_t *context, void *buffer, size_t bytes )
{
    ( void ) context; ( void ) buffer; ( void ) bytes;
    return -1;
}

static uint32_t scripted_time( void )
{
    uint32_t now = g_now;

    g_clock_reads++;
    g_now += g_step;

    return now;
}

static bool stub_callback( MQTTContext_t *c,
                           MQTTPacketInfo_t *p,
                           MQTTDeserializedInfo_t *d,
                           MQTTSuccessFailReasonCode_t *r,
                           MQTTPropBuilder_t *in,
                           MQTTPropBuilder_t *out )
{
    ( void ) c; ( void ) p; ( void ) d; ( void ) r; ( void ) in; ( void ) out;
    return true;
}

static uint8_t g_network[ 256 ];

static void reset( const int32_t *script, size_t steps, uint32_t step )
{
    memcpy( g_script, script, steps * sizeof script[ 0 ] );
    g_steps = steps;
    g_at = 0U;
    g_calls = 0U;
    g_sent_len = 0U;
    g_log[ 0 ] = '\0';
    g_log_len = 0U;
    g_now = 0U;
    g_step = step;
    g_clock_reads = 0U;
}

static MQTTStatus_t fresh( MQTTContext_t *context, int connect )
{
    static TransportInterface_t transport;
    static MQTTFixedBuffer_t network;
    MQTTStatus_t status;

    memset( &transport, 0, sizeof transport );
    transport.send = scripted_send;
    transport.recv = scripted_recv;
    /* `writev` stays NULL, so `sendMessageVector` takes its per-vector `send`
     * path. A transport that offers `writev` is a different set of answers and
     * a later slice. */
    transport.writev = NULL;
    transport.pNetworkContext = NULL;

    network.pBuffer = g_network;
    network.size = sizeof g_network;

    status = MQTT_Init( context, &transport, scripted_time, stub_callback, &network );

    if( connect == 1 )
    {
        context->connectStatus = MQTTConnected;
    }
    else if( connect == 2 )
    {
        context->connectStatus = MQTTDisconnectPending;
    }
    else
    {
        /* Left as `MQTT_Init` set it. */
    }

    return status;
}

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

/* ---- PINGREQ, which is the shortest road to `sendBuffer` ------------------- */

typedef struct
{
    const char *name;
    int32_t     script[ MAX_SCRIPT ];
    size_t      steps;
    uint32_t    step;
    int         connect;   /* 0 not, 1 connected, 2 disconnect-pending */
} PingCase_t;

static const PingCase_t PING_CASES[] = {
    /* --- the transport takes it all --- */
    { "all-at-once",       { 2 }, 1, 0U, 1 },
    /* A clock that moves, so the recorded transmit time is not zero.
     * Without this, dropping the `lastPacketTxTime` assignment changes
     * no line in the trace. */
    { "all-at-once-moving-clock", { 2 }, 1, 7U, 1 },
    /* --- and in pieces, which is where the offset arithmetic shows --- */
    { "one-byte-at-a-time", { 1, 1 }, 2, 0U, 1 },
    { "one-byte-at-a-time-moving-clock", { 1, 1 }, 2, 7U, 1 },
    /* --- nothing, then everything: a transport that was busy --- */
    { "busy-then-ready",   { 0, 2 }, 2, 0U, 1 },
    { "busy-then-in-pieces", { 0, 1, 0, 1 }, 4, 0U, 1 },

    /* --- a transport that never takes anything, with a clock that moves --- */
    { "always-busy-timeout", { 0 }, 1, 10001U, 1 },
    /* A step that lands the elapsed time EXACTLY on the timeout. The C
     * gives up at `>=`, so this stops one check sooner than a `>` would,
     * and nothing else here tells those two apart. */
    { "timeout-exactly-on-the-boundary", { 0 }, 1, 10000U, 1 },
    /* A clock that never advances is NOT a case here. coreMQTT's config header
     * says that if the time function is a no-op then MQTT_SEND_TIMEOUT_MS must
     * be set to zero; with a non-zero timeout and a frozen clock, `sendBuffer`
     * loops for ever by construction against a transport that never accepts.
     * That is a documented precondition, not a defect, and a differential case
     * that cannot terminate is not a case. */

    /* --- failures --- */
    { "fails-at-once",     { -1 }, 1, 0U, 1 },
    { "one-byte-then-fails", { 1, -1 }, 2, 0U, 1 },

    /* --- and the connection states, which are checked before any send --- */
    { "not-connected",     { 2 }, 1, 0U, 0 },
    { "disconnect-pending", { 2 }, 1, 0U, 2 },
};

#define N_PING_CASES    ( sizeof( PING_CASES ) / sizeof( PING_CASES[ 0 ] ) )

static void run_ping_case( size_t i )
{
    const PingCase_t *c = &PING_CASES[ i ];
    MQTTContext_t context;
    MQTTStatus_t status;

    reset( c->script, c->steps, c->step );
    memset( &context, 0, sizeof context );
    ( void ) fresh( &context, c->connect );

    status = MQTT_Ping( &context );

    printf( "ping %u %s script=", ( unsigned ) i, c->name );

    {
        size_t k;

        for( k = 0U; k < c->steps; k++ )
        {
            printf( "%s%ld", ( k == 0U ) ? "" : ",", ( long ) c->script[ k ] );
        }
    }

    printf( " step=%u connected=%u -> %s calls=%u log=%s bytes=",
            ( unsigned ) c->step, ( unsigned ) c->connect, status_name( status ),
            ( unsigned ) g_calls, ( g_log_len == 0U ) ? "-" : g_log );
    put_hex( g_sent, g_sent_len );
    printf( " connect=%d waiting=%u txtime=%u\n", ( int ) context.connectStatus,
            context.waitingForPingResp ? 1U : 0U,
            ( unsigned ) context.lastPacketTxTime );
}

/* ---- DISCONNECT, which is the shortest road to `sendMessageVector` --------- */

/* `MQTT_Disconnect` builds a two-vector message -- a fixed header and a
 * reason code -- and pushes it with `sendMessageVector`. That is the only
 * public call that reaches the vector sender without also needing a
 * subscription list or a payload, so it is the one that proves the vector
 * arithmetic. */
static const PingCase_t VECTOR_CASES[] = {
    { "all-at-once",        { 64 }, 1, 0U, 1 },
    { "one-byte-at-a-time", { 1 }, 1, 0U, 1 },
    { "two-at-a-time",      { 2 }, 1, 0U, 1 },
    /* A partial that lands exactly on a vector boundary, and one that does
     * not: the second is where `iov_base` has to be advanced INSIDE a vector. */
    { "stops-on-a-boundary", { 2, 64 }, 2, 0U, 1 },
    { "stops-mid-vector",   { 1, 64 }, 2, 0U, 1 },
    { "busy-then-ready",    { 0, 64 }, 2, 0U, 1 },
    { "always-busy-timeout", { 0 }, 1, 10001U, 1 },
    /* A step that lands the elapsed time EXACTLY on the timeout.
     *
     * `sendBuffer` gives up here and `sendMessageVector` does NOT: the first
     * compares with `>=` and the second with `>`, thirty lines apart in one
     * file. So this case is one transport call longer than the ping case of
     * the same name, and that is the only place the difference shows. */
    { "timeout-exactly-on-the-boundary", { 0 }, 1, 10000U, 1 },
    { "fails-at-once",      { -1 }, 1, 0U, 1 },
    { "one-byte-then-fails", { 1, -1 }, 2, 0U, 1 },
    { "not-connected",      { 64 }, 1, 0U, 0 },
    /* `MQTT_Disconnect` refuses only `MQTTNotConnected`: a context whose
     * transport has already failed is still allowed to try to send a
     * DISCONNECT, which is the one thing it might still manage. */
    { "disconnect-pending", { 64 }, 1, 0U, 2 },
};

#define N_VECTOR_CASES    ( sizeof( VECTOR_CASES ) / sizeof( VECTOR_CASES[ 0 ] ) )

static void run_vector_case( size_t i )
{
    const PingCase_t *c = &VECTOR_CASES[ i ];
    MQTTContext_t context;
    MQTTStatus_t status;

    reset( c->script, c->steps, c->step );
    memset( &context, 0, sizeof context );
    ( void ) fresh( &context, c->connect );

    status = MQTT_Disconnect( &context, NULL, 0U );

    printf( "vec %u %s script=", ( unsigned ) i, c->name );

    {
        size_t k;

        for( k = 0U; k < c->steps; k++ )
        {
            printf( "%s%ld", ( k == 0U ) ? "" : ",", ( long ) c->script[ k ] );
        }
    }

    printf( " step=%u connected=%u -> %s calls=%u log=%s bytes=",
            ( unsigned ) c->step, ( unsigned ) c->connect, status_name( status ),
            ( unsigned ) g_calls, ( g_log_len == 0U ) ? "-" : g_log );
    put_hex( g_sent, g_sent_len );
    printf( " connect=%d reads=%u\n", ( int ) context.connectStatus,
            ( unsigned ) g_clock_reads );
}

/* ---- the elapsed-time arithmetic, which is a wrap away from wrong ---------- */

static void print_elapsed( void )
{
    /* `calculateElapsedTime` is `later - start` on uint32_t, so it is correct
     * across the wrap BY CONSTRUCTION -- and only if nobody "fixes" it with a
     * signed comparison. It is static, so it is reached here through
     * `sendBuffer`'s timeout: a clock that starts near the wrap and steps over
     * it must still time out. */
    static const struct
    {
        const char *name;
        uint32_t    start;
        uint32_t    step;
    } CASES[] = {
        { "far-from-the-wrap", 0U,          10001U },
        { "at-the-wrap",       0xFFFFFFFFU, 10001U },
        { "one-before-the-wrap", 0xFFFFD8F0U, 10001U },
    };

    size_t i;

    for( i = 0U; i < sizeof( CASES ) / sizeof( CASES[ 0 ] ); i++ )
    {
        MQTTContext_t context;
        MQTTStatus_t status;
        static const int32_t BUSY[] = { 0 };

        reset( BUSY, 1U, CASES[ i ].step );
        g_now = CASES[ i ].start;

        memset( &context, 0, sizeof context );
        ( void ) fresh( &context, 1 );

        status = MQTT_Ping( &context );

        printf( "elapsed %s start=%u step=%u -> %s calls=%u reads=%u\n",
                CASES[ i ].name, ( unsigned ) CASES[ i ].start,
                ( unsigned ) CASES[ i ].step, status_name( status ),
                ( unsigned ) g_calls, ( unsigned ) g_clock_reads );
    }
}

int main( void )
{
    size_t i;

    printf( "geometry pings=%u vectors=%u\n", ( unsigned ) N_PING_CASES,
            ( unsigned ) N_VECTOR_CASES );

    for( i = 0U; i < N_PING_CASES; i++ )
    {
        run_ping_case( i );
    }

    for( i = 0U; i < N_VECTOR_CASES; i++ )
    {
        run_vector_case( i );
    }

    print_elapsed();

    printf( "end\n" );
    return 0;
}
