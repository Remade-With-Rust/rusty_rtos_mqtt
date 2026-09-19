/* The C arm of K7's CONNECT/CONNACK differential.
 *
 * `MQTT_Connect` is the first function in coreMQTT that both SENDS and
 * RECEIVES, so this driver scripts both directions and logs both. A transport
 * is given
 *
 *   - a send step, as in `send_driver.c`: how many bytes it accepts per call;
 *   - a receive step: how many bytes it delivers per call; and
 *   - the bytes it is going to deliver, which are a CONNACK written by hand.
 *
 * What comes out is the status, both call logs, the bytes that went out, and
 * the whole connection context afterwards -- because a CONNACK's job is to set
 * that context, and a status alone would bless a reader that dropped every
 * property. */

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "core_mqtt.h"
#include "core_mqtt_state.h"

#define MAX_LOG      4096
#define MAX_SENT     512
#define MAX_IN       256

/* ---- the scripted transport ------------------------------------------------ */

static int32_t g_send_step;

/* One step per call, held at the last once exhausted -- the same shape as
 * `send_driver.c`'s. One number could not express a transport that alternates
 * between delivering and not, which is the only thing that can tell a polling
 * timeout that RESETS on data from one that does not. */
#define MAX_RECV_STEPS  8
static int32_t g_recv_script[ MAX_RECV_STEPS ];
static size_t g_recv_steps;
static size_t g_recv_at;

static uint8_t g_in[ MAX_IN ];
static size_t g_in_len;
static size_t g_in_at;

static uint8_t g_sent[ MAX_SENT ];
static size_t g_sent_len;

static char g_slog[ MAX_LOG ];
static size_t g_slog_len;
static unsigned g_scalls;

static char g_rlog[ MAX_LOG ];
static size_t g_rlog_len;
static unsigned g_rcalls;

static uint32_t g_now;
static uint32_t g_clock_step;
static unsigned g_clock_reads;

static void log_to( char *log, size_t *len, size_t cap, const char *fmt,
                    unsigned a, long b )
{
    int n = snprintf( &log[ *len ], cap - *len, "%s%u:%ld",
                      ( *len == 0U ) ? "" : ",", a, b );

    ( void ) fmt;

    if( ( n > 0 ) && ( ( size_t ) n < ( cap - *len ) ) )
    {
        *len += ( size_t ) n;
    }
}

static int32_t scripted_send( NetworkContext_t *c, const void *buffer, size_t bytes )
{
    int32_t answer = g_send_step;

    ( void ) c;
    g_scalls++;

    if( answer > ( int32_t ) bytes )
    {
        answer = ( int32_t ) bytes;
    }

    log_to( g_slog, &g_slog_len, sizeof g_slog, "", ( unsigned ) bytes, ( long ) answer );

    if( ( answer > 0 ) && ( ( g_sent_len + ( size_t ) answer ) <= sizeof g_sent ) )
    {
        memcpy( &g_sent[ g_sent_len ], buffer, ( size_t ) answer );
        g_sent_len += ( size_t ) answer;
    }

    return answer;
}

/* The receive side of the same idea. A step of -1 fails; a step of 0 delivers
 * nothing; anything else delivers that many bytes of whatever is left of the
 * scripted CONNACK, clamped to what was asked for. Running out delivers
 * nothing, which is how a timeout is reached rather than a hang. */
static int32_t scripted_recv( NetworkContext_t *c, void *buffer, size_t bytes )
{
    int32_t answer;
    size_t left = g_in_len - g_in_at;

    ( void ) c;
    g_rcalls++;

    if( g_recv_at < g_recv_steps )
    {
        answer = g_recv_script[ g_recv_at ];
        g_recv_at++;
    }
    else if( g_recv_steps > 0U )
    {
        answer = g_recv_script[ g_recv_steps - 1U ];
    }
    else
    {
        answer = 0;
    }

    if( answer > 0 )
    {
        if( answer > ( int32_t ) bytes )
        {
            answer = ( int32_t ) bytes;
        }

        if( answer > ( int32_t ) left )
        {
            answer = ( int32_t ) left;
        }
    }

    log_to( g_rlog, &g_rlog_len, sizeof g_rlog, "", ( unsigned ) bytes, ( long ) answer );

    if( answer > 0 )
    {
        memcpy( buffer, &g_in[ g_in_at ], ( size_t ) answer );
        g_in_at += ( size_t ) answer;
    }

    return answer;
}

static uint32_t scripted_time( void )
{
    uint32_t now = g_now;

    g_clock_reads++;
    g_now += g_clock_step;

    return now;
}

static bool stub_callback( MQTTContext_t *c, MQTTPacketInfo_t *p,
                           MQTTDeserializedInfo_t *d, MQTTSuccessFailReasonCode_t *r,
                           MQTTPropBuilder_t *in, MQTTPropBuilder_t *out )
{
    ( void ) c; ( void ) p; ( void ) d; ( void ) r; ( void ) in; ( void ) out;
    return true;
}

/* ---- the retransmit store, for the resumed-session cases ------------------- */

static bool g_have_store;
static bool g_retrieve_answer;
static unsigned g_retrieve_calls;
static uint32_t g_retrieved[ 16 ];
static size_t g_retrieved_len;
static unsigned g_clear_calls;
static uint32_t g_cleared[ 16 ];
static size_t g_cleared_len;

/* One canned packet, which is what a resumed session would push again. */
static uint8_t g_stored_packet[] = { 0x62, 0x02, 0x00, 0x05 };

static bool store_packet( MQTTContext_t *context, uint32_t packetId, MQTTVec_t *vec )
{
    ( void ) context; ( void ) packetId; ( void ) vec;
    return true;
}

static bool retrieve_packet( MQTTContext_t *context, uint32_t packetId,
                             uint8_t **pPacket, size_t *pLength )
{
    ( void ) context;

    g_retrieve_calls++;

    /* The KEY, which is the only place the incoming-publish flag shows. */
    if( g_retrieved_len < ( sizeof g_retrieved / sizeof g_retrieved[ 0 ] ) )
    {
        g_retrieved[ g_retrieved_len++ ] = packetId;
    }

    if( !g_retrieve_answer )
    {
        return false;
    }

    *pPacket = g_stored_packet;
    *pLength = sizeof g_stored_packet;

    return true;
}

static void clear_packet( MQTTContext_t *context, uint32_t packetId )
{
    ( void ) context;

    g_clear_calls++;

    if( g_cleared_len < ( sizeof g_cleared / sizeof g_cleared[ 0 ] ) )
    {
        g_cleared[ g_cleared_len++ ] = packetId;
    }
}

/* ---- the context ----------------------------------------------------------- */

static uint8_t g_network[ 256 ];
static MQTTPubAckInfo_t g_out_records[ 4 ];
static MQTTPubAckInfo_t g_in_records[ 4 ];

static void reset( int32_t sendStep, const int32_t *recvScript, size_t recvSteps,
                   uint32_t clockStep, const uint8_t *connack, size_t connackLen )
{
    size_t i;

    g_send_step = sendStep;
    g_recv_steps = ( recvSteps <= MAX_RECV_STEPS ) ? recvSteps : MAX_RECV_STEPS;
    g_recv_at = 0U;

    for( i = 0U; i < g_recv_steps; i++ )
    {
        g_recv_script[ i ] = recvScript[ i ];
    }

    g_clock_step = clockStep;

    g_scalls = 0U;
    g_rcalls = 0U;
    g_sent_len = 0U;
    g_slog[ 0 ] = '\0';
    g_slog_len = 0U;
    g_rlog[ 0 ] = '\0';
    g_rlog_len = 0U;
    g_now = 0U;
    g_clock_reads = 0U;

    memset( g_in, 0, sizeof g_in );
    g_in_len = ( connackLen <= sizeof g_in ) ? connackLen : sizeof g_in;
    memcpy( g_in, connack, g_in_len );
    g_in_at = 0U;

    g_retrieve_calls = 0U;
    g_retrieved_len = 0U;
    memset( g_retrieved, 0, sizeof g_retrieved );
    g_clear_calls = 0U;
    g_cleared_len = 0U;
    memset( g_cleared, 0, sizeof g_cleared );
}

/* Put two messages in flight before connecting, so a resumed session has
 * something to resend and a clean one has something to clear. Without this the
 * store callbacks are wired up and never called, and every poison on the
 * resumption path passes. */
static void seed_records( int kind )
{
    if( kind == 1 )
    {
        g_out_records[ 0 ].packetId = 5U;
        g_out_records[ 0 ].qos = MQTTQoS1;
        g_out_records[ 0 ].publishState = MQTTPubAckPending;

        g_out_records[ 1 ].packetId = 6U;
        g_out_records[ 1 ].qos = MQTTQoS2;
        g_out_records[ 1 ].publishState = MQTTPubCompPending;
    }
    else if( kind == 2 )
    {
        /* ONLY a PUBREL awaiting its PUBCOMP. `handleCleanSession` clears the
         * stored PUBLISHes, memsets the outgoing array, and THEN asks
         * `MQTT_PubrelToResend` -- which reads that same array -- what PUBRELs
         * to clear. This case is the one that shows the answer is always
         * "none". */
        g_out_records[ 0 ].packetId = 6U;
        g_out_records[ 0 ].qos = MQTTQoS2;
        g_out_records[ 0 ].publishState = MQTTPubCompPending;
    }
    else
    {
        /* Nothing in flight. */
    }
}

static void fresh( MQTTContext_t *context )
{
    static TransportInterface_t transport;
    static MQTTFixedBuffer_t network;

    memset( &transport, 0, sizeof transport );
    transport.send = scripted_send;
    transport.recv = scripted_recv;
    transport.writev = NULL;
    transport.pNetworkContext = NULL;

    memset( g_network, 0, sizeof g_network );
    network.pBuffer = g_network;
    network.size = sizeof g_network;

    memset( g_out_records, 0, sizeof g_out_records );
    memset( g_in_records, 0, sizeof g_in_records );

    memset( context, 0, sizeof *context );
    ( void ) MQTT_Init( context, &transport, scripted_time, stub_callback, &network );
    ( void ) MQTT_InitStatefulQoS( context, g_out_records, 4U, g_in_records, 4U,
                                   NULL, 0U );

    if( g_have_store )
    {
        ( void ) MQTT_InitRetransmits( context, store_packet, retrieve_packet,
                                       clear_packet );
    }
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

static const char * status_name( MQTTStatus_t s )
{
    switch( s )
    {
        case MQTTSuccess:                return "Success";
        case MQTTBadParameter:           return "BadParameter";
        case MQTTNoMemory:               return "NoMemory";
        case MQTTSendFailed:             return "SendFailed";
        case MQTTRecvFailed:             return "RecvFailed";
        case MQTTBadResponse:            return "BadResponse";
        case MQTTServerRefused:          return "ServerRefused";
        case MQTTNoDataAvailable:        return "NoDataAvailable";
        case MQTTIllegalState:           return "IllegalState";
        case MQTTStateCollision:         return "StateCollision";
        case MQTTKeepAliveTimeout:       return "KeepAliveTimeout";
        case MQTTNeedMoreBytes:          return "NeedMoreBytes";
        case MQTTStatusConnected:        return "StatusConnected";
        case MQTTStatusNotConnected:     return "StatusNotConnected";
        case MQTTStatusDisconnectPending: return "StatusDisconnectPending";
        case MQTTPublishStoreFailed:     return "PublishStoreFailed";
        case MQTTPublishRetrieveFailed:  return "PublishRetrieveFailed";
        default:                         return "?";
    }
}

/* ---- the cases ------------------------------------------------------------- */

static void conn_case( const char *name, const char *clientId, bool cleanSession,
                       uint16_t keepAlive, const uint8_t *props, size_t propsLen,
                       bool withWill, uint32_t timeoutMs, int32_t sendStep,
                       const int32_t *recvScript, size_t recvSteps, uint32_t clockStep,
                       const uint8_t *connack, size_t connackLen,
                       bool haveStore, bool retrieveAnswer, bool preConnected,
                       int seed )
{
    static uint8_t propsBuffer[ 64 ];

    MQTTContext_t context;
    MQTTConnectInfo_t info;
    MQTTPublishInfo_t will;
    MQTTPublishInfo_t *pWill = NULL;
    MQTTPropBuilder_t builder;
    MQTTPropBuilder_t *pBuilder = NULL;
    MQTTStatus_t status;
    bool sessionPresent = false;

    g_have_store = haveStore;
    g_retrieve_answer = retrieveAnswer;

    reset( sendStep, recvScript, recvSteps, clockStep, connack, connackLen );
    fresh( &context );

    seed_records( seed );

    if( preConnected )
    {
        context.connectStatus = MQTTConnected;
    }

    if( propsLen > 0U )
    {
        memset( propsBuffer, 0, sizeof propsBuffer );
        memcpy( propsBuffer, props, propsLen );
        memset( &builder, 0, sizeof builder );
        builder.pBuffer = propsBuffer;
        builder.bufferLength = sizeof propsBuffer;
        builder.currentIndex = propsLen;
        pBuilder = &builder;
    }

    memset( &info, 0, sizeof info );
    info.cleanSession = cleanSession;
    info.keepAliveSeconds = keepAlive;
    info.pClientIdentifier = clientId;
    info.clientIdentifierLength = strlen( clientId );

    if( withWill )
    {
        memset( &will, 0, sizeof will );
        will.qos = MQTTQoS0;
        will.pTopicName = "w/t";
        will.topicNameLength = 3U;
        will.pPayload = "bye";
        will.payloadLength = 3U;
        pWill = &will;
    }

    status = MQTT_Connect( &context, &info, pWill, timeoutMs, &sessionPresent,
                           pBuilder, NULL );

    printf( "conn %s id=%s clean=%u ka=%u props=", name, clientId,
            cleanSession ? 1U : 0U, ( unsigned ) keepAlive );
    put_hex( props, propsLen );
    printf( " will=%u timeout=%lu sstep=%ld rscript=", withWill ? 1U : 0U,
            ( unsigned long ) timeoutMs, ( long ) sendStep );

    {
        size_t k;

        for( k = 0U; k < recvSteps; k++ )
        {
            printf( "%s%ld", ( k == 0U ) ? "" : "/", ( long ) recvScript[ k ] );
        }
    }

    printf( " cstep=%lu pre=%u store=%u/%u seed=%u connack=",
            ( unsigned long ) clockStep, preConnected ? 1U : 0U,
            haveStore ? 1U : 0U, retrieveAnswer ? 1U : 0U, ( unsigned ) seed );
    put_hex( connack, connackLen );

    printf( " -> %s session=%u state=%d", status_name( status ),
            sessionPresent ? 1U : 0U, ( int ) context.connectStatus );
    printf( " scalls=%u slog=%s sbytes=", g_scalls,
            ( g_slog_len == 0U ) ? "-" : g_slog );
    put_hex( g_sent, g_sent_len );
    printf( " rcalls=%u rlog=%s", g_rcalls, ( g_rlog_len == 0U ) ? "-" : g_rlog );
    printf( " ka=%u out=%lu in=%lu", ( unsigned ) context.keepAliveIntervalSec,
            ( unsigned long ) context.outgoingPublishRecordMaxCount,
            ( unsigned long ) context.incomingPublishRecordMaxCount );
    printf( " maxqos=%u retain=%u maxpkt=%lu alias=%u wild=%u subid=%u shared=%u"
            " recvmax=%u expiry=%lu",
            ( unsigned ) context.connectionProperties.serverMaxQos,
            ( unsigned ) context.connectionProperties.retainAvailable,
            ( unsigned long ) context.connectionProperties.serverMaxPacketSize,
            ( unsigned ) context.connectionProperties.serverTopicAliasMax,
            ( unsigned ) context.connectionProperties.isWildcardAvailable,
            ( unsigned ) context.connectionProperties.isSubscriptionIdAvailable,
            ( unsigned ) context.connectionProperties.isSharedAvailable,
            ( unsigned ) context.connectionProperties.serverReceiveMax,
            ( unsigned long ) context.connectionProperties.sessionExpiry );
    printf( " retrieve=%u retrieved=", g_retrieve_calls );

    if( g_retrieved_len == 0U )
    {
        printf( "-" );
    }
    else
    {
        size_t k;

        for( k = 0U; k < g_retrieved_len; k++ )
        {
            printf( "%s%lu", ( k == 0U ) ? "" : "/",
                    ( unsigned long ) g_retrieved[ k ] );
        }
    }

    printf( " clear=%u cleared=", g_clear_calls );

    if( g_cleared_len == 0U )
    {
        printf( "-" );
    }
    else
    {
        size_t k;

        for( k = 0U; k < g_cleared_len; k++ )
        {
            printf( "%s%lu", ( k == 0U ) ? "" : "/",
                    ( unsigned long ) g_cleared[ k ] );
        }
    }

    printf( " rec0=%u/%d rec1=%u/%d", ( unsigned ) g_out_records[ 0 ].packetId,
            ( int ) g_out_records[ 0 ].publishState,
            ( unsigned ) g_out_records[ 1 ].packetId,
            ( int ) g_out_records[ 1 ].publishState );
    printf( "\n" );
}

int main( void )
{
    /* A minimal CONNACK: type, remaining length 3, flags, reason code, and a
     * property length of zero. */
    static const uint8_t PLAIN[] = { 0x20, 0x03, 0x00, 0x00, 0x00 };
    /* The same, with Session Present set. */
    static const uint8_t RESUMED[] = { 0x20, 0x03, 0x01, 0x00, 0x00 };
    /* A refusal: reason code 0x87, Not Authorized. */
    static const uint8_t REFUSED[] = { 0x20, 0x03, 0x00, 0x87, 0x00 };
    /* One that grants a smaller Receive Maximum than the context has records,
     * so the cap is visible. */
    static const uint8_t SMALL_RECV_MAX[] = {
        0x20, 0x06, 0x00, 0x00, 0x03, 0x21, 0x00, 0x02
    };
    /* One that sets Maximum QoS to 1, Retain Available to 0 and a server keep
     * alive of 30 -- three properties whose ABSENT value is not zero, so a
     * reader that forgot to default them would show here. */
    static const uint8_t LIMITS[] = {
        0x20, 0x0A, 0x00, 0x00, 0x07,
        0x24, 0x01,             /* Maximum QoS = 1 */
        0x25, 0x00,             /* Retain Available = 0 */
        0x13, 0x00, 0x1E        /* Server Keep Alive = 30 */
    };
    /* A packet that is not a CONNACK at all. */
    static const uint8_t NOT_CONNACK[] = { 0xD0, 0x00 };
    /* A CONNACK whose header promises more than it delivers. */
    static const uint8_t TRUNCATED[] = { 0x20, 0x03, 0x00 };
    /* One whose remaining length is 257, which will not fit a 256-byte network
     * buffer -- "MQTT spec doesn't allow 'dropping' packets", so the C gives up
     * rather than reading and discarding. */
    static const uint8_t TOO_LARGE[] = { 0x20, 0x81, 0x02, 0x00, 0x00, 0x00 };
    /* One carrying a Session Expiry Interval, which is the one property that
     * lands OUTSIDE the server half of the context. */
    static const uint8_t EXPIRY[] = {
        0x20, 0x08, 0x00, 0x00, 0x05, 0x11, 0x00, 0x00, 0x00, 0x3C
    };

    /* A CONNECT property section the application supplies: Session Expiry of
     * 60, and a Maximum Packet Size of 200 which fits the 256-byte buffer. */
    static const uint8_t APP_PROPS[] = {
        0x11, 0x00, 0x00, 0x00, 0x3C,
        0x27, 0x00, 0x00, 0x00, 0xC8
    };
    /* The same with a Maximum Packet Size LARGER than the network buffer,
     * which the C refuses. */
    static const uint8_t TOO_BIG[] = { 0x27, 0x00, 0x00, 0x10, 0x00 };
    static const uint8_t NO_PROPS[ 1 ] = { 0 };

    /* The receive scripts. The last step is held once the script runs out, so
     * `ALL` delivers everything on every call and `NONE` never delivers. */
    static const int32_t ALL[] = { 64 };
    static const int32_t ONE[] = { 1 };
    static const int32_t NONE[] = { 0 };
    static const int32_t FAIL[] = { -1 };
    /* Deliver, then nothing, then deliver: the only shape that separates a
     * polling timeout that RESETS on data from one that does not. */
    static const int32_t DRIBBLE[] = { 1, 1, 1, 0, 1, 0, 1 };
    /* The header arrives and then the transport dies, which is the only way
     * into `recvExact`'s own failure branch. */
    static const int32_t HEADER_THEN_FAIL[] = { 1, 1, -1 };

    printf( "geometry\n" );

    /* --- the ordinary connection, and what the CONNACK does to it --- */
    conn_case( "plain","c1",true,60,NO_PROPS,0U,false,0U,64,ALL, 1,0,PLAIN,sizeof PLAIN,false,false,false,0 );
    conn_case( "resumed","c1",false,60,NO_PROPS,0U,false,0U,64,ALL, 1,0,RESUMED,sizeof RESUMED,false,false,false,0 );
    conn_case( "refused","c1",true,60,NO_PROPS,0U,false,0U,64,ALL, 1,0,REFUSED,sizeof REFUSED,false,false,false,0 );
    /* A clean session that comes back resumed is the broker disagreeing. */
    conn_case( "clean-but-resumed","c1",true,60,NO_PROPS,0U,false,0U,64,ALL, 1,0,RESUMED,sizeof RESUMED,false,false,false,0 );
    conn_case( "server-limits","c1",true,60,NO_PROPS,0U,false,0U,64,ALL, 1,0,LIMITS,sizeof LIMITS,false,false,false,0 );
    conn_case( "small-receive-max","c1",true,60,NO_PROPS,0U,false,0U,64,ALL, 1,0,SMALL_RECV_MAX,sizeof SMALL_RECV_MAX,false,false,false,0 );

    /* --- the shapes of the CONNECT itself --- */
    conn_case( "empty-client-id","",true,60,NO_PROPS,0U,false,0U,64,ALL, 1,0,PLAIN,sizeof PLAIN,false,false,false,0 );
    conn_case( "with-will","c1",true,60,NO_PROPS,0U,true,0U,64,ALL, 1,0,PLAIN,sizeof PLAIN,false,false,false,0 );
    conn_case( "app-properties","c1",true,60,APP_PROPS,sizeof APP_PROPS,false,0U,64,ALL, 1,0,PLAIN,sizeof PLAIN,false,false,false,0 );
    conn_case( "max-packet-over-buffer","c1",true,60,TOO_BIG,sizeof TOO_BIG,false,0U,64,ALL, 1,0,PLAIN,sizeof PLAIN,false,false,false,0 );
    conn_case( "zero-keep-alive","c1",true,0,NO_PROPS,0U,false,0U,64,ALL, 1,0,PLAIN,sizeof PLAIN,false,false,false,0 );

    /* --- the transport, in pieces and failing --- */
    conn_case( "send-one-byte-at-a-time","c1",true,60,NO_PROPS,0U,false,0U,1,ALL, 1,0,PLAIN,sizeof PLAIN,false,false,false,0 );
    conn_case( "recv-one-byte-at-a-time","c1",true,60,NO_PROPS,0U,false,0U,64,ONE, 1,0,PLAIN,sizeof PLAIN,false,false,false,0 );
    conn_case( "send-fails","c1",true,60,NO_PROPS,0U,false,0U,-1,ALL, 1,0,PLAIN,sizeof PLAIN,false,false,false,0 );
    conn_case( "recv-fails","c1",true,60,NO_PROPS,0U,false,0U,64,FAIL, 1,0,PLAIN,sizeof PLAIN,false,false,false,0 );

    /* --- what arrives when it is not a CONNACK, or not all of one --- */
    conn_case( "not-a-connack","c1",true,60,NO_PROPS,0U,false,0U,64,ALL, 1,0,NOT_CONNACK,sizeof NOT_CONNACK,false,false,false,0 );
    /* The header promises three bytes and one arrives, so the body read polls
     * until its ten-millisecond timeout. The clock has to move for that to
     * end. */
    conn_case( "truncated-body","c1",true,60,NO_PROPS,0U,false,0U,64,ALL, 1,11,TRUNCATED,sizeof TRUNCATED,false,false,false,0 );

    /* --- the two timeouts, which are reached differently --- */
    /* Nothing to read at all, and no clock-based timeout: the retry count
     * decides, and it is checked BEFORE it is incremented. */
    conn_case( "no-connack-retry-count","c1",true,60,NO_PROPS,0U,false,0U,64,NONE, 1,0,PLAIN,sizeof PLAIN,false,false,false,0 );
    /* The same with a clock-based timeout instead. */
    conn_case( "no-connack-timeout","c1",true,60,NO_PROPS,0U,false,100U,64,NONE, 1,40,PLAIN,sizeof PLAIN,false,false,false,0 );

    /* --- already connected --- */
    conn_case( "already-connected","c1",true,60,NO_PROPS,0U,false,0U,64,ALL, 1,0,PLAIN,sizeof PLAIN,false,false,true,0 );

    /* --- a clean session clears the store, a resumed one reads it --- */
    conn_case( "clean-with-store","c1",true,60,NO_PROPS,0U,false,0U,64,ALL, 1,0,PLAIN,sizeof PLAIN,true,true,false,0 );
    conn_case( "resumed-with-store","c1",false,60,NO_PROPS,0U,false,0U,64,ALL, 1,0,RESUMED,sizeof RESUMED,true,true,false,0 );
    conn_case( "resumed-store-refuses","c1",false,60,NO_PROPS,0U,false,0U,64,ALL, 1,0,RESUMED,sizeof RESUMED,true,false,false,0 );

    /* --- and with two messages already in flight --- */
    conn_case( "clean-seeded-with-store","c1",true,60,NO_PROPS,0U,false,0U,64,ALL, 1,0,PLAIN,sizeof PLAIN,true,true,false,1 );
    conn_case( "clean-seeded-no-store","c1",true,60,NO_PROPS,0U,false,0U,64,ALL, 1,0,PLAIN,sizeof PLAIN,false,false,false,1 );
    conn_case( "resumed-seeded-with-store","c1",false,60,NO_PROPS,0U,false,0U,64,ALL, 1,0,RESUMED,sizeof RESUMED,true,true,false,1 );
    /* Without a store the PUBRELs go out as "default" acknowledgements built
     * from the state, and the PUBLISHes cannot go out at all. */
    conn_case( "resumed-seeded-no-store","c1",false,60,NO_PROPS,0U,false,0U,64,ALL, 1,0,RESUMED,sizeof RESUMED,false,false,false,1 );
    conn_case( "resumed-seeded-retrieve-fails","c1",false,60,NO_PROPS,0U,false,0U,64,ALL, 1,0,RESUMED,sizeof RESUMED,true,false,false,1 );

    /* A clean session with ONLY a PUBREL in flight: the clear callback should
     * see it and does not. */
    conn_case( "clean-pubrel-only-with-store","c1",true,60,NO_PROPS,0U,false,0U,64,ALL, 1,0,PLAIN,sizeof PLAIN,true,true,false,2 );
    conn_case( "resumed-pubrel-only-no-store","c1",false,60,NO_PROPS,0U,false,0U,64,ALL, 1,0,RESUMED,sizeof RESUMED,false,false,false,2 );

    /* --- the polling timeout, which resets on every byte --- */
    /* Data, nothing, data...: with a nine-millisecond step each GAP is under
     * the ten-millisecond timeout and the total is not, so a timeout that does
     * not reset gives up part-way and this one does not. */
    conn_case( "dribbled-body", "c1", true, 60, NO_PROPS, 0U, false, 0U, 64,
               DRIBBLE, 7, 9, PLAIN, sizeof PLAIN, false, false, false, 0 );
    /* A step that lands the gap EXACTLY on the timeout. */
    conn_case( "dribbled-on-the-boundary", "c1", true, 60, NO_PROPS, 0U, false,
               0U, 64, DRIBBLE, 7, 10, PLAIN, sizeof PLAIN, false, false, false,
               0 );
    /* The header arrives and then the transport fails, which is the only path
     * into `recvExact`'s own failure branch -- and the only case where it sets
     * the connection to disconnect-pending. */
    conn_case( "body-fails", "c1", true, 60, NO_PROPS, 0U, false, 0U, 64,
               HEADER_THEN_FAIL, 3, 0, PLAIN, sizeof PLAIN, false, false, false,
               0 );
    /* A packet bigger than the buffer. */
    conn_case( "connack-too-large", "c1", true, 60, NO_PROPS, 0U, false, 0U, 64,
               ALL, 1, 0, TOO_LARGE, sizeof TOO_LARGE, false, false, false, 0 );
    /* A clock-based timeout reached EXACTLY. */
    conn_case( "connack-timeout-on-the-boundary", "c1", true, 60, NO_PROPS, 0U,
               false, 100U, 64, NONE, 1, 50, PLAIN, sizeof PLAIN, false, false,
               false, 0 );
    /* The one property that is not in the server half. */
    conn_case( "session-expiry", "c1", true, 60, NO_PROPS, 0U, false, 0U, 64,
               ALL, 1, 0, EXPIRY, sizeof EXPIRY, false, false, false, 0 );
    /* An empty client identifier followed by a will, so the empty string is no
     * longer the LAST vector -- which is the only position where an extra
     * zero-length vector can be seen. */
    conn_case( "empty-client-id-with-will", "", true, 60, NO_PROPS, 0U, true, 0U,
               64, ALL, 1, 0, PLAIN, sizeof PLAIN, false, false, false, 0 );

    printf( "end\n" );

    return 0;
}
