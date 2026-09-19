/* The C arm of K7's receive-loop differential.
 *
 * `MQTT_ProcessLoop` and `MQTT_ReceiveLoop` are one pass over whatever the
 * transport has, so this driver scripts the transport in both directions, the
 * clock, and the APPLICATION CALLBACK -- which is the third input, and the one
 * a status alone cannot see. The callback logs every packet it is handed, says
 * whether it was offered somewhere to put a reply, and can be told to refuse,
 * to set a reason code, or to add a property.
 *
 * What comes out is the status, both call logs, the bytes that went back, the
 * BUFFER INDEX, the connection, the records, and the store's keys. */

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "core_mqtt.h"
#include "core_mqtt_state.h"

#define MAX_LOG      4096
#define MAX_SENT     512
#define MAX_IN       512
#define MAX_RECV_STEPS  8

/* ---- the scripted transport ------------------------------------------------ */

static int32_t g_send_step;
static int32_t g_recv_script[ MAX_RECV_STEPS ];
static size_t g_recv_steps;
static size_t g_recv_at;

static uint8_t g_in[ MAX_IN ];
static size_t g_in_len;
static size_t g_in_pos;

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

static void log_pair( char *log, size_t *len, size_t cap, unsigned a, long b )
{
    int n = snprintf( &log[ *len ], cap - *len, "%s%u:%ld",
                      ( *len == 0U ) ? "" : ",", a, b );

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

    log_pair( g_slog, &g_slog_len, sizeof g_slog, ( unsigned ) bytes, ( long ) answer );

    if( ( answer > 0 ) && ( ( g_sent_len + ( size_t ) answer ) <= sizeof g_sent ) )
    {
        memcpy( &g_sent[ g_sent_len ], buffer, ( size_t ) answer );
        g_sent_len += ( size_t ) answer;
    }

    return answer;
}

static int32_t scripted_recv( NetworkContext_t *c, void *buffer, size_t bytes )
{
    int32_t answer;
    size_t left = g_in_len - g_in_pos;

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

    log_pair( g_rlog, &g_rlog_len, sizeof g_rlog, ( unsigned ) bytes, ( long ) answer );

    if( answer > 0 )
    {
        memcpy( buffer, &g_in[ g_in_pos ], ( size_t ) answer );
        g_in_pos += ( size_t ) answer;
    }

    return answer;
}

static uint32_t scripted_time( void )
{
    uint32_t now = g_now;

    g_now += g_clock_step;

    return now;
}

/* ---- the application callback ---------------------------------------------- */

/* What the application is told to do. */
static bool g_cb_accept;
static int  g_cb_reason;      /* -1 for "set none" */
static bool g_cb_add_property;

static char g_cblog[ MAX_LOG ];
static size_t g_cblog_len;
static unsigned g_cbcalls;

/* `pReasonCode` is set by `handlePublishAcks`, `handleSubUnsubAck` and
 * `handleIncomingDisconnect` and NOT by `handleIncomingPublish` or the PINGRESP
 * branch -- which declare `MQTTDeserializedInfo_t deserializedInfo;` with no
 * initialiser and fill in three of its four members. Reading it for those two
 * is reading the stack, and this driver printed four different garbage values
 * before it was noticed. So the count is printed as `?` where the library never
 * wrote the pointer, and the trace says so rather than pretending. */
static bool sets_reason_code( unsigned type )
{
    switch( type & 0xF0U )
    {
        case 0x40U: /* PUBACK  */
        case 0x50U: /* PUBREC  */
        case 0x60U: /* PUBREL  */
        case 0x70U: /* PUBCOMP */
        case 0x90U: /* SUBACK  */
        case 0xB0U: /* UNSUBACK */
        case 0xE0U: /* DISCONNECT */
            return true;

        default:
            return false;
    }
}

static void log_event( unsigned type, unsigned id, bool offeredReply,
                       const MQTTReasonCodeInfo_t *codes )
{
    int n;

    if( sets_reason_code( type ) )
    {
        unsigned reasons = ( codes == NULL ) ? 0U : ( unsigned ) codes->reasonCodeLength;

        n = snprintf( &g_cblog[ g_cblog_len ], sizeof g_cblog - g_cblog_len,
                      "%s%02x/%u/%u/%u", ( g_cblog_len == 0U ) ? "" : ",",
                      type, id, offeredReply ? 1U : 0U, reasons );
    }
    else
    {
        n = snprintf( &g_cblog[ g_cblog_len ], sizeof g_cblog - g_cblog_len,
                      "%s%02x/%u/%u/?", ( g_cblog_len == 0U ) ? "" : ",",
                      type, id, offeredReply ? 1U : 0U );
    }

    if( ( n > 0 ) && ( ( size_t ) n < ( sizeof g_cblog - g_cblog_len ) ) )
    {
        g_cblog_len += ( size_t ) n;
    }
}

static bool app_callback( MQTTContext_t *context, MQTTPacketInfo_t *packet,
                          MQTTDeserializedInfo_t *info,
                          MQTTSuccessFailReasonCode_t *reasonCode,
                          MQTTPropBuilder_t *ackProps,
                          MQTTPropBuilder_t *incomingProps )
{
    ( void ) context; ( void ) incomingProps;

    g_cbcalls++;

    log_event( ( unsigned ) packet->type,
               ( unsigned ) info->packetIdentifier,
               ( reasonCode != NULL ) || ( ackProps != NULL ),
               info->pReasonCode );

    if( ( g_cb_reason >= 0 ) && ( reasonCode != NULL ) )
    {
        *reasonCode = ( MQTTSuccessFailReasonCode_t ) g_cb_reason;
    }

    if( g_cb_add_property && ( ackProps != NULL ) )
    {
        /* A Reason String, which every publish acknowledgement may carry. */
        ( void ) MQTTPropAdd_ReasonString( ackProps, "no", 2U, NULL );
    }

    return g_cb_accept;
}

/* ---- the retransmit store -------------------------------------------------- */

static bool g_have_store;
static bool g_store_answer;
static unsigned g_store_calls;
static unsigned g_clear_calls;
static uint32_t g_keys[ 16 ];
static size_t g_keys_len;

static void note_key( uint32_t key )
{
    if( g_keys_len < ( sizeof g_keys / sizeof g_keys[ 0 ] ) )
    {
        g_keys[ g_keys_len++ ] = key;
    }
}

static bool store_packet( MQTTContext_t *context, uint32_t packetId, MQTTVec_t *vec )
{
    ( void ) context; ( void ) vec;

    g_store_calls++;
    note_key( packetId );

    return g_store_answer;
}

static bool retrieve_packet( MQTTContext_t *context, uint32_t packetId,
                             uint8_t **pPacket, size_t *pLength )
{
    ( void ) context; ( void ) packetId; ( void ) pPacket; ( void ) pLength;
    return false;
}

static void clear_packet( MQTTContext_t *context, uint32_t packetId )
{
    ( void ) context;

    g_clear_calls++;
    note_key( packetId );
}

/* ---- the context ----------------------------------------------------------- */

static uint8_t g_network[ 64 ];
static uint8_t g_ack_props[ 32 ];
static MQTTPubAckInfo_t g_out_records[ 4 ];
static MQTTPubAckInfo_t g_in_records[ 4 ];

static void reset( int32_t sendStep, const int32_t *recvScript, size_t recvSteps,
                   uint32_t clockStep, const uint8_t *incoming, size_t incomingLen )
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
    g_now = 0U;

    g_scalls = 0U;
    g_rcalls = 0U;
    g_cbcalls = 0U;
    g_sent_len = 0U;
    g_slog[ 0 ] = '\0';
    g_slog_len = 0U;
    g_rlog[ 0 ] = '\0';
    g_rlog_len = 0U;
    g_cblog[ 0 ] = '\0';
    g_cblog_len = 0U;

    memset( g_in, 0, sizeof g_in );
    g_in_len = ( incomingLen <= sizeof g_in ) ? incomingLen : sizeof g_in;
    memcpy( g_in, incoming, g_in_len );
    g_in_pos = 0U;

    g_store_calls = 0U;
    g_clear_calls = 0U;
    g_keys_len = 0U;
    memset( g_keys, 0, sizeof g_keys );
    memset( g_ack_props, 0, sizeof g_ack_props );
}

/* An acknowledgement for a message there is no record of is
 * `MQTTBadResponse` -- "a duplicate acknowledgement, or a forged one". So every
 * ack case has to put its subject in flight first, and WHICH array it goes in
 * is half the test: a PUBREL answers an INCOMING publish and the other three
 * answer outgoing ones. */
static void seed_records( int kind )
{
    switch( kind )
    {
        case 1: /* an outgoing QoS 1 publish awaiting its PUBACK */
            g_out_records[ 0 ].packetId = 5U;
            g_out_records[ 0 ].qos = MQTTQoS1;
            g_out_records[ 0 ].publishState = MQTTPubAckPending;
            break;

        case 2: /* an outgoing QoS 2 publish awaiting its PUBREC */
            g_out_records[ 0 ].packetId = 5U;
            g_out_records[ 0 ].qos = MQTTQoS2;
            g_out_records[ 0 ].publishState = MQTTPubRecPending;
            break;

        case 3: /* an INCOMING QoS 2 publish awaiting the broker's PUBREL */
            g_in_records[ 0 ].packetId = 6U;
            g_in_records[ 0 ].qos = MQTTQoS2;
            g_in_records[ 0 ].publishState = MQTTPubRelPending;
            break;

        case 4: /* an outgoing PUBREL awaiting its PUBCOMP */
            g_out_records[ 0 ].packetId = 6U;
            g_out_records[ 0 ].qos = MQTTQoS2;
            g_out_records[ 0 ].publishState = MQTTPubCompPending;
            break;

        case 5: /* an INCOMING QoS 1 publish already on file, so the next one
                 * with the same identifier collides */
            g_in_records[ 0 ].packetId = 5U;
            g_in_records[ 0 ].qos = MQTTQoS1;
            g_in_records[ 0 ].publishState = MQTTPubAckSend;
            break;

        case 6: /* the same at QoS 2 */
            g_in_records[ 0 ].packetId = 6U;
            g_in_records[ 0 ].qos = MQTTQoS2;
            g_in_records[ 0 ].publishState = MQTTPubRecSend;
            break;

        default:
            break;
    }
}

static void fresh( MQTTContext_t *context, bool useAckProps )
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
    ( void ) MQTT_Init( context, &transport, scripted_time, app_callback, &network );
    ( void ) MQTT_InitStatefulQoS( context, g_out_records, 4U, g_in_records, 4U,
                                   useAckProps ? g_ack_props : NULL,
                                   useAckProps ? sizeof g_ack_props : 0U );

    if( g_have_store )
    {
        ( void ) MQTT_InitRetransmits( context, store_packet, retrieve_packet,
                                       clear_packet );
    }

    context->connectStatus = MQTTConnected;
    context->keepAliveIntervalSec = 60U;
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
        case MQTTSuccess:                 return "Success";
        case MQTTBadParameter:            return "BadParameter";
        case MQTTNoMemory:                return "NoMemory";
        case MQTTSendFailed:              return "SendFailed";
        case MQTTRecvFailed:              return "RecvFailed";
        case MQTTBadResponse:             return "BadResponse";
        case MQTTServerRefused:           return "ServerRefused";
        case MQTTNoDataAvailable:         return "NoDataAvailable";
        case MQTTIllegalState:            return "IllegalState";
        case MQTTStateCollision:          return "StateCollision";
        case MQTTKeepAliveTimeout:        return "KeepAliveTimeout";
        case MQTTNeedMoreBytes:           return "NeedMoreBytes";
        case MQTTStatusConnected:         return "StatusConnected";
        case MQTTStatusNotConnected:      return "StatusNotConnected";
        case MQTTStatusDisconnectPending: return "StatusDisconnectPending";
        case MQTTPublishStoreFailed:      return "PublishStoreFailed";
        case MQTTPublishRetrieveFailed:   return "PublishRetrieveFailed";
        case MQTTEventCallbackFailed:     return "EventCallbackFailed";
        default:                          return "?";
    }
}

/* ---- the cases -------------------------------------------------------------- */

static void loop_case( const char *name, bool process, const uint8_t *incoming,
                       size_t incomingLen, int32_t sendStep,
                       const int32_t *recvScript, size_t recvSteps,
                       uint32_t clockStep, bool accept, int reason,
                       bool addProperty, bool useAckProps, bool haveStore,
                       bool storeAnswer, bool waitingPing, uint32_t lastTx,
                       uint32_t lastRx, int seed, uint32_t pingTime, int calls )
{
    MQTTContext_t context;
    MQTTStatus_t status;

    g_have_store = haveStore;
    g_store_answer = storeAnswer;
    g_cb_accept = accept;
    g_cb_reason = reason;
    g_cb_add_property = addProperty;

    reset( sendStep, recvScript, recvSteps, clockStep, incoming, incomingLen );
    fresh( &context, useAckProps );
    seed_records( seed );

    context.waitingForPingResp = waitingPing;
    context.lastPacketTxTime = lastTx;
    context.lastPacketRxTime = lastRx;
    context.pingReqSendTimeMs = pingTime;

    /* Two calls where a packet arrives in pieces: the first ends in
     * `MQTTNeedMoreBytes` and the SECOND has to read into the middle of the
     * buffer. Nothing in a single call can tell a reader that starts at the
     * index from one that starts at the front. */
    {
        int call;

        status = MQTTSuccess;

        for( call = 0; call < calls; call++ )
        {
            status = process ? MQTT_ProcessLoop( &context )
                             : MQTT_ReceiveLoop( &context );
        }
    }

    printf( "%s %s in=", process ? "proc" : "recv", name );
    put_hex( incoming, incomingLen );
    printf( " sstep=%ld rscript=", ( long ) sendStep );

    {
        size_t k;

        for( k = 0U; k < recvSteps; k++ )
        {
            printf( "%s%ld", ( k == 0U ) ? "" : "/", ( long ) recvScript[ k ] );
        }
    }

    printf( " cstep=%lu cb=%u/%d/%u props=%u store=%u/%u ping=%u tx=%lu rx=%lu",
            ( unsigned long ) clockStep, accept ? 1U : 0U, reason,
            addProperty ? 1U : 0U, useAckProps ? 1U : 0U, haveStore ? 1U : 0U,
            storeAnswer ? 1U : 0U, waitingPing ? 1U : 0U,
            ( unsigned long ) lastTx, ( unsigned long ) lastRx );
    printf( " seed=%d pingtime=%lu calls=%d", seed, ( unsigned long ) pingTime, calls );

    printf( " -> %s state=%d index=%lu", status_name( status ),
            ( int ) context.connectStatus, ( unsigned long ) context.index );
    printf( " scalls=%u slog=%s sbytes=", g_scalls,
            ( g_slog_len == 0U ) ? "-" : g_slog );
    put_hex( g_sent, g_sent_len );
    printf( " rcalls=%u rlog=%s", g_rcalls, ( g_rlog_len == 0U ) ? "-" : g_rlog );
    printf( " cbcalls=%u cblog=%s", g_cbcalls,
            ( g_cblog_len == 0U ) ? "-" : g_cblog );
    printf( " waiting=%u sent=%u", context.waitingForPingResp ? 1U : 0U,
            context.controlPacketSent ? 1U : 0U );
    printf( " out0=%u/%d in0=%u/%d",
            ( unsigned ) g_out_records[ 0 ].packetId,
            ( int ) g_out_records[ 0 ].publishState,
            ( unsigned ) g_in_records[ 0 ].packetId,
            ( int ) g_in_records[ 0 ].publishState );
    printf( " store=%u clear=%u keys=", g_store_calls, g_clear_calls );

    if( g_keys_len == 0U )
    {
        printf( "-" );
    }
    else
    {
        size_t k;

        for( k = 0U; k < g_keys_len; k++ )
        {
            printf( "%s%lu", ( k == 0U ) ? "" : "/", ( unsigned long ) g_keys[ k ] );
        }
    }

    printf( "\n" );
    fflush( stdout );
}

int main( void )
{
    /* --- the packets --- */
    /* A QoS 0 PUBLISH of "hi" to "a": 30 07 | 0001 61 | 00 | 6869 */
    static const uint8_t PUB0[] = { 0x30, 0x06, 0x00, 0x01, 'a', 0x00, 'h', 'i' };
    /* QoS 1, packet id 5. */
    static const uint8_t PUB1[] = { 0x32, 0x08, 0x00, 0x01, 'a', 0x00, 0x05, 0x00, 'h', 'i' };
    /* The same with DUP set. */
    static const uint8_t PUB1_DUP[] = { 0x3A, 0x08, 0x00, 0x01, 'a', 0x00, 0x05, 0x00, 'h', 'i' };
    /* QoS 2, packet id 6. */
    static const uint8_t PUB2[] = { 0x34, 0x08, 0x00, 0x01, 'a', 0x00, 0x06, 0x00, 'h', 'i' };
    /* Two QoS 0 publishes back to back, to prove one read handles both. */
    /* Two QoS 1 publishes with DIFFERENT packet identifiers -- because two
     * that differ only in their topic are indistinguishable in the callback
     * log, and a sender that handled the first one twice would look right. */
    static const uint8_t PUB1_TWICE[] = {
        0x32, 0x08, 0x00, 0x01, 'a', 0x00, 0x05, 0x00, 'h', 'i',
        0x32, 0x08, 0x00, 0x01, 'b', 0x00, 0x07, 0x00, 'h', 'i'
    };
    /* The acknowledgements, with a two-byte body (packet id only). */
    static const uint8_t PUBACK[]  = { 0x40, 0x02, 0x00, 0x05 };
    static const uint8_t PUBREC[]  = { 0x50, 0x02, 0x00, 0x05 };
    static const uint8_t PUBREL[]  = { 0x62, 0x02, 0x00, 0x06 };
    static const uint8_t PUBCOMP[] = { 0x70, 0x02, 0x00, 0x06 };
    static const uint8_t SUBACK[]  = { 0x90, 0x04, 0x00, 0x01, 0x00, 0x00 };
    static const uint8_t UNSUBACK[] = { 0xB0, 0x04, 0x00, 0x01, 0x00, 0x00 };
    static const uint8_t PINGRESP[] = { 0xD0, 0x00 };
    /* A DISCONNECT with a reason code, and one that will not parse. */
    static const uint8_t DISCONNECT[] = { 0xE0, 0x02, 0x00, 0x00 };
    static const uint8_t DISCONNECT_BAD[] = { 0xE0, 0x02, 0xFF, 0x00 };
    /* A packet type a client may not receive. */
    static const uint8_t BAD_TYPE[] = { 0x10, 0x02, 0x00, 0x00 };
    /* A packet that will not fit the 64-byte network buffer. */
    static const uint8_t TOO_LARGE[] = { 0x30, 0x7F, 0x00, 0x01, 'a', 0x00 };
    static const uint8_t NOTHING[ 1 ] = { 0 };
    /* A QoS 1 publish whose identifier is already on file, so the state engine
     * answers `MQTTStateCollision` and the duplicate path runs. */
    static const uint8_t PUB1_AGAIN[] = { 0x32, 0x08, 0x00, 0x01, 'a', 0x00, 0x05, 0x00, 'h', 'i' };
    static const uint8_t PUB2_AGAIN[] = { 0x34, 0x08, 0x00, 0x01, 'a', 0x00, 0x06, 0x00, 'h', 'i' };

    /* --- the receive scripts --- */
    static const int32_t ALL[] = { 64 };
    static const int32_t NONE[] = { 0 };
    static const int32_t FAIL[] = { -1 };
    static const int32_t ONE[] = { 1 };
    /* Half a packet, then the rest -- but ONE call per loop pass, so the first
     * pass ends in NeedMoreBytes and the second finishes it. */
    static const int32_t HALF[] = { 4 };

    printf( "geometry\n" );

    /* --- nothing to read --- */
    loop_case( "idle", true, NOTHING, 0U, 64, NONE, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );
    loop_case( "recv-fails", true, PUB0, sizeof PUB0, 64, FAIL, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );

    /* --- the publishes --- */
    loop_case( "qos0", true, PUB0, sizeof PUB0, 64, ALL, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );
    loop_case( "qos1", true, PUB1, sizeof PUB1, 64, ALL, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );
    loop_case( "qos2", true, PUB2, sizeof PUB2, 64, ALL, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );
    loop_case( "qos1-callback-refuses", true, PUB1, sizeof PUB1, 64, ALL, 1, 0, false, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );
    loop_case( "qos0-callback-refuses", true, PUB0, sizeof PUB0, 64, ALL, 1, 0, false, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );
    /* The application sets a reason code, which routes to the OTHER sender. */
    loop_case( "qos1-with-reason", true, PUB1, sizeof PUB1, 64, ALL, 1, 0, true, 0x00, false, true, false, false, false, 0U, 0U, 0, 0U, 1 );
    loop_case( "qos1-with-property", true, PUB1, sizeof PUB1, 64, ALL, 1, 0, true, -1, true, true, false, false, false, 0U, 0U, 0, 0U, 1 );
    /* A reason code the type may not carry: 0x92 is for PUBREL and PUBCOMP. */
    loop_case( "qos1-bad-reason", true, PUB1, sizeof PUB1, 64, ALL, 1, 0, true, 0x92, false, true, false, false, false, 0U, 0U, 0, 0U, 1 );
    /* The store sees a PUBREC and a PUBREL and not a PUBACK. */
    loop_case( "qos1-with-store", true, PUB1, sizeof PUB1, 64, ALL, 1, 0, true, -1, false, false, true, true, false, 0U, 0U, 0, 0U, 1 );
    loop_case( "qos2-with-store", true, PUB2, sizeof PUB2, 64, ALL, 1, 0, true, -1, false, false, true, true, false, 0U, 0U, 0, 0U, 1 );
    loop_case( "qos2-store-refuses", true, PUB2, sizeof PUB2, 64, ALL, 1, 0, true, -1, false, false, true, false, false, 0U, 0U, 0, 0U, 1 );
    /* A send that fails after the callback has already been told. */
    loop_case( "qos1-send-fails", true, PUB1, sizeof PUB1, -1, ALL, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );
    loop_case( "qos1-dup", true, PUB1_DUP, sizeof PUB1_DUP, 64, ALL, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );

    /* --- reassembly --- */
    loop_case( "two-publishes-one-read", true, PUB1_TWICE, sizeof PUB1_TWICE, 64, ALL, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );
    loop_case( "half-a-publish", true, PUB0, sizeof PUB0, 64, HALF, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );
    loop_case( "one-byte-at-a-time", true, PUB0, sizeof PUB0, 64, ONE, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );

    /* --- the acknowledgements --- */
    loop_case( "puback", true, PUBACK, sizeof PUBACK, 64, ALL, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 1, 0U, 1 );
    loop_case( "pubrec", true, PUBREC, sizeof PUBREC, 64, ALL, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 2, 0U, 1 );
    loop_case( "pubrel", true, PUBREL, sizeof PUBREL, 64, ALL, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 3, 0U, 1 );
    loop_case( "pubcomp", true, PUBCOMP, sizeof PUBCOMP, 64, ALL, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 4, 0U, 1 );
    loop_case( "puback-with-store", true, PUBACK, sizeof PUBACK, 64, ALL, 1, 0, true, -1, false, false, true, true, false, 0U, 0U, 1, 0U, 1 );
    loop_case( "pubcomp-with-store", true, PUBCOMP, sizeof PUBCOMP, 64, ALL, 1, 0, true, -1, false, false, true, true, false, 0U, 0U, 4, 0U, 1 );
    loop_case( "suback", true, SUBACK, sizeof SUBACK, 64, ALL, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );
    loop_case( "unsuback", true, UNSUBACK, sizeof UNSUBACK, 64, ALL, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );
    loop_case( "suback-callback-refuses", true, SUBACK, sizeof SUBACK, 64, ALL, 1, 0, false, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );

    /* --- the PINGRESP, which the two loops treat differently --- */
    loop_case( "pingresp", true, PINGRESP, sizeof PINGRESP, 64, ALL, 1, 0, true, -1, false, false, false, false, true, 0U, 0U, 0, 0U, 1 );
    loop_case( "pingresp", false, PINGRESP, sizeof PINGRESP, 64, ALL, 1, 0, true, -1, false, false, false, false, true, 0U, 0U, 0, 0U, 1 );

    /* --- the DISCONNECT --- */
    loop_case( "disconnect", true, DISCONNECT, sizeof DISCONNECT, 64, ALL, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );
    loop_case( "disconnect-malformed", true, DISCONNECT_BAD, sizeof DISCONNECT_BAD, 64, ALL, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );
    loop_case( "disconnect-callback-refuses", true, DISCONNECT, sizeof DISCONNECT, 64, ALL, 1, 0, false, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );

    /* --- what a client may not receive, and what will not fit --- */
    loop_case( "bad-packet-type", true, BAD_TYPE, sizeof BAD_TYPE, 64, ALL, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );
    loop_case( "too-large", true, TOO_LARGE, sizeof TOO_LARGE, 64, ALL, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );

    /* --- keep alive, which only runs when nothing arrived --- */
    /* Nothing to read and the transmitter has been silent past the interval. */
    loop_case( "keepalive-tx-timeout", true, NOTHING, 0U, 64, NONE, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );
    /* The same in the receive loop, which does not manage it. */
    loop_case( "keepalive-tx-timeout", false, NOTHING, 0U, 64, NONE, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );
    /* A PINGRESP already owed, and the clock past the five-second limit. */
    loop_case( "keepalive-pingresp-timeout", true, NOTHING, 0U, 64, NONE, 1, 6000, true, -1, false, false, false, false, true, 0U, 0U, 0, 0U, 1 );
    /* Owed, and NOT past it. */
    loop_case( "keepalive-pingresp-waiting", true, NOTHING, 0U, 64, NONE, 1, 1, true, -1, false, false, false, false, true, 0U, 0U, 0, 0U, 1 );
    /* A busy transmitter: the receiver's own thirty-second timeout. */
    loop_case( "keepalive-rx-timeout", true, NOTHING, 0U, 64, NONE, 1, 0, true, -1, false, false, false, false, false, 0xFFFFFFFFU, 1U, 0, 0U, 1 );
    /* And a packet arriving means keep alive is NOT checked. */
    loop_case( "keepalive-not-checked-when-busy", true, PUB0, sizeof PUB0, 64, ALL, 1, 0, true, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 1 );

    /* --- the gaps the poisons found --- */

    /* A packet in two pieces across TWO calls: the second read has to land at
     * the index, not at the front. */
    loop_case( "half-then-the-rest", true, PUB0, sizeof PUB0, 64, HALF, 1, 0,
               true, -1, false, false, false, false, false, 0U, 0U, 0, 0U, 2 );

    /* A publish whose identifier is already on file: the state engine says
     * collision, and a QoS 1 duplicate is STILL handed to the application
     * [MQTT-4.3.2-5] while a QoS 2 one is not. */
    loop_case( "qos1-collision", true, PUB1_AGAIN, sizeof PUB1_AGAIN, 64, ALL, 1,
               0, true, -1, false, false, false, false, false, 0U, 0U, 5, 0U, 1 );
    loop_case( "qos2-collision", true, PUB2_AGAIN, sizeof PUB2_AGAIN, 64, ALL, 1,
               0, true, -1, false, false, false, false, false, 0U, 0U, 6, 0U, 1 );

    /* A reason code the SENT acknowledgement may not carry: a PUBREL arrives,
     * a PUBCOMP goes back, and 0x87 is for a PUBACK or a PUBREC. */
    loop_case( "pubrel-bad-reason", true, PUBREL, sizeof PUBREL, 64, ALL, 1, 0,
               true, 0x87, false, true, false, false, false, 0U, 0U, 3, 0U, 1 );
    /* And one it may: 0x92 is for a PUBREL or a PUBCOMP. */
    loop_case( "pubrel-good-reason", true, PUBREL, sizeof PUBREL, 64, ALL, 1, 0,
               true, 0x92, false, true, false, false, false, 0U, 0U, 3, 0U, 1 );

    /* TWO acknowledgements with properties in one pass. Nothing else can see
     * whether the property buffer is emptied between them: with one
     * acknowledgement a buffer that is never emptied looks identical. */
    loop_case( "two-publishes-with-properties", true, PUB1_TWICE, sizeof PUB1_TWICE,
               64, ALL, 1, 0, true, 0x00, true, true, false, false, false, 0U,
               0U, 0, 0U, 1 );

    /* The PINGRESP timeout, exactly on the boundary and one past it. The clock
     * reads zero first, so the send time is set BELOW zero to make the
     * subtraction wrap -- which is the same arithmetic the send timeout uses. */
    loop_case( "pingresp-on-the-boundary", true, NOTHING, 0U, 64, NONE, 1, 0,
               true, -1, false, false, false, false, true, 0U, 0U, 0,
               0xFFFFEC78U, 1 );
    loop_case( "pingresp-one-past", true, NOTHING, 0U, 64, NONE, 1, 0, true, -1,
               false, false, false, false, true, 0U, 0U, 0, 0xFFFFEC77U, 1 );

    /* Forty seconds since anything was sent: past the thirty-second clamp and
     * under the sixty-second keep alive, so only a CLAMPED timeout pings. */
    loop_case( "keepalive-clamped-to-thirty", true, NOTHING, 0U, 64, NONE, 1, 0,
               true, -1, false, false, false, false, false, 0xFFFF6398U, 0U, 0,
               0U, 1 );

    /* A PINGRESP already owed AND a stale transmit time: the first branch has
     * to stop the second from sending another PINGREQ. */
    loop_case( "pingresp-owed-and-stale", true, NOTHING, 0U, 64, NONE, 1, 0,
               true, -1, false, false, false, false, true, 0xFFFF6398U, 0U, 0,
               0xFFFFFFFFU, 1 );

    printf( "end\n" );

    return 0;
}
