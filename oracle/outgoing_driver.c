/* The C arm of K7's coreMQTT OUTGOING-PACKET differential.
 *
 * `core_mqtt.c` is compiled VERBATIM out of the pinned checkout; nothing here
 * copies or edits it.
 *
 * # The packets a client builds from the caller's own buffers
 *
 * SUBSCRIBE, UNSUBSCRIBE and PUBLISH are assembled WITHOUT COPYING: the topic
 * filters and the payload stay where the application put them, and the library
 * describes the packet as an array of pointers for the transport to gather. So
 * what is proven here is the array -- how many vectors, in what order, with
 * what in them -- and the only way to see that from outside is the bytes that
 * reach the wire and the sequence of offers that carried them.
 *
 * # One call per topic filter, and that is not an accident
 *
 * `MQTT_SUB_UNSUB_MAX_VECTORS` defaults to 4 and a topic costs 3 vectors
 * (length, filter, options), so the inner loop's guard
 * `ioVectorLength <= 4 - 3` is false as soon as the header and the property
 * length are in place. A SUBSCRIBE therefore goes out as ONE `sendMessageVector`
 * call for its header, then one per filter -- the packet is split across
 * several gathers of the same stream. The call log is the only place that is
 * visible, and a rewrite that buffered the packet would pass a bytes-only
 * comparison.
 *
 * # And the store callback, which is the only door to two public functions
 *
 * `MQTT_GetBytesInMQTTVec` and `MQTT_SerializeMQTTVec` take an `MQTTVec_t`,
 * which the public header declares and does not define. The only place an
 * application ever holds one is inside `MQTTStorePacketForRetransmit`, which
 * `sendPublishWithoutCopy` invokes for a QoS 1 or 2 publish when retransmits
 * are enabled. So the driver registers one, and prints what it serialized.
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core_mqtt.h"

#define MAX_SENT      512
#define MAX_LOG       512
#define MAX_STORED    512

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
        case MQTTPublishStoreFailed:      return "PublishStoreFailed";
        case MQTTPublishRetrieveFailed:   return "PublishRetrieveFailed";
        case MQTTRecvFailed:              return "RecvFailed";
        case MQTTBadResponse:             return "BadResponse";
        case MQTTServerRefused:           return "ServerRefused";
        case MQTTNoDataAvailable:         return "NoDataAvailable";
        case MQTTIllegalState:            return "IllegalState";
        case MQTTStateCollision:          return "StateCollision";
        case MQTTKeepAliveTimeout:        return "KeepAliveTimeout";
        case MQTTNeedMoreBytes:           return "NeedMoreBytes";
        case MQTTEndOfProperties:         return "EndOfProperties";
        default:                          return "OTHER";
    }
}

/* ---- the scripted transport, as the send slice built it -------------------- */

static int32_t g_step_value;
static size_t g_calls;
static uint8_t g_sent[ MAX_SENT ];
static size_t g_sent_len;
static char g_log[ MAX_LOG ];
static size_t g_log_len;
static uint32_t g_now;

static void log_call( size_t offered, int32_t answered )
{
    int n;

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

static int32_t scripted_send( NetworkContext_t *c, const void *buffer, size_t bytes )
{
    int32_t answer = g_step_value;

    ( void ) c;
    g_calls++;

    if( answer > ( int32_t ) bytes )
    {
        answer = ( int32_t ) bytes;
    }

    log_call( bytes, answer );

    if( ( answer > 0 ) && ( ( g_sent_len + ( size_t ) answer ) <= sizeof g_sent ) )
    {
        memcpy( &g_sent[ g_sent_len ], buffer, ( size_t ) answer );
        g_sent_len += ( size_t ) answer;
    }

    return answer;
}

/* `TransportInterface_t.writev`, which coreMQTT calls with the vectors that
 * are still outstanding and a COUNT. The count is the whole point: it is the
 * only thing that makes a gather boundary visible from outside the library.
 * With the per-vector fallback, a packet split into two gathers and the same
 * packet split into three produce the identical sequence of calls. */
static int32_t scripted_writev( NetworkContext_t *c, TransportOutVector_t *vec,
                                size_t count )
{
    int32_t answer = g_step_value;
    size_t bytes = 0U;
    size_t i;
    int n;

    ( void ) c;
    g_calls++;

    for( i = 0U; i < count; i++ )
    {
        bytes += vec[ i ].iov_len;
    }

    if( answer > ( int32_t ) bytes )
    {
        answer = ( int32_t ) bytes;
    }

    n = snprintf( &g_log[ g_log_len ], sizeof g_log - g_log_len, "%sv%u/%u:%ld",
                  ( g_log_len == 0U ) ? "" : ",", ( unsigned ) count,
                  ( unsigned ) bytes, ( long ) answer );

    if( ( n > 0 ) && ( ( size_t ) n < ( sizeof g_log - g_log_len ) ) )
    {
        g_log_len += ( size_t ) n;
    }

    if( answer > 0 )
    {
        size_t left = ( size_t ) answer;

        for( i = 0U; ( i < count ) && ( left > 0U ); i++ )
        {
            size_t take = ( vec[ i ].iov_len < left ) ? vec[ i ].iov_len : left;

            if( ( g_sent_len + take ) <= sizeof g_sent )
            {
                memcpy( &g_sent[ g_sent_len ], vec[ i ].iov_base, take );
                g_sent_len += take;
            }

            left -= take;
        }
    }

    return answer;
}

static int32_t scripted_recv( NetworkContext_t *c, void *b, size_t n )
{
    ( void ) c; ( void ) b; ( void ) n;
    return -1;
}

static uint32_t scripted_time( void )
{
    return g_now;
}

static bool stub_callback( MQTTContext_t *c, MQTTPacketInfo_t *p,
                           MQTTDeserializedInfo_t *d, MQTTSuccessFailReasonCode_t *r,
                           MQTTPropBuilder_t *in, MQTTPropBuilder_t *out )
{
    ( void ) c; ( void ) p; ( void ) d; ( void ) r; ( void ) in; ( void ) out;
    return true;
}

/* ---- the store callback, and the two functions only it can reach ----------- */

static uint8_t g_stored[ MAX_STORED ];
static size_t g_stored_len;
static bool g_store_answer;
static size_t g_store_calls;

static bool store_packet( MQTTContext_t *context, uint32_t packetId, MQTTVec_t *vec )
{
    size_t needed = 0U;

    ( void ) context;
    ( void ) packetId;

    g_store_calls++;

    if( ( MQTT_GetBytesInMQTTVec( vec, &needed ) == MQTTSuccess ) &&
        ( needed <= sizeof g_stored ) )
    {
        MQTT_SerializeMQTTVec( g_stored, vec );
        g_stored_len = needed;
    }

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
    ( void ) context; ( void ) packetId;
}

/* ---- the context ----------------------------------------------------------- */

static uint8_t g_network[ 512 ];
static MQTTPubAckInfo_t g_out_records[ 8 ];
static MQTTPubAckInfo_t g_in_records[ 8 ];

static bool g_use_writev;

static void reset( int32_t step )
{
    g_step_value = step;
    g_calls = 0U;
    g_sent_len = 0U;
    g_log[ 0 ] = '\0';
    g_log_len = 0U;
    g_now = 0U;
    g_stored_len = 0U;
    g_store_calls = 0U;
    g_store_answer = true;
}

static void fresh( MQTTContext_t *context, bool retransmits )
{
    static TransportInterface_t transport;
    static MQTTFixedBuffer_t network;

    memset( &transport, 0, sizeof transport );
    transport.send = scripted_send;
    transport.recv = scripted_recv;
    transport.writev = g_use_writev ? scripted_writev : NULL;
    transport.pNetworkContext = NULL;

    network.pBuffer = g_network;
    network.size = sizeof g_network;

    /* The record arrays are file-scope, so they MUST be cleared per case: a
     * publish of packet id 5 in one case would otherwise still be on file in
     * the next, and the second would answer `MQTTStateCollision` for a reason
     * that has nothing to do with what it is testing. Found exactly that way. */
    memset( g_out_records, 0, sizeof g_out_records );
    memset( g_in_records, 0, sizeof g_in_records );

    memset( context, 0, sizeof *context );
    ( void ) MQTT_Init( context, &transport, scripted_time, stub_callback, &network );
    ( void ) MQTT_InitStatefulQoS( context, g_out_records, 8U, g_in_records, 8U,
                                   NULL, 0U );

    if( retransmits )
    {
        ( void ) MQTT_InitRetransmits( context, store_packet, retrieve_packet,
                                       clear_packet );
    }

    context->connectStatus = MQTTConnected;
}

static const char * state_name( MQTTPublishState_t state )
{
    switch( state )
    {
        case MQTTStateNull:       return "Null";
        case MQTTPublishSend:     return "PublishSend";
        case MQTTPubAckSend:      return "PubAckSend";
        case MQTTPubRecSend:      return "PubRecSend";
        case MQTTPubRelSend:      return "PubRelSend";
        case MQTTPubCompSend:     return "PubCompSend";
        case MQTTPubAckPending:   return "PubAckPending";
        case MQTTPubRecPending:   return "PubRecPending";
        case MQTTPubRelPending:   return "PubRelPending";
        case MQTTPubCompPending:  return "PubCompPending";
        case MQTTPublishDone:     return "PublishDone";
        default:                  return "?";
    }
}

/* The first outgoing record, which is where a QoS 1 or 2 PUBLISH leaves its
 * mark. Printing it is what makes the state wiring OBSERVABLE: without it, a
 * publish that reserved a record and never advanced it looks identical on the
 * wire to one that did both. */
static void print_record( const MQTTContext_t *context )
{
    printf( " rec=%u/%s", ( unsigned ) g_out_records[ 0 ].packetId,
            state_name( g_out_records[ 0 ].publishState ) );
    ( void ) context;
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

static void print_tail( MQTTStatus_t status )
{
    printf( " -> %s calls=%u log=%s bytes=", status_name( status ),
            ( unsigned ) g_calls, ( g_log_len == 0U ) ? "-" : g_log );
    put_hex( g_sent, g_sent_len );
}

/* ---- SUBSCRIBE and UNSUBSCRIBE --------------------------------------------- */

/* The filters are named by shape so the trace carries its own inputs. */
static const char * filter_of( const char *shape, size_t *length )
{
    if( strcmp( shape, "a" ) == 0 )       { *length = 1U; return "a"; }
    if( strcmp( shape, "ab" ) == 0 )      { *length = 2U; return "ab"; }
    if( strcmp( shape, "a/b" ) == 0 )     { *length = 3U; return "a/b"; }
    if( strcmp( shape, "long" ) == 0 )    { *length = 8U; return "aaaaaaaa"; }
    /* `$share//a/b`: a shared subscription with an EMPTY share name, which the
     * SUBSCRIBE validator refuses and the UNSUBSCRIBE one never looks at. */
    if( strcmp( shape, "badshare" ) == 0 ) { *length = 11U; return "$share//a/b"; }

    *length = 3U;
    return "a/b";
}

static void sub_case_wv( const char *name, const char *shapes, int count,
                         MQTTQoS_t qos, uint16_t packetId, int32_t step,
                         bool unsubscribe, const uint8_t *props, size_t propsLen,
                         bool connected, bool writev )
{
    static uint8_t propsBuffer[ 32 ];

    MQTTContext_t context;
    MQTTSubscribeInfo_t list[ 4 ];
    MQTTPropBuilder_t builder;
    MQTTPropBuilder_t *pBuilder = NULL;
    MQTTStatus_t status;
    char shapeCopy[ 64 ];
    char *token;
    int i = 0;

    reset( step );
    g_use_writev = writev;
    fresh( &context, false );
    g_use_writev = false;

    if( !connected )
    {
        context.connectStatus = MQTTNotConnected;
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

    /* The shapes are a comma-separated list, so the trace line says exactly
     * which filters went in. */
    snprintf( shapeCopy, sizeof shapeCopy, "%s", shapes );
    token = strtok( shapeCopy, "," );

    while( ( token != NULL ) && ( i < count ) )
    {
        size_t length;

        memset( &list[ i ], 0, sizeof list[ i ] );
        list[ i ].pTopicFilter = filter_of( token, &length );
        list[ i ].topicFilterLength = length;
        list[ i ].qos = qos;
        i++;
        token = strtok( NULL, "," );
    }

    status = unsubscribe
             ? MQTT_Unsubscribe( &context, list, ( size_t ) count, packetId, pBuilder )
             : MQTT_Subscribe( &context, list, ( size_t ) count, packetId, pBuilder );

    printf( "%s %s shapes=%s n=%d qos=%u id=%u step=%ld props=",
            unsubscribe ? "unsub" : "sub", name, shapes, count,
            ( unsigned ) qos, ( unsigned ) packetId, ( long ) step );
    put_hex( props, propsLen );
    printf( " conn=%u wv=%u", connected ? 1U : 0U, writev ? 1U : 0U );
    print_tail( status );
    printf( "\n" );
}

static void sub_case_conn( const char *name, const char *shapes, int count,
                           MQTTQoS_t qos, uint16_t packetId, int32_t step,
                           bool unsubscribe, const uint8_t *props, size_t propsLen,
                           bool connected )
{
    sub_case_wv( name, shapes, count, qos, packetId, step, unsubscribe, props,
                 propsLen, connected, false );
}

static void sub_case( const char *name, const char *shapes, int count,
                      MQTTQoS_t qos, uint16_t packetId, int32_t step,
                      bool unsubscribe, const uint8_t *props, size_t propsLen )
{
    sub_case_wv( name, shapes, count, qos, packetId, step, unsubscribe,
                 props, propsLen, true, false );
}

/* ---- PUBLISH ---------------------------------------------------------------- */

static void pub_case_wv( const char *name, MQTTQoS_t qos, uint16_t packetId,
                         const char *topic, const char *payload, int32_t step,
                         bool retransmits, bool storeAnswer, bool dup, bool retain,
                         const uint8_t *props, size_t propsLen, bool connected,
                         bool writev )
{
    static uint8_t propsBuffer[ 32 ];

    MQTTContext_t context;
    MQTTPublishInfo_t info;
    MQTTPropBuilder_t builder;
    MQTTPropBuilder_t *pBuilder = NULL;
    MQTTStatus_t status;

    reset( step );
    g_use_writev = writev;
    fresh( &context, retransmits );
    g_use_writev = false;
    g_store_answer = storeAnswer;

    if( !connected )
    {
        context.connectStatus = MQTTNotConnected;
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
    info.qos = qos;
    info.dup = dup;
    info.retain = retain;
    info.pTopicName = topic;
    info.topicNameLength = strlen( topic );
    info.pPayload = ( strlen( payload ) > 0U ) ? payload : NULL;
    info.payloadLength = strlen( payload );

    status = MQTT_Publish( &context, &info, packetId, pBuilder );

    printf( "pub %s qos=%u id=%u topic=%s payload=%s step=%ld retx=%u store=%u "
            "dup=%u retain=%u props=",
            name, ( unsigned ) qos, ( unsigned ) packetId, topic,
            ( strlen( payload ) > 0U ) ? payload : "-", ( long ) step,
            retransmits ? 1U : 0U, storeAnswer ? 1U : 0U, dup ? 1U : 0U,
            retain ? 1U : 0U );
    put_hex( props, propsLen );
    printf( " conn=%u wv=%u", connected ? 1U : 0U, writev ? 1U : 0U );
    print_tail( status );
    printf( " storecalls=%u stored=", ( unsigned ) g_store_calls );
    put_hex( g_stored, g_stored_len );
    print_record( &context );
    printf( "\n" );
}

static void pub_case_conn( const char *name, MQTTQoS_t qos, uint16_t packetId,
                           const char *topic, const char *payload, int32_t step,
                           bool retransmits, bool storeAnswer, bool dup, bool retain,
                           const uint8_t *props, size_t propsLen, bool connected )
{
    pub_case_wv( name, qos, packetId, topic, payload, step, retransmits,
                 storeAnswer, dup, retain, props, propsLen, connected, false );
}

static void pub_case( const char *name, MQTTQoS_t qos, uint16_t packetId,
                      const char *topic, const char *payload, int32_t step,
                      bool retransmits, bool storeAnswer, bool dup, bool retain,
                      const uint8_t *props, size_t propsLen )
{
    pub_case_wv( name, qos, packetId, topic, payload, step, retransmits,
                 storeAnswer, dup, retain, props, propsLen, true, false );
}


/* ---- the record the publish leaves behind ----------------------------------- */

/* A second PUBLISH on a packet id that is still in flight. The state machine
 * refuses it -- unless the caller says it is a re-delivery, and then the record
 * it collided with is the one being re-sent. Two publishes in one case, because
 * the collision only exists BETWEEN them. */
static void collide_case( const char *name, MQTTQoS_t qos, uint16_t packetId, bool dup )
{
    MQTTContext_t context;
    MQTTPublishInfo_t first;
    MQTTPublishInfo_t second;
    MQTTStatus_t firstStatus;
    MQTTStatus_t secondStatus;

    reset( 64 );
    fresh( &context, false );

    memset( &first, 0, sizeof first );
    first.qos = qos;
    first.pTopicName = "a/b";
    first.topicNameLength = 3U;
    first.pPayload = "hello";
    first.payloadLength = 5U;

    second = first;
    second.dup = dup;

    firstStatus = MQTT_Publish( &context, &first, packetId, NULL );

    /* Only the SECOND publish's calls are interesting, so the log is cleared
     * between them and the transport is left alone. */
    g_calls = 0U;
    g_sent_len = 0U;
    g_log[ 0 ] = '\0';
    g_log_len = 0U;

    secondStatus = MQTT_Publish( &context, &second, packetId, NULL );

    printf( "collide %s qos=%u id=%u dup=%u first=%s", name, ( unsigned ) qos,
            ( unsigned ) packetId, dup ? 1U : 0U, status_name( firstStatus ) );
    print_tail( secondStatus );
    print_record( &context );
    printf( "\n" );
}

/* ---- MQTT_CancelCallback ---------------------------------------------------- */

/* `stateful` is whether QoS was ever enabled, and `publishFirst` whether there
 * is a record to cancel. */
static void cancel_case( const char *name, bool stateful, bool publishFirst,
                         uint16_t publishId, uint16_t cancelId )
{
    MQTTContext_t context;
    MQTTPublishInfo_t info;
    MQTTStatus_t status;

    reset( 64 );
    fresh( &context, false );

    if( !stateful )
    {
        /* `MQTT_InitStatefulQoS` is what sets these, and cancelling without it
         * is the refusal this case is for. */
        context.outgoingPublishRecords = NULL;
        context.outgoingPublishRecordMaxCount = 0U;
    }

    if( publishFirst )
    {
        memset( &info, 0, sizeof info );
        info.qos = MQTTQoS1;
        info.pTopicName = "a/b";
        info.topicNameLength = 3U;
        info.pPayload = "hello";
        info.payloadLength = 5U;
        ( void ) MQTT_Publish( &context, &info, publishId, NULL );
    }

    status = MQTT_CancelCallback( &context, cancelId );

    printf( "cancel %s stateful=%u published=%u/%u id=%u -> %s\n", name,
            stateful ? 1U : 0U, publishFirst ? 1U : 0U, ( unsigned ) publishId,
            ( unsigned ) cancelId, status_name( status ) );
}

int main( void )
{
    static const uint8_t NO_PROPS[ 1 ] = { 0 };
    /* A subscription identifier of 7, which is legal in a SUBSCRIBE. */
    static const uint8_t SUB_PROPS[] = { 0x0B, 0x07 };
    /* A user property, which is legal in an UNSUBSCRIBE and a PUBLISH. */
    static const uint8_t USER_PROPS[] = { 0x26, 0x00, 0x01, 'k', 0x00, 0x01, 'v' };

    printf( "geometry\n" );

    /* --- SUBSCRIBE: one filter, several, and every transport answer --- */
    sub_case( "one-filter", "a/b", 1, MQTTQoS0, 1U, 64, false, NO_PROPS, 0U );
    sub_case( "one-filter-qos1", "a/b", 1, MQTTQoS1, 1U, 64, false, NO_PROPS, 0U );
    sub_case( "one-filter-qos2", "a/b", 1, MQTTQoS2, 1U, 64, false, NO_PROPS, 0U );
    sub_case( "short-filter", "a", 1, MQTTQoS0, 1U, 64, false, NO_PROPS, 0U );
    sub_case( "long-filter", "long", 1, MQTTQoS0, 1U, 64, false, NO_PROPS, 0U );
    /* Two and three filters: the packet is split across one gather each. */
    sub_case( "two-filters", "a/b,ab", 2, MQTTQoS0, 1U, 64, false, NO_PROPS, 0U );
    sub_case( "three-filters", "a/b,ab,a", 3, MQTTQoS0, 1U, 64, false, NO_PROPS, 0U );
    /* With a property section. */
    sub_case( "with-properties", "a/b", 1, MQTTQoS0, 1U, 64, false,
              SUB_PROPS, sizeof SUB_PROPS );
    /* A transport that dribbles, and one that fails. */
    sub_case( "one-byte-at-a-time", "a/b", 1, MQTTQoS0, 1U, 1, false, NO_PROPS, 0U );
    sub_case( "two-filters-one-byte", "a/b,ab", 2, MQTTQoS0, 1U, 1, false,
              NO_PROPS, 0U );
    sub_case( "fails", "a/b", 1, MQTTQoS0, 1U, -1, false, NO_PROPS, 0U );
    /* A shared subscription with an empty share name: SUBSCRIBE looks at the
     * options and UNSUBSCRIBE does not, so this pair is the only place in the
     * trace where the two validators disagree. */
    sub_case( "bad-share", "badshare", 1, MQTTQoS0, 1U, 64, false, NO_PROPS, 0U );
    /* And the refusals' ORDER: a context with no connection still validates
     * first, so a bad list answers `BadParameter` and a good one answers
     * `StatusNotConnected`. */
    sub_case_conn( "not-connected", "a/b", 1, MQTTQoS0, 1U, 64, false,
                   NO_PROPS, 0U, false );
    sub_case_conn( "not-connected-bad-id", "a/b", 1, MQTTQoS0, 0U, 64, false,
                   NO_PROPS, 0U, false );

    /* --- UNSUBSCRIBE: no options byte, so two vectors per filter --- */
    sub_case( "one-filter", "a/b", 1, MQTTQoS0, 1U, 64, true, NO_PROPS, 0U );
    sub_case( "two-filters", "a/b,ab", 2, MQTTQoS0, 1U, 64, true, NO_PROPS, 0U );
    sub_case( "with-properties", "a/b", 1, MQTTQoS0, 1U, 64, true,
              USER_PROPS, sizeof USER_PROPS );
    sub_case( "one-byte-at-a-time", "a/b", 1, MQTTQoS0, 1U, 1, true, NO_PROPS, 0U );
    sub_case( "fails", "a/b", 1, MQTTQoS0, 1U, -1, true, NO_PROPS, 0U );
    sub_case( "bad-share", "badshare", 1, MQTTQoS0, 1U, 64, true, NO_PROPS, 0U );
    sub_case_conn( "not-connected", "a/b", 1, MQTTQoS0, 1U, 64, true,
                   NO_PROPS, 0U, false );
    sub_case_conn( "not-connected-bad-id", "a/b", 1, MQTTQoS0, 0U, 64, true,
                   NO_PROPS, 0U, false );

    /* --- PUBLISH: the vector count changes with QoS, properties and payload --- */
    pub_case( "qos0", MQTTQoS0, 0U, "a/b", "hello", 64, false, true, false, false,
              NO_PROPS, 0U );
    pub_case( "qos0-no-payload", MQTTQoS0, 0U, "a/b", "", 64, false, true, false,
              false, NO_PROPS, 0U );
    pub_case( "qos1", MQTTQoS1, 5U, "a/b", "hello", 64, false, true, false, false,
              NO_PROPS, 0U );
    pub_case( "qos2", MQTTQoS2, 6U, "a/b", "hello", 64, false, true, false, false,
              NO_PROPS, 0U );
    pub_case( "retain", MQTTQoS0, 0U, "a/b", "hello", 64, false, true, false, true,
              NO_PROPS, 0U );
    pub_case( "with-properties", MQTTQoS0, 0U, "a/b", "hello", 64, false, true,
              false, false, USER_PROPS, sizeof USER_PROPS );
    pub_case( "one-byte-at-a-time", MQTTQoS0, 0U, "a/b", "hello", 1, false, true,
              false, false, NO_PROPS, 0U );
    pub_case( "fails", MQTTQoS0, 0U, "a/b", "hello", -1, false, true, false, false,
              NO_PROPS, 0U );

    /* --- and the store callback, which is the only door to the vector helpers.
     * The DUP flag is raised in the stored copy and lowered again afterwards,
     * so `bytes` and `stored` differ in exactly one bit. --- */
    pub_case( "qos1-stored", MQTTQoS1, 5U, "a/b", "hello", 64, true, true, false,
              false, NO_PROPS, 0U );
    pub_case( "qos1-stored-already-dup", MQTTQoS1, 5U, "a/b", "hello", 64, true,
              true, true, false, NO_PROPS, 0U );
    pub_case( "qos2-stored", MQTTQoS2, 6U, "a/b", "hello", 64, true, true, false,
              false, NO_PROPS, 0U );
    pub_case( "qos1-store-refuses", MQTTQoS1, 5U, "a/b", "hello", 64, true, false,
              false, false, NO_PROPS, 0U );
    pub_case( "qos0-is-never-stored", MQTTQoS0, 0U, "a/b", "hello", 64, true, true,
              false, false, NO_PROPS, 0U );
    pub_case( "qos1-stored-with-properties", MQTTQoS1, 5U, "a/b", "hello", 64,
              true, true, false, false, USER_PROPS, sizeof USER_PROPS );

    pub_case_conn( "not-connected", MQTTQoS0, 0U, "a/b", "hello", 64, false,
                   true, false, false, NO_PROPS, 0U, false );
    pub_case_conn( "not-connected-empty-topic", MQTTQoS0, 0U, "", "hello", 64,
                   false, true, false, false, NO_PROPS, 0U, false );
    /* A QoS 1 publish with no payload, so the flattened copy has a vector
     * fewer -- the one place the store sees a shorter list. */
    pub_case( "qos1-stored-no-payload", MQTTQoS1, 5U, "a/b", "", 64, true, true,
              false, false, NO_PROPS, 0U );

    /* --- the SAME packets over a GATHERED write ---
     *
     * `writev` is handed the outstanding vectors AND their count, so one call
     * is one gather. That count is the only place the geometry exists: with
     * the per-vector fallback above, `v2` then `v3` and `v5` in one go produce
     * the identical sequence of offers. */
    sub_case_wv( "one-filter-writev", "a/b", 1, MQTTQoS0, 1U, 64, false,
                 NO_PROPS, 0U, true, true );
    sub_case_wv( "three-filters-writev", "a/b,ab,a", 3, MQTTQoS0, 1U, 64, false,
                 NO_PROPS, 0U, true, true );
    sub_case_wv( "with-properties-writev", "a/b", 1, MQTTQoS0, 1U, 64, false,
                 SUB_PROPS, sizeof SUB_PROPS, true, true );
    sub_case_wv( "one-byte-at-a-time-writev", "a/b", 1, MQTTQoS0, 1U, 1, false,
                 NO_PROPS, 0U, true, true );
    sub_case_wv( "one-filter-writev", "a/b", 1, MQTTQoS0, 1U, 64, true,
                 NO_PROPS, 0U, true, true );
    sub_case_wv( "two-filters-writev", "a/b,ab", 2, MQTTQoS0, 1U, 64, true,
                 NO_PROPS, 0U, true, true );
    sub_case_wv( "with-properties-writev", "a/b", 1, MQTTQoS0, 1U, 64, true,
                 USER_PROPS, sizeof USER_PROPS, true, true );
    pub_case_wv( "qos1-writev", MQTTQoS1, 5U, "a/b", "hello", 64, false, true,
                 false, false, NO_PROPS, 0U, true, true );
    pub_case_wv( "qos1-writev-in-pieces", MQTTQoS1, 5U, "a/b", "hello", 3, false,
                 true, false, false, NO_PROPS, 0U, true, true );
    /* A publish with NO payload, over a gathered write. This is the only case
     * that can see an empty trailing vector: the per-vector fallback never
     * offers one, because its loop stops on BYTES and there are none left. */
    pub_case_wv( "qos0-writev-no-payload", MQTTQoS0, 0U, "a/b", "", 64, false,
                 true, false, false, NO_PROPS, 0U, true, true );

    /* --- a packet id that is already in flight --- */
    collide_case( "qos1-same-id", MQTTQoS1, 5U, false );
    collide_case( "qos1-same-id-dup", MQTTQoS1, 5U, true );
    collide_case( "qos2-same-id", MQTTQoS2, 6U, false );
    collide_case( "qos0-has-no-record", MQTTQoS0, 0U, false );

    /* --- cancelling one --- */
    cancel_case( "no-qos", false, false, 0U, 5U );
    cancel_case( "no-such-record", true, false, 0U, 5U );
    cancel_case( "the-one-in-flight", true, true, 5U, 5U );
    cancel_case( "another-one", true, true, 5U, 6U );

    /* NO `zero-id` CASE. `MQTT_CancelCallback` passes a packet id of 0 straight
     * through to `findInRecord`, which `assert( packetId != 0 )` -- so on a
     * build with assertions live the C ABORTS rather than answering, and this
     * driver dies with it. That is not a defect and it is not an answer: it is
     * a documented precondition, and a differential case the C cannot complete
     * is not a case. The Rust arm answers `BadParameter` there, which is the
     * error the caller already handles, and `PublishRecords::remove` says so
     * beside the check. */

    printf( "end\n" );
    return 0;
}
