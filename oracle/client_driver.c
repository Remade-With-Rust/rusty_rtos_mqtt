/* The C arm of K7's coreMQTT CLIENT-CONTEXT differential.
 *
 * `core_mqtt.c` is compiled VERBATIM out of the pinned checkout; nothing here
 * copies or edits it.
 *
 * # The last of `core_mqtt.c` that needs no transport
 *
 * The constructors, the packet-identifier allocator, the vector helpers, and
 * the validators an outgoing SUBSCRIBE, UNSUBSCRIBE or PUBLISH goes through.
 * `MQTT_Subscribe` validates BEFORE it looks at the connection status and long
 * before it touches the transport, so an unconnected context is enough to
 * exercise every one of them: a bad list answers `MQTTBadParameter` and a good
 * one answers `MQTTStatusNotConnected`, and those are two different answers.
 *
 * # What this driver does NOT ask
 *
 * `MQTT_Init` has six refusals and five of them are null-pointer checks. A
 * Rust `MqttContext::new` takes a buffer, not a pointer, and has nowhere to put
 * a null -- so those cases are absent from this trace rather than failing in
 * it, which is the rule the fifteenth slice set: a trace should ask only what
 * both arms can answer. What IS asked is the state a good init leaves behind,
 * which is the part with content.
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core_mqtt.h"

#define BUFFER_BYTES    256

static const char * status_name( MQTTStatus_t status )
{
    switch( status )
    {
        case MQTTSuccess:                return "Success";
        case MQTTBadParameter:           return "BadParameter";
        case MQTTNoMemory:               return "NoMemory";
        case MQTTSendFailed:             return "SendFailed";
        case MQTTStatusConnected:        return "StatusConnected";
        case MQTTStatusNotConnected:     return "StatusNotConnected";
        case MQTTStatusDisconnectPending: return "StatusDisconnectPending";
        case MQTTIllegalState:           return "IllegalState";
        default:                         return "OTHER";
    }
}

/* The transport and the callbacks exist so `MQTT_Init` will accept them. No
 * case in this trace reaches a send or a receive. */
static int32_t stub_send( NetworkContext_t *c, const void *b, size_t n )
{
    ( void ) c; ( void ) b; ( void ) n;
    return -1;
}

static int32_t stub_recv( NetworkContext_t *c, void *b, size_t n )
{
    ( void ) c; ( void ) b; ( void ) n;
    return -1;
}

static uint32_t stub_time( void )
{
    return 0U;
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

static uint8_t g_buffer[ BUFFER_BYTES ];

/* A context freshly through `MQTT_Init`, which is the state everything else
 * starts from. */
static MQTTStatus_t fresh( MQTTContext_t *context )
{
    static TransportInterface_t transport;
    static MQTTFixedBuffer_t network;

    memset( &transport, 0, sizeof transport );
    transport.send = stub_send;
    transport.recv = stub_recv;
    transport.pNetworkContext = NULL;

    network.pBuffer = g_buffer;
    network.size = sizeof g_buffer;

    return MQTT_Init( context, &transport, stub_time, stub_callback, &network );
}

/* ---- what a good init leaves behind ---------------------------------------- */

static void print_init( void )
{
    MQTTContext_t context;
    MQTTStatus_t status;

    memset( &context, 0xAA, sizeof context );
    status = fresh( &context );

    printf( "init %s connect=%d nextid=%u recvmax=%u maxpkt=%u rpi=%u "
            "smaxqos=%u retain=%u keepalive=%u outrecords=%u inrecords=%u "
            "ackprops=%u\n",
            status_name( status ),
            ( int ) context.connectStatus,
            ( unsigned ) context.nextPacketId,
            ( unsigned ) context.connectionProperties.receiveMax,
            ( unsigned ) context.connectionProperties.maxPacketSize,
            context.connectionProperties.requestProblemInfo ? 1U : 0U,
            ( unsigned ) context.connectionProperties.serverMaxQos,
            ( unsigned ) context.connectionProperties.retainAvailable,
            ( unsigned ) context.connectionProperties.serverKeepAlive,
            ( unsigned ) context.outgoingPublishRecordMaxCount,
            ( unsigned ) context.incomingPublishRecordMaxCount,
            context.ackPropsBuffer.pBuffer == NULL ? 0U : 1U );
}

/* ---- MQTT_InitStatefulQoS: two exclusive-ors and a buffer ------------------ */

static void stateful_case( const char *name,
                           bool outgoing,
                           size_t outgoingCount,
                           bool incoming,
                           size_t incomingCount,
                           bool props,
                           size_t propsLength,
                           bool initFirst )
{
    static MQTTPubAckInfo_t outRecords[ 4 ];
    static MQTTPubAckInfo_t inRecords[ 4 ];
    static uint8_t propsBuffer[ 32 ];

    MQTTContext_t context;
    MQTTStatus_t status;

    memset( &context, 0, sizeof context );

    if( initFirst )
    {
        ( void ) fresh( &context );
    }

    status = MQTT_InitStatefulQoS( &context,
                                   outgoing ? outRecords : NULL, outgoingCount,
                                   incoming ? inRecords : NULL, incomingCount,
                                   props ? propsBuffer : NULL, propsLength );

    printf( "stateful %s out=%u/%u in=%u/%u props=%u/%u inited=%u -> %s "
            "outmax=%u inmax=%u ackbuf=%u\n",
            name, outgoing ? 1U : 0U, ( unsigned ) outgoingCount,
            incoming ? 1U : 0U, ( unsigned ) incomingCount,
            props ? 1U : 0U, ( unsigned ) propsLength, initFirst ? 1U : 0U,
            status_name( status ),
            ( unsigned ) context.outgoingPublishRecordMaxCount,
            ( unsigned ) context.incomingPublishRecordMaxCount,
            ( unsigned ) context.ackPropsBuffer.bufferLength );
}

/* ---- the connection status, and the packet identifier ---------------------- */

static void print_connect_status( void )
{
    static const struct
    {
        const char *name;
        MQTTConnectionStatus_t value;
    } STATES[] = {
        { "not-connected",     MQTTNotConnected },
        { "connected",         MQTTConnected },
        { "disconnect-pending", MQTTDisconnectPending },
    };

    size_t i;

    for( i = 0U; i < sizeof( STATES ) / sizeof( STATES[ 0 ] ); i++ )
    {
        MQTTContext_t context;

        memset( &context, 0, sizeof context );
        ( void ) fresh( &context );
        context.connectStatus = STATES[ i ].value;

        printf( "connectstatus %s -> %s\n", STATES[ i ].name,
                status_name( MQTT_CheckConnectStatus( &context ) ) );
    }
}

/* The allocator, driven across the wrap. A packet id of zero is not legal, so
 * the sequence after 65,535 is the part worth printing. */
static void print_packet_ids( void )
{
    MQTTContext_t context;
    unsigned i;

    memset( &context, 0, sizeof context );
    ( void ) fresh( &context );

    printf( "packetid from-start" );

    for( i = 0U; i < 4U; i++ )
    {
        printf( " %u", ( unsigned ) MQTT_GetPacketId( &context ) );
    }

    printf( " next=%u\n", ( unsigned ) context.nextPacketId );

    context.nextPacketId = 65534U;
    printf( "packetid at-the-wrap" );

    for( i = 0U; i < 4U; i++ )
    {
        printf( " %u", ( unsigned ) MQTT_GetPacketId( &context ) );
    }

    printf( " next=%u\n", ( unsigned ) context.nextPacketId );
}

/* ---- the vector helpers are NOT here --------------------------------------
 *
 * `MQTT_GetBytesInMQTTVec` and `MQTT_SerializeMQTTVec` take an `MQTTVec_t`,
 * which the public header declares and does not define: it is handed to the
 * application inside a `MQTTStorePacketForRetransmit` callback and nowhere
 * else. So they cannot be called from outside the library at all, and they
 * belong with the send path that invokes that callback rather than here.
 */

/* ---- the subscription validators ------------------------------------------- */

/* A list is described by up to three entries; each entry names a shape rather
 * than spelling its bytes, so the trace carries its own inputs. */
typedef struct
{
    const char *shape;
    MQTTQoS_t   qos;
    uint8_t     retainHandling;
    bool        noLocal;
} Entry_t;

static void fill( MQTTSubscribeInfo_t *info, const Entry_t *entry )
{
    /* Every filter sits inside a LONGER NUL-terminated buffer, because
     * `checkWildcardSubscriptions` reaches for the filter with `strchr` and the
     * struct carries a length. Where the two disagree is a case here. */
    static const char PLAIN[] = "a/b";
    static const char WILD[] = "a/#";
    static const char PLUS[] = "a/+";
    static const char SHARED[] = "$share/g/a/b";
    static const char SHARED_NO_NAME[] = "$share//a/b";
    static const char SHARED_NO_TOPIC[] = "$share/g/";
    static const char SHARED_WILD_NAME[] = "$share/g#/a/b";
    /* Three characters of filter, and a '#' one byte PAST them. A length-
     * respecting reader sees no wildcard; `strchr` sees one. */
    static const char HIDDEN_WILD[] = "abc#";
    /* Exactly the prefix. The C tests `length > 7` BEFORE comparing seven
     * bytes, so this is not a shared subscription at all -- and nothing else
     * in this trace distinguishes `> 7` from `>= 7`. */
    static const char SHARE_PREFIX[] = "$share/";

    memset( info, 0, sizeof *info );
    info->qos = entry->qos;
    info->retainHandlingOption = ( MQTTRetainHandling_t ) entry->retainHandling;
    info->noLocalOption = entry->noLocal;
    info->retainAsPublishedOption = false;

    if( strcmp( entry->shape, "plain" ) == 0 )
    {
        info->pTopicFilter = PLAIN;
        info->topicFilterLength = 3U;
    }
    else if( strcmp( entry->shape, "wild" ) == 0 )
    {
        info->pTopicFilter = WILD;
        info->topicFilterLength = 3U;
    }
    else if( strcmp( entry->shape, "plus" ) == 0 )
    {
        info->pTopicFilter = PLUS;
        info->topicFilterLength = 3U;
    }
    else if( strcmp( entry->shape, "shared" ) == 0 )
    {
        info->pTopicFilter = SHARED;
        info->topicFilterLength = 12U;
    }
    else if( strcmp( entry->shape, "shared-no-name" ) == 0 )
    {
        info->pTopicFilter = SHARED_NO_NAME;
        info->topicFilterLength = 11U;
    }
    else if( strcmp( entry->shape, "shared-no-topic" ) == 0 )
    {
        info->pTopicFilter = SHARED_NO_TOPIC;
        info->topicFilterLength = 9U;
    }
    else if( strcmp( entry->shape, "shared-wild-name" ) == 0 )
    {
        info->pTopicFilter = SHARED_WILD_NAME;
        info->topicFilterLength = 13U;
    }
    else if( strcmp( entry->shape, "hidden-wild" ) == 0 )
    {
        info->pTopicFilter = HIDDEN_WILD;
        info->topicFilterLength = 3U;
    }
    else if( strcmp( entry->shape, "share-prefix" ) == 0 )
    {
        info->pTopicFilter = SHARE_PREFIX;
        info->topicFilterLength = 7U;
    }
    else if( strcmp( entry->shape, "empty" ) == 0 )
    {
        info->pTopicFilter = PLAIN;
        info->topicFilterLength = 0U;
    }
    else
    {
        info->pTopicFilter = PLAIN;
        info->topicFilterLength = 3U;
    }
}

static void sub_case( const char *name,
                      int count,
                      const Entry_t *entries,
                      uint16_t packetId,
                      bool statefulQoS,
                      uint8_t wildcardAvailable,
                      uint8_t sharedAvailable,
                      bool unsubscribe )
{
    static MQTTPubAckInfo_t outRecords[ 4 ];
    static MQTTPubAckInfo_t inRecords[ 4 ];

    MQTTContext_t context;
    MQTTSubscribeInfo_t list[ 3 ];
    MQTTStatus_t status;
    int i;

    memset( &context, 0, sizeof context );
    ( void ) fresh( &context );

    if( statefulQoS )
    {
        ( void ) MQTT_InitStatefulQoS( &context, outRecords, 4U, inRecords, 4U,
                                       NULL, 0U );
    }

    context.connectionProperties.isWildcardAvailable = wildcardAvailable;
    context.connectionProperties.isSharedAvailable = sharedAvailable;

    for( i = 0; i < count; i++ )
    {
        fill( &list[ i ], &entries[ i ] );
    }

    status = unsubscribe
             ? MQTT_Unsubscribe( &context, list, ( size_t ) count, packetId, NULL )
             : MQTT_Subscribe( &context, list, ( size_t ) count, packetId, NULL );

    printf( "sub %s which=%s n=%d id=%u stateful=%u wildcard=%u shared=%u shapes=",
            name, unsubscribe ? "unsubscribe" : "subscribe", count,
            ( unsigned ) packetId, statefulQoS ? 1U : 0U,
            ( unsigned ) wildcardAvailable, ( unsigned ) sharedAvailable );

    for( i = 0; i < count; i++ )
    {
        printf( "%s%s:%u:%u:%u", ( i == 0 ) ? "" : ",", entries[ i ].shape,
                ( unsigned ) entries[ i ].qos,
                ( unsigned ) entries[ i ].retainHandling,
                entries[ i ].noLocal ? 1U : 0U );
    }

    printf( " -> %s\n", status_name( status ) );
}

/* ---- the publish validator ------------------------------------------------- */

static void pub_case( const char *name,
                      MQTTQoS_t qos,
                      uint16_t packetId,
                      bool hasPayload,
                      size_t payloadLength,
                      size_t topicLength,
                      bool statefulQoS )
{
    static MQTTPubAckInfo_t outRecords[ 4 ];
    static MQTTPubAckInfo_t inRecords[ 4 ];
    static const char TOPIC[] = "a/b";
    static const char PAYLOAD[] = "hello";

    MQTTContext_t context;
    MQTTPublishInfo_t info;
    MQTTStatus_t status;

    memset( &context, 0, sizeof context );
    ( void ) fresh( &context );

    if( statefulQoS )
    {
        ( void ) MQTT_InitStatefulQoS( &context, outRecords, 4U, inRecords, 4U,
                                       NULL, 0U );
    }

    memset( &info, 0, sizeof info );
    info.qos = qos;
    info.pTopicName = TOPIC;
    info.topicNameLength = topicLength;
    info.pPayload = hasPayload ? PAYLOAD : NULL;
    info.payloadLength = payloadLength;

    status = MQTT_Publish( &context, &info, packetId, NULL );

    printf( "pub %s qos=%u id=%u payload=%u/%u topiclen=%u stateful=%u -> %s\n",
            name, ( unsigned ) qos, ( unsigned ) packetId,
            hasPayload ? 1U : 0U, ( unsigned ) payloadLength,
            ( unsigned ) topicLength, statefulQoS ? 1U : 0U,
            status_name( status ) );
}

int main( void )
{
    static const Entry_t PLAIN0 = { "plain", MQTTQoS0, 0U, false };
    static const Entry_t PLAIN1 = { "plain", MQTTQoS1, 0U, false };
    static const Entry_t EMPTY = { "empty", MQTTQoS0, 0U, false };
    static const Entry_t WILD = { "wild", MQTTQoS0, 0U, false };
    static const Entry_t HIDDEN = { "hidden-wild", MQTTQoS0, 0U, false };
    static const Entry_t SHARED = { "shared", MQTTQoS0, 0U, false };
    static const Entry_t SHARED_LOCAL = { "shared", MQTTQoS0, 0U, true };
    static const Entry_t SHARED_NO_NAME = { "shared-no-name", MQTTQoS0, 0U, false };
    static const Entry_t SHARED_NO_TOPIC = { "shared-no-topic", MQTTQoS0, 0U, false };
    static const Entry_t SHARED_WILD_NAME = { "shared-wild-name", MQTTQoS0, 0U, false };
    static const Entry_t SHARE_PREFIX_ONLY = { "share-prefix", MQTTQoS0, 0U, false };

    Entry_t one[ 1 ];
    Entry_t two[ 2 ];

    printf( "geometry\n" );

    print_init();

    /* `MQTT_InitStatefulQoS` has four refusals and all four are about a
     * POINTER disagreeing with a COUNT, or about being called before
     * `MQTT_Init`. A Rust `enable_qos` takes slices and is a method on a
     * context that already exists, so none of the four can be expressed --
     * three more of the pointer-and-length family. They are absent from this
     * trace rather than failing in it. What is compared is the state it sets,
     * which is the part with content. */
    stateful_case( "both-lists", true, 4U, true, 4U, false, 0U, true );
    stateful_case( "neither-list", false, 0U, false, 0U, false, 0U, true );
    stateful_case( "with-ack-props", true, 4U, true, 4U, true, 32U, true );
    stateful_case( "ack-props-zero-length", true, 4U, true, 4U, true, 0U, true );

    print_connect_status();
    print_packet_ids();

    /* --- one subscription, every way it can be wrong --- */
    one[ 0 ] = PLAIN0;
    sub_case( "plain", 1, one, 1U, true, 1U, 1U, false );
    sub_case( "unsubscribe-plain", 1, one, 1U, true, 1U, 1U, true );
    sub_case( "zero-packet-id", 1, one, 0U, true, 1U, 1U, false );

    one[ 0 ] = EMPTY;
    sub_case( "empty-filter", 1, one, 1U, true, 1U, 1U, false );

    /* A QoS of 3 and a retain-handling option of 3 are absent from this trace.
     * `MQTTQoS_t` and `MQTTRetainHandling_t` can hold them and Rust's `QoS` and
     * `RetainHandling` cannot, so a Rust caller cannot build the list at all --
     * the same family as the null-pointer cases, one level up: it is the TYPE
     * that refuses, before any validator runs. `validateTopicFilter`'s two
     * `> 2` checks are therefore unreachable rather than transcribed, and the
     * last-one-wins defect below is shown with an empty filter, which both arms
     * can express. */

    one[ 0 ] = PLAIN1;
    sub_case( "qos1-without-records", 1, one, 1U, false, 1U, 1U, false );
    sub_case( "qos1-with-records", 1, one, 1U, true, 1U, 1U, false );

    one[ 0 ] = WILD;
    sub_case( "wildcard-allowed", 1, one, 1U, true, 1U, 1U, false );
    sub_case( "wildcard-forbidden", 1, one, 1U, true, 0U, 1U, false );

    /* `checkWildcardSubscriptions` uses `strchr`, which does not know the
     * filter's length. The '#' here is one byte PAST the filter. */
    one[ 0 ] = HIDDEN;
    sub_case( "wildcard-past-the-length", 1, one, 1U, true, 0U, 1U, false );

    one[ 0 ] = SHARED;
    sub_case( "shared", 1, one, 1U, true, 1U, 1U, false );
    sub_case( "shared-forbidden", 1, one, 1U, true, 1U, 0U, false );

    one[ 0 ] = SHARED_LOCAL;
    sub_case( "shared-with-no-local", 1, one, 1U, true, 1U, 1U, false );

    one[ 0 ] = SHARED_NO_NAME;
    sub_case( "shared-empty-name", 1, one, 1U, true, 1U, 1U, false );

    one[ 0 ] = SHARED_NO_TOPIC;
    sub_case( "shared-no-topic", 1, one, 1U, true, 1U, 1U, false );

    one[ 0 ] = SHARED_WILD_NAME;
    sub_case( "shared-wildcard-in-name", 1, one, 1U, true, 1U, 1U, false );

    /* Exactly seven bytes, which is the boundary of `length > 7`. */
    one[ 0 ] = SHARE_PREFIX_ONLY;
    sub_case( "share-prefix-exactly", 1, one, 1U, true, 1U, 1U, false );

    /* An UNSUBSCRIBE looks at the filter and nothing else, so a shared
     * subscription that a SUBSCRIBE refuses goes through unexamined. Without
     * this case, deleting the whole unsubscribe short-circuit changes no
     * answer. */
    one[ 0 ] = SHARED_NO_NAME;
    sub_case( "unsubscribe-shared-no-name", 1, one, 1U, true, 1U, 1U, true );

    one[ 0 ] = SHARED_LOCAL;
    sub_case( "unsubscribe-shared-no-local", 1, one, 1U, true, 1U, 1U, true );

    /* --- two subscriptions, and which of them decides --- */
    two[ 0 ] = EMPTY;
    two[ 1 ] = PLAIN0;
    sub_case( "bad-then-good", 2, two, 1U, true, 1U, 1U, false );

    two[ 0 ] = PLAIN0;
    two[ 1 ] = EMPTY;
    sub_case( "good-then-bad", 2, two, 1U, true, 1U, 1U, false );

    two[ 0 ] = PLAIN1;
    two[ 1 ] = PLAIN0;
    sub_case( "qos1-then-qos0-without-records", 2, two, 1U, false, 1U, 1U, false );

    /* --- the publish validator --- */
    pub_case( "qos0", MQTTQoS0, 0U, true, 5U, 3U, true );
    pub_case( "qos1-with-id", MQTTQoS1, 1U, true, 5U, 3U, true );
    pub_case( "qos1-zero-id", MQTTQoS1, 0U, true, 5U, 3U, true );
    pub_case( "qos1-without-records", MQTTQoS1, 1U, true, 5U, 3U, false );
    pub_case( "payload-without-pointer", MQTTQoS0, 0U, false, 5U, 3U, true );
    pub_case( "no-payload-at-all", MQTTQoS0, 0U, false, 0U, 3U, true );
    pub_case( "huge-topic", MQTTQoS0, 0U, true, 5U, 65536U, true );
    pub_case( "zero-topic", MQTTQoS0, 0U, true, 5U, 0U, true );

    printf( "end\n" );
    return 0;
}
