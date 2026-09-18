/* The C arm of K7's coreMQTT PROPERTY-BUILDER differential.
 *
 * `core_mqtt_prop_serializer.c` is compiled VERBATIM out of the pinned
 * checkout; nothing here copies or edits it.
 *
 * # A FOURTH copy of "which property may go in which packet"
 *
 * The centre of this file is `isValidPropertyInPacketType`, a 199-line switch
 * keyed on the PACKET TYPE that answers whether one property belongs there.
 * The package has already proven three other answers to the same question: the
 * six outgoing validators (slice 14), the CONNECT context filler (slice 15),
 * and the incoming deserializers' tables (slices 6-9).
 *
 * The function is static, so the only way to ask it anything is through the
 * eighteen public adders. That is what this driver does: for every one of the
 * 256 packet-type bytes it tries all eighteen, and prints the accepted set as
 * an eighteen-bit mask. Sixteen named types are printed and all 256 are
 * digested, so a change anywhere in the switch moves a number here.
 *
 * # And the bytes
 *
 * The rest of the file is five primitives and eighteen thin wrappers over
 * them, and what they produce is BYTES. Every `add` case prints the section
 * after the call -- the cursor, the field bitmask and the bytes themselves --
 * because a builder that returned the right status while writing the wrong id
 * would pass a status-only comparison, and because the property id byte is
 * written by the primitive and the value by its caller.
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core_mqtt_serializer.h"

#define BUFFER_BYTES    64

static const char * status_name( MQTTStatus_t status )
{
    switch( status )
    {
        case MQTTSuccess:      return "Success";
        case MQTTBadParameter: return "BadParameter";
        case MQTTNoMemory:     return "NoMemory";
        default:               return "OTHER";
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

/* The house digest: FNV-1a's multiplier, and a seed one digit short of its
 * offset basis, as every driver in this package uses. */
static uint64_t fnv( uint64_t state, uint64_t value )
{
    return ( state ^ value ) * 1099511628211ULL;
}

#define FNV_SEED    1469598103934665603ULL

/* ---- the eighteen adders, by name ------------------------------------------ */

enum
{
    P_SESSION_EXPIRY = 0,
    P_RECEIVE_MAX,
    P_MAX_PACKET_SIZE,
    P_TOPIC_ALIAS_MAX,
    P_REQUEST_RESP_INFO,
    P_REQUEST_PROB_INFO,
    P_AUTH_METHOD,
    P_AUTH_DATA,
    P_PAYLOAD_FORMAT,
    P_MESSAGE_EXPIRY,
    P_WILL_DELAY,
    P_TOPIC_ALIAS,
    P_RESPONSE_TOPIC,
    P_CORRELATION_DATA,
    P_CONTENT_TYPE,
    P_REASON_STRING,
    P_SUBSCRIPTION_ID,
    P_USER_PROP,
    P_COUNT
};

static const char * const P_NAMES[ P_COUNT ] = {
    "session-expiry", "receive-max", "max-packet-size", "topic-alias-max",
    "request-resp-info", "request-prob-info", "auth-method", "auth-data",
    "payload-format", "message-expiry", "will-delay", "topic-alias",
    "response-topic", "correlation-data", "content-type", "reason-string",
    "subscription-id", "user-prop"
};

/* One adder, with a value chosen so that only the TABLE can refuse it. */
static MQTTStatus_t add_one( int which,
                             MQTTPropBuilder_t *builder,
                             const uint8_t *packetType )
{
    static const char TEXT[] = "ab";
    MQTTUserProperty_t user;

    user.pKey = "k";
    user.keyLength = 1U;
    user.pValue = "v";
    user.valueLength = 1U;

    switch( which )
    {
        case P_SESSION_EXPIRY:    return MQTTPropAdd_SessionExpiry( builder, 30U, packetType );
        case P_RECEIVE_MAX:       return MQTTPropAdd_ReceiveMax( builder, 10U, packetType );
        case P_MAX_PACKET_SIZE:   return MQTTPropAdd_MaxPacketSize( builder, 1024U, packetType );
        case P_TOPIC_ALIAS_MAX:   return MQTTPropAdd_TopicAliasMax( builder, 5U, packetType );
        case P_REQUEST_RESP_INFO: return MQTTPropAdd_RequestRespInfo( builder, true, packetType );
        case P_REQUEST_PROB_INFO: return MQTTPropAdd_RequestProbInfo( builder, true, packetType );
        case P_AUTH_METHOD:       return MQTTPropAdd_AuthMethod( builder, TEXT, 2U, packetType );
        case P_AUTH_DATA:         return MQTTPropAdd_AuthData( builder, TEXT, 2U, packetType );
        case P_PAYLOAD_FORMAT:    return MQTTPropAdd_PayloadFormat( builder, true, packetType );
        case P_MESSAGE_EXPIRY:    return MQTTPropAdd_MessageExpiry( builder, 60U, packetType );
        case P_WILL_DELAY:        return MQTTPropAdd_WillDelayInterval( builder, 15U, packetType );
        case P_TOPIC_ALIAS:       return MQTTPropAdd_TopicAlias( builder, 7U, packetType );
        case P_RESPONSE_TOPIC:    return MQTTPropAdd_ResponseTopic( builder, TEXT, 2U, packetType );
        case P_CORRELATION_DATA:  return MQTTPropAdd_CorrelationData( builder, TEXT, 2U, packetType );
        case P_CONTENT_TYPE:      return MQTTPropAdd_ContentType( builder, TEXT, 2U, packetType );
        case P_REASON_STRING:     return MQTTPropAdd_ReasonString( builder, TEXT, 2U, packetType );
        case P_SUBSCRIPTION_ID:   return MQTTPropAdd_SubscriptionId( builder, 3U, packetType );
        default:                  return MQTTPropAdd_UserProp( builder, &user, packetType );
    }
}

/* ---- the table, one packet type at a time ---------------------------------- */

/* Authentication Data refuses to go anywhere unless Authentication Method is
 * already in the section, which is coreMQTT's own rule rather than MQTT's. So
 * the mask is taken on a builder that already holds a method: otherwise that
 * one bit would read zero for every packet type and say nothing about the
 * table. */
static uint32_t mask_for( uint8_t packetType )
{
    uint8_t buffer[ BUFFER_BYTES ];
    uint32_t mask = 0U;
    int which;

    for( which = 0; which < P_COUNT; which++ )
    {
        MQTTPropBuilder_t builder;

        memset( buffer, 0, sizeof buffer );
        ( void ) MQTTPropertyBuilder_Init( &builder, buffer, sizeof buffer );

        if( which == P_AUTH_DATA )
        {
            /* Put a method in first, with NO packet type, so only the data's
             * own table check can refuse it. */
            ( void ) MQTTPropAdd_AuthMethod( &builder, "ab", 2U, NULL );
        }

        if( add_one( which, &builder, &packetType ) == MQTTSuccess )
        {
            mask |= ( 1UL << which );
        }
    }

    return mask;
}

typedef struct
{
    const char *name;
    uint8_t     type;
} NamedType_t;

static const NamedType_t NAMED_TYPES[] = {
    { "connect",     MQTT_PACKET_TYPE_CONNECT },
    { "connack",     MQTT_PACKET_TYPE_CONNACK },
    { "publish",     MQTT_PACKET_TYPE_PUBLISH },
    { "puback",      MQTT_PACKET_TYPE_PUBACK },
    { "pubrec",      MQTT_PACKET_TYPE_PUBREC },
    { "pubrel",      MQTT_PACKET_TYPE_PUBREL },
    { "pubcomp",     MQTT_PACKET_TYPE_PUBCOMP },
    { "subscribe",   MQTT_PACKET_TYPE_SUBSCRIBE },
    { "suback",      MQTT_PACKET_TYPE_SUBACK },
    { "unsubscribe", MQTT_PACKET_TYPE_UNSUBSCRIBE },
    { "unsuback",    MQTT_PACKET_TYPE_UNSUBACK },
    { "pingreq",     MQTT_PACKET_TYPE_PINGREQ },
    { "pingresp",    MQTT_PACKET_TYPE_PINGRESP },
    { "disconnect",  MQTT_PACKET_TYPE_DISCONNECT },
    { "auth",        MQTT_PACKET_TYPE_AUTH },
    { "unknown-00",  0x00 },
};

#define N_NAMED_TYPES    ( sizeof( NAMED_TYPES ) / sizeof( NAMED_TYPES[ 0 ] ) )

static void print_masks( void )
{
    size_t i;
    unsigned type;
    uint64_t digest = FNV_SEED;

    for( i = 0; i < N_NAMED_TYPES; i++ )
    {
        uint32_t mask = mask_for( NAMED_TYPES[ i ].type );
        int which;

        printf( "allowed %s type=%02x mask=%05x props=", NAMED_TYPES[ i ].name,
                ( unsigned ) NAMED_TYPES[ i ].type, ( unsigned ) mask );

        if( mask == 0U )
        {
            printf( "-" );
        }
        else
        {
            int first = 1;

            for( which = 0; which < P_COUNT; which++ )
            {
                if( ( mask & ( 1UL << which ) ) != 0U )
                {
                    printf( "%s%s", first ? "" : ",", P_NAMES[ which ] );
                    first = 0;
                }
            }
        }

        printf( "\n" );
    }

    /* And every type byte, because the switch has a `default` arm and the
     * sixteen above are the ones somebody thought to name. */
    for( type = 0U; type < 256U; type++ )
    {
        digest = fnv( digest, ( uint64_t ) mask_for( ( uint8_t ) type ) );
    }

    printf( "allowed-sweep types=256 digest=%016llx\n",
            ( unsigned long long ) digest );
}

/* ---- the bytes ------------------------------------------------------------- */

/* A case is a short program: init a builder of `capacity` bytes, then run a
 * list of steps, printing the state after each. */
typedef struct
{
    int      which;
    int      variant;     /* 0 = the ordinary value; others are per-adder */
    bool     withType;
    uint8_t  packetType;
} Step_t;

typedef struct
{
    const char *name;
    size_t      capacity;
    size_t      steps;
    Step_t      step[ 4 ];
} AddCase_t;

/* Variants: a value the adder itself refuses, or a second call. */
#define V_ORDINARY    0
#define V_ZERO        1
#define V_WILDCARD    2
#define V_EMPTY       3
#define V_HUGE        4
#define V_HUGE_OVER   5

static MQTTStatus_t run_step( const Step_t *step, MQTTPropBuilder_t *builder )
{
    const uint8_t *type = step->withType ? &step->packetType : NULL;
    static const char WILDCARD[] = "a/#";
    MQTTUserProperty_t user;

    switch( step->variant )
    {
        case V_ORDINARY:
            return add_one( step->which, builder, type );

        case V_ZERO:

            switch( step->which )
            {
                case P_RECEIVE_MAX:     return MQTTPropAdd_ReceiveMax( builder, 0U, type );
                case P_MAX_PACKET_SIZE: return MQTTPropAdd_MaxPacketSize( builder, 0U, type );
                case P_TOPIC_ALIAS:     return MQTTPropAdd_TopicAlias( builder, 0U, type );
                default:                return MQTTPropAdd_SubscriptionId( builder, 0U, type );
            }

        case V_WILDCARD:
            return MQTTPropAdd_ResponseTopic( builder, WILDCARD, 3U, type );

        case V_EMPTY:

            switch( step->which )
            {
                case P_REASON_STRING: return MQTTPropAdd_ReasonString( builder, "", 0U, type );
                case P_CONTENT_TYPE:  return MQTTPropAdd_ContentType( builder, "", 0U, type );
                default:
                    user.pKey = "k";
                    user.keyLength = 1U;
                    user.pValue = "";
                    user.valueLength = 0U;
                    return MQTTPropAdd_UserProp( builder, &user, type );
            }

        case V_HUGE:
            /* The largest a variable-length integer may carry. */
            return MQTTPropAdd_SubscriptionId( builder, 268435455U, type );

        default:
            /* One over it. */
            return MQTTPropAdd_SubscriptionId( builder, 268435456U, type );
    }
}

static const AddCase_t ADD_CASES[] = {
    /* --- one of each, so every id byte and every width is in the trace --- */
    { "session-expiry",   32, 1, { { P_SESSION_EXPIRY, V_ORDINARY, false, 0 } } },
    { "receive-max",      32, 1, { { P_RECEIVE_MAX, V_ORDINARY, false, 0 } } },
    { "max-packet-size",  32, 1, { { P_MAX_PACKET_SIZE, V_ORDINARY, false, 0 } } },
    { "topic-alias-max",  32, 1, { { P_TOPIC_ALIAS_MAX, V_ORDINARY, false, 0 } } },
    { "request-resp",     32, 1, { { P_REQUEST_RESP_INFO, V_ORDINARY, false, 0 } } },
    { "request-prob",     32, 1, { { P_REQUEST_PROB_INFO, V_ORDINARY, false, 0 } } },
    { "auth-method",      32, 1, { { P_AUTH_METHOD, V_ORDINARY, false, 0 } } },
    { "payload-format",   32, 1, { { P_PAYLOAD_FORMAT, V_ORDINARY, false, 0 } } },
    { "message-expiry",   32, 1, { { P_MESSAGE_EXPIRY, V_ORDINARY, false, 0 } } },
    { "will-delay",       32, 1, { { P_WILL_DELAY, V_ORDINARY, false, 0 } } },
    { "topic-alias",      32, 1, { { P_TOPIC_ALIAS, V_ORDINARY, false, 0 } } },
    { "response-topic",   32, 1, { { P_RESPONSE_TOPIC, V_ORDINARY, false, 0 } } },
    { "correlation-data", 32, 1, { { P_CORRELATION_DATA, V_ORDINARY, false, 0 } } },
    { "content-type",     32, 1, { { P_CONTENT_TYPE, V_ORDINARY, false, 0 } } },
    { "reason-string",    32, 1, { { P_REASON_STRING, V_ORDINARY, false, 0 } } },
    { "subscription-id",  32, 1, { { P_SUBSCRIPTION_ID, V_ORDINARY, false, 0 } } },
    { "user-prop",        32, 1, { { P_USER_PROP, V_ORDINARY, false, 0 } } },

    /* --- authentication data needs a method already in the section --- */
    { "auth-data-alone",  32, 1, { { P_AUTH_DATA, V_ORDINARY, false, 0 } } },
    { "auth-method-then-data", 32, 2, { { P_AUTH_METHOD, V_ORDINARY, false, 0 },
                                        { P_AUTH_DATA, V_ORDINARY, false, 0 } } },

    /* --- a property may be added once, and a user property many times --- */
    { "session-expiry-twice", 32, 2, { { P_SESSION_EXPIRY, V_ORDINARY, false, 0 },
                                       { P_SESSION_EXPIRY, V_ORDINARY, false, 0 } } },
    { "user-prop-twice",  32, 2, { { P_USER_PROP, V_ORDINARY, false, 0 },
                                   { P_USER_PROP, V_ORDINARY, false, 0 } } },

    /* --- the values the adders refuse themselves --- */
    { "receive-max-zero", 32, 1, { { P_RECEIVE_MAX, V_ZERO, false, 0 } } },
    { "max-packet-zero",  32, 1, { { P_MAX_PACKET_SIZE, V_ZERO, false, 0 } } },
    { "topic-alias-zero", 32, 1, { { P_TOPIC_ALIAS, V_ZERO, false, 0 } } },
    { "subscription-id-zero", 32, 1, { { P_SUBSCRIPTION_ID, V_ZERO, false, 0 } } },
    { "subscription-id-at-the-maximum", 32, 1, { { P_SUBSCRIPTION_ID, V_HUGE, false, 0 } } },
    { "subscription-id-one-over", 32, 1, { { P_SUBSCRIPTION_ID, V_HUGE_OVER, false, 0 } } },
    { "response-topic-wildcard", 32, 1, { { P_RESPONSE_TOPIC, V_WILDCARD, false, 0 } } },
    { "reason-string-empty", 32, 1, { { P_REASON_STRING, V_EMPTY, false, 0 } } },
    { "content-type-empty", 32, 1, { { P_CONTENT_TYPE, V_EMPTY, false, 0 } } },
    { "user-prop-empty-value", 32, 1, { { P_USER_PROP, V_EMPTY, false, 0 } } },

    /* --- the buffer, at and below what each width needs --- */
    { "uint8-in-two-bytes",   2, 1, { { P_PAYLOAD_FORMAT, V_ORDINARY, false, 0 } } },
    { "uint8-in-one-byte",    1, 1, { { P_PAYLOAD_FORMAT, V_ORDINARY, false, 0 } } },
    { "uint16-in-three-bytes", 3, 1, { { P_TOPIC_ALIAS, V_ORDINARY, false, 0 } } },
    { "uint16-in-two-bytes",   2, 1, { { P_TOPIC_ALIAS, V_ORDINARY, false, 0 } } },
    { "uint32-in-five-bytes",  5, 1, { { P_SESSION_EXPIRY, V_ORDINARY, false, 0 } } },
    { "uint32-in-four-bytes",  4, 1, { { P_SESSION_EXPIRY, V_ORDINARY, false, 0 } } },
    { "utf8-in-five-bytes",    5, 1, { { P_CONTENT_TYPE, V_ORDINARY, false, 0 } } },
    { "utf8-in-four-bytes",    4, 1, { { P_CONTENT_TYPE, V_ORDINARY, false, 0 } } },
    { "auth-method-in-four-bytes", 4, 1, { { P_AUTH_METHOD, V_ORDINARY, false, 0 } } },
    { "reason-string-in-four-bytes", 4, 1, { { P_REASON_STRING, V_ORDINARY, false, 0 } } },
    { "user-prop-in-seven-bytes", 7, 1, { { P_USER_PROP, V_ORDINARY, false, 0 } } },
    { "user-prop-in-six-bytes",   6, 1, { { P_USER_PROP, V_ORDINARY, false, 0 } } },
    { "subscription-id-in-two-bytes", 2, 1, { { P_SUBSCRIPTION_ID, V_ORDINARY, false, 0 } } },
    { "subscription-id-in-one-byte",  1, 1, { { P_SUBSCRIPTION_ID, V_ORDINARY, false, 0 } } },
    /* Two that fit, then one that does not. */
    { "two-then-full",    6, 3, { { P_PAYLOAD_FORMAT, V_ORDINARY, false, 0 },
                                  { P_TOPIC_ALIAS, V_ORDINARY, false, 0 },
                                  { P_SESSION_EXPIRY, V_ORDINARY, false, 0 } } },

    /* --- with a packet type, which brings the table into it --- */
    { "will-delay-in-connect", 32, 1,
      { { P_WILL_DELAY, V_ORDINARY, true, MQTT_PACKET_TYPE_CONNECT } } },
    { "will-delay-in-publish", 32, 1,
      { { P_WILL_DELAY, V_ORDINARY, true, MQTT_PACKET_TYPE_PUBLISH } } },
    { "topic-alias-in-publish", 32, 1,
      { { P_TOPIC_ALIAS, V_ORDINARY, true, MQTT_PACKET_TYPE_PUBLISH } } },
    { "topic-alias-in-connect", 32, 1,
      { { P_TOPIC_ALIAS, V_ORDINARY, true, MQTT_PACKET_TYPE_CONNECT } } },
    { "reason-string-in-puback", 32, 1,
      { { P_REASON_STRING, V_ORDINARY, true, MQTT_PACKET_TYPE_PUBACK } } },
    { "reason-string-in-subscribe", 32, 1,
      { { P_REASON_STRING, V_ORDINARY, true, MQTT_PACKET_TYPE_SUBSCRIBE } } },
    { "user-prop-in-pingreq", 32, 1,
      { { P_USER_PROP, V_ORDINARY, true, MQTT_PACKET_TYPE_PINGREQ } } },

    /* --- and a whole CONNECT section, built the way an application would --- */
    { "a-connect-section", 32, 4,
      { { P_SESSION_EXPIRY, V_ORDINARY, true, MQTT_PACKET_TYPE_CONNECT },
        { P_RECEIVE_MAX, V_ORDINARY, true, MQTT_PACKET_TYPE_CONNECT },
        { P_MAX_PACKET_SIZE, V_ORDINARY, true, MQTT_PACKET_TYPE_CONNECT },
        { P_USER_PROP, V_ORDINARY, true, MQTT_PACKET_TYPE_CONNECT } } },
};

#define N_ADD_CASES    ( sizeof( ADD_CASES ) / sizeof( ADD_CASES[ 0 ] ) )

static void run_add_case( size_t i )
{
    const AddCase_t *c = &ADD_CASES[ i ];
    uint8_t buffer[ BUFFER_BYTES ];
    MQTTPropBuilder_t builder;
    size_t step;

    memset( buffer, 0, sizeof buffer );
    ( void ) MQTTPropertyBuilder_Init( &builder, buffer, c->capacity );

    printf( "add %u %s cap=%u", ( unsigned ) i, c->name, ( unsigned ) c->capacity );

    for( step = 0; step < c->steps; step++ )
    {
        MQTTStatus_t status = run_step( &c->step[ step ], &builder );

        printf( " | %s(%s%s)->%s", P_NAMES[ c->step[ step ].which ],
                c->step[ step ].variant == V_ORDINARY ? "" : "odd",
                c->step[ step ].withType ? ",typed" : "",
                status_name( status ) );
    }

    printf( " index=%u fieldset=%08x bytes=", ( unsigned ) builder.currentIndex,
            ( unsigned ) builder.fieldSet );
    put_hex( buffer, builder.currentIndex );

    /* The cursor must never pass the buffer the caller supplied. Where it does,
     * the C has written into memory it was not given -- see `addPropUtf8`,
     * whose size check counts the two length bytes and the body and forgets the
     * property ID byte. The marker is here so the checked-in trace says it. */
    if( builder.currentIndex > c->capacity )
    {
        printf( " OVERFLOW" );
    }

    printf( "\n" );
}

int main( void )
{
    size_t i;

    printf( "geometry adders=%u cases=%u types=%u\n", ( unsigned ) P_COUNT,
            ( unsigned ) N_ADD_CASES, ( unsigned ) N_NAMED_TYPES );

    print_masks();

    for( i = 0; i < N_ADD_CASES; i++ )
    {
        run_add_case( i );
    }

    printf( "end\n" );
    return 0;
}
