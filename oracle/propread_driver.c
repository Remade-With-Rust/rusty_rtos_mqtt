/* The C arm of K7's coreMQTT PROPERTY-READER differential.
 *
 * `core_mqtt_prop_deserializer.c` is compiled VERBATIM out of the pinned
 * checkout; nothing here copies or edits it.
 *
 * # The other half of the builder
 *
 * The previous slice proved `core_mqtt_prop_serializer.c`, which assembles a
 * property section. This is the file that walks one back: the same
 * `MQTTPropBuilder_t`, a caller-owned cursor, and twenty-two getters that each
 * demand ONE property identifier and refuse anything else.
 *
 * # Two more tables over the same alphabet
 *
 * `MQTT_GetNextPropertyType` answers whether a byte is a property identifier at
 * all -- twenty-seven `case` labels and a `default` that refuses.
 * `MQTT_SkipNextProperty` maps the same byte to a WIDTH, in five groups, so it
 * can step past a property it does not care about. Two tables, one alphabet,
 * written separately: so both are swept over all 256 bytes and both accepted
 * sets are printed, and the interesting line is the one where they differ.
 *
 * A byte the type check accepts and the skipper cannot skip strands a reader
 * half way through a section; a byte the skipper handles and the type check
 * refuses is the same bug from the other end.
 *
 * # And the cursor is the answer
 *
 * Every one of these functions advances a caller-owned index, so what matters
 * is not only the status and the value but WHERE THE CURSOR ENDS. A getter
 * that returned the right value and left the cursor one byte out would pass a
 * value-only comparison and desynchronise the next call. Every line prints the
 * index after the call.
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core_mqtt_serializer.h"

#define MAX_SECTION    32

static const char * status_name( MQTTStatus_t status )
{
    switch( status )
    {
        case MQTTSuccess:           return "Success";
        case MQTTBadParameter:      return "BadParameter";
        case MQTTBadResponse:       return "BadResponse";
        case MQTTEndOfProperties:   return "EndOfProperties";
        default:                    return "OTHER";
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

/* ---- the twenty-two getters, by name --------------------------------------- */

enum
{
    G_SESSION_EXPIRY = 0,
    G_RECEIVE_MAX,
    G_MAX_QOS,
    G_RETAIN_AVAILABLE,
    G_MAX_PACKET_SIZE,
    G_ASSIGNED_CLIENT_ID,
    G_TOPIC_ALIAS_MAX,
    G_REASON_STRING,
    G_WILDCARD_ID,
    G_SUBS_ID_AVAILABLE,
    G_SHARED_SUB_AVAILABLE,
    G_SERVER_KEEP_ALIVE,
    G_RESPONSE_INFO,
    G_SERVER_REF,
    G_AUTH_METHOD,
    G_AUTH_DATA,
    G_PAYLOAD_FORMAT,
    G_MESSAGE_EXPIRY,
    G_TOPIC_ALIAS,
    G_RESPONSE_TOPIC,
    G_CORRELATION_DATA,
    G_SUBSCRIPTION_ID,
    G_CONTENT_TYPE,
    G_USER_PROP,
    G_NEXT_TYPE,
    G_SKIP,
    G_COUNT
};

static const char * const G_NAMES[ G_COUNT ] = {
    "session-expiry", "receive-max", "max-qos", "retain-available",
    "max-packet-size", "assigned-client-id", "topic-alias-max", "reason-string",
    "wildcard-available", "subs-id-available", "shared-sub-available",
    "server-keep-alive", "response-info", "server-ref", "auth-method",
    "auth-data", "payload-format", "message-expiry", "topic-alias",
    "response-topic", "correlation-data", "subscription-id", "content-type",
    "user-prop", "next-type", "skip"
};

/* One getter, printing whatever it hands back. */
static MQTTStatus_t get_one( int which,
                             const MQTTPropBuilder_t *builder,
                             size_t *index,
                             bool print )
{
    MQTTStatus_t status;
    uint32_t u32 = 0xAAAAAAAAU;
    uint16_t u16 = 0xAAAAU;
    uint8_t u8 = 0xAAU;
    const char *text = NULL;
    size_t length = 0U;
    MQTTUserProperty_t user;

    memset( &user, 0, sizeof user );

    switch( which )
    {
        case G_SESSION_EXPIRY:
            status = MQTTPropGet_SessionExpiry( builder, index, &u32 );
            break;

        case G_RECEIVE_MAX:
            status = MQTTPropGet_ReceiveMax( builder, index, &u16 );
            break;

        case G_MAX_QOS:
            status = MQTTPropGet_MaxQos( builder, index, &u8 );
            break;

        case G_RETAIN_AVAILABLE:
            status = MQTTPropGet_RetainAvailable( builder, index, &u8 );
            break;

        case G_MAX_PACKET_SIZE:
            status = MQTTPropGet_MaxPacketSize( builder, index, &u32 );
            break;

        case G_ASSIGNED_CLIENT_ID:
            status = MQTTPropGet_AssignedClientId( builder, index, &text, &length );
            break;

        case G_TOPIC_ALIAS_MAX:
            status = MQTTPropGet_TopicAliasMax( builder, index, &u16 );
            break;

        case G_REASON_STRING:
            status = MQTTPropGet_ReasonString( builder, index, &text, &length );
            break;

        case G_WILDCARD_ID:
            status = MQTTPropGet_WildcardId( builder, index, &u8 );
            break;

        case G_SUBS_ID_AVAILABLE:
            status = MQTTPropGet_SubsIdAvailable( builder, index, &u8 );
            break;

        case G_SHARED_SUB_AVAILABLE:
            status = MQTTPropGet_SharedSubAvailable( builder, index, &u8 );
            break;

        case G_SERVER_KEEP_ALIVE:
            status = MQTTPropGet_ServerKeepAlive( builder, index, &u16 );
            break;

        case G_RESPONSE_INFO:
            status = MQTTPropGet_ResponseInfo( builder, index, &text, &length );
            break;

        case G_SERVER_REF:
            status = MQTTPropGet_ServerRef( builder, index, &text, &length );
            break;

        case G_AUTH_METHOD:
            status = MQTTPropGet_AuthMethod( builder, index, &text, &length );
            break;

        case G_AUTH_DATA:
            status = MQTTPropGet_AuthData( builder, index, &text, &length );
            break;

        case G_PAYLOAD_FORMAT:
            status = MQTTPropGet_PayloadFormatIndicator( builder, index, &u8 );
            break;

        case G_MESSAGE_EXPIRY:
            status = MQTTPropGet_MessageExpiryInterval( builder, index, &u32 );
            break;

        case G_TOPIC_ALIAS:
            status = MQTTPropGet_TopicAlias( builder, index, &u16 );
            break;

        case G_RESPONSE_TOPIC:
            status = MQTTPropGet_ResponseTopic( builder, index, &text, &length );
            break;

        case G_CORRELATION_DATA:
            status = MQTTPropGet_CorrelationData( builder, index, &text, &length );
            break;

        case G_SUBSCRIPTION_ID:
            status = MQTTPropGet_SubscriptionId( builder, index, &u32 );
            break;

        case G_CONTENT_TYPE:
            status = MQTTPropGet_ContentType( builder, index, &text, &length );
            break;

        case G_USER_PROP:
            status = MQTTPropGet_UserProp( builder, index, &user );
            break;

        case G_NEXT_TYPE:
            status = MQTT_GetNextPropertyType( builder, index, &u8 );
            break;

        default:
            status = MQTT_SkipNextProperty( builder, index );
            break;
    }

    if( print )
    {
        printf( "%s", status_name( status ) );

        /* Only on success, and only what that getter produces: a sentinel
         * printed in both arms is a constant compared with itself. */
        if( status == MQTTSuccess )
        {
            switch( which )
            {
                case G_SESSION_EXPIRY:
                case G_MAX_PACKET_SIZE:
                case G_MESSAGE_EXPIRY:
                case G_SUBSCRIPTION_ID:
                    printf( " value=%u", ( unsigned ) u32 );
                    break;

                case G_RECEIVE_MAX:
                case G_TOPIC_ALIAS_MAX:
                case G_SERVER_KEEP_ALIVE:
                case G_TOPIC_ALIAS:
                    printf( " value=%u", ( unsigned ) u16 );
                    break;

                case G_MAX_QOS:
                case G_RETAIN_AVAILABLE:
                case G_WILDCARD_ID:
                case G_SUBS_ID_AVAILABLE:
                case G_SHARED_SUB_AVAILABLE:
                case G_PAYLOAD_FORMAT:
                case G_NEXT_TYPE:
                    printf( " value=%u", ( unsigned ) u8 );
                    break;

                case G_USER_PROP:
                    printf( " key=" );
                    put_hex( ( const uint8_t * ) user.pKey, user.keyLength );
                    printf( " val=" );
                    put_hex( ( const uint8_t * ) user.pValue, user.valueLength );
                    break;

                case G_SKIP:
                    /* Nothing but the cursor, which every line prints. */
                    break;

                default:
                    printf( " text=" );
                    put_hex( ( const uint8_t * ) text, length );
                    break;
            }
        }
    }

    return status;
}

/* ---- the two tables, swept ------------------------------------------------- */

/* Every byte through `MQTT_GetNextPropertyType`, and every byte through
 * `MQTT_SkipNextProperty`, with both accepted sets printed. They are written
 * separately over the same alphabet, so the line that matters is the one where
 * they differ. */
static void sweep_tables( void )
{
    unsigned id;
    unsigned known = 0U, skippable = 0U, differ = 0U;
    int first_known = 1, first_skip = 1;
    uint64_t digest = FNV_SEED;
    char known_set[ 512 ];
    char skip_set[ 512 ];
    size_t known_len = 0U, skip_len = 0U;

    known_set[ 0 ] = '\0';
    skip_set[ 0 ] = '\0';

    for( id = 0U; id < 256U; id++ )
    {
        /* A section wide enough for the widest property, so a refusal is the
         * TABLE's and never the buffer's. */
        uint8_t bytes[ MAX_SECTION ];
        MQTTPropBuilder_t builder;
        size_t index;
        MQTTStatus_t a, b;

        memset( bytes, 0, sizeof bytes );
        bytes[ 0 ] = ( uint8_t ) id;
        /* A two-byte string body, which also serves as a valid two-byte
         * integer and as the first half of a user property. */
        bytes[ 1 ] = 0x00;
        bytes[ 2 ] = 0x02;
        bytes[ 3 ] = 'a';
        bytes[ 4 ] = 'b';
        bytes[ 5 ] = 0x00;
        bytes[ 6 ] = 0x02;
        bytes[ 7 ] = 'c';
        bytes[ 8 ] = 'd';

        memset( &builder, 0, sizeof builder );
        builder.pBuffer = bytes;
        builder.bufferLength = sizeof bytes;
        builder.currentIndex = 9U;

        index = 0U;
        a = get_one( G_NEXT_TYPE, &builder, &index, false );

        index = 0U;
        b = get_one( G_SKIP, &builder, &index, false );

        digest = fnv( digest, ( uint64_t ) a );
        digest = fnv( digest, ( uint64_t ) b );
        digest = fnv( digest, ( uint64_t ) index );

        if( a == MQTTSuccess )
        {
            known_len += ( size_t ) snprintf( &known_set[ known_len ],
                                              sizeof known_set - known_len,
                                              "%s%02x", first_known ? "" : ",", id );
            first_known = 0;
            known++;
        }

        if( b == MQTTSuccess )
        {
            skip_len += ( size_t ) snprintf( &skip_set[ skip_len ],
                                             sizeof skip_set - skip_len,
                                             "%s%02x", first_skip ? "" : ",", id );
            first_skip = 0;
            skippable++;
        }

        /* A byte one table knows and the other cannot handle. */
        if( ( a == MQTTSuccess ) != ( b == MQTTSuccess ) )
        {
            differ++;
        }
    }

    printf( "known accepted=%s n=%u\n", known_len == 0U ? "-" : known_set, known );
    printf( "skippable accepted=%s n=%u\n", skip_len == 0U ? "-" : skip_set, skippable );
    printf( "tables differ=%u digest=%016llx\n", differ,
            ( unsigned long long ) digest );
}

/* Every GETTER against every identifier: 24 x 256, digested.
 *
 * A getter demands one identifier and refuses every other, so this is the
 * whole of that rule -- 6,144 calls saying that no getter has been wired to
 * the wrong constant. */
static void sweep_getters( void )
{
    int which;

    for( which = 0; which < G_NEXT_TYPE; which++ )
    {
        unsigned id;
        unsigned accepted = 0U;
        int first = 1;
        uint64_t digest = FNV_SEED;

        printf( "getter %s accepted=", G_NAMES[ which ] );

        for( id = 0U; id < 256U; id++ )
        {
            uint8_t bytes[ MAX_SECTION ];
            MQTTPropBuilder_t builder;
            size_t index = 0U;
            MQTTStatus_t status;

            memset( bytes, 0, sizeof bytes );
            bytes[ 0 ] = ( uint8_t ) id;
            bytes[ 1 ] = 0x00;
            bytes[ 2 ] = 0x02;
            bytes[ 3 ] = 'a';
            bytes[ 4 ] = 'b';
            bytes[ 5 ] = 0x00;
            bytes[ 6 ] = 0x02;
            bytes[ 7 ] = 'c';
            bytes[ 8 ] = 'd';

            memset( &builder, 0, sizeof builder );
            builder.pBuffer = bytes;
            builder.bufferLength = sizeof bytes;
            builder.currentIndex = 9U;

            status = get_one( which, &builder, &index, false );
            digest = fnv( digest, ( uint64_t ) status );
            digest = fnv( digest, ( uint64_t ) index );

            if( status == MQTTSuccess )
            {
                printf( "%s%02x", first ? "" : ",", id );
                first = 0;
                accepted++;
            }
        }

        printf( " n=%u digest=%016llx\n", accepted, ( unsigned long long ) digest );
    }
}

/* ---- the named cases ------------------------------------------------------- */

typedef struct
{
    const char *name;
    size_t      length;
    uint8_t     section[ MAX_SECTION ];
    size_t      start;
    int         steps;
    int         step[ 4 ];
} ReadCase_t;

static const ReadCase_t CASES[] = {
    /* --- one of each width, read by the getter that owns it --- */
    { "session-expiry", 5, { 0x11, 0x00, 0x00, 0x0E, 0x10 }, 0, 1, { G_SESSION_EXPIRY } },
    { "receive-max",    3, { 0x21, 0x00, 0x14 }, 0, 1, { G_RECEIVE_MAX } },
    { "max-qos",        2, { 0x24, 0x01 }, 0, 1, { G_MAX_QOS } },
    { "reason-string",  5, { 0x1F, 0x00, 0x02, 'h', 'i' }, 0, 1, { G_REASON_STRING } },
    { "subscription-id", 2, { 0x0B, 0x07 }, 0, 1, { G_SUBSCRIPTION_ID } },
    { "user-prop",      7, { 0x26, 0x00, 0x01, 'k', 0x00, 0x01, 'v' }, 0, 1, { G_USER_PROP } },

    /* --- the wrong getter for the property under the cursor --- */
    { "receive-max-read-as-session-expiry", 3, { 0x21, 0x00, 0x14 }, 0, 1,
      { G_SESSION_EXPIRY } },
    { "session-expiry-read-as-user-prop", 5, { 0x11, 0x00, 0x00, 0x0E, 0x10 }, 0, 1,
      { G_USER_PROP } },

    /* --- the cursor, which is the answer --- */
    { "two-in-a-row", 8, { 0x24, 0x01, 0x21, 0x00, 0x14, 0x25, 0x01, 0x00 }, 0, 2,
      { G_MAX_QOS, G_RECEIVE_MAX } },
    { "start-past-the-first", 5, { 0x24, 0x01, 0x21, 0x00, 0x14 }, 2, 1,
      { G_RECEIVE_MAX } },
    { "start-at-the-end", 2, { 0x24, 0x01 }, 2, 1, { G_MAX_QOS } },
    { "start-past-the-end", 2, { 0x24, 0x01 }, 3, 1, { G_MAX_QOS } },
    { "empty-section", 0, { 0 }, 0, 1, { G_NEXT_TYPE } },

    /* --- skipping, which is what a reader does with a property it does not
     * want, and the only way to reach the one after it --- */
    { "skip-then-read", 8, { 0x24, 0x01, 0x21, 0x00, 0x14, 0x25, 0x01, 0x00 }, 0, 2,
      { G_SKIP, G_RECEIVE_MAX } },
    { "skip-a-string", 8, { 0x1F, 0x00, 0x02, 'h', 'i', 0x24, 0x01, 0x00 }, 0, 2,
      { G_SKIP, G_MAX_QOS } },
    { "skip-a-user-prop", 9, { 0x26, 0x00, 0x01, 'k', 0x00, 0x01, 'v', 0x24, 0x01 }, 0, 2,
      { G_SKIP, G_MAX_QOS } },
    { "skip-a-subscription-id", 4, { 0x0B, 0x81, 0x01, 0x00 }, 0, 2,
      { G_SKIP, G_NEXT_TYPE } },
    { "skip-twice-off-the-end", 2, { 0x24, 0x01 }, 0, 2, { G_SKIP, G_SKIP } },

    /* --- truncated values, one per width --- */
    { "truncated-uint32", 3, { 0x11, 0x00, 0x00 }, 0, 1, { G_SESSION_EXPIRY } },
    { "truncated-uint16", 2, { 0x21, 0x00 }, 0, 1, { G_RECEIVE_MAX } },
    { "truncated-string", 4, { 0x1F, 0x00, 0x09, 'h' }, 0, 1, { G_REASON_STRING } },
    { "truncated-user-prop", 5, { 0x26, 0x00, 0x01, 'k', 0x00 }, 0, 1, { G_USER_PROP } },
    { "truncated-skip", 3, { 0x11, 0x00, 0x00 }, 0, 1, { G_SKIP } },
    /* A subscription identifier whose continuation byte runs off the end. */
    { "truncated-subscription-id", 2, { 0x0B, 0x81 }, 0, 1, { G_SUBSCRIPTION_ID } },
    /* Non-minimal: two spellings of one value, and only one is legal. */
    { "non-minimal-subscription-id", 3, { 0x0B, 0x80, 0x00 }, 0, 1, { G_SUBSCRIPTION_ID } },

    /* --- a whole CONNACK-shaped section, walked by type --- */
    { "walk-by-type", 10, { 0x24, 0x01, 0x25, 0x01, 0x21, 0x00, 0x14, 0x11, 0x00, 0x00 }, 0, 3,
      { G_NEXT_TYPE, G_SKIP, G_NEXT_TYPE } },
};

#define N_CASES    ( sizeof( CASES ) / sizeof( CASES[ 0 ] ) )

static void run_case( size_t i )
{
    const ReadCase_t *c = &CASES[ i ];
    uint8_t bytes[ MAX_SECTION ];
    MQTTPropBuilder_t builder;
    size_t index = c->start;
    int step;

    memset( bytes, 0, sizeof bytes );
    memcpy( bytes, c->section, c->length );

    memset( &builder, 0, sizeof builder );
    builder.pBuffer = bytes;
    builder.bufferLength = sizeof bytes;
    builder.currentIndex = c->length;

    printf( "read %u %s at=%u props=", ( unsigned ) i, c->name,
            ( unsigned ) c->start );
    put_hex( c->section, c->length );

    for( step = 0; step < c->steps; step++ )
    {
        printf( " | %s->", G_NAMES[ c->step[ step ] ] );
        ( void ) get_one( c->step[ step ], &builder, &index, true );
        printf( " at=%u", ( unsigned ) index );
    }

    printf( "\n" );
}

int main( void )
{
    size_t i;

    printf( "geometry getters=%u cases=%u\n", ( unsigned ) G_NEXT_TYPE,
            ( unsigned ) N_CASES );

    sweep_tables();
    sweep_getters();

    for( i = 0; i < N_CASES; i++ )
    {
        run_case( i );
    }

    printf( "end\n" );
    return 0;
}
