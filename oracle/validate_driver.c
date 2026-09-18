/* The C arm of K7's coreMQTT OUTGOING-PROPERTY-VALIDATOR differential.
 *
 * `core_mqtt_serializer.c` is compiled VERBATIM out of the pinned checkout;
 * nothing here copies or edits it.
 *
 * # Why these six together
 *
 * MQTT 5 lets almost every packet carry properties, and a DIFFERENT SET for
 * each. coreMQTT enforces that with six hand-written validators -- one for
 * CONNECT, the will, SUBSCRIBE, PUBLISH, the publish acknowledgements and
 * UNSUBSCRIBE -- plus the DISCONNECT's, already proven in its own slice.
 *
 * Six tables is exactly the shape this package has learned to sweep: each is a
 * switch on one byte, so each is swept over all 256 identifiers at each of six
 * value SHAPES, and the accepted set is printed. Thirty-six sweeps, and together
 * they are the whole map of which property may go in which outgoing packet.
 *
 * A table is also where a hand transcription of a specification goes wrong --
 * two of coreMQTT's READING-side tables are already known to be one entry short
 * (§5.4). Printing all six accepted sets is the cheapest way to find out
 * whether the writing side fares better.
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core_mqtt_serializer.h"

#define MAX_SECTION    24

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

static const char * status_name( MQTTStatus_t status )
{
    switch( status )
    {
        case MQTTSuccess:      return "Success";
        case MQTTBadParameter: return "BadParameter";
        case MQTTBadResponse:  return "BadResponse";
        default:               return "OTHER";
    }
}

/* Which validator a case or a sweep is aimed at. */
enum
{
    V_CONNECT = 0,
    V_WILL,
    V_SUBSCRIBE,
    V_PUBLISH,
    V_PUBACK,
    V_UNSUBSCRIBE,
    V_COUNT
};

static const char * const V_NAMES[ V_COUNT ] = {
    "connect", "will", "subscribe", "publish", "puback", "unsubscribe"
};

/* Run one validator over one property section, and report everything it
 * hands back -- two of them have out-parameters, and a validator that answered
 * the right status while setting the wrong number would pass a status-only
 * comparison. */
static MQTTStatus_t run_validator( int which,
                                   const uint8_t *section,
                                   size_t length,
                                   bool subscriptionIdAvailable,
                                   uint16_t serverTopicAliasMax,
                                   bool print )
{
    uint8_t bytes[ MAX_SECTION ];
    MQTTPropBuilder_t builder;
    MQTTStatus_t status;

    /* The out-parameters, pre-set to a value neither validator can produce so
     * "left alone" is visible. */
    bool requestProblemInfo = true;
    uint32_t maxPacketSize = 0xAAAAAAAAU;
    uint16_t topicAlias = 0xAAAAU;

    memset( bytes, 0, sizeof bytes );
    memcpy( bytes, section, length );

    memset( &builder, 0, sizeof builder );
    builder.pBuffer = bytes;
    builder.bufferLength = sizeof bytes;
    builder.currentIndex = length;

    switch( which )
    {
        case V_CONNECT:
            status = MQTT_ValidateConnectProperties( &builder, &requestProblemInfo,
                                                     &maxPacketSize );
            break;

        case V_WILL:
            status = MQTT_ValidateWillProperties( &builder );
            break;

        case V_SUBSCRIBE:
            status = MQTT_ValidateSubscribeProperties( subscriptionIdAvailable, &builder );
            break;

        case V_PUBLISH:
            status = MQTT_ValidatePublishProperties( serverTopicAliasMax, &builder,
                                                     &topicAlias );
            break;

        case V_PUBACK:
            status = MQTT_ValidatePublishAckProperties( &builder );
            break;

        default:
            status = MQTT_ValidateUnsubscribeProperties( &builder );
            break;
    }

    if( print )
    {
        printf( " -> %s", status_name( status ) );

        if( which == V_CONNECT )
        {
            printf( " rpi=%u maxpkt=%u", requestProblemInfo ? 1U : 0U,
                    ( unsigned ) maxPacketSize );
        }
        else if( which == V_PUBLISH )
        {
            printf( " alias=%u", ( unsigned ) topicAlias );
        }
        else
        {
            /* Nothing else has an out-parameter. */
        }
    }

    return status;
}

/* ---- the named cases ------------------------------------------------------ */

typedef struct
{
    const char *name;
    int         which;
    size_t      length;
    uint8_t     section[ MAX_SECTION ];
    bool        subscriptionIdAvailable;
    uint16_t    serverTopicAliasMax;
} ValidateCase_t;

static const ValidateCase_t CASES[] = {
    /* --- CONNECT --- */
    { "connect-empty",           V_CONNECT, 0, { 0 }, true, 10 },
    { "connect-session-expiry",  V_CONNECT, 5, { 0x11, 0x00, 0x00, 0x0E, 0x10 }, true, 10 },
    { "connect-receive-max",     V_CONNECT, 3, { 0x21, 0x00, 0x14 }, true, 10 },
    { "connect-receive-max-zero", V_CONNECT, 3, { 0x21, 0x00, 0x00 }, true, 10 },
    { "connect-max-packet-size", V_CONNECT, 5, { 0x27, 0x00, 0x01, 0x00, 0x00 }, true, 10 },
    { "connect-max-packet-zero", V_CONNECT, 5, { 0x27, 0x00, 0x00, 0x00, 0x00 }, true, 10 },
    { "connect-request-problem-off", V_CONNECT, 2, { 0x17, 0x00 }, true, 10 },
    { "connect-request-problem-on", V_CONNECT, 2, { 0x17, 0x01 }, true, 10 },
    { "connect-request-problem-two", V_CONNECT, 2, { 0x17, 0x02 }, true, 10 },
    { "connect-request-response", V_CONNECT, 2, { 0x19, 0x01 }, true, 10 },
    { "connect-repeated-session-expiry", V_CONNECT, 10,
      { 0x11, 0x00, 0x00, 0x0E, 0x10, 0x11, 0x00, 0x00, 0x0E, 0x10 }, true, 10 },
    { "connect-auth-method",     V_CONNECT, 4, { 0x15, 0x00, 0x01, 'x' }, true, 10 },
    { "connect-will-property",   V_CONNECT, 5, { 0x18, 0x00, 0x00, 0x00, 0x0A }, true, 10 },
    /* [MQTT-3.1.2-32]: authentication data without a method is a protocol
     * error, and the C checks it AFTER the walk rather than in the arm -- so
     * the sweep sees 0x16 refused and only a paired case shows why. */
    { "connect-auth-data-alone", V_CONNECT, 4, { 0x16, 0x00, 0x01, 'd' }, true, 10 },
    { "connect-auth-method-then-data", V_CONNECT, 8,
      { 0x15, 0x00, 0x01, 'x', 0x16, 0x00, 0x01, 'd' }, true, 10 },
    { "connect-auth-data-then-method", V_CONNECT, 8,
      { 0x16, 0x00, 0x01, 'd', 0x15, 0x00, 0x01, 'x' }, true, 10 },

    /* --- the will --- */
    { "will-empty",              V_WILL, 0, { 0 }, true, 10 },
    { "will-delay",              V_WILL, 5, { 0x18, 0x00, 0x00, 0x00, 0x0A }, true, 10 },
    { "will-payload-format",     V_WILL, 2, { 0x01, 0x01 }, true, 10 },
    { "will-payload-format-two", V_WILL, 2, { 0x01, 0x02 }, true, 10 },
    { "will-content-type",       V_WILL, 4, { 0x03, 0x00, 0x01, 't' }, true, 10 },
    { "will-session-expiry",     V_WILL, 5, { 0x11, 0x00, 0x00, 0x0E, 0x10 }, true, 10 },
    { "will-repeated-delay",     V_WILL, 10,
      { 0x18, 0x00, 0x00, 0x00, 0x0A, 0x18, 0x00, 0x00, 0x00, 0x0A }, true, 10 },

    /* --- SUBSCRIBE --- */
    { "subscribe-empty",         V_SUBSCRIBE, 0, { 0 }, true, 10 },
    { "subscribe-id",            V_SUBSCRIBE, 2, { 0x0B, 0x07 }, true, 10 },
    /* The server said it does not support subscription identifiers. */
    { "subscribe-id-unavailable", V_SUBSCRIBE, 2, { 0x0B, 0x07 }, false, 10 },
    { "subscribe-id-zero",       V_SUBSCRIBE, 2, { 0x0B, 0x00 }, true, 10 },
    { "subscribe-user-property", V_SUBSCRIBE, 7,
      { 0x26, 0x00, 0x01, 'k', 0x00, 0x01, 'v' }, true, 10 },
    { "subscribe-repeated-id",   V_SUBSCRIBE, 4, { 0x0B, 0x07, 0x0B, 0x08 }, true, 10 },

    /* --- PUBLISH --- */
    { "publish-empty",           V_PUBLISH, 0, { 0 }, true, 10 },
    { "publish-payload-format",  V_PUBLISH, 2, { 0x01, 0x01 }, true, 10 },
    { "publish-topic-alias",     V_PUBLISH, 3, { 0x23, 0x00, 0x05 }, true, 10 },
    { "publish-topic-alias-over", V_PUBLISH, 3, { 0x23, 0x00, 0x0B }, true, 10 },
    { "publish-topic-alias-zero", V_PUBLISH, 3, { 0x23, 0x00, 0x00 }, true, 10 },
    { "publish-alias-when-none-allowed", V_PUBLISH, 3, { 0x23, 0x00, 0x01 }, true, 0 },
    { "publish-subscription-id", V_PUBLISH, 2, { 0x0B, 0x07 }, true, 10 },
    /* The will validator refuses a payload format that is neither 0 nor 1, and
     * refuses a repeat. Does the publish one? */
    { "publish-payload-format-two", V_PUBLISH, 2, { 0x01, 0x02 }, true, 10 },
    { "publish-repeated-payload-format", V_PUBLISH, 4,
      { 0x01, 0x01, 0x01, 0x00 }, true, 10 },
    { "publish-repeated-topic-alias", V_PUBLISH, 6,
      { 0x23, 0x00, 0x05, 0x23, 0x00, 0x06 }, true, 10 },

    /* --- the publish acknowledgements --- */
    { "puback-empty",            V_PUBACK, 0, { 0 }, true, 10 },
    { "puback-reason-string",    V_PUBACK, 4, { 0x1F, 0x00, 0x01, 'x' }, true, 10 },
    { "puback-user-property",    V_PUBACK, 7,
      { 0x26, 0x00, 0x01, 'k', 0x00, 0x01, 'v' }, true, 10 },
    { "puback-session-expiry",   V_PUBACK, 5, { 0x11, 0x00, 0x00, 0x0E, 0x10 }, true, 10 },

    /* --- UNSUBSCRIBE, which takes ONE property and no other --- */
    { "unsubscribe-empty",       V_UNSUBSCRIBE, 0, { 0 }, true, 10 },
    { "unsubscribe-user-property", V_UNSUBSCRIBE, 7,
      { 0x26, 0x00, 0x01, 'k', 0x00, 0x01, 'v' }, true, 10 },
    { "unsubscribe-reason-string", V_UNSUBSCRIBE, 4, { 0x1F, 0x00, 0x01, 'x' }, true, 10 },
    { "unsubscribe-two-user-properties", V_UNSUBSCRIBE, 14,
      { 0x26, 0x00, 0x01, 'a', 0x00, 0x01, 'b',
        0x26, 0x00, 0x01, 'c', 0x00, 0x01, 'd' }, true, 10 },

    /* --- a truncated value, in every table that has a string --- */
    { "connect-truncated-string", V_CONNECT, 4, { 0x15, 0x00, 0x09, 'x' }, true, 10 },
    { "puback-truncated-string", V_PUBACK, 4, { 0x1F, 0x00, 0x09, 'x' }, true, 10 },

    /* --- WHICH validators deduplicate, and which do not ---
     *
     * The will validator carries a bit mask and refuses every repeat. The
     * PUBLISH validator declares its `used` flag INSIDE the loop, so the flag
     * is false at every property and dedupes nothing -- except the topic alias,
     * whose flag is declared outside it. These four cases are the difference,
     * and without them the transcription could dedupe the publish table and
     * still match every other line of this trace. */
    { "publish-repeated-content-type", V_PUBLISH, 8,
      { 0x03, 0x00, 0x01, 't', 0x03, 0x00, 0x01, 't' }, true, 10 },
    { "publish-repeated-message-expiry", V_PUBLISH, 10,
      { 0x02, 0x00, 0x00, 0x00, 0x0A, 0x02, 0x00, 0x00, 0x00, 0x0A }, true, 10 },
    { "will-repeated-payload-format", V_WILL, 4, { 0x01, 0x01, 0x01, 0x00 }, true, 10 },
    /* The acknowledgement validator's flag is declared OUTSIDE its loop, so it
     * dedupes -- the same code shape as the publish one, one brace apart. */
    { "puback-repeated-reason-string", V_PUBACK, 8,
      { 0x1F, 0x00, 0x01, 'x', 0x1F, 0x00, 0x01, 'x' }, true, 10 },

    /* --- the two statuses, in the two tables that mix them ---
     *
     * SUBSCRIBE answers BadParameter for everything it decides itself and
     * BadResponse for anything the shared decoders refuse; a malformed
     * subscription identifier is the second kind, and nothing else in this
     * trace tells the two apart. */
    { "subscribe-truncated-id",  V_SUBSCRIBE, 2, { 0x0B, 0xFF }, true, 10 },
    { "publish-truncated-string", V_PUBLISH, 4, { 0x03, 0x00, 0x09, 't' }, true, 10 },
    /* The alias bound is `max < alias`, so the maximum itself is legal. */
    { "publish-topic-alias-at-max", V_PUBLISH, 3, { 0x23, 0x00, 0x0A }, true, 10 },
};

#define N_CASES    ( sizeof( CASES ) / sizeof( CASES[ 0 ] ) )

static void run_case( size_t i )
{
    const ValidateCase_t *c = &CASES[ i ];

    printf( "case %u %s v=%s subid=%u alias=%u props=", ( unsigned ) i, c->name,
            V_NAMES[ c->which ], c->subscriptionIdAvailable ? 1U : 0U,
            ( unsigned ) c->serverTopicAliasMax );
    put_hex( c->section, c->length );

    ( void ) run_validator( c->which, c->section, c->length,
                            c->subscriptionIdAvailable, c->serverTopicAliasMax,
                            true );

    printf( "\n" );
}

/* ---- the sweeps ----------------------------------------------------------- */

/* Every property identifier, at one value shape, through one validator.
 *
 * Six validators x six shapes = thirty-six sweeps, and together the accepted
 * sets are the whole map of which property may go in which outgoing packet. */
static void sweep( int which,
                   const char *shapeName,
                   const uint8_t *value,
                   size_t valueLength )
{
    unsigned id;
    unsigned accepted = 0U;
    unsigned refused = 0U;
    int first = 1;

    printf( "sweep %s %s accepted=", V_NAMES[ which ], shapeName );

    for( id = 0U; id < 256U; id++ )
    {
        uint8_t section[ MAX_SECTION ];
        MQTTStatus_t status;

        memset( section, 0, sizeof section );
        section[ 0 ] = ( uint8_t ) id;
        memcpy( &section[ 1 ], value, valueLength );

        status = run_validator( which, section, valueLength + 1U, true, 10U, false );

        if( status == MQTTSuccess )
        {
            printf( "%s%02x", first ? "" : ",", id );
            first = 0;
            accepted++;
        }
        else
        {
            refused++;
        }
    }

    printf( " n=%u refused=%u\n", accepted, refused );
}

int main( void )
{
    size_t i;
    int which;

    /* The two-byte value is 0x0001 and the four-byte one 0x00000001, because
     * several tables refuse a ZERO Receive Maximum, Maximum Packet Size or
     * Topic Alias -- and the sweep must not confuse "unknown identifier" with
     * "known identifier, illegal value". The varint is 7 for the same reason:
     * a subscription identifier of zero is refused. */
    static const uint8_t ONE_BYTE[] = { 0x01 };
    static const uint8_t TWO_BYTE[] = { 0x00, 0x01 };
    static const uint8_t FOUR_BYTE[] = { 0x00, 0x00, 0x00, 0x01 };
    static const uint8_t STRING[] = { 0x00, 0x01, 'x' };
    static const uint8_t VARINT[] = { 0x07 };
    static const uint8_t USER_PROP[] = { 0x00, 0x01, 'k', 0x00, 0x01, 'v' };

    printf( "geometry cases=%u validators=%u\n", ( unsigned ) N_CASES,
            ( unsigned ) V_COUNT );

    for( i = 0; i < N_CASES; i++ )
    {
        run_case( i );
    }

    for( which = 0; which < V_COUNT; which++ )
    {
        sweep( which, "one-byte", ONE_BYTE, sizeof ONE_BYTE );
        sweep( which, "two-byte", TWO_BYTE, sizeof TWO_BYTE );
        sweep( which, "four-byte", FOUR_BYTE, sizeof FOUR_BYTE );
        sweep( which, "string", STRING, sizeof STRING );
        sweep( which, "varint", VARINT, sizeof VARINT );
        sweep( which, "user-property", USER_PROP, sizeof USER_PROP );
    }

    printf( "end\n" );
    return 0;
}
