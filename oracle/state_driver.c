/* The C arm of K7's coreMQTT publish-state differential.
 *
 * `core_mqtt_state.c` is compiled VERBATIM out of the pinned checkout;
 * nothing here copies or edits it.
 *
 * # Why this is the first slice of coreMQTT
 *
 * coreMQTT v5.0.2 is 21,102 lines. `core_mqtt_state.c` is 1,206 of them and
 * includes nothing but its own header: no bytes, no transport, no clock. It is
 * a pure state machine over two arrays of records, and it is where MQTT's
 * hardest correctness lives -- QoS 1 and 2 delivery state that has to survive a
 * broker going away mid-handshake and the session being resumed.
 *
 * # What is compared
 *
 * Not only the status each call returns, but **both record arrays after every
 * operation**. The records ARE the state, and their ORDER is load-bearing: MQTT
 * 5.0 requires message ordering, so the library compacts rather than reuses
 * holes, appends rather than fills, and deliberately moves a record to the end
 * when a PUBREC arrives so that PUBRELs resend in the right order. A
 * transcription that produced every correct status while ordering the array
 * differently would resend a session's backlog out of order.
 *
 * The trace carries its own scenarios, as the sntp ones do.
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "core_mqtt_state.h"

#define MAX_RECORDS    8
#define MAX_OPS        24

static const char *status_name( MQTTStatus_t s )
{
    switch( s )
    {
        case MQTTSuccess:            return "Success";
        case MQTTBadParameter:       return "BadParameter";
        case MQTTNoMemory:           return "NoMemory";
        case MQTTStateCollision:     return "StateCollision";
        case MQTTIllegalState:       return "IllegalState";
        case MQTTBadResponse:        return "BadResponse";
        default:                     return "UNEXPECTED";
    }
}

static const char *state_name( MQTTPublishState_t s )
{
    switch( s )
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
        default:                  return "UNEXPECTED";
    }
}

/* ---- the operation script ---------------------------------------------- */

typedef struct
{
    char kind;      /* r reserve, p publish, a ack, x remove, P/R resend, C cursor reset */
    uint16_t packetId;
    uint8_t a;      /* qos, or ack type */
    uint8_t b;      /* operation: 0 = send, 1 = receive */
} Op_t;

typedef struct
{
    const char *name;
    size_t outgoingCount;
    size_t incomingCount;
    Op_t ops[ MAX_OPS ];
    size_t opCount;
} Scenario_t;

static void print_records( const char *tag, const MQTTPubAckInfo_t *records, size_t count )
{
    size_t i;

    printf( "records %s %u", tag, ( unsigned ) count );

    for( i = 0; i < count; i++ )
    {
        printf( " %u:%u:%s", records[ i ].packetId, ( unsigned ) records[ i ].qos,
                state_name( records[ i ].publishState ) );
    }

    printf( "\n" );
}

static void print_cfg( const Scenario_t *sc )
{
    size_t i;

    printf( "cfg records %u %u\n", ( unsigned ) sc->outgoingCount, ( unsigned ) sc->incomingCount );
    printf( "cfg ops %u", ( unsigned ) sc->opCount );

    for( i = 0; i < sc->opCount; i++ )
    {
        printf( " %c:%u:%u:%u", sc->ops[ i ].kind, sc->ops[ i ].packetId,
                sc->ops[ i ].a, sc->ops[ i ].b );
    }

    printf( "\n" );
}

static void run_scenario( size_t id, const Scenario_t *sc )
{
    MQTTContext_t context;
    static MQTTPubAckInfo_t outgoing[ MAX_RECORDS ];
    static MQTTPubAckInfo_t incoming[ MAX_RECORDS ];
    MQTTStateCursor_t cursor = MQTT_STATE_CURSOR_INITIALIZER;
    size_t i;

    printf( "scenario %u %s\n", ( unsigned ) id, sc->name );
    print_cfg( sc );

    memset( &context, 0, sizeof context );
    memset( outgoing, 0, sizeof outgoing );
    memset( incoming, 0, sizeof incoming );

    context.outgoingPublishRecords = outgoing;
    context.outgoingPublishRecordMaxCount = sc->outgoingCount;
    context.incomingPublishRecords = incoming;
    context.incomingPublishRecordMaxCount = sc->incomingCount;

    for( i = 0; i < sc->opCount; i++ )
    {
        const Op_t *op = &sc->ops[ i ];
        MQTTStatus_t status;
        MQTTPublishState_t newState = MQTTStateNull;
        uint16_t found;

        switch( op->kind )
        {
            case 'r':
                status = MQTT_ReserveState( &context, op->packetId, ( MQTTQoS_t ) op->a );
                printf( "op %u r -> %s\n", ( unsigned ) i, status_name( status ) );
                break;

            case 'p':
                status = MQTT_UpdateStatePublish( &context, op->packetId,
                                                  ( MQTTStateOperation_t ) op->b,
                                                  ( MQTTQoS_t ) op->a, &newState );
                printf( "op %u p -> %s %s\n", ( unsigned ) i, status_name( status ),
                        state_name( newState ) );
                break;

            case 'a':
                status = MQTT_UpdateStateAck( &context, op->packetId,
                                              ( MQTTPubAckType_t ) op->a,
                                              ( MQTTStateOperation_t ) op->b, &newState );
                printf( "op %u a -> %s %s\n", ( unsigned ) i, status_name( status ),
                        state_name( newState ) );
                break;

            case 'x':
                status = MQTT_RemoveStateRecord( &context, op->packetId );
                printf( "op %u x -> %s\n", ( unsigned ) i, status_name( status ) );
                break;

            case 'P':
                found = MQTT_PublishToResend( &context, &cursor );
                printf( "op %u P -> %u cursor=%u\n", ( unsigned ) i, found,
                        ( unsigned ) cursor );
                break;

            case 'R':
                newState = MQTTStateNull;
                found = MQTT_PubrelToResend( &context, &cursor, &newState );
                printf( "op %u R -> %u %s cursor=%u\n", ( unsigned ) i, found,
                        state_name( newState ), ( unsigned ) cursor );
                break;

            case 'C':
                cursor = MQTT_STATE_CURSOR_INITIALIZER;
                printf( "op %u C -> cursor=%u\n", ( unsigned ) i, ( unsigned ) cursor );
                break;

            default:
                printf( "op %u ? -> UNEXPECTED\n", ( unsigned ) i );
                break;
        }

        /* The records ARE the state, and their ORDER is load-bearing. */
        print_records( "out", outgoing, sc->outgoingCount );
        print_records( "in", incoming, sc->incomingCount );
    }

    printf( "end scenario %u\n", ( unsigned ) id );
}

/* ---- building scenarios -------------------------------------------------- */

static Scenario_t sc;

static void reset( const char *name, size_t outgoing, size_t incoming )
{
    memset( &sc, 0, sizeof sc );
    sc.name = name;
    sc.outgoingCount = outgoing;
    sc.incomingCount = incoming;
}

static void push( char kind, uint16_t packetId, uint8_t a, uint8_t b )
{
    sc.ops[ sc.opCount ].kind = kind;
    sc.ops[ sc.opCount ].packetId = packetId;
    sc.ops[ sc.opCount ].a = a;
    sc.ops[ sc.opCount ].b = b;
    sc.opCount++;
}

#define SEND     0
#define RECEIVE  1

#define QOS0     0
#define QOS1     1
#define QOS2     2

/* MQTTPubAckType_t: MQTTPuback = 0, MQTTPubrec, MQTTPubrel, MQTTPubcomp. */
#define PUBACK   0
#define PUBREC   1
#define PUBREL   2
#define PUBCOMP  3

int main( void )
{
    size_t id = 0;
    int k;

    printf( "geometry maxrecords=%u\n", ( unsigned ) MAX_RECORDS );

    /* 1. QoS 0 needs no record at all, in either direction. */
    reset( "qos0-keeps-no-record", 4, 4 );
    push( 'r', 1, QOS0, 0 );
    push( 'p', 1, QOS0, SEND );
    push( 'p', 2, QOS0, RECEIVE );
    run_scenario( id++, &sc );

    /* 2. The QoS 1 outgoing round trip. */
    reset( "qos1-outgoing", 4, 4 );
    push( 'r', 1, QOS1, 0 );
    push( 'p', 1, QOS1, SEND );
    push( 'a', 1, PUBACK, RECEIVE );
    run_scenario( id++, &sc );

    /* 3. The QoS 2 outgoing round trip, which is where the record MOVES. */
    reset( "qos2-outgoing", 4, 4 );
    push( 'r', 1, QOS2, 0 );
    push( 'p', 1, QOS2, SEND );
    push( 'a', 1, PUBREC, RECEIVE );
    push( 'a', 1, PUBREL, SEND );
    push( 'a', 1, PUBCOMP, RECEIVE );
    run_scenario( id++, &sc );

    /* 4. The QoS 1 incoming round trip. */
    reset( "qos1-incoming", 4, 4 );
    push( 'p', 5, QOS1, RECEIVE );
    push( 'a', 5, PUBACK, SEND );
    run_scenario( id++, &sc );

    /* 5. The QoS 2 incoming round trip. */
    reset( "qos2-incoming", 4, 4 );
    push( 'p', 5, QOS2, RECEIVE );
    push( 'a', 5, PUBREC, SEND );
    push( 'a', 5, PUBREL, RECEIVE );
    push( 'a', 5, PUBCOMP, SEND );
    run_scenario( id++, &sc );

    /* 6. Reserving the same packet id twice. */
    reset( "reserve-collision", 4, 4 );
    push( 'r', 1, QOS1, 0 );
    push( 'r', 1, QOS1, 0 );
    push( 'r', 1, QOS2, 0 );
    run_scenario( id++, &sc );

    /* 7. Filling the outgoing records and asking for one more. */
    reset( "outgoing-records-exhausted", 2, 2 );
    push( 'r', 1, QOS1, 0 );
    push( 'r', 2, QOS1, 0 );
    push( 'r', 3, QOS1, 0 );
    run_scenario( id++, &sc );

    /* 8. COMPACTION. Fill the array, free a record in the middle, then add:
     *    the library compacts rather than filling the hole, because the
     *    relative order of the records is the resend order. */
    reset( "compaction-preserves-order", 4, 4 );
    push( 'r', 1, QOS1, 0 );
    push( 'r', 2, QOS1, 0 );
    push( 'r', 3, QOS1, 0 );
    push( 'r', 4, QOS1, 0 );
    push( 'x', 2, 0, 0 );          /* a hole in the middle */
    push( 'r', 5, QOS1, 0 );       /* must compact, then append */
    run_scenario( id++, &sc );

    /* 9. A PUBREC on an outgoing QoS 2 publish DELETES the record and adds it
     *    back at the end, so that PUBRELs resend in order. With two in flight
     *    the move is visible. */
    reset( "pubrec-moves-the-record-to-the-end", 4, 4 );
    push( 'r', 1, QOS2, 0 );
    push( 'p', 1, QOS2, SEND );
    push( 'r', 2, QOS2, 0 );
    push( 'p', 2, QOS2, SEND );
    push( 'a', 1, PUBREC, RECEIVE );   /* record 1 should move behind record 2 */
    run_scenario( id++, &sc );

    /* 10. The resend cursors after a session is reestablished. */
    reset( "resend-cursors", 4, 4 );
    push( 'r', 1, QOS1, 0 );
    push( 'p', 1, QOS1, SEND );        /* PubAckPending */
    push( 'r', 2, QOS2, 0 );
    push( 'p', 2, QOS2, SEND );        /* PubRecPending */
    push( 'r', 3, QOS2, 0 );
    push( 'p', 3, QOS2, SEND );
    push( 'a', 3, PUBREC, RECEIVE );   /* PubRelSend */
    push( 'P', 0, 0, 0 );              /* publishes to resend */
    push( 'P', 0, 0, 0 );
    push( 'P', 0, 0, 0 );
    push( 'C', 0, 0, 0 );
    push( 'R', 0, 0, 0 );              /* pubrels to resend */
    push( 'R', 0, 0, 0 );
    run_scenario( id++, &sc );

    /* 11. A DUPLICATE incoming QoS 2 publish after a reconnect: the broker
     *     resends because it never saw our PUBREC. PubRelPending to
     *     PubRelPending is a legal transition to the SAME state. */
    reset( "duplicate-incoming-qos2-publish", 4, 4 );
    push( 'p', 5, QOS2, RECEIVE );
    push( 'a', 5, PUBREC, SEND );      /* PubRelPending */
    push( 'p', 5, QOS2, RECEIVE );     /* the broker resends */
    push( 'a', 5, PUBREC, SEND );      /* same state again */
    run_scenario( id++, &sc );

    /* 12. A DUPLICATE PUBREL: PubCompSend to PubCompSend. */
    reset( "duplicate-pubrel", 4, 4 );
    push( 'p', 5, QOS2, RECEIVE );
    push( 'a', 5, PUBREC, SEND );
    push( 'a', 5, PUBREL, RECEIVE );   /* PubCompSend */
    push( 'a', 5, PUBREL, RECEIVE );   /* again */
    run_scenario( id++, &sc );

    /* 13. Resending an outgoing publish keeps its state. */
    reset( "resending-a-publish-keeps-its-state", 4, 4 );
    push( 'r', 1, QOS1, 0 );
    push( 'p', 1, QOS1, SEND );
    push( 'p', 1, QOS1, SEND );        /* PubAckPending -> PubAckPending */
    run_scenario( id++, &sc );

    /* 14. Illegal transitions. */
    reset( "illegal-transitions", 4, 4 );
    push( 'r', 1, QOS1, 0 );
    push( 'a', 1, PUBREC, RECEIVE );   /* a PUBREC for a QoS1 publish */
    push( 'p', 1, QOS2, SEND );        /* the QoS does not match the record */
    run_scenario( id++, &sc );

    /* 15. An ack with no record at all. */
    reset( "ack-without-a-record", 4, 4 );
    push( 'a', 9, PUBACK, RECEIVE );
    push( 'a', 9, PUBREL, RECEIVE );
    run_scenario( id++, &sc );

    /* 16. Removing records that are not there, and ones that are. */
    reset( "removing-records", 4, 4 );
    push( 'x', 1, 0, 0 );              /* nothing to remove */
    push( 'r', 1, QOS1, 0 );
    push( 'x', 1, 0, 0 );
    push( 'x', 1, 0, 0 );              /* already gone */
    run_scenario( id++, &sc );

    /* 17. Packet id zero is never valid above QoS 0. */
    reset( "packet-id-zero", 4, 4 );
    push( 'r', 0, QOS1, 0 );
    push( 'p', 0, QOS1, SEND );
    push( 'a', 0, PUBACK, RECEIVE );
    run_scenario( id++, &sc );

    /* 18. A single record, so every add has to compact first. */
    reset( "one-record-only", 1, 1 );
    push( 'r', 1, QOS1, 0 );
    push( 'r', 2, QOS1, 0 );           /* no room */
    push( 'x', 1, 0, 0 );
    push( 'r', 2, QOS1, 0 );           /* now there is */
    run_scenario( id++, &sc );

    /* 19. Incoming records fill up independently of outgoing ones. */
    reset( "incoming-records-exhausted", 4, 2 );
    push( 'p', 1, QOS2, RECEIVE );
    push( 'p', 2, QOS2, RECEIVE );
    push( 'p', 3, QOS2, RECEIVE );
    run_scenario( id++, &sc );

    /* 20. A full array of outgoing QoS2 publishes taken through PUBREC, which
     *     exercises the delete-and-append path against a FULL array -- the one
     *     place addRecord's compaction and its append interact. */
    reset( "pubrec-reordering-on-a-full-array", 4, 4 );
    for( k = 1; k <= 4; k++ )
    {
        push( 'r', ( uint16_t ) k, QOS2, 0 );
        push( 'p', ( uint16_t ) k, QOS2, SEND );
    }
    push( 'a', 1, PUBREC, RECEIVE );
    push( 'a', 2, PUBREC, RECEIVE );
    push( 'C', 0, 0, 0 );
    push( 'R', 0, 0, 0 );
    push( 'R', 0, 0, 0 );
    push( 'R', 0, 0, 0 );
    run_scenario( id++, &sc );

    /* 21. An ack type outside the enum. */
    reset( "ack-type-out-of-range", 4, 4 );
    push( 'r', 1, QOS1, 0 );
    push( 'p', 1, QOS1, SEND );
    push( 'a', 1, 4, RECEIVE );        /* one past MQTTPubcomp */
    run_scenario( id++, &sc );

    /* 22. A QoS 2 incoming publish whose PUBCOMP completes, freeing the record,
     *     followed by a new publish reusing the same packet id. */
    reset( "packet-id-reuse-after-completion", 4, 4 );
    push( 'p', 5, QOS2, RECEIVE );
    push( 'a', 5, PUBREC, SEND );
    push( 'a', 5, PUBREL, RECEIVE );
    push( 'a', 5, PUBCOMP, SEND );     /* record freed */
    push( 'p', 5, QOS2, RECEIVE );     /* the same id again */
    run_scenario( id++, &sc );

    /* 24. The ack/QoS sanity check, in the ONE shape where dropping it
     *     changes the answer. MQTT_CalculateStateAck refuses a PUBACK unless
     *     the record is QoS 1, and everything else unless it is QoS 2; a
     *     mismatch yields MQTTStateNull, which then fails the transition.
     *
     *     For most mismatches the transition would have failed anyway, so the
     *     check is invisible. Not here: a QoS 1 publish sitting in
     *     PubAckPending, handed a PUBCOMP, would compute PublishDone -- and
     *     PubAckPending -> PublishDone is a LEGAL transition. Without the
     *     check this succeeds and quietly completes a handshake that never
     *     happened. */
    reset( "pubcomp-for-a-qos1-publish", 4, 4 );
    push( 'r', 1, QOS1, 0 );
    push( 'p', 1, QOS1, SEND );        /* PubAckPending */
    push( 'a', 1, PUBCOMP, RECEIVE );  /* must be refused */
    run_scenario( id++, &sc );

    /* 25. The mirror, which does NOT distinguish: a QoS 2 publish in
     *     PubRecPending handed a PUBACK computes PublishDone, and
     *     PubRecPending -> PublishDone is already illegal. Kept so the
     *     asymmetry is on the record rather than inferred. */
    reset( "puback-for-a-qos2-publish", 4, 4 );
    push( 'r', 1, QOS2, 0 );
    push( 'p', 1, QOS2, SEND );        /* PubRecPending */
    push( 'a', 1, PUBACK, RECEIVE );
    run_scenario( id++, &sc );

    printf( "end\n" );
    return 0;
}
