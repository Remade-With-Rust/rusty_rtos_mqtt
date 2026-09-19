#!/bin/sh
# Generate the C arm of K7's coreMQTT publish-state differential.
#
# `core_mqtt_state.c` is compiled VERBATIM out of the pinned checkout in the
# umbrella; this script never copies or edits it. Fetch it first with
#
#     cargo run --manifest-path tools/kairos/Cargo.toml -- oracle fetch --lib coreMQTT
#
# The trace it writes is checked in, so the Rust side diffs it in CI with no C
# toolchain -- the same arrangement every other Kairos differential uses.
#
# MQTT_DO_NOT_USE_CUSTOM_CONFIG is the library's own switch for building
# without an application config header. It selects the SHIPPED defaults rather
# than replacing anything, so the C under test is still stock.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
lib="$here/../../oracle/coreMQTT"
src="$lib/source/core_mqtt_state.c"

[ -f "$src" ] || { echo "no core_mqtt_state.c at $src -- run \`kairos oracle fetch --lib coreMQTT\` first" >&2; exit 1; }

cc -O2 -g -w -DMQTT_DO_NOT_USE_CUSTOM_CONFIG \
   -I "$lib/source/include" \
   -I "$lib/source/interface" \
   -o "$here/state_driver" \
   "$src" "$here/state_driver.c"

"$here/state_driver" > "$here/state.trace"
echo "wrote $(wc -l < "$here/state.trace") lines to $here/state.trace"

# The fixed-header differential's C arm. The serializer pulls in its private
# helpers and the MQTT 5 property codecs.
ser="$lib/source/core_mqtt_serializer.c"
priv="$lib/source/core_mqtt_serializer_private.c"
props="$lib/source/core_mqtt_prop_serializer.c"
propd="$lib/source/core_mqtt_prop_deserializer.c"

cc -O2 -g -w -DMQTT_DO_NOT_USE_CUSTOM_CONFIG \
   -I "$lib/source/include" \
   -I "$lib/source/interface" \
   -o "$here/header_driver" \
   "$ser" "$priv" "$props" "$propd" "$here/header_driver.c"

"$here/header_driver" > "$here/header.trace"
echo "wrote $(wc -l < "$here/header.trace") lines to $here/header.trace"

# The property-primitive differential's C arm.
cc -O2 -g -w -DMQTT_DO_NOT_USE_CUSTOM_CONFIG \
   -I "$lib/source/include" \
   -I "$lib/source/interface" \
   -o "$here/property_driver" \
   "$ser" "$priv" "$props" "$propd" "$here/property_driver.c"

"$here/property_driver" > "$here/property.trace"
echo "wrote $(wc -l < "$here/property.trace") lines to $here/property.trace"

# The fixed-header writer differential's C arm.
cc -O2 -g -w -DMQTT_DO_NOT_USE_CUSTOM_CONFIG \
   -I "$lib/source/include" \
   -I "$lib/source/interface" \
   -o "$here/writer_driver" \
   "$ser" "$priv" "$props" "$propd" "$here/writer_driver.c"

"$here/writer_driver" > "$here/writer.trace"
echo "wrote $(wc -l < "$here/writer.trace") lines to $here/writer.trace"

# The packet-size differential's C arm.
cc -O2 -g -w -DMQTT_DO_NOT_USE_CUSTOM_CONFIG \
   -I "$lib/source/include" \
   -I "$lib/source/interface" \
   -o "$here/size_driver" \
   "$ser" "$priv" "$props" "$propd" "$here/size_driver.c"

"$here/size_driver" > "$here/size.trace"
echo "wrote $(wc -l < "$here/size.trace") lines to $here/size.trace"

# The acknowledgement-deserializer differential's C arm: the first INCOMING
# slice, so the first one whose whole input an attacker chooses.
cc -O2 -g -w -DMQTT_DO_NOT_USE_CUSTOM_CONFIG \
   -I "$lib/source/include" \
   -I "$lib/source/interface" \
   -o "$here/ack_driver" \
   "$ser" "$priv" "$props" "$propd" "$here/ack_driver.c"

"$here/ack_driver" > "$here/ack.trace"
echo "wrote $(wc -l < "$here/ack.trace") lines to $here/ack.trace"

# The CONNACK differential's C arm: the packet that sets every limit the rest
# of the session runs under.
cc -O2 -g -w -DMQTT_DO_NOT_USE_CUSTOM_CONFIG \
   -I "$lib/source/include" \
   -I "$lib/source/interface" \
   -o "$here/connack_driver" \
   "$ser" "$priv" "$props" "$propd" "$here/connack_driver.c"

"$here/connack_driver" > "$here/connack.trace"
echo "wrote $(wc -l < "$here/connack.trace") lines to $here/connack.trace"

# The incoming-PUBLISH differential's C arm: the last packet a broker can send,
# and the only one that carries application data.
cc -O2 -g -w -DMQTT_DO_NOT_USE_CUSTOM_CONFIG \
   -I "$lib/source/include" \
   -I "$lib/source/interface" \
   -o "$here/publish_driver" \
   "$ser" "$priv" "$props" "$propd" "$here/publish_driver.c"

"$here/publish_driver" > "$here/publish.trace"
echo "wrote $(wc -l < "$here/publish.trace") lines to $here/publish.trace"

# The DISCONNECT differential's C arm, BOTH DIRECTIONS: the packet the earlier
# "every packet a broker can send" claim had missed.
cc -O2 -g -w -DMQTT_DO_NOT_USE_CUSTOM_CONFIG \
   -I "$lib/source/include" \
   -I "$lib/source/interface" \
   -o "$here/disconnect_driver" \
   "$ser" "$priv" "$props" "$propd" "$here/disconnect_driver.c"

"$here/disconnect_driver" > "$here/disconnect.trace"
echo "wrote $(wc -l < "$here/disconnect.trace") lines to $here/disconnect.trace"

# The CONNECT differential's C arm: the packet that starts a session, sized and
# then serialized in one case because the C's own comment makes that the API
# contract.
cc -O2 -g -w -DMQTT_DO_NOT_USE_CUSTOM_CONFIG \
   -I "$lib/source/include" \
   -I "$lib/source/interface" \
   -o "$here/connect_driver" \
   "$ser" "$priv" "$props" "$propd" "$here/connect_driver.c"

"$here/connect_driver" > "$here/connect.trace"
echo "wrote $(wc -l < "$here/connect.trace") lines to $here/connect.trace"

# The outgoing-PUBLISH differential's C arm: three serializers that must agree
# with each other as prefixes.
cc -O2 -g -w -DMQTT_DO_NOT_USE_CUSTOM_CONFIG \
   -I "$lib/source/include" \
   -I "$lib/source/interface" \
   -o "$here/outpublish_driver" \
   "$ser" "$priv" "$props" "$propd" "$here/outpublish_driver.c"

"$here/outpublish_driver" > "$here/outpublish.trace"
echo "wrote $(wc -l < "$here/outpublish.trace") lines to $here/outpublish.trace"

# The remaining-outgoing-packets differential's C arm: SUBSCRIBE, UNSUBSCRIBE,
# the publish acknowledgements and PINGREQ.
cc -O2 -g -w -DMQTT_DO_NOT_USE_CUSTOM_CONFIG \
   -I "$lib/source/include" \
   -I "$lib/source/interface" \
   -o "$here/outbound_driver" \
   "$ser" "$priv" "$props" "$propd" "$here/outbound_driver.c"

"$here/outbound_driver" > "$here/outbound.trace"
echo "wrote $(wc -l < "$here/outbound.trace") lines to $here/outbound.trace"

# The transport-reader differential's C arm: the first function in the package
# that takes a CALLBACK rather than a buffer, so the call sequence is compared.
cc -O2 -g -w -DMQTT_DO_NOT_USE_CUSTOM_CONFIG \
   -I "$lib/source/include" \
   -I "$lib/source/interface" \
   -o "$here/reader_driver" \
   "$ser" "$priv" "$props" "$propd" "$here/reader_driver.c"

"$here/reader_driver" > "$here/reader.trace"
echo "wrote $(wc -l < "$here/reader.trace") lines to $here/reader.trace"

# The outgoing-property-validator differential's C arm: six tables, thirty-six
# sweeps -- the whole map of which property may go in which outgoing packet.
cc -O2 -g -w -DMQTT_DO_NOT_USE_CUSTOM_CONFIG \
   -I "$lib/source/include" \
   -I "$lib/source/interface" \
   -o "$here/validate_driver" \
   "$ser" "$priv" "$props" "$propd" "$here/validate_driver.c"

"$here/validate_driver" > "$here/validate.trace"
echo "wrote $(wc -l < "$here/validate.trace") lines to $here/validate.trace"

# The connection-context differential's C arm: the last of
# `core_mqtt_serializer.c` -- the two constructors, the context filler, the
# outgoing PUBLISH's parameter validator, and the BUFFERED twin of the
# transport reader, driven side by side with the callback-driven one.
cc -O2 -g -w -DMQTT_DO_NOT_USE_CUSTOM_CONFIG \
   -I "$lib/source/include" \
   -I "$lib/source/interface" \
   -o "$here/context_driver" \
   "$ser" "$priv" "$props" "$propd" "$here/context_driver.c"

"$here/context_driver" > "$here/context.trace"
echo "wrote $(wc -l < "$here/context.trace") lines to $here/context.trace"

# The property-builder differential's C arm: `core_mqtt_prop_serializer.c`,
# whose centre is a FOURTH copy of which property may go in which packet --
# static, so it is asked through the eighteen public adders.
cc -O2 -g -w -DMQTT_DO_NOT_USE_CUSTOM_CONFIG \
   -I "$lib/source/include" \
   -I "$lib/source/interface" \
   -o "$here/propbuild_driver" \
   "$ser" "$priv" "$props" "$propd" "$here/propbuild_driver.c"

"$here/propbuild_driver" > "$here/propbuild.trace"
echo "wrote $(wc -l < "$here/propbuild.trace") lines to $here/propbuild.trace"

# The property-reader differential's C arm: `core_mqtt_prop_deserializer.c`,
# the other half of the builder -- and two more tables over the same alphabet,
# swept and printed side by side.
cc -O2 -g -w -DMQTT_DO_NOT_USE_CUSTOM_CONFIG \
   -I "$lib/source/include" \
   -I "$lib/source/interface" \
   -o "$here/propread_driver" \
   "$ser" "$priv" "$props" "$propd" "$here/propread_driver.c"

"$here/propread_driver" > "$here/propread.trace"
echo "wrote $(wc -l < "$here/propread.trace") lines to $here/propread.trace"

# The topic-matching differential's C arm: the part of `core_mqtt.c` that needs
# no transport -- the matcher, the two ack reason-code getters and the two
# tables that name things. This one links `core_mqtt.c` itself.
core="$lib/source/core_mqtt.c"

cc -O2 -g -w -DMQTT_DO_NOT_USE_CUSTOM_CONFIG \
   -I "$lib/source/include" \
   -I "$lib/source/interface" \
   -o "$here/topic_driver" \
   "$core" "$src" "$ser" "$priv" "$props" "$propd" "$here/topic_driver.c"

"$here/topic_driver" > "$here/topic.trace"
echo "wrote $(wc -l < "$here/topic.trace") lines to $here/topic.trace"

# The client-context differential's C arm: the last of `core_mqtt.c` that needs
# no transport -- the constructors, the packet-id allocator, the vector helpers
# and the subscribe/publish validators.
cc -O2 -g -w -DMQTT_DO_NOT_USE_CUSTOM_CONFIG \
   -I "$lib/source/include" \
   -I "$lib/source/interface" \
   -o "$here/client_driver" \
   "$core" "$src" "$ser" "$priv" "$props" "$propd" "$here/client_driver.c"

"$here/client_driver" > "$here/client.trace"
echo "wrote $(wc -l < "$here/client.trace") lines to $here/client.trace"

# The send-plumbing differential's C arm: a SCRIPTED transport and clock, and
# the two functions that stand between the library and the network.
cc -O2 -g -w -DMQTT_DO_NOT_USE_CUSTOM_CONFIG \
   -I "$lib/source/include" \
   -I "$lib/source/interface" \
   -o "$here/send_driver" \
   "$core" "$src" "$ser" "$priv" "$props" "$propd" "$here/send_driver.c"

"$here/send_driver" > "$here/send.trace"
echo "wrote $(wc -l < "$here/send.trace") lines to $here/send.trace"
