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
