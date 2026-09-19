#!/usr/bin/env python3
"""Extract coreMQTT's two logging tables from the pinned source.

`logConnackResponse` and `logAckResponse` are the last two functions in the
library, and they are the only two whose behaviour a DIFFERENTIAL cannot see:
their whole output goes to `LogError` and `LogDebug`, which
`core_mqtt_config_defaults.h` defines as nothing. Turning them on would mean
compiling the pinned C with a configuration it does not ship, which is the one
thing every other arm of this oracle refuses to do.

So the oracle for these two is the pinned SOURCE TEXT, and this script is how it
is read: it parses the two switches out of `core_mqtt_serializer.c` and writes
one line per reason code. `tests/logtable.rs` diffs the Rust tables against it,
so a message that drifts in either direction fails the build — and when the pin
moves, this script is re-run like every other trace generator.

    python3 oracle/logtable.py > oracle/logtable.trace
"""

import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SOURCE = os.path.join(HERE, '..', '..', 'oracle', 'coreMQTT', 'source',
                      'core_mqtt_serializer.c')
HEADER = os.path.join(HERE, '..', '..', 'oracle', 'coreMQTT', 'source', 'include',
                      'core_mqtt_serializer.h')


def read(path):
    with open(path, encoding='utf-8', errors='replace') as handle:
        return handle.read()


def reason_code_values(header):
    """Every `MQTT_REASON_*` name and its numeric value."""
    values = {}

    for name, value in re.findall(r'(MQTT_REASON_[A-Z0-9_]+)\s*=\s*(0x[0-9A-Fa-f]+|\d+)',
                                  header):
        values[name] = int(value, 0)

    return values


def body_of(source, name):
    """The text of one function, from its definition to its closing brace.

    The DEFINITION, not the forward declaration: the one whose parameter list is
    followed by a brace rather than a semicolon.
    """
    pattern = re.compile(
        r'^static\s[^;{]*?\b' + re.escape(name) + r'\s*\([^;{]*?\)\s*\n\{',
        re.M | re.S,
    )
    start = None

    for match in pattern.finditer(source):
        start = match.start()

    if start is None:
        raise SystemExit('no definition of ' + name)

    depth = 0
    at = source.index('{', start)

    for index in range(at, len(source)):
        if source[index] == '{':
            depth += 1
        elif source[index] == '}':
            depth -= 1
            if depth == 0:
                return source[start:index + 1]

    raise SystemExit('unterminated body for ' + name)


def table_of(body, values):
    """Each `case X:` and the message the branch logs, in source order."""
    out = []
    pending = []

    for line in body.splitlines():
        case = re.search(r'case\s*\(?\s*(?:\(\s*uint8_t\s*\)\s*)?(MQTT_REASON_[A-Z0-9_]+)', line)

        if case:
            pending.append(case.group(1))
            continue

        if re.search(r'^\s*default:', line):
            pending.append('DEFAULT')
            continue

        message = re.search(r'Log(?:Error|Debug|Warn|Info)\(\s*\(\s*"((?:[^"\\]|\\.)*)"', line)

        if message and pending:
            for name in pending:
                out.append((name, message.group(1)))
            pending = []
            continue

        # A branch that falls to `break` without logging anything.
        if re.search(r'^\s*break;', line) and pending:
            for name in pending:
                out.append((name, ''))
            pending = []

    return [(name, values.get(name), text) for name, text in out]


def main():
    source = read(SOURCE)
    values = reason_code_values(read(HEADER))

    print('geometry')

    for function in ('logConnackResponse', 'logAckResponse'):
        rows = table_of(body_of(source, function), values)

        print('%s rows=%d' % (function, len(rows)))

        for name, value, text in rows:
            code = '-' if value is None else '0x%02x' % value
            print('%s %s %s %s' % (function, code, name, text if text else '-'))

    print('end')

    return 0


if __name__ == '__main__':
    sys.exit(main())
