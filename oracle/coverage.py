#!/usr/bin/env python3
"""How much of coreMQTT is remade, counted from the pinned source.

A percentage is a claim, and a claim nobody recomputes is one that drifts. This
reads the pinned C, enumerates every function definition in it, and checks each
against `oracle/REMADE.txt` -- the list of functions this package has remade and
proven. It is the only place the number comes from; the README and the ledger
quote its output.

    python oracle/coverage.py            # the summary
    python oracle/coverage.py --todo     # and what is left, largest first
    python oracle/coverage.py --check    # exit 1 if REMADE.txt names a function
                                         # the pinned source does not have

Three numbers, because one of them is misleading on its own:

  functions       what share of coreMQTT's functions are remade. THE headline:
                  a function is the unit that can be transcribed and diffed.
  function lines  what share of the lines INSIDE functions are remade. Lower
                  than the first while the big ones are outstanding.
  all lines       the same numerator over every line in the five .c files,
                  including 2,577 lines of includes, macros, doxygen and
                  `/*---*/` rules. It CANNOT reach 100 %, because there is
                  nothing in a doxygen block to transcribe, and it is reported
                  only so nobody reconstructs it and thinks it means something.
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SOURCE = os.path.join(HERE, '..', '..', 'oracle', 'coreMQTT', 'source')

FILES = [
    'core_mqtt_state.c',
    'core_mqtt_serializer.c',
    'core_mqtt_serializer_private.c',
    'core_mqtt_prop_serializer.c',
    'core_mqtt_prop_deserializer.c',
    'core_mqtt.c',
]


def definitions(path):
    """Every function DEFINITION in `path`, as (name, line count).

    A definition starts at column zero, has no `;` before its opening brace,
    and ends at the matching close. That excludes the forward declarations near
    the top of `core_mqtt.c`, which otherwise scan to the next unrelated brace
    and report a function four times its real size.
    """
    with open(path, encoding='utf-8', errors='replace') as handle:
        src = handle.read().splitlines()

    out, i = [], 0

    while i < len(src):
        line = src[i]
        match = re.match(
            r'^(static\s+)?([A-Za-z_][A-Za-z0-9_ \*]*?)\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(',
            line)

        if match and not line.startswith(' ') and ';' not in line:
            j, ok = i, True

            while j < len(src) and src[j].strip() != '{':
                if ';' in src[j]:
                    ok = False
                    break
                j += 1

            if ok and j < len(src):
                depth, k = 0, j

                while k < len(src):
                    depth += src[k].count('{') - src[k].count('}')
                    if depth == 0:
                        break
                    k += 1

                out.append((match.group(3), k - i + 1))
                i = k
        i += 1

    return out, len(src)


def remade():
    """The function names in REMADE.txt, ignoring comments and blank lines."""
    names = set()
    path = os.path.join(HERE, 'REMADE.txt')

    with open(path, encoding='utf-8') as handle:
        for line in handle:
            line = line.split('#', 1)[0].strip()
            if line:
                names.add(line)

    return names


def main():
    done = remade()
    seen = set()
    counts = {'done_f': 0, 'done_l': 0, 'todo_f': 0, 'todo_l': 0}
    total_lines = 0
    todo = []

    for name in FILES:
        found, lines = definitions(os.path.join(SOURCE, name))
        total_lines += lines

        for function, size in found:
            seen.add(function)

            if function in done:
                counts['done_f'] += 1
                counts['done_l'] += size
            else:
                counts['todo_f'] += 1
                counts['todo_l'] += size
                todo.append((size, function, name))

    functions = counts['done_f'] + counts['todo_f']
    body_lines = counts['done_l'] + counts['todo_l']

    if '--check' in sys.argv:
        stale = sorted(done - seen)

        if stale:
            print('REMADE.txt names %d function(s) the pinned source does not '
                  'have:' % len(stale))
            for function in stale:
                print('  ' + function)
            return 1

        print('REMADE.txt is consistent with the pinned source: '
              '%d names, all present.' % len(done))

    print('functions      %3d / %3d   %5.1f %%'
          % (counts['done_f'], functions, 100.0 * counts['done_f'] / functions))
    print('function lines %5d / %5d %5.1f %%'
          % (counts['done_l'], body_lines, 100.0 * counts['done_l'] / body_lines))
    print('all lines      %5d / %5d %5.1f %%   (%d lines are preamble and '
          'cannot be remade)'
          % (counts['done_l'], total_lines,
             100.0 * counts['done_l'] / total_lines,
             total_lines - body_lines))

    if '--todo' in sys.argv:
        print()
        print('%d functions left, %d lines:' % (counts['todo_f'], counts['todo_l']))
        for size, function, name in sorted(todo, reverse=True):
            print('  %4d  %-40s %s' % (size, function, name))

    return 0


if __name__ == '__main__':
    sys.exit(main())
