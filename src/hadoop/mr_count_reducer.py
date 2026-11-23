#!/usr/bin/env python3
"""Count each key emitted by a Hadoop Streaming mapper."""
import sys

current = None
count = 0
for line in sys.stdin:
    key, value = line.rstrip("\n").split("\t", 1)
    if current is not None and key != current:
        print(f"{current}\t{count}")
        count = 0
    current = key
    count += int(value)
if current is not None:
    print(f"{current}\t{count}")
