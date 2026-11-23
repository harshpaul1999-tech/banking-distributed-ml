#!/usr/bin/env python3
"""Emit job<TAB>balance,count for Hadoop Streaming."""
import sys
from csv_rows import read_bank_rows

for row in read_bank_rows(sys.stdin):
    try:
        print(f"{row['job']}\t{float(row['balance'])},1")
    except (KeyError, TypeError, ValueError):
        continue
