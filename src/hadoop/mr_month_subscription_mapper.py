#!/usr/bin/env python3
"""Emit month|subscription<TAB>1 for monthly campaign counts."""
import sys
from csv_rows import read_bank_rows

for row in read_bank_rows(sys.stdin):
    month = row.get("month", "unknown") or "unknown"
    subscribed = row.get("y", "unknown") or "unknown"
    print(f"{month}|{subscribed}\t1")
