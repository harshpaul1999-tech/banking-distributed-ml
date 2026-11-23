#!/usr/bin/env python3
"""Emit poutcome<TAB>duration,count for average contact duration."""
import sys
from csv_rows import read_bank_rows

for row in read_bank_rows(sys.stdin):
    try:
        print(f"{row['poutcome']}\t{float(row['duration'])},1")
    except (KeyError, TypeError, ValueError):
        continue
