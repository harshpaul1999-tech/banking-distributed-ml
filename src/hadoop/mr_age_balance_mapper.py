#!/usr/bin/env python3
"""Emit age,balance pairs for one-pass Pearson correlation reduction."""
import sys
from csv_rows import read_bank_rows

for row in read_bank_rows(sys.stdin):
    try:
        print(f"ALL\t{float(row['age'])},{float(row['balance'])}")
    except (KeyError, TypeError, ValueError):
        continue
