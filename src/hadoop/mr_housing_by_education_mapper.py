#!/usr/bin/env python3
"""Emit education|housing<TAB>1 for a count-by-category job."""
import sys
from csv_rows import read_bank_rows

for row in read_bank_rows(sys.stdin):
    education = row.get("education", "unknown") or "unknown"
    housing = row.get("housing", "unknown") or "unknown"
    print(f"{education}|{housing}\t1")
