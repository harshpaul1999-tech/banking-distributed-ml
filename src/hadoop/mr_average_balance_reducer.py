#!/usr/bin/env python3
"""Aggregate mapper values into average balance per job."""
import sys

current = None
balance_sum = 0.0
count = 0

def emit(key, total, n):
    if key is not None and n:
        print(f"{key}\t{total / n:.2f}\t{n}")

for line in sys.stdin:
    key, value = line.rstrip("\n").split("\t", 1)
    amount, n = value.split(",", 1)
    if current is not None and key != current:
        emit(current, balance_sum, count)
        balance_sum, count = 0.0, 0
    current = key
    balance_sum += float(amount)
    count += int(n)
emit(current, balance_sum, count)
