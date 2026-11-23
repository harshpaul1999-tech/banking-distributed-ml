#!/usr/bin/env python3
"""Summarize age and balance and calculate Pearson correlation."""
import math
import sys

n = 0
sx = sy = sx2 = sy2 = sxy = 0.0
for line in sys.stdin:
    _, value = line.rstrip("\n").split("\t", 1)
    age, balance = map(float, value.split(",", 1))
    n += 1
    sx += age
    sy += balance
    sx2 += age * age
    sy2 += balance * balance
    sxy += age * balance
if n:
    numerator = sxy - sx * sy / n
    denominator = math.sqrt((sx2 - sx * sx / n) * (sy2 - sy * sy / n))
    corr = numerator / denominator if denominator else float("nan")
    print(f"rows={n}\tmean_age={sx / n:.2f}\tmean_balance={sy / n:.2f}\tpearson_r={corr:.4f}")
