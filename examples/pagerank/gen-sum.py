#!/usr/bin/python3

import sys

stdin = sys.stdin

s = 0.0

for line in stdin:
    ls = line.split()
    rank = float(ls[1])
    s += rank

print(s)