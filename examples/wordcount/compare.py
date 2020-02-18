#!/usr/bin/python3

import sys

set1 = dict()
set2 = dict()

sets = [set1, set2]

for i in range(len(sets)):
    with open(sys.argv[i+1]) as inf:
        for line in inf:
            ls = line.split()
            if len(ls) != 2:
                continue

            k, v = ls[0], ls[1]
            if k not in sets[i]:
                sets[i][k] = 0
            sets[i][k] += int(v)

match = True

for k in set1:
    if set1[k] != set2[k]:
        match = False
        print("[{}]".format(k), set1[k], set2[k])
        print()

if match:
    print("MATCH!")
    exit(0)

print("DO NOT MATCH :(")
exit(1)
