#!/usr/bin/python3

import sys

set1 = dict()
set2 = dict()

sets = [set1, set2]

for i in range(len(sets)):
    with open(sys.argv[i+1]) as inf:
        for line in inf:
            ls = line.split()
            k, v = ls[0], ls[1]
            sets[i][k] = v

for k in set1:
    if set1[k] != set2[k]:
        return False

return True
