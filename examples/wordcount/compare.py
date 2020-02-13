#!/usr/bin/python3

import sys

set1 = dict()
set2 = dict()

sets = [set1, set2]

for i in range(len(sets)):
    with open(sys.argv[i+1]) as inf:
        for line in inf:
            ls = line.split()
            try:
                k, v = ' '.join(ls[:-1]), ls[-1]
                sets[i][k] = v
            except:
                print(line, i)

match = True

for k in set1:
    if set1[k] != set2[k]:
        match = False
        print(k)
        print(set1[k])
        print(set2[k])
        print()

if match:
    print("MATCH!")
    exit(0)

print("DO NOT MATCH :(")
exit(1)
