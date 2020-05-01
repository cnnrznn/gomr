#!/usr/bin/python3

# Given a graph on STDIN in edge form
# v1 v2
# generate an initial ranks file where for
# every vertex v there is an entry
# v 1.0

import sys

stdin = sys.stdin

nodes = set()

for line in stdin:
    if line.startswith('#'):
        continue

    ls = line.split()
    v1 = int(ls[0])
    v2 = int(ls[1])

    nodes.add(v1)
    nodes.add(v2)

for node in nodes:
    print(node, 1.0)