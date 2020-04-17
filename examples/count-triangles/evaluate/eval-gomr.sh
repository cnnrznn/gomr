#!/bin/bash

for par in 1 2 4 8 16 32
do
    for trial in $(seq 10)
    do
    /usr/bin/time -f "${par} %e" ../count-triangles ../edges.csv $par $par >out.gomr.txt
    done
done 2>>times.gomr.txt
