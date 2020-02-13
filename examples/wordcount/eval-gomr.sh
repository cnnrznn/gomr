#!/bin/bash

for par in 1 2 4 8
do
    for trial in $(seq 10)
    do
    /usr/bin/time -f "${par} %e" ./wordcount moby.txt $par >out.go.txt
    done
done 2>>times.go.txt
