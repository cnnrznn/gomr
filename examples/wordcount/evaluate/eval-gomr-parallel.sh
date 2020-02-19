#!/bin/bash

for par in 1 2 4 8 16 32
do
    for trial in $(seq 10)
    do
    /usr/bin/time -f "${par} %e" ../wordcount-parallel moby.txt $par >out.gomr.parallel.txt
    done
done 2>>times.gomr.parallel.txt
