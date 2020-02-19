#!/bin/bash

for par in 1 2 4 8 16 32
do
    for trial in $(seq 10)
    do
    /usr/bin/time -f "${par} %e" ../wordcount-multiplex moby.txt $par >out.gomr.multiplex.txt
    done
done 2>>times.gomr.multiplex.txt
