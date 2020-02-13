#!/bin/bash

for par in 1 2 4 8
do
    for trial in $(seq 10)
    do
    /usr/bin/time -f "${par} %e" sh -c "spark-submit spark.py moby.txt $par >out.spark.txt 2>/dev/null"
    done
done 2>>times.spark.txt
