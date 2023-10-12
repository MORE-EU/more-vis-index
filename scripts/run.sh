#!/bin/bash

a=$1
q=$2
p=$3
aggFactor=$4
reductionRatio=$5
type=$6

tables=("bebeze")
#tables=("synthetic1m" "synthetic2m" "synthetic4m" "synthetic8m" "synthetic16m" "synthetic32m" "synthetic64m" "synthetic128m" "synthetic256m" "synthetic512m" "synthetic1b")
#tables=("intel_lab" "manufacturing" "soccer")
modes=("ttiMinMax")

for table in "${tables[@]}"
do
    for mode in "${modes[@]}"
    do
        out="output_q=${q}_a=${a}_p=${p}_a_ratio=${aggFactor}_r_ratio=${reductionRatio}"
        #out="output_scalability_q=${q}_a=${a}_p=${p}_a_ratio=${aggFactor}_r_ratio=${reductionRatio}"
        #out="m4-0.1"
        java -jar target/experiments.jar -c timeQueries -seqCount 50 -type "$type" -mode "$mode" -measures 2 -timeCol datetime -zoomFactor 2 -viewport 1000,600 -runs 5 -out "$out" -minShift 0.001 -maxShift 1 -schema more -table "$table" -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a "$a" -q "$q" -p "$p" -agg "$aggFactor" -reduction "$reductionRatio"
    done
done