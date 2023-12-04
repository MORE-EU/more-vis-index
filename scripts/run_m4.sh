#!/bin/bash0,
a=$1
q=$2
p=$3
aggFactor=$4
reductionRatio=$5
type=$6

tables=("intel_lab_exp" "soccer_exp" "manufacturing_exp")
modes=("m4")

for table in "${tables[@]}"
do
    for mode in "${modes[@]}"
    do
        out="m4-$q"
        java -jar target/experiments.jar -c timeQueries -measureChange 3 -seqCount 100 -type "$type" -mode "$mode" -measures 2 -timeCol datetime -zoomFactor 2 -viewport 1000,600 -runs 1 -out "$out" -minShift 0.1 -maxShift 0.5 -schema more -table "$table" -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a "$a" -q "$q" -p "$p" -agg "$aggFactor" -reduction "$reductionRatio"
    done
done