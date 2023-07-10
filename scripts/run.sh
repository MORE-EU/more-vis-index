  #!/bin/bash

a=$1
q=$2
p=$3
type=$4

tables=("soccer_exp" "intel_lab_exp" "manufacturing_exp")
modes=("ttiMinMax" "m4")

for table in "${tables[@]}"
do
    for mode in "${modes[@]}"
    do
        out="output_q=${q}_a=${a}_p=${p}"
        java -jar target/experiments.jar -c timeQueries -seqCount 25 -type "$type" -mode "$mode" -measures 2 -timeCol datetime -zoomFactor 1.8 -viewport 1000,600 -runs 1 -out "$out" -minShift 0.001 -maxShift 1 -schema more -table "$table" -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a "$a" -q "$q" -p "$p"
    done
done