#!/bin/bash

#a=$1
#q=$2
#p=$3
#aggFactor=$4
#reductionRatio=$5
#type=$6

# # Agg Ratio
# sh scripts/run.sh 0.95 0.1 1 2 6 influx
# sh scripts/run.sh 0.95 0.1 1 4 6 influx
sh scripts/run.sh 0.95 0.1 1 6 6 influx
# sh scripts/run.sh 0.95 0.1 1 8 6 influx
# sh scripts/run.sh 0.95 0.1 1 10 6 influx
# sh scripts/run.sh 0.95 0.1 1 12 6 influx
# sh scripts/run.sh 0.95 0.1 1 16 6 influx

## Data Reduction
# sh scripts/run.sh 0.95 0.1 1 6 2 influx
# sh scripts/run.sh 0.95 0.1 1 6 4 influx
# sh scripts/run.sh 0.95 0.1 1 6 6 influx
# sh scripts/run.sh 0.95 0.1 1 6 8 influx
# sh scripts/run.sh 0.95 0.1 1 6 10 influx

## Error Bound
# sh scripts/run.sh 0.9 0.1 1 6 6 influx
# sh scripts/run.sh 0.95 0.1 1 6 6 influx
# sh scripts/run.sh 0.99 0.1 1 6 6 influx

# Start Query  Selectivity
# sh scripts/run.sh 0.95 0.01 1 6 6 influx
# sh scripts/run.sh 0.95 0.05 1 6 6 influx
# sh scripts/run.sh 0.95 0.1 1 6 6 influx
# sh scripts/run.sh 0.95 0.5 1 6 6 influx

## No prefetching
# sh scripts/run.sh 0.95 0.1 0 6 6 influx

