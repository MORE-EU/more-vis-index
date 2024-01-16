#!/bin/bash

#a=$1
#q=$2
#p=$3
#aggFactor=$4
#reductionRatio=$5
#type=$6

# # Agg Ratio
sh scripts/run.sh 0.95 0.1 1 2 4 postgres
sh scripts/run.sh 0.95 0.1 1 4 4 postgres
sh scripts/run.sh 0.95 0.1 1 6 4 postgres
sh scripts/run.sh 0.95 0.1 1 8 4 postgres

## Data Reduction
sh scripts/run.sh 0.95 0.1 1 4 2 postgres
sh scripts/run.sh 0.95 0.1 1 4 4 postgres
sh scripts/run.sh 0.95 0.1 1 4 6 postgres
sh scripts/run.sh 0.95 0.1 1 4 8 postgres
sh scripts/run.sh 0.95 0.1 1 4 10 postgres

## Error Bound
sh scripts/run.sh 0.9 0.1 1 4 4 postgres
sh scripts/run.sh 0.95 0.1 1 4 4 postgres
sh scripts/run.sh 0.99 0.1 1 4 4 postgres

# Start Query  Selectivity
sh scripts/run.sh 0.95 0.01 1 4 4 postgres
sh scripts/run.sh 0.95 0.05 1 4 4 postgres
sh scripts/run.sh 0.95 0.1 1 4 4 postgres
sh scripts/run.sh 0.95 0.5 1 4 4 postgres

## No prefetching
sh scripts/run.sh 0.95 0.1 0 4 4 postgres

