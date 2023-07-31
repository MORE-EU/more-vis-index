  #!/bin/bash

#a=$1
#q=$2
#p=$3
#aggFactor=$4
#reductionRatio=$5
#type=$6

# Agg Ratio
#sh scripts/run.sh 0.95 0.1 0 2 4 influx
#sh scripts/run.sh 0.95 0.1 0 4 4 influx
#
### Error Bound
#sh scripts/run.sh 0.8 0.1 0 4 4 influx
#sh scripts/run.sh 0.9 0.1 0 4 4 influx
#sh scripts/run.sh 0.95 0.1 0 4 4 influx
#sh scripts/run.sh 0.99 0.1 0 4 4 influx
##
### Data Reduction
#sh scripts/run.sh 0.95 0.1 0 4 2 influx
#sh scripts/run.sh 0.95 0.1 0 4 4 influx
#sh scripts/run.sh 0.95 0.1 0 4 10 influx
##
#sh scripts/run.sh 0.95 0.01 0 4 2 influx
#sh scripts/run.sh 0.95 0.01 0 4 4 influx
#sh scripts/run.sh 0.95 0.01 0 4 10 influx
##
### Start Query Selectivity
#sh scripts/run.sh 0.95 0.01 0 4 4 influx
#sh scripts/run.sh 0.95 0.05 0 4 4 influx
#sh scripts/run.sh 0.95 0.1 0 4 4 influx
#sh scripts/run.sh 0.95 0.5 0 4 4 influx

## PREFETCHING
#sh scripts/run.sh 0.95 0.1 0 4 4 influx
#
## Agg Ratio
#sh scripts/run.sh 0.95 0.1 1 2 4 influx
#sh scripts/run.sh 0.95 0.1 1 4 4 influx
#
### Error Bound
#sh scripts/run.sh 0.8 0.1 1 4 4 influx
#sh scripts/run.sh 0.9 0.1 1 4 4 influx
#sh scripts/run.sh 0.95 0.1 1 4 4 influx
#sh scripts/run.sh 0.99 0.1 1 4 4 influx
##
### Data Reduction
#sh scripts/run.sh 0.95 0.1 1 4 2 influx
#sh scripts/run.sh 0.95 0.1 1 4 4 influx
#sh scripts/run.sh 0.95 0.1 1 4 10 influx
##
#sh scripts/run.sh 0.95 0.01 1 4 2 influx
#sh scripts/run.sh 0.95 0.01 1 4 4 influx
#sh scripts/run.sh 0.95 0.01 1 4 10 influx
##
### Start Query Selectivity
#sh scripts/run.sh 0.95 0.01 1 4 4 influx
#sh scripts/run.sh 0.95 0.05 1 4 4 influx
#sh scripts/run.sh 0.95 0.1 1 4 4 influx
#sh scripts/run.sh 0.95 0.5 1 4 4 influx


sh scripts/run.sh 0.95 0.005 1 4 2 influx
sh scripts/run.sh 0.95 0.005 1 4 4 influx
sh scripts/run.sh 0.95 0.005 1 4 10 influx