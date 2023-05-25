java -jar ../target/experiments.jar -path /data/data2/bstam/more-vis/real_exp/intel_lab.csv -c timeInitialization -seqCount 50 -type postgres \
-measures 3,4 -timeCol datetime -zoomFactor 2 -out output -minShift 0.01 -maxShift 1 \
-minFilters 0 -maxFilters 20 -startTime "2114-02-12 01:00:00.000" -endTime "2114-02-19 00:00:00.000" \
-postgreSQLCfg postgreSQL.cfg -schema more -table intel_lab_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"

# java -jar ../target/experiments.jar -path /data/data2/bstam/more-vis/real_exp/soccer.csv -c timeInitialization -seqCount 50 -type postgres \
# -measures 3,4 -timeCol datetime -zoomFactor 2 -out output -minShift 0.01 -maxShift 1 \
# -minFilters 0 -maxFilters 20 -startTime "2114-02-12 01:00:00.000" -endTime "2114-02-19 00:00:00.000" \
# -postgreSQLCfg postgreSQL.cfg -schema more -table soccer_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"

# java -jar ../target/experiments.jar -path /data/data2/bstam/more-vis/real_exp/manufacturing.csv -c timeInitialization -seqCount 50 -type postgres \
# -measures 3,4 -timeCol datetime -zoomFactor 2 -out output -minShift 0.01 -maxShift 1 \
# -minFilters 0 -maxFilters 20 -startTime "2114-02-12 01:00:00.000" -endTime "2114-02-19 00:00:00.000" \
# -postgreSQLCfg postgreSQL.cfg  -schema more -table manufacturing_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"
