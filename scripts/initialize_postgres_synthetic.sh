java -jar ../target/experiments.jar -path /data/data2/bstam/more-vis/synthetic/1m.csv -c timeInitialization -seqCount 50 -type postgres \
-measures 3,4 -timeCol datetime -zoomFactor 2 -out output -minShift 0.01 -maxShift 1 \
-minFilters 0 -maxFilters 20 -startTime "2114-02-12 01:00:00.000" -endTime "2114-02-19 00:00:00.000" \
-postgreSQLCfg postgreSQL.cfg -influxDBCfg influxDB.cfg -schema more -table synthetic1m -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"

java -jar ../target/experiments.jar -path /data/data2/bstam/more-vis/synthetic/2m.csv -c timeInitialization -seqCount 50 -type postgres \
-measures 3,4 -timeCol datetime -zoomFactor 2 -out output -minShift 0.01 -maxShift 1 \
-minFilters 0 -maxFilters 20 -startTime "2114-02-12 01:00:00.000" -endTime "2114-02-19 00:00:00.000" \
-postgreSQLCfg postgreSQL.cfg -influxDBCfg influxDB.cfg -schema more -table synthetic2m -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"

java -jar ../target/experiments.jar -path /data/data2/bstam/more-vis/synthetic/4m.csv -c timeInitialization -seqCount 50 -type postgres \
-measures 3,4 -timeCol datetime -zoomFactor 2 -out output -minShift 0.01 -maxShift 1 \
-minFilters 0 -maxFilters 20 -startTime "2114-02-12 01:00:00.000" -endTime "2114-02-19 00:00:00.000" \
-postgreSQLCfg postgreSQL.cfg -influxDBCfg influxDB.cfg -schema more -table synthetic4m -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"


java -jar ../target/experiments.jar -path /data/data2/bstam/more-vis/synthetic/8m.csv -c timeInitialization -seqCount 50 -type postgres \
-measures 3,4 -timeCol datetime -zoomFactor 2 -out output -minShift 0.01 -maxShift 1 \
-minFilters 0 -maxFilters 20 -startTime "2114-02-12 01:00:00.000" -endTime "2114-02-19 00:00:00.000" \
-postgreSQLCfg postgreSQL.cfg -influxDBCfg influxDB.cfg -schema more -table synthetic8m -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"


java -jar ../target/experiments.jar -path /data/data2/bstam/more-vis/synthetic/16m.csv -c timeInitialization -seqCount 50 -type postgres \
-measures 3,4 -timeCol datetime -zoomFactor 2 -out output -minShift 0.01 -maxShift 1 \
-minFilters 0 -maxFilters 20 -startTime "2114-02-12 01:00:00.000" -endTime "2114-02-19 00:00:00.000" \
-postgreSQLCfg postgreSQL.cfg -influxDBCfg influxDB.cfg -schema more -table synthetic16m -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"


java -jar ../target/experiments.jar -path /data/data2/bstam/more-vis/synthetic/32m.csv -c timeInitialization -seqCount 50 -type postgres \
-measures 3,4 -timeCol datetime -zoomFactor 2 -out output -minShift 0.01 -maxShift 1 \
-minFilters 0 -maxFilters 20 -startTime "2114-02-12 01:00:00.000" -endTime "2114-02-19 00:00:00.000" \
-postgreSQLCfg postgreSQL.cfg -influxDBCfg influxDB.cfg -schema more -table synthetic32m -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"


java -jar ../target/experiments.jar -path /data/data2/bstam/more-vis/synthetic/64m.csv -c timeInitialization -seqCount 50 -type postgres \
-measures 3,4 -timeCol datetime -zoomFactor 2 -out output -minShift 0.01 -maxShift 1 \
-minFilters 0 -maxFilters 20 -startTime "2114-02-12 01:00:00.000" -endTime "2114-02-19 00:00:00.000" \
-postgreSQLCfg postgreSQL.cfg -influxDBCfg influxDB.cfg -schema more -table synthetic64m -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"


# java -jar ../target/experiments.jar -path /data/data2/bstam/more-vis/synthetic/128m.csv -c timeInitialization -seqCount 50 -type postgres \
# -measures 3,4 -timeCol datetime -zoomFactor 2 -out output -minShift 0.01 -maxShift 1 \
# -minFilters 0 -maxFilters 20 -startTime "2114-02-12 01:00:00.000" -endTime "2114-02-19 00:00:00.000" \
# -postgreSQLCfg postgreSQL.cfg -influxDBCfg influxDB.cfg -schema more -table synthetic128m -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"


# java -jar ../target/experiments.jar -path /data/data2/bstam/more-vis/synthetic/256m.csv -c timeInitialization -seqCount 50 -type postgres \
# -measures 3,4 -timeCol datetime -zoomFactor 2 -out output -minShift 0.01 -maxShift 1 \
# -minFilters 0 -maxFilters 20 -startTime "2114-02-12 01:00:00.000" -endTime "2114-02-19 00:00:00.000" \
# -postgreSQLCfg postgreSQL.cfg -influxDBCfg influxDB.cfg -schema more -table synthetic256m -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"


# java -jar ../target/experiments.jar -path /data/data2/bstam/more-vis/synthetic/512m.csv -c timeInitialization -seqCount 50 -type postgres \
# -measures 3,4 -timeCol datetime -zoomFactor 2 -out output -minShift 0.01 -maxShift 1 \
# -minFilters 0 -maxFilters 20 -startTime "2114-02-12 01:00:00.000" -endTime "2114-02-19 00:00:00.000" \
# -postgreSQLCfg postgreSQL.cfg -influxDBCfg influxDB.cfg -schema more -table synthetic512m -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"


# java -jar ../target/experiments.jar -path /data/data2/bstam/more-vis/synthetic/1b.csv -c timeInitialization -seqCount 50 -type postgres \
# -measures 3,4 -timeCol datetime -zoomFactor 2 -out output -minShift 0.01 -maxShift 1 \
# -minFilters 0 -maxFilters 20 -startTime "2114-02-12 01:00:00.000" -endTime "2114-02-19 00:00:00.000" \
# -postgreSQLCfg postgreSQL.cfg -influxDBCfg influxDB.cfg -schema more -table synthetic1b -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"

