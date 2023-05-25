java -jar target/experiments.jar \
-path /data/data2/bstam/more-vis/real/intel_lab.csv -c timeQueries -seqCount 50 -type postgres \
-measures 3,4 -timeCol datetime -zoomFactor 2 -out output -minShift 0.01 -maxShift 1 \
-minFilters 0 -maxFilters 20 -startTime "2004-04-01 08:03:00.000" -endTime "2004-04-05 11:00:00.000" \
-postgreSQLCfg postgreSQL.cfg -schema more -table intel_lab -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"

java -jar target/experiments.jar \
-path /data/data2/bstam/more-vis/real/soccer.csv -c timeQueries -seqCount 50 -type postgres \
-measures 3,4 -timeCol datetime -zoomFactor 2 -out output -minShift 0.01 -maxShift 1 \
-minFilters 0 -maxFilters 20 -startTime "1970-06-21 22:58:32.000" -endTime "1970-06-22 09:12:00.000" \
-postgreSQLCfg postgreSQL.cfg -schema more -table soccer -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"

java -jar target/experiments.jar \
-path /data/data2/bstam/more-vis/real/manufacturing.csv -c timeQueries -seqCount 50 -type postgres \
-measures 3,4 -timeCol datetime -zoomFactor 2 -out output -minShift 0.01 -maxShift 1  \
-minFilters 0 -maxFilters 20 -startTime "2012-02-24 11:48:57.000" -endTime "2012-02-24 12:02:00.000"  \
-postgreSQLCfg postgreSQL.cfg -schema more -table manufacturing -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"
