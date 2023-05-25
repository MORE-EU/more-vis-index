# # 2004-02-28 00:58:46-2114-02-19 02:18:16
# java -jar ../target/experiments.jar -path /data/data2/bstam/more-vis/real_exp/intel_lab.csv -c timeQueries -seqCount 50 -type postgres \
# -measures 3,4 -timeCol datetime -zoomFactor 2 -out output -minShift 0.01 -maxShift 1 \
# -minFilters 0 -maxFilters 20 -startTime "2114-01-13 09:00:00.000" -endTime "2114-02-19 02:17:00.000" \
# -postgreSQLCfg postgreSQL.cfg -schema more -table intel_lab_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"

# # 1970-05-04 02:08:00 -- 1976-01-14 10:54:00:00
java -jar ../target/experiments.jar -path /data/data2/bstam/more-vis/real_exp/soccer.csv -c timeQueries -seqCount 50 -type postgres \
-measures 3,4 -timeCol datetime -zoomFactor 2 -out output -minShift 0.01 -maxShift 1 \
-minFilters 0 -maxFilters 20 -startTime "1975-12-24 15:22:00.000" -endTime "1976-01-14 10:54:00.000" \
-postgreSQLCfg postgreSQL.cfg -schema more -table soccer_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"

# # 2012-02-22 05:10 -  2012-02-25 08:10
# java -jar ../target/experiments.jar -path /data/data2/bstam/more-vis/real_exp/manufacturing.csv -c timeQueries -seqCount 50 -type postgres \
# -measures 3,4 -timeCol datetime -zoomFactor 2 -out output -minShift 0.01 -maxShift 1 \
# -minFilters 0 -maxFilters 20 -startTime "2012-02-25 04:42:11.000" -endTime "2012-02-25 05:10:31.000" \
# -postgreSQLCfg postgreSQL.cfg -schema more -table manufacturing_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]"
