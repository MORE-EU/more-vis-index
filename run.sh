java -jar target/experiments.jar -c timeQueries -seqCount 25 -type influx -mode all \
-measures 2 -timeCol datetime -zoomFactor 2 -viewport 1000,600 -runs 1 -out 1month80percent1plus -minShift 0.1 \
-maxShift 1 -startTime "2113-01-01 01:21:51.000" -endTime "2113-01-31 09:53:21.000" \
-schema more -table intel_lab_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.8

java -jar target/experiments.jar -c timeQueries -seqCount 25 -type influx -mode all \
-measures 2 -timeCol datetime -zoomFactor 2 -viewport 1000,600 -runs 1 -out 1month85percent1plus -minShift 0.1 \
-maxShift 1 -startTime "2113-01-01 01:21:51.000" -endTime "2113-01-31 09:53:21.000" \
-schema more -table intel_lab_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.85

java -jar target/experiments.jar -c timeQueries -seqCount 25 -type influx -mode all \
-measures 2 -timeCol datetime -zoomFactor 2 -viewport 1000,600 -runs 1 -out 1month90percent1plus -minShift 0.1 \
-maxShift 1 -startTime "2113-01-01 01:21:51.000" -endTime "2113-01-31 09:53:21.000" \
-schema more -table intel_lab_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.9

java -jar target/experiments.jar -c timeQueries -seqCount 25 -type influx -mode all \
-measures 2 -timeCol datetime -zoomFactor 2 -viewport 1000,600 -runs 1 -out 1month95percent1plus -minShift 0.1  \
-maxShift 1 -startTime "2113-01-01 01:21:51.000" -endTime "2113-01-31 09:53:21.000" \
-schema more -table intel_lab_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.95

######### 1 YEAR

java -jar target/experiments.jar -c timeQueries -seqCount 25 -type influx -mode all \
-measures 2 -timeCol datetime -zoomFactor 2 -viewport 1000,600 -runs 1 -out 1year80percent1plus -minShift 0.1 \
-maxShift 1 -startTime "2112-01-01 01:21:51.000" -endTime "2113-01-31 09:53:21.000" \
-schema more -table intel_lab_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.8

java -jar target/experiments.jar -c timeQueries -seqCount 25 -type influx -mode all \
-measures 2 -timeCol datetime -zoomFactor 2 -viewport 1000,600 -runs 1 -out 1year85percent1plus -minShift 0.1 \
-maxShift 1 -startTime "2112-01-01 01:21:51.000" -endTime "2113-01-31 09:53:21.000" \
-schema more -table intel_lab_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.85

java -jar target/experiments.jar -c timeQueries -seqCount 25 -type influx -mode all \
-measures 2 -timeCol datetime -zoomFactor 2 -viewport 1000,600 -runs 1 -out 1year90percent1plus -minShift 0.1 \
-maxShift 1 -startTime "2112-01-01 01:21:51.000" -endTime "2113-01-31 09:53:21.000" \
-schema more -table intel_lab_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.9

java -jar target/experiments.jar -c timeQueries -seqCount 25 -type influx -mode all \
-measures 2 -timeCol datetime -zoomFactor 2 -viewport 1000,600 -runs 1 -out 1year95percent1plus -minShift 0.1  \
-maxShift 1 -startTime "2112-01-01 01:21:51.000" -endTime "2113-01-31 09:53:21.000" \
-schema more -table intel_lab_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.95


# ######### 10 YEAR

# java -jar target/experiments.jar -c timeQueries -seqCount 25 -type influx -mode all \
# -measures 2 -timeCol datetime -zoomFactor 2 -viewport 1000,600 -runs 1 -out 5year80percent1plus -minShift 0.1 \
# -maxShift 1 -startTime "2103-01-01 01:21:51.000" -endTime "2113-01-31 09:53:21.000" \
# -schema more -table intel_lab_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.8

# java -jar target/experiments.jar -c timeQueries -seqCount 25 -type influx -mode all \
# -measures 2 -timeCol datetime -zoomFactor 2 -viewport 1000,600 -runs 1 -out 5year85percent1plus -minShift 0.1 \
# -maxShift 1 -startTime "2103-01-01 01:21:51.000" -endTime "2113-01-31 09:53:21.000" \
# -schema more -table intel_lab_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.85

# java -jar target/experiments.jar -c timeQueries -seqCount 25 -type influx -mode all \
# -measures 2 -timeCol datetime -zoomFactor 2 -viewport 1000,600 -runs 1 -out 5year90percent1plus -minShift 0.1 \
# -maxShift 1 -startTime "2103-01-01 01:21:51.000" -endTime "2113-01-31 09:53:21.000" \
# -schema more -table intel_lab_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.9

# java -jar target/experiments.jar -c timeQueries -seqCount 25 -type influx -mode all \
# -measures 2 -timeCol datetime -zoomFactor 2 -viewport 1000,600 -runs 1 -out 5year95percent1plus -minShift 0.1  \
# -maxShift 1 -startTime "2103-01-01 01:21:51.000" -endTime "2113-01-31 09:53:21.000" \
# -schema more -table intel_lab_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.95
