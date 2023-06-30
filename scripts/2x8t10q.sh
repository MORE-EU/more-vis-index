java -jar target/experiments.jar -c timeQueries -seqCount 25 -type influx -mode all -measures 2 -timeCol datetime -zoomFactor 1.4 \
-viewport 1000,600 -runs 1 -out 80a2x8t10q -minShift 0.1 -maxShift 1 -schema more -table \
intel_lab_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.8 -q 0.1
 
java -jar target/experiments.jar -c timeQueries -seqCount 25 -type influx -mode all -measures 2 -timeCol datetime -zoomFactor 1.4 \
 -viewport 1000,600 -runs 1 -out 80a2x8t10q -minShift 0.1 -maxShift 1 -schema more -table \
manufacturing_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.8 -q 0.1
  
java -jar target/experiments.jar -c timeQueries -seqCount 25 -type influx -mode all -measures 2 -timeCol datetime -zoomFactor 1.4 \
-viewport 1000,600 -runs 1 -out 80a2x8t10q -minShift 0.1 -maxShift 1 -schema more -table \
soccer_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.8 -q 0.1

java -jar target/experiments.jar -c timeQueries -seqCount 25 -type influx -mode all -measures 2 -timeCol datetime -zoomFactor 1.4 \
-viewport 1000,600 -runs 1 -out 90a2x8t10q -minShift 0.1 -maxShift 1 -schema more -table \
intel_lab_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.9 -q 0.1
 
 java -jar target/experiments.jar -c timeQueries -seqCount 25 -type influx -mode all -measures 2 -timeCol datetime -zoomFactor 1.4 \
 -viewport 1000,600 -runs 1 -out 90a2x8t10q -minShift 0.1 -maxShift 1 -schema more -table \
manufacturing_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.9 -q 0.1
  
java -jar target/experiments.jar -c timeQueries -seqCount 25 -type influx -mode all -measures 2 -timeCol datetime -zoomFactor 1.4 \
-viewport 1000,600 -runs 1 -out 90a2x8t10q -minShift 0.1 -maxShift 1 -schema more -table \
soccer_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.9 -q 0.1
  
java -jar target/experiments.jar -c timeQueries -seqCount 25 -type influx -mode all -measures 2 -timeCol datetime -zoomFactor 1.4 \
-viewport 1000,600 -runs 1 -out 95a2x8t10q -minShift 0.1 -maxShift 1 -schema more -table \
intel_lab_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.95 -q 0.1
 
 java -jar target/experiments.jar -c timeQueries -seqCount 25 -type influx -mode all -measures 2 -timeCol datetime -zoomFactor 1.4 \
 -viewport 1000,600 -runs 1 -out 95a2x8t10q -minShift 0.1 -maxShift 1 -schema more -table \
manufacturing_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.95 -q 0.1
  
java -jar target/experiments.jar -c timeQueries -seqCount 25 -type influx -mode all -measures 2 -timeCol datetime -zoomFactor 1.4 \
-viewport 1000,600 -runs 1 -out 95a2x8t10q -minShift 0.1 -maxShift 1 -schema more -table \
soccer_exp -timeFormat "yyyy-MM-dd[ HH:mm:ss.SSS]" -a 0.95 -q 0.1
  