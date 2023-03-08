CREATE SCHEMA IF NOT EXISTS more;

CREATE TABLE more.intel_lab_tmp(
   datetime        TIMESTAMP NOT NULL
  ,moteid      FLOAT
  ,temperature      FLOAT
  ,humidity     FLOAT
  ,light    FLOAT
  ,voltage FLOAT
);

COPY more.intel_lab_tmp(datetime, moteid, temperature,
humidity, light, voltage)
FROM '%path'
DELIMITER ','
CSV HEADER;

CREATE TABLE more.intel_lab(
   datetime   TIMESTAMP NOT NULL
  ,value      FLOAT
  ,id         INT NOT NULL
);

INSERT INTO more.intel_lab(datetime, value, id)
SELECT datetime, moteid, 1 FROM more.intel_lab_tmp;

INSERT INTO more.intel_lab(datetime, value, id)
SELECT datetime, temperature, 2 FROM more.intel_lab_tmp;

INSERT INTO more.intel_lab(datetime, value, id)
SELECT datetime, humidity, 3 FROM more.intel_lab_tmp;

INSERT INTO more.intel_lab(datetime, value, id)
SELECT datetime, light, 4 FROM more.intel_lab_tmp;

INSERT INTO more.intel_lab(datetime, value, id)
SELECT datetime, voltage, 5 FROM more.intel_lab_tmp;

CREATE INDEX intel_lab_index ON more.intel_lab(id, datetime);



