CREATE SCHEMA IF NOT EXISTS more;

CREATE TABLE more.intel_lab_exp_tmp(
   timestamp        TIMESTAMP NOT NULL
  ,moteid      FLOAT
  ,temperature      FLOAT
  ,humidity     FLOAT
  ,light    FLOAT
  ,voltage FLOAT
);

COPY more.intel_lab_exp_tmp(timestamp, moteid, temperature,
humidity, light, voltage)
FROM '%path'
DELIMITER ','
CSV HEADER;

CREATE TABLE more.intel_lab_exp(
    epoch BIGINT NOT NULL
  ,timestamp   TIMESTAMP NOT NULL
  ,value      FLOAT
  ,id         INT NOT NULL
  ,col VARCHAR NOT NULL
);

INSERT INTO more.intel_lab_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, moteid, 0, 'value_1' FROM more.intel_lab_exp_tmp;

INSERT INTO more.intel_lab_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, temperature, 1, 'value_2' FROM more.intel_lab_exp_tmp;

INSERT INTO more.intel_lab_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, humidity, 2, 'value_3' FROM more.intel_lab_exp_tmp;

INSERT INTO more.intel_lab_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, light, 3, 'value_4' FROM more.intel_lab_exp_tmp;

INSERT INTO more.intel_lab_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, voltage, 4, 'value_5' FROM more.intel_lab_exp_tmp;

DROP TABLE more.intel_lab_exp_tmp;

CREATE INDEX intel_lab_exp_index ON more.intel_lab_exp(epoch, id);
