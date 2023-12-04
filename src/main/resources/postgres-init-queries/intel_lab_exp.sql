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
  ,PRIMARY KEY(id, epoch)
);

INSERT INTO more.intel_lab_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, humidity, 0, 'humidity' FROM more.intel_lab_exp_tmp;

INSERT INTO more.intel_lab_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, light, 1, 'light' FROM more.intel_lab_exp_tmp;

INSERT INTO more.intel_lab_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, moteid, 2, 'moteid' FROM more.intel_lab_exp_tmp;

INSERT INTO more.intel_lab_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, temperature, 3, 'temperature' FROM more.intel_lab_exp_tmp;

INSERT INTO more.intel_lab_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, voltage, 4, 'voltage' FROM more.intel_lab_exp_tmp;

DROP TABLE more.intel_lab_exp_tmp;

