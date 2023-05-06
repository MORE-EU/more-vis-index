CREATE SCHEMA IF NOT EXISTS more;

CREATE TABLE more.manufacturing_tmp(
   timestamp        TIMESTAMP NOT NULL
  ,moteid      FLOAT
  ,temperature      FLOAT
  ,humidity     FLOAT
  ,light    FLOAT
  ,voltage FLOAT
);

COPY more.manufacturing(0, 1, 2, 3, 4, 5, 6, 7)
FROM '%path'
DELIMITER ','
CSV HEADER;

CREATE TABLE more.manufacturing(
    epoch LONG NOT NULL
  ,timestamp   TIMESTAMP NOT NULL
  ,value      FLOAT
  ,id         INT NOT NULL
  ,col VARCHAR NOT NULL
);

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, datetime, 0, 0, 'value_1' FROM more.manufacturing_tmp;

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, datetime, 1, 1, 'value_2' FROM more.manufacturing_tmp;

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, datetime, 2, 2, 'value_3' FROM more.manufacturing_tmp;

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, datetime, 3, 3, 'value_4' FROM more.manufacturing_tmp;

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, datetime, 4, 4, 'value_5' FROM more.manufacturing_tmp;

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, datetime, 5, 5, 'value_6' FROM more.manufacturing_tmp;

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, datetime, 6, 6, 'value_7' FROM more.manufacturing_tmp;

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, datetime, 7, 7, 'value_8' FROM more.manufacturing_tmp;

DROP TABLE more.manufacturing_tmp;

CREATE INDEX manufacturing_index ON more.manufacturing(epoch, id);
