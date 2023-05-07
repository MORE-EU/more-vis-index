CREATE SCHEMA IF NOT EXISTS more;

CREATE TABLE more.manufacturing_tmp(
     timestamp        TIMESTAMP NOT NULL
    ,value_1      FLOAT
    ,value_2      FLOAT
    ,value_3    FLOAT
    ,value_4 FLOAT
    ,value_5 FLOAT
    ,value_6 FLOAT
    ,value_7 FLOAT
);

COPY more.manufacturing_tmp(timestamp,value_1,value_2,value_3,value_4,value_5,value_6,value_7)
    FROM '%path'
    DELIMITER ','
    CSV HEADER;

CREATE TABLE more.manufacturing(
     epoch BIGINT NOT NULL
    ,timestamp   TIMESTAMP NOT NULL
    ,value      FLOAT
    ,id         INT NOT NULL
    ,col VARCHAR NOT NULL
);

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, value_1,, 0, 'value_1' FROM more.manufacturing_tmp;

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, value_2, 1, 'value_2' FROM more.manufacturing_tmp;

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, value_3, 2, 'value_3' FROM more.manufacturing_tmp;

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, value_4, 3, 'value_4' FROM more.manufacturing_tmp;

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, value_5, 4, 'value_5' FROM more.manufacturing_tmp;

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, value_6, 5, 'value_6' FROM more.manufacturing_tmp;

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, value_7, 6, 'value_7' FROM more.manufacturing_tmp;


DROP TABLE more.manufacturing_tmp;

CREATE INDEX manufacturing_index ON more.manufacturing(epoch, id);
