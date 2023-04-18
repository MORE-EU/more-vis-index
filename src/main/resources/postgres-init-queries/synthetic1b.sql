CREATE SCHEMA IF NOT EXISTS more;

CREATE TABLE more.synthetic1b_tmp(
     timestamp        TIMESTAMP NOT NULL PRIMARY KEY
    ,value_1      FLOAT NOT NULL
    ,value_2      FLOAT NOT NULL
    ,value_3     FLOAT NOT NULL
    ,value_4    FLOAT NOT NULL
    ,value_5 FLOAT NOT NULL

);
COPY more.synthetic1b_tmp(timestamp, value_1, value_2, value_3, value_4, value_5)
    FROM '%path'
    DELIMITER ','
    CSV HEADER;


CREATE TABLE more.synthetic1b(
     epoch   BIGINT NOT NULL
    ,timestamp  TIMESTAMP NOT NULL
    ,value      FLOAT
    ,id         BIGINT NOT NULL
    ,col        VARCHAR NOT NULL
);

INSERT INTO more.synthetic1b(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, value_1, 0, 'value_1' FROM more.synthetic1b_tmp;

INSERT INTO more.synthetic1b(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, value_2, 1, 'value_2' FROM more.synthetic1b_tmp;

INSERT INTO more.synthetic1b(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, value_3, 2, 'value_3' FROM more.synthetic1b_tmp;

INSERT INTO more.synthetic1b(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, value_4, 3, 'value_4' FROM more.synthetic1b_tmp;

INSERT INTO more.synthetic1b(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, value_5, 4, 'value_5' FROM more.synthetic1b_tmp;

DROP TABLE more.synthetic1b_tmp;

CREATE INDEX synthetic1b_index ON more.synthetic1b(epoch, id);



