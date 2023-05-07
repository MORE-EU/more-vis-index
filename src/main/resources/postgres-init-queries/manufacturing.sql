CREATE SCHEMA IF NOT EXISTS more;

CREATE TABLE more.manufacturing_tmp(
                                           timestamp        TIMESTAMP NOT NULL
    ,[0]      FLOAT
    ,[1]      FLOAT
    ,[2]     FLOAT
    ,[3]    FLOAT
    ,[4] FLOAT
    ,[5] FLOAT
    ,[6] FLOAT
    ,[7] FLOAT
);

COPY more.manufacturing_tmp([0], [1], [2], [3], [4], [5], [6], [7])
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
SELECT date_part('epoch', timestamp) * 1000, timestamp, [0], 0, 'value_1' FROM more.manufacturing_tmp;

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, [1], 1, 'value_2' FROM more.manufacturing_tmp;

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, [2], 2, 'value_3' FROM more.manufacturing_tmp;

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, [3], 3, 'value_4' FROM more.manufacturing_tmp;

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, [4], 4, 'value_5' FROM more.manufacturing_tmp;

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, [5], 5, 'value_6' FROM more.manufacturing_tmp;

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, [6], 6, 'value_7' FROM more.manufacturing_tmp;

INSERT INTO more.manufacturing(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, [7], 7, 'value_8' FROM more.manufacturing_tmp;

DROP TABLE more.manufacturing_tmp;

CREATE INDEX manufacturing_index ON more.manufacturing(epoch, id);
