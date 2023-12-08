CREATE SCHEMA IF NOT EXISTS more;

CREATE TABLE more.soccer_exp_tmp(
                                timestamp        TIMESTAMP NOT NULL
    ,x      FLOAT
    ,y      FLOAT
    ,z     FLOAT
    ,v    FLOAT
    ,a FLOAT
    ,vx      FLOAT
    ,vy      FLOAT
    ,vz     FLOAT
    ,ax      FLOAT
    ,ay      FLOAT
    ,az     FLOAT

);

COPY more.soccer_exp_tmp(timestamp, x, y, z, v, a, vx, vy, vz, ax, ay, az)
    FROM '%path'
    DELIMITER ','
    CSV HEADER;

CREATE TABLE more.soccer_exp(
    epoch BIGINT NOT NULL
    ,timestamp   TIMESTAMP NOT NULL
    ,value      FLOAT
    ,id         INT NOT NULL
    ,col VARCHAR NOT NULL
    ,PRIMARY KEY(id, epoch)
);


INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, a, 0, 'a' FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, ax, 1, 'ax' FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, ay, 2, 'ay' FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, az, 3, 'az' FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, v, 4, 'v' FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, vx, 5, 'vx' FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, vy, 6, 'vy' FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, vz, 7, 'vz' FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, x, 8, 'x' FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, y, 9, 'y' FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, z, 10, 'z' FROM more.soccer_exp_tmp;



--
--

DROP TABLE more.soccer_exp_tmp;
