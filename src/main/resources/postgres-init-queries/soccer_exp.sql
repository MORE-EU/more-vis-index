CREATE SCHEMA IF NOT EXISTS more;

CREATE TABLE more.soccer_exp_tmp(
                                timestamp        TIMESTAMP NOT NULL
    ,x      FLOAT
    ,y      FLOAT
    ,z     FLOAT
    ,abs_vel    FLOAT
    ,abs_accel FLOAT
    ,vx      FLOAT
    ,vy      FLOAT
    ,vz     FLOAT
    ,ax      FLOAT
    ,ay      FLOAT
    ,az     FLOAT
);

COPY more.soccer_exp_tmp(timestamp, x, y, z, abs_vel, abs_accel, vx, vy, vz, ax, ay, az)
    FROM '%path'
    DELIMITER ','
    CSV HEADER;

CREATE TABLE more.soccer_exp(
    epoch BIGINT NOT NULL
    ,timestamp   TIMESTAMP NOT NULL
    ,value      FLOAT
    ,id         INT NOT NULL
    ,col VARCHAR NOT NULL
);

INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, x, 0, 'value_1' FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, y, 1, 'value_2' FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, z, 2, 'value_3' FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, abs_vel, 3, 'value_4' FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, abs_accel, 4, 'value_5' FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, vx, 5, 'value_6' FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, vy, 6, 'value_7' FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, vz, 7, 'value_8' FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, ax, 8, 'value_9' FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, ay, 9, 'value_10' FROM more.soccer_exp_tmp;

INSERT INTO more.soccer_exp(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, az, 10, 'value_11' FROM more.soccer_exp_tmp;

DROP TABLE more.soccer_exp_tmp;

CREATE INDEX soccer_exp_index ON more.soccer_exp(epoch, id);
