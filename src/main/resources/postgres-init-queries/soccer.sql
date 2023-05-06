CREATE SCHEMA IF NOT EXISTS more;

CREATE TABLE more.soccer_tmp(
   timestamp        TIMESTAMP NOT NULL
  ,moteid      FLOAT
  ,temperature      FLOAT
  ,humidity     FLOAT
  ,light    FLOAT
  ,voltage FLOAT
);

COPY more.soccer(timestamp, x,	y,	z,	abs_vel,	abs_accel,	vx,	vy,	vz,	ax,	ay,	az)
FROM '%path'
DELIMITER ','
CSV HEADER;

CREATE TABLE more.soccer(
    epoch LONG NOT NULL
  ,timestamp   TIMESTAMP NOT NULL
  ,value      FLOAT
  ,id         INT NOT NULL
  ,col VARCHAR NOT NULL
);

INSERT INTO more.soccer(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, datetime, x, 0, 'value_1' FROM more.soccer_tmp;

INSERT INTO more.soccer(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, datetime, y, 1, 'value_2' FROM more.soccer_tmp;

INSERT INTO more.soccer(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, datetime, z, 2, 'value_3' FROM more.soccer_tmp;

INSERT INTO more.soccer(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, datetime, abs_vel, 3, 'value_4' FROM more.soccer_tmp;

INSERT INTO more.soccer(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, datetime, abs_accel, 4, 'value_5' FROM more.soccer_tmp;

INSERT INTO more.soccer(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, datetime, vx, 5, 'value_6' FROM more.soccer_tmp;

INSERT INTO more.soccer(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, datetime, vy, 6, 'value_7' FROM more.soccer_tmp;

INSERT INTO more.soccer(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, datetime, vz, 7, 'value_8' FROM more.soccer_tmp;

INSERT INTO more.soccer(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, datetime, ax, 8, 'value_9' FROM more.soccer_tmp;

INSERT INTO more.soccer(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, datetime, ay, 9, 'value_10' FROM more.soccer_tmp;

INSERT INTO more.soccer(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, datetime, az, 10, 'value_11' FROM more.soccer_tmp;

DROP TABLE more.soccer_tmp;

CREATE INDEX soccer_index ON more.soccer(epoch, id);
