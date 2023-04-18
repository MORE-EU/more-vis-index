CREATE SCHEMA IF NOT EXISTS more;

CREATE TABLE more.bebeze_tmp(
   timestamp        TIMESTAMP NOT NULL PRIMARY KEY
  ,wind_speed      FLOAT NOT NULL
  ,pitch_angle      FLOAT NOT NULL
  ,roto_speed     FLOAT NOT NULL
  ,active_power    FLOAT NOT NULL
  ,cos_nacelle_dir FLOAT NOT NULL  
  ,sin_nacelle_dir FLOAT NOT NULL
  ,cos_wind_dir    FLOAT NOT NULL
  ,sin_wind_dir    FLOAT NOT NULL
  ,nacelle_direction FLOAT NOT NULL
  ,wind_direction FLOAT NOT NULL
);
COPY more.bebeze_tmp(timestamp, wind_speed, pitch_angle,
                 roto_speed, active_power, cos_nacelle_dir, sin_nacelle_dir,
                 cos_wind_dir, sin_wind_dir, nacelle_direction, wind_direction)
    FROM '%path'
    DELIMITER ','
    CSV HEADER;


CREATE TABLE more.bebeze(
   epoch   BIGINT NOT NULL
  ,timestamp  TIMESTAMP NOT NULL
  ,value      FLOAT
  ,id         BIGINT NOT NULL
  ,col        VARCHAR NOT NULL
);

INSERT INTO more.bebeze(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, wind_speed, 0, 'wind_speed' FROM more.bebeze_tmp;

INSERT INTO more.bebeze(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, pitch_angle, 1, 'pitch_angle' FROM more.bebeze_tmp;

INSERT INTO more.bebeze(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, roto_speed, 2, 'roto_speed' FROM more.bebeze_tmp;

INSERT INTO more.bebeze(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, active_power, 3,  'active_power' FROM more.bebeze_tmp;

INSERT INTO more.bebeze(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, cos_nacelle_dir, 4, 'cos_nacelle_dir' FROM more.bebeze_tmp;

INSERT INTO more.bebeze(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, sin_nacelle_dir, 5, 'sin_nacelle_dir' FROM more.bebeze_tmp;

INSERT INTO more.bebeze(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, cos_wind_dir, 6, 'cos_wind_dir' FROM more.bebeze_tmp;

INSERT INTO more.bebeze(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, sin_wind_dir, 7, 'sin_wind_dir' FROM more.bebeze_tmp;

INSERT INTO more.bebeze(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, nacelle_direction, 8, 'nacelle_direction' FROM more.bebeze_tmp;

INSERT INTO more.bebeze(epoch, timestamp, value, id, col)
SELECT date_part('epoch', timestamp) * 1000, timestamp, wind_direction, 9, 'wind_direction' FROM more.bebeze_tmp;


DROP TABLE more.bebeze_tmp;

CREATE INDEX bebeze_index ON more.bebeze(epoch, id);



