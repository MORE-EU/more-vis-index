DROP TABLE IF EXISTS more.bebeze;

DROP SCHEMA IF EXISTS more;

CREATE SCHEMA more;

CREATE TABLE more.bebeze_tmp(
   datetime        TIMESTAMP NOT NULL PRIMARY KEY
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

COPY more.bebeze_tmp(datetime, wind_speed, pitch_angle,
roto_speed, active_power, cos_nacelle_dir, sin_nacelle_dir,
cos_wind_dir, sin_wind_dir, nacelle_direction, wind_direction)
FROM '/opt/more-workspace/BEBEZE/bbz1.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE more.bebeze(
   datetime        TIMESTAMP NOT NULL
  ,value      FLOAT
  ,id         INT NOT NULL
);


INSERT INTO more.bebeze(datetime, value, id)
SELECT datetime, wind_speed, 1 FROM more.bebeze_tmp;

INSERT INTO more.bebeze(datetime, value, id)
SELECT datetime, pitch_angle, 2 FROM more.bebeze_tmp;

INSERT INTO more.bebeze(datetime, value, id)
SELECT datetime, roto_speed, 3 FROM more.bebeze_tmp;

INSERT INTO more.bebeze(datetime, value, id)
SELECT datetime, active_power, 4 FROM more.bebeze_tmp;


DROP TABLE more.bebeze_tmp;

CREATE INDEX bebeze_index ON more.bebeze(id, datetime);



