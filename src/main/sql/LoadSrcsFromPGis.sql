drop table if exists receiver_lvl_day_zone, receiver_lvl_evening_zone, receiver_lvl_night_zone;

create table receiver_lvl_day_zone (idrecepteur integer, idsource integer, 
					  att63 double precision, att125 double precision, att250 double precision, att500 double precision, 
					  att1000 double precision, att2000 double precision, att4000 double precision, att8000 double precision);

create table receiver_lvl_evening_zone as select * from receiver_lvl_day_zone;

create table receiver_lvl_night_zone as select * from receiver_lvl_day_zone;
