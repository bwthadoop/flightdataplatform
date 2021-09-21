create database if not exists flight_data;

use flight_data;



CREATE TABLE if not exists Airport (
    airport_id int,
    city varchar(50),
    state varchar(50),
    name varchar(250),
	primary key (airport_id)
);

