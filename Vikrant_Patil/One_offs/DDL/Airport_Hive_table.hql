create database if not exists flight_data;
use flight_data;

create external table if not exists Airport(
airport_id int,
city string,
state string,
name string
)
row format delimited
fields terminated by ','
lines terminated by '\n'
location 'user/hadoop/flight_data/${2}_${date_time}';