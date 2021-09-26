create database if not exists flight_data;
use flight_data;

Create external table if not exists flight_data.flights
(
dayofmonth string,
dayofweek string,
carrier string,
Originairport string,
destairport string,
depdelay string,
arrdelay string,
entrydate string
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as text file
location '/user/hadoop/flight_data/${2}_${date_time}'