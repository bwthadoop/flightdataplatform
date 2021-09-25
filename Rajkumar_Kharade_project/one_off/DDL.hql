

create external table if not exists practice.bwt_airport_data
(
airport_id string,
city string,
state string,
airportname string
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as text file
location '/user/hadoop/flight_data/MySql/${2}_${Date_type}'



Create external table if not exists practice.bwt_flight_data
(
dayofmonth string,
dayofweek string,
carrier string,
Originaurport string,
destairport string,
depdelay string,
arrdelay string,
entrydate string
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as text file
location '/user/hadoop/flight_data/linux/${Date_type}'
