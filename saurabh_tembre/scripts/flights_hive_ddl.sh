$1=spark
$2=flights

hive -e "create table flights_temp
         (
         DayofMonth	int,
         DayOfWeek	int,
         Carrier string,
         OriginAirportID int,
         DestAirportID int,
         DepDelay int,
         ArrDelay int,
         EntryDate date
         )
         row format delimited
         fields terminated by ','
         lines terminated by '/n'
         "
hive -e " create external table if not exists ${1}.flights
         (
         airport_id int,
         city string,
         name string
         )
         partitioned by(state string)
         row format delimited
         fields terminated by ','
         lines terminated by '/n'
         location '${target_base_path}/${2}'
         "