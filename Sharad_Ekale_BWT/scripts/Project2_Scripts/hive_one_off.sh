########################################################
# Script name : Hive_table_creation                    #
# Author      : Sharad Ekale                           #
# Create date : 2021-09-19                             #
# Description : Creating hive external table local     #
#               location bases                         #
########################################################

#Create datetime var for log file and write log into file.
date_time_log=$(date +"%Y/%m/%d %H%M%S")
log_datetime=$(date +%Y%m%d%H%M%S)
bash_name=$(basename  "$0" | cut -f 1 -d '.')
log_location="${target_basepath}"${bash_name}"_"${log_datetime}.log
exec 2>>${log_location}

#creating database
hive -e "CREATE DATABASE IF NOT EXISTS bwt_flightdata"
if [ $? -ne 0 ]; then
  echo ${date_time_log} "ERROR:" "Failed to Create Database" ${bash_name} >>"${log_location}"
  exit 1
  fi
 echo ${date_time_log} "INFO:" "Datebase Created Successfully" ${bash_name} >>"${log_location}"

#Creating Hive Tables
hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS bwt_flightdata.airports(
airport_id int,
city STRING,
state STRING,
name STRING
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile
LOCATION '/user/cloudera/flightdata/airports/'"

if [ $? -ne 0 ]; then
  echo ${date_time_log} "ERROR:" "Failed to Create Table(airports)" ${bash_name} >>"${log_location}"
  exit 1
  fi
 echo ${date_time_log} "INFO:" "Table(airports) Created Successfully" ${bash_name} >>"${log_location}"

#Creating Hive Tables
hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS bwt_flightdata.flights(
DayofMonth INT,
DayOfWeek INT,
Carrier STRING,
OriginAirportID BIGINT,
DestAirportID BIGINT,
DepDelay INT,
ArrDelay INT
)
partitioned by (EntryDate DATE)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile
LOCATION '/user/cloudera/flightdata/flight/'"

if [ $? -ne 0 ]; then
  echo ${date_time_log} "ERROR:" "Failed to Create Table(flights)" ${bash_name} >>"${log_location}"
  exit 1
  fi
 echo ${date_time_log} "INFO:" "Table(flights) Created Successfully" ${bash_name} >>"${log_location}"