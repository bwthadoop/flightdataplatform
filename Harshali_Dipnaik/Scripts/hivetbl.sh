#########################################################
#script name : hivetbl.sh                             #
#Author name: Harshali Dipnaik                          #
#Date:20-09-2021                                        #
#Description : Hive Table Creation                      #
#run_command syntax : bash script_name.sh  #
#########################################################

#Create datetime var for log file and write log into file.
current_date=$(date +"%Y_%m_%d")
date_time_log=$(date +"%Y/%m/%d %H%M%S")

#Get a bash name
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
hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS airporttbl(
airport_id STRING,
city STRING,
state STRING,
name STRING
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile
LOCATION '/user/airportdata/tblairport'"

if [ $? -ne 0 ]; then
  echo ${date_time_log} "ERROR:" "Failed to Create Airport Table" ${bash_name} >>"${log_location}"
  exit 1
  fi
 echo ${date_time_log} "INFO:" "Airport Table  Created Successfully" ${bash_name} >>"${log_location}"

#Creating Hive Tables
hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS flighttbl(
DayofMonth STRING,
DayOfWeek STRING,
Carrier STRING,
OriginAirportID STRING,
DestAirportID STRING,
DepDelay STRING,
ArrDelay STRING
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile
LOCATION '/user/hadoop/flightdata/${current_date}'"

if [ $? -ne 0 ]; then
  echo ${date_time_log} "ERROR:" "Failed to Create Flight Table" ${bash_name} >>"${log_location}"
  exit 1
  fi
 echo ${date_time_log} "INFO:" "Flight Table Created Successfully" ${bash_name} >>"${log_location}"

