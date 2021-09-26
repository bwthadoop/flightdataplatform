######################################################################
# Script name : hive HQL                                             #
# Author      : Rucha Vanjari                                        #
# Create date : 2021-09-19                                           #
# Description : Create hive external table                           #
#                                                                    #
######################################################################

#Create datetime var for log file and write log into file.

date_time=$(date +"%Y/%m/%d %H%M%S")
log_datetime=$(date +%Y%m%d%H%M%S)
bash_name=$(basename  "$0" | cut -f 1 -d '.')
log_location="${target_basepath}"${bash_name}"_"${log_datetime}.log
exec 2>>${log_location}

#creating database.

hive -e "CREATE DATABASE IF NOT EXISTS bwt_flightdata"

if [ $? -ne 0 ]; then
  echo ${date_time} "ERROR:" "Failed to Create Database" ${bash_name} >>"${log_location}"
  exit 1
  fi
 echo ${date_time} "INFO:" "Datebase Created Successfully" ${bash_name} >>"${log_location}"

#Creating Hive Tables.

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
LOCATION '${target_basepath}'";

if [ $? -ne 0 ]; then
  echo ${date_time} "ERROR:" "Failed to Create Table(airports)" ${bash_name} >>"${log_location}"
  exit 1
  fi
 echo ${date_time} "INFO:" "Table(airports) Created Successfully" ${bash_name} >>"${log_location}"

#Creating Hive Tables for flights data.

hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS bwt_flightdata.flights(
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
stored as textfile
location '${target_basepath}';"
if [ $? -ne 0 ]; then
  echo ${date_time} "ERROR:" "Failed to Create Table(flights)" ${bash_name} >>"${log_location}"
  exit 1
  fi
 echo ${date_time} "INFO:" "Table(flights) Created Successfully" ${bash_name} >>"${log_location}"
