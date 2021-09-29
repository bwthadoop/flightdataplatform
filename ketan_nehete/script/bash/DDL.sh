################################################################################################
# Script name             : DDL.sh                                                             #
# Author                  : Ketan Nehete                                                       #
# Create date             : 2021-09-17                                                         #
# Description             : Creating hive tables                                               #
# Run command             : bash DDL.sh                                                        #
# Run command Syntax      : bash <Script_Name>                                                 #
# Example                 : bash DDL                                                           #
################################################################################################

logcreation() {
    LOG_FILE1=/home/hadoop/ProjectLogFile/sparkprojectlogs/$0
    log_date=$(date '+%Y%m%d_%H%M%S')
    exec > >(tee ${LOG_FILE1}_${log_date}.log) 2>&1
}
logcreation

#creating databases
hive -e "create database if not exists bwt_flightdata_arc"

#creating table bwt_airport_data
hive -e "create external table if not exists bwt_flightdata_arc.bwt_airport_data
(
airport_id string,
city string,
state string,
airportname string
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile
location '/user/hadoop/airport/mysql';"



#creating table for bwt_airport_data
hive -e "Create external table if not exists bwt_flightdata_arc.bwt_flight_data
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
stored as textfile
location '/user/hadoop/flightdata/linux';"