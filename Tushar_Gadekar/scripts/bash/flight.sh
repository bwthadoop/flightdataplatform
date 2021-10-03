###########################################################
# Script name :Incremental_load.sh                         #
# Autor:       Tushar_Gadekar                               #
# Description: data migration using bash                    #
# creator:   Tushar G
# Created date: 16-Sep-2021
#                                                           #
###########################################################

echo -e "\e[34mWelcome to migration Project_2\e[0m"
##!/bin/bash
date=$(date +"%F")
id=$(date +"%F%T")
Script_name="flight.sh"
log_date=$(date +'%Y%m%d_%H%M%S')
localpath="/home/hadoop/Hadoop_21/Project2/split_data_files/flights.csv"
hdfspath="/user/flight_data/flight/${date}/"
LOGFILE="${id}.${Script_name}.log"

echo "${LOGFILE}:data Migration Bash Run"
#Bring credentiol_config.config file into Script
. config1/project2_credentiol.config

if [ $? -ne 0 ]; then
  echo -e "\e[1m${log_date}:[Error]:${Script_name}:\e[31mFailed to import project2_credentiol.config file"
  exit 1
fi
echo -e "\e[32m${log_date}:[INFO]:${Script_name}:successfully import project2_credentiol.config file\e[0m"

#Bring project_job.config file into Script
. config1/project2_job.config
if [ $? -ne 0 ]; then
  echo -e "\e[1m${log_date}:[Error]:${Script_name}:\e[31mFailed to import  project2_job.config file"
  exit 1
fi
echo -e "\e[32m${log_date}:[INFO]:${Script_name}:successfully import project2_job.config file\e[0m"

##split file using date
awk -F', ' '
  ($8 ==01/01/2020)
  { print}' /home/hadoop/Hadoop_21/Project2/Fulltabledata/flights.csv >${localpath}

#fetching sql password
sql_pwd=$(echo ${password_file/file:\/\//})

job_id=$(date '+%H%M%S')
job_name1="${job_name}_${Script_name}"

# insert record into audit_tb
mysql -u"${username}" -p"$(cat ${sql_pwd})" -e "insert into ${audit_database_name}.${audit_table_name}(job_id,job_name
  ,run_date,run_status) values(${job_id},'${job_name1}',current_date(),'RUNNING')"

## Error Handling
if [ $? -ne 0 ]; then
  echo "${log_date}:Error:${Script_name}:Failed to insert data into audit table"
  exit 1
fi
echo -e "\e[32m${log_date}:[INFO]:${Script_name}:successfully insert data into audit table for job_id=${job_id}\e[0m"

## sqoop import implemantation
echo "${LOGFILE}**************** mkdir ${hdfspath} to hdfs location ******************"

#!/bin/bash
if [ ! -d "$hdfspath" ]; then
  hadoop fs -mkdir -p "$hdfspath"
fi
echo "${LOGFILE}****************copyFromLocal to hdfs location ******************"
hadoop fs -copyFromLocal -f ${localpath} ${hdfspath}

## Error Handling
if [ $? -ne 0 ]; then
  # shellcheck disable=SC2154

  echo "${log_date}:Error:${Script_name}: while copyFromLocal job:${localpath}.${hdfspath}"

  # update record into audit_tb if job failed
  # shellcheck disable=SC2086
  mysql -u${username} -p"$(cat ${sql_pwd})" -e "update ${audit_database_name}.${audit_table_name} set run_status='FAILED'
where job_id=${job_id} and job_name='${job_name1}'"
  if [ $? -ne 0 ]; then
    echo "${log_date}:Error:${Script_name}:Failed to update data into audit table"
    exit 1
  fi
  echo -e "\e[32m${log_date}:[INFO]:${Script_name}:successfully update data into audit table for job_id=${job_id}\e[0m"

  exit 1
fi
echo -e "\e[32m${log_date}:[INFO]:${Script_name}:Successfuly data load in HDFS location for:${Script_name}\e[0m"

hive -e "
        create database IF NOT EXISTS bwt_flightdata_arc;
        use bwt_flightdata_arc;


       CREATE  TABLE IF NOT EXISTS bwt_flightdata_arc.temp_flight (
       DayofMonth	            string,
       DayOfWeek              string,
       Carrier                string,
       OriginAirportID        string,
       DestAirportID          string,
       DepDelay               string,
       ArrDelay               string,
       EntryDate              string)
        row format delimited
        fields terminated by ','
        lines terminated by '\n'
        stored as textfile;


        load data inpath '${hdfspath}' overwrite into table bwt_flightdata_arc.temp_flight;

        CREATE  TABLE IF NOT EXISTS bwt_flightdata_arc.flight
      (DayofMonth	            string,
       DayOfWeek              string,
       Carrier                string,
       OriginAirportID        string,
       DestAirportID          string,
       DepDelay               string,
       ArrDelay               string)
       PARTITIONED BY (EntryDate   string)
        row format delimited
        fields terminated by ','
        lines terminated by '\n'
        stored as textfile
       location '/user/flight_data/flight/';

     set hive.exec.dynamic.partition=true;
     set hive.exec.dynamic.partition.mode=nonstrict;

     insert overwrite table flight partition (EnteryDate) select DayofMonth,DayOfWeek,Carrier,OriginAirportID,DestAirportID,DepDelay,ArrDelay,EntryDate from temp_flight;"

echo Hive create table add partition: EntryDate=${day} ok...

# update succes entery in audit_table
# shellcheck disable=SC2086
mysql -u${username} -p"$(cat ${sql_pwd})" -e "update ${audit_database_name}.${audit_table_name} set run_status='COMPLETED'
where job_id=${job_id} and job_name='${job_name1}'"
# shellcheck disable=SC2181
if [ $? -ne 0 ]; then
  echo "${log_date}:Error:${Script_name}:Failed to update data into audit table"
  exit 1
fi
echo -e "\e[32m${log_date}:[INFO]:${Script_name}:Successfuly updated record to audit_tb=${audit_database_name}.${audit_table_name} for job_id=${job_id}\e[0m"
