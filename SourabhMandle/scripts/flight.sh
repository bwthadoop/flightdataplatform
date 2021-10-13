##################################################################################
# Script name    :Flight.sh                                                      #
# Author         :Sourabh Mandle                                                  #
# Description    :Flight data migration bash                                      #
# creator        :Sourabh Mandle                                                  #
# Created date   :20-sept-2021                                                    #
#                                                                                 #
###################################################################################


#log_date=$(date '+%Y-%m-%d %H:%M:%S')
#log=/home/hadoop/sourabh1/project2/bash/logfiles$0_${log_date}.log

echo "-----Importing Credential file-------" #>>$log

. config_files/credential_config.config

if [ $? -ne 0 ]; then
    echo "Error:failed to import credential_config.config file" #>>$log
    exit 1
fi

echo "INFO:successfully import credential_config.config file" #>>$log

echo "-------Importing Job config file------" #>>$log

. config_files/project_job.config

if [ $? -ne 0 ]; then
    echo "Error:failed to import project_job.config file" #>>$log
    exit 1
fi
echo "INFO:successfully imported project_job.config file" #>>$log

#creating the data for log file

# shellcheck disable=SC2046
echo "Date:" $(date "+%Y-%m-%d %H:%M:%S") #>>$log
# shellcheck disable=SC2154
echo "Host:" "${hostname}" #>>$log
# shellcheck disable=SC2154
echo "Port:" "${port_name}" #>>$log
# shellcheck disable=SC2154
echo "Database:" "${database_name}" #>>$log
echo "Table:" $2 #>>$log

job_id=$(date '+%H%M%S')
job_name="${job_name1}_${table_name}"

#insert record in audit table

# shellcheck disable=SC2154
mysql -u"${username}" -p"${password}" -e "insert into ${audit_dataset_name}.${audit_table_name}(job_id,job_name,run_date,run_status) values(${job_id},'${job_name2}',current_date(),'RUNNING')" #>>$log

if [ $? -ne 0 ]; then
    echo "Error:failed to insert record into an audit table for job_id : ${job_id}" #>>$log
    exit 1
fi
echo "INFO:successfully inserted record into an audit table for job_id : ${job_id}" #>>$log

#Running sqoop job
#exec &> >(tee -a "$log")
sqoop import --connect "jdbc:mysql://${hostname}:${port}/${database_name}" --username "${username}" --password password --table "$1" --target-dir "${target_dir_path2}"/"$1" --delete-target-dir --split-by "$2"



#validate sqoop job
if [ $? -ne 0 ]; then
    echo " ERROR: Sqoop import job failure" #>>$log
      exit 1
    fi
    echo "INFO:successfully run sqoop job" #>>$log

#update_record into audit table

mysql -u${username} -p${password} -e "update ${audit_dataset_name}.${audit_table_name} set run_status='FAILED' where job_id=${job_id} and job_name='${job_name1}'" #>>$log

    if [ $? -ne 0 ]; then
        echo "Error:failed to update record into an audit table for job_id : ${job_id}" #>>$log
        exit 1

    echo "INFO:successfully updated record into an audit table for job_id : ${job_id}" #>>$log

    exit 1
else
    echo " INFO: sqoop import successfully run" #>>$log
fi

#retrive maximum update timestamp value

last_incremental_val=$(mysql -u${username} -p${password} -e "select max(update_datetime) from ${audit_dataset_name}.${audit_table_name}")

if [ $? -ne 0 ]; then
    echo "Error:failed to get max_update date value from mysql table : $1.$2" #>>$log
    exit 1
fi


max_update_date=$(echo ${last_incremental_val} | cut -d' ' -f2)
max_update_time=$(echo ${last_incremental_val} | cut -d' ' -f3)

max_update_datetime="${max_update_date}${max_update_time}"

if [ $? -ne 0 ]; then
    echo "Error:failed to retrive max_update_datetime value" #>>$log
    exit 1
fi


echo "INFO: current job max datetime value : ${max_update_datetime}" #>>$log

#update success entry in audit table

mysql -u${username} -p${password} -e "update ${audit_dataset_name}.${audit_table_name} set run_status='COMPLETED', last_job_value='${max_update_datetime}' where job_id=${job_id} and job_name='${job_name2}'" #>>$log

if [ $? -ne 0 ]; then
    echo "Error:failed to update record into an audit table for job_id : ${job_id}" #>>$log
    exit 1
fi
echo "INFO: Successfully updated record into audit table : ${audit_dataset_name}.${audit_table_name} for job_id=${job_id}" #>>$log

hive -e"
          CREATE DATABASE IF NOT EXISTS bwt_flightdata_arc;
          use bwt_flightdata_arc;

          CREATE TABLE IF NOT EXISTS bwt_flightdata_arc.flight
          (DayofMonth string,
          DayofWeek string,
          Carrier string,
          OriginAirportID bigint,
          DestAirportID bigint,
          DepDelay string,
          ArrDelay string,
          EntryDate string)
          row format delimited
          fields terminated by ','
          lines terminated by '\n'
          stored as textfile;

          LOAD DATA INPATH '${target_dir_path2}/$1/*' OVERWRITE INTO TABLE bwt_flightdata_arc.flight;"

           if [ $? -ne 0 ]; then
              echo "Error:failed to load data into hive table : bwt_flighttdata_arc" #>>$log
              exit 1
          fi

          echo "INFO: Successfully loaded the data into hive table : bwt_flighttdata_arc" #>>$log

hive -e"  use bwt_flightdata_arc;
          set hive.exec.dynamic.partition=true;
          set hive.exec.dynamic.partition.mode=nonstrict;

          CREATE TABLE IF NOT EXISTS flight_partition.flight
          (DayofMonth string,
           DayofWeek string,
           Carrier string,
           OriginAirportID bigint,
           DestAirportID bigint,
           DepDelay string,
           ArrDelay string)
           PARTITIONED BY (EntryDate string)
           row format delimited
           fields terminated by ','
           lines terminated by '\n'
           stored as textfile
           location '${target_dir_path2}/$1';

           INSERT OVERWRITE TABLE flight_partition select DayofMonth,DayofWeek,Carrier,OriginAirportID,DestAirportID,DepDelay,ArrDelay,EntryDate from flight_partition.flight;

           "

if [ $? -ne 0 ]; then
    echo "Error:failed to load data into hive table : bwt_flighttdata_arc" #>>$log
    exit 1
fi

echo "INFO: Successfully loaded the data into hive table : bwt_flighttdata_arc" #>>$log
