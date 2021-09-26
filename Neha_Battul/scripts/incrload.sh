######################################################################
#script name: incrementalload.sh                                     #
#Author     : Miss Neha                                              #
#Create date: 2021-09-17                                             #
#description: Import data into hdfs from linux using copy from local #
#               [daily_load job]                                     #
######################################################################


#!/bin/bash

#variable creation

DATETIME=$(date '+%y-%m-%d_%H-%M-%S')
DATE=$(date -I)
source=/home/hadoop/project2/flights.csv
destination=/project2/bwt_flightdata/Linux/${Date}/${file_name}

#log_file creation

log_file=/home/hadoop/project2/Log_File/${0}${DATETIME}.log

exec > >(tee -a $log_file)
exec 2>&1

if [ $? -ne  0 ]; then
echo "Date: ${DATETIME}"

echo "Script name: ${0}"

echo "${DATETIME} Error : log file can not be imported"
exit 1
fi
echo "Date: ${DATETIME}"

echo "Script name: ${0}"

echo "${DATETIME} INFO : log file is imported successfully"


# Import configuration files

. /home/hadoop/project2/config/credential_config.config
if [ $? -ne 0 ]; then
    echo "${DATETIME} ERROR : failed to import credential_config.config"
    exit 1
fi
    echo "${DATETIME} INFO : credential_config.config Successfully imported"

. /home/hadoop/project2/config/job_config.config
if [ $? -ne 0 ]; then
    echo "${DATETIME} ERROR : failed to import job_config.config"
    exit 1
fi
    echo "${DATETIME} INFO : job_config.config Successfully imported"

job_id=$(date '+%H%M%S')
job_name="${job_name}_${table_name}"

#insert record into audit table

mysql -u${username} -p${auth_value} -e "insert into ${audit_dataset_name}.${audit_table_name} (job_id,job_name,status,job_start_time) values(${job_id},'${job_name}','RUNNING',now())"

if [ $? -ne 0 ]; then
    echo "${DATETIME} ERROR : failed to insert records in audit table for job_id:${job_id}"
    exit 1
fi
echo "${DATETIME} INFO : Successfully inserted records in audit table for job_id:${job_id}"


#checking if output directory exists or not on HDFS!

hadoop fs -test -d ~/"${destination}_${DATETIME}"
if [ $? == 0 ]; then
   hadoop fs -rm -r ~/"${destination}"hadoop fs -test -d ~/
else
    echo ${DATETIME} "Output file doesn't exist and will be created when hadoop runs"
fi

#creating a directory
directory=$(hadoop fs  -mkdir -p ${destination}_${DATETIME}.csv)

if [ $? -ne 0 ]
then
echo "ERROR : failed to create directory for date : ${DATETIME}"
fi
echo "INFO : successfully created directory for date : ${DATETIME}"


#copying data from local to hdfs
copydata=$(hadoop fs -copyFromLocal -p ${source} ${destination}_${DATETIME}.csv)

    #if sqoop import job fail then update audit table status failed
    #update record in audit_table

mysql -u${username} -p${auth_value} -e "update ${audit_dataset_name}.${audit_table_name} set status='FAILED' where job_id=${job_id} and job_name='${job_name}'"
    if [$? -ne 0]; then
        echo "${DATETIME} ERROR : failed to update records in audit table for job_id:${job_id}"
        exit 1
    fi
    echo "${DATETIME} INFO : Successfully updated records in audit table for job_id:${job_id}"
    exit 1
fi
  echo ${DATETIME} "INFO: Successfully copy data from local ${source} to hdfs ${destination}"

#update status entry in audit table
     mysql -u${username} -p${auth_value} -e "update ${audit_dataset_name}.${audit_table_name} set status='COMPLETED' where job_id=${job_id} and job_name='${job_name}'"
    if [ $? -ne 0 ]; then
        echo "${DATETIME} ERROR : failed to update records in audit table for job_id:${job_id}"
        exit 1
    fi
    echo "${DATETIME} INFO : Successfully update record into audit table:${audit_dataset_name}.${audit_table_name} for job_id = ${job_id} "









