###############################################################################
# Project Name: Flight Data Migration                                         #
# Script Name : Load data in HDFS location                                    #
# Author      : Saurabh Tembre                                                #
# Create date : 2021-09-27                                                    #
# Description : Loading Data from Local to into Hive through HDFS             #
###############################################################################

#If condition for credential.config file
. config/credentials.config
if [ $? -ne 0 ]
then
  echo ${ERROR} "Failed to Import Credential.config File"
fi
  echo ${INFO} "Credential.config File Imported Successfully"

#If condition for job.config file
. config/jobs.config
if [ $? -ne 0 ]
then
  echo ${ERROR} "Failed to Import Job.config File"
fi
  echo ${INFO} "Job.config File Imported Successfully"

#insert record into audit table
$1-spark
mysql --defaults-extra-file=${mysql_password} -e "insert into ${1}.${flight_audit_table}(job_id,job_name,job_status,run_date) values (${job_id},'${job_name}','RUNNING',current_date)"

if [ $? -ne 0 ]
then
  echo -e ${ERROR} " Failed to Insert Data into Audit Table"
  exit 1
fi
  echo -e ${INFO}" Inserted Record into ${audit_table} for ${job_name}"

hadoop fs -copyFromLocal ${src_path} ${target_base_path}/flights.csv

if [ $? -ne 0 ]
then
echo -e ${ERROR} " Copying File Failed"

#updating table if job fail
mysql --defaults-extra-file=${mysql_password} -e "update ${1}.${flight_audit_table} set run_status='FAILED' where job_id=${job_id} and job_name='${job_name}'"

if [ $? -ne 0 ]
  then
    echo -e ${ERROR} " Failed to Update Audit Table"
    exit 1
  fi
    echo -e ${INFO}" Updated Record into ${flight_audit_table} for ${job_name}"
    exit 1
  fi
    echo -e ${INFO}" Copying Job Successful"

#hive DDL commands for flight data
. flights_ddl.sh