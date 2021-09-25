###############################################################################
# Script Name : airport_full_load.sh                                          #
# Author      : Rajkumar Kharade                                              #
# Create Date : 16-09-2021                                                    #
# Description : Import data into HDFS location from mysql using sqoop import  #
#               [History load job] on-time job                                #
# Bash Run Command : bash airport_daily_load.sh <DB_name> <Table_name>        #
###############################################################################

#!/bin/bash

# Logs creation

logcreation()
{
LOG_FILE=/home/hadoop/Spark_project/Bash_Script/Spark_project_logs/$0
log_id=$(date +"%F-%T")
exec > >(tee ${LOG_FILE}_${log_id}) 2>&1
}
logcreation
echo "Script Name : " $0
echo "Date : " $(date)
echo "database Name : " $1
echo "table Name : " $2

#checking parameter
if [[ "$#" = 2 || "$#" = 3 ]];
then
  echo "INFO:Number of parameter is correct"
else
  echo "Check the number of Parameter"
    exit 1
fi

# Date format variable

Date_type=$(date +"%F")

# Importing credential_config file

. spark_credential_config
if [ $? -ne 0 ]
then
  echo "ERROR : failed to import credential_config file"
fi
echo "INFO : successfully imported credential_config file"


# Importing job_config file

. spark_job_config
if [ $? -ne 0 ]
then
  echo "ERROR : failed to import job_config file"
fi
echo "INFO : successfully imported job_config file"

job_id=$(date '+%H%M%S')
job_name="${job_name}_${table_name}"

# insert record in audit_tb
mysql -u${user_name} -p${auth_value} -e "insert into ${audit_dataset_name}.${audit_table_name}(job_id,job_name,run_date,run_status) values(${job_id},'${job_name}',current_date,'RUNNING')"

if [ $? -ne 0 ]
then
  echo "ERROR : failed to insert record into flight_audit_tb"
fi
echo "INFO : successfully inserted record into flight_audit_tb for job_id : ${job_id}"


# Sqoop import command

sqoop import --connect "jdbc:mysql://${host_name}:${port_name}/${1}" --username ${user_name} --password-file ${auth_value_path} --table ${2} --target-dir ${target_base_path}/${2}_${Date_type} --delete-target-dir

if [ $? -ne 0 ]
then
  echo "ERROR : failed to import data from mysql to HDFS for table= ${2}"

  # update record in audit table
    mysql -u${user_name} -p${auth_value} -e "update ${audit_dataset_name}.${audit_table_name} set run_status='failed' where job_id=${job_id} and job_name='${job_name}'"
    if [ $? -ne 0 ]
    then
    echo "ERROR : failed to update record into flight_audit_tb for job_id : ${job_id}"
    fi
    echo "INFO : successfully updated record into flight_audit_tb for job_id : ${job_id}"

fi
echo "INFO : successfully imported data from mysql to HDFS for table= ${2}"

# update success entry in audit_tb

mysql -u${user_name} -p${auth_value} -e "update ${audit_dataset_name}.${audit_table_name} set run_status='COMPLETED' where job_id=${job_id} and job_name='${job_name}'"

if [ $? -ne 0 ]
then
  echo "ERROR : failed to update record into flight_audit_tb for job_id : ${job_id}"
fi
echo "INFO : successfully updated record into flight_audit_tb for job_id : ${job_id}"

# Creating airport_data and flight_data external tables using .hql file

#hive -f DDL.hql
#
#if [ $? -ne 0 ]
#then
#  echo "ERROR : failed to create hive tables"
#fi
#echo "INFO : successfully created hive tables"