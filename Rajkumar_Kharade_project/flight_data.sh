#################################################################################
# Script Name : flight_daily_load.sh                                            #
# Author      : Rajkumar Kharade                                                #
# Create Date : 16-09-2021                                                      #
# Description : Import data into HDFS location from linux using copy from local #
# Bash Run Command : bash flight_data.sh <local path> <HDFS path> <Y/N>         #
#################################################################################

#!/bin/bash

# HDFS path=flight_data/linux

#Local path=/home/hadoop/Spark_project/flight_data.csv

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
echo "Local path : " $1
echo "HDFS path : " $2

#checking parameter
if [[ "$#" = 2 || "$#" = 3 ]];
then
  if [[ ${3} = "Y" || ${3} = "N" ${3} = "y" || ${3} = "n" || ${3} = "" ]];
  then
      echo "INFO:Number of parameter is correct"
  else
     echo "Check 3rd parameter"
     exit 1
  fi
else
  echo "Check the number of Parameter"
    exit 1
fi

# Date format variable

Date_type=$(date +"%F")

. spark_credential_config
if [ $? -ne 0 ]
then
  echo "ERROR : failed to import credential_config file"
  exit 1
fi
echo "INFO : successfully imported credential_config file"

. flight_job_config
if [ $? -ne 0 ]
then
  echo "ERROR : failed to import job_config file"
  exit 1
fi
echo "INFO : successfully imported job_config file"

job_id=$(date '+%H%M%S')
job_name="${job_name}_${table_name}"

# insert record in audit_tb
mysql -u${user_name} -p${auth_value} -e "insert into ${audit_dataset_name}.${audit_table_name}(job_id,job_name,run_date,run_status) values(${job_id},'${job_name}',current_date,'RUNNING')"

if [ $? -ne 0 ]
then
  echo "ERROR : failed to insert record into flight_audit_tb"
  exit 1
fi
echo "INFO : successfully inserted record into flight_audit_tb for job_id : ${job_id}"

# Create directory for current date

hadoop fs -mkdir -p ${2}/${Date_type};

if [ $? -ne 0 ]
then
  echo "ERROR : failed to create directory for date : ${Date_type}"
  exit 1
fi
echo "INFO : successfully created directory for date : ${Date_type}"

# Copy data for perticuler date from linux to HDFS

hadoop fs -copyFromLocal ${1} ${2}/${Date_type};

# To resolve the file already exist error, take user input to overwrite the data

   userinput=${3}

  if [ "$userinput" == "N" ];
  then

   hadoop fs -copyFromLocal ${1} ${2}/${Date_type};

   if [ $? -ne 0 ]
   then

   # update record in audit table
         mysql -u${user_name} -p${auth_value} -e "update ${audit_dataset_name}.${audit_table_name} set run_status='failed' where job_id=${job_id} and job_name='${job_name}'"
         if [ $? -ne 0 ]
         then
         echo "ERROR : failed to update record into flight_audit_tb for job_id : ${job_id}"
         exit 1
         fi
         echo "INFO : successfully updated record into flight_audit_tb for job_id : ${job_id}"

   exit 1
  fi
fi

  if [ "$userinput" == "Y" ];
      then
         hadoop fs -copyFromLocal -f ${1} ${2}/${Date_type};

         echo "Info: Successfully overwrite the data"
  fi

# update success entry in audit_tb

mysql -u${user_name} -p${auth_value} -e "update ${audit_dataset_name}.${audit_table_name} set run_status='COMPLETED' where job_id=${job_id} and job_name='${job_name}'"

if [ $? -ne 0 ]
then
  echo "ERROR : failed to update record into flight_audit_tb for job_id : ${job_id}"
fi
echo "INFO : successfully updated record into flight_audit_tb for job_id : ${job_id}"
