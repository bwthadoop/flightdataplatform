#####################################################
# Script Name : fullload.sh
# Author      : Govind Pal
# Create Date : 16-09-2021
# Description : Import data into HDFS location from mysql using sqoop import
#               [History load job] on-time job
#####################################################

#hadoop@hadoop:~/flight_project/airport_script$ bash airportscript.sh
#hadoop@hadoop:~/flight_project/airport_script$ bash airportscript.sh
#hadoop@hadoop:~$ hadoop fs -cat /user/flight_data/flight_tb/2021-09-20
##!/bin/bash

# Logs creation

logcreation()
{
LOG_FILE=/home/hadoop/flight_project/log/$0
log_id=$(date +"%F-%T")
timeAndDate=`date`
exec > >(tee ${LOG_FILE}_${log_id}) 2>&1
}
logcreation
echo "Script Name : " $0
echo "Date : " $(date)


########### Validations.##################

if [ -z $1 ] ; then
  echo " [$timeAndDate] ERROR ::Please Provide Database Name!" && exit 1;
fi

if [ -z $2 ] ; then
  echo " [$timeAndDate] ERROR ::Please Provide Table Name!" && exit 1;
fi


echo "Script Name : " $0
echo "Date : " $(date)
echo "database Name : " $1
echo "table Name : " $2



# Date format variable

Date_type=$(date +"%F")


# Importing credential_config file

. /home/hadoop/flight_project/airport_script/credential_config.config

if [ $? -ne 0 ]
then
  echo "ERROR : failed to import credential_config file"
fi
echo "INFO : successfully imported credential_config file"


# Importing job_config file

. /home/hadoop/flight_project/airport_script/job_config.config
if [ $? -ne 0 ]
then
  echo "ERROR : failed to import job_config file"
fi
echo "INFO : successfully imported job_config file"


##-----------mysql insert running job status----------

job_id=$(date '+%Y%m%d%H%M%S')
job_name=$0

mysql -u${user_name} -p${auth_value_path} -e "insert into ${audit_db}.${audit_tb} (job_id,job_name,run_status) values(${job_id},'${job_name}','RUNNING')"

if [ $? -ne 0 ]; then
    echo" [$timeAndDate] ERROR :: failed to insert records in audit table"
    exit 1
fi
echo"[$timeAndDate] INFO :: Successfully inserted records in audit table for job_id:${job_id}"
job_status="COMPLETED"


###############################Sqoop job started####################################################




#$ crontab -e
#0 10 * * 1  .weeklyscript.sh ...for monday jobs need to import in main file
# Sqoop import command

sqoop import --connect "jdbc:mysql://${host_name}:${port_name}/${database}" --username ${user_name} --password ${auth_value_path} --table ${table} --target-dir ${target_bash_path} --delete-target-dir -m4

if [ $? -ne 0 ]
then
  echo "ERROR : failed to import data from mysql to HDFS for table= ${table}"
job_status="FAILED"
fi
echo "INFO : successfully imported data from mysql to HDFS for table= ${table}"

currenttime=$(date '+%Y-%m-%d %H:%M:%S')

#update  status entry in audit table
     mysql -u${user_name} -p${auth_value_path} -e "update ${audit_db}.${audit_tb} set run_status='${job_status}',job_end_time='${currenttime}' where job_id=${job_id}"
    if [ $? -ne 0 ]; then
        echo"[$timeAndDate] ERROR :: failed to update records in audit table for job_id:${job_id}"
        exit 1
    fi
    echo "[$timeAndDate] INFO :: Successfully update record into audit table:${audit_db}.${audit_tb} for job_id =${job_id}"