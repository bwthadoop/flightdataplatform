############################################################################
#script name: Weeklyload.sh                                              
#author : Shubham Bora               
#Project: flight_data
#job:full load weekly data
############################################################################


###########Declaration of variables.##################

#!/bin/bash

# Logs creation

script_path="$(dirname "$( realpath ${BASH_SOURCE[0]} )" )"

logcreation()
{
LOG_FILE=${script_path}/log/$0
log_id=$(date +"%F-%T")
timeAndDate=`date`
exec > >(tee ${LOG_FILE}_${log_id}) 2>&1

}
logcreation



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

###################First we check the credential ########################

. ${script_path}/credential.config

if [ $? -ne 0 ]; then

echo "[$timeAndDate] ERROR :: failed to import credential config file"
exit 1

fi
echo "[$timeAndDate] INFO ::successfully imported credential config file"

###################check job_config details###################################

. ${script_path}/jobconfig.config

if [ $? -ne 0 ]; then

  echo "[$timeAndDate] ERROR :: failed to import job_config.config file"
exit 1
fi
echo "[$timeAndDate] INFO ::successfully imported job config file "


##-----------mysql insert running job status----------

job_id=$(date '+%Y%m%d%H%M%S')
job_name=$0

mysql -u${username} -p${auth_value} -e "insert into ${audit_db}.${audit_tb} (job_id, job_name,run_date,run_status) values(${job_id},'${job_name}','RUNNING')"

if [ $? -ne 0 ]; then
    echo" [$timeAndDate] ERROR :: failed to insert records in audit table"
    exit 1
fi
echo"[$timeAndDate] INFO :: Successfully inserted records in audit table for job_id:${job_id}"
job_status="COMPLETED"

#------------------------
###############################Sqoop job started####################################################




#$ crontab -e
#0 10 * * 1  .weeklyscript.sh ...for monday jobs need to import in main file

sqoop import --connect "jdbc:mysql://${host_name}:${port_name}/${database_name}" --username ${user_name} --password ${auth_value} --table ${table_name} --target-dir ${target_bash_path}/${table_name} --delete-target-dir -m 4

if [ $? -ne 0 ]
then
  echo "ERROR : failed to import data from mysql to HDFS for table= ${table_name}"
	job_status="FAILED"

fi
echo "INFO : successfully imported data from mysql to HDFS for table= ${table_name}"



currenttime=$(date '+%Y-%m-%d %H:%M:%S')

#update  status entry in audit table
     mysql -u${username} -p${auth_value} -e "update ${audit_db}.${audit_tb} set run_status='${job_status}',job_end_time='${currenttime}' where job_id=${job_id}"
    if [ $? -ne 0 ]; then
        echo"[$timeAndDate] ERROR :: failed to update records in audit table for job_id:${job_id}"
        exit 1
    fi
    echo "[$timeAndDate] INFO :: Successfully update record into audit table:${audit_db}.${audit_tb} for job_id =${job_id}"



