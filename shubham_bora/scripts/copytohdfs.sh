############################################################################
#script name: flight_daily_load.sh                                              
#author : Shubham Bora               
#Project: flight_data
#job: to copy data from local machine to HDFS
############################################################################

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

################################################
echo "Script Name : " $0
echo "Date : " [$timeAndDate]

# Date format varible

dateval=$(date +"%F")    ##to ask sir what should be date:: the current date or to take date as input which will also be filename(01/01/20)



###################First we check the credential ########################

. ${script_path}/config/credential.config

if [ $? -ne 0 ]; then

echo "[$timeAndDate] ERROR :: failed to import credential config file"
exit 1

fi
echo "[$timeAndDate] INFO :: Successfully imported credential config file"


###################check job_config details###################################

. ${script_path}/config/jobconfig.config

if [ $? -ne 0 ]; then

  echo "[$timeAndDate] ERROR :: failed to import job_config.config file"
exit 1
fi
echo "[$timeAndDate] INFO :: successfully imported job config file "

###################################################################################

# Create folder

hadoop fs -mkdir -p /user/flight_data/flight_tb/${dateval};

if [ $? -ne 0 ]
then
  echo "[$timeAndDate] ERROR :: failed to create folder for date : ${dateval}"
fi
echo "[$timeAndDate] INFO :: successfully created folder for date : ${dateval}"


##-----------mysql insert running job status----------

job_id=$(date '+%Y%m%d%H%M%S')
job_name=$0

mysql -u${username} -p${auth_value} -e "insert into ${audit_db}.${audit_tb} (job_id, job_name,run_status) values(${job_id},'${job_name}','RUNNING')"

if [ $? -ne 0 ]; then
    echo" [$timeAndDate] ERROR :: failed to insert records in audit table"
    exit 1
fi
echo"[$timeAndDate] INFO :: Successfully inserted records in audit table for job_id:${job_id}"


#------------------------

# Copy data for perticuler date from linux to HDFS

FILE=${hdfspath}/${dateval}   #filepath
if [ -f "$FILE" ]; then
    echo "[$timeAndDate] ERROR ::this $FILE already exist"
	job_status="FAILED"

else

hadoop fs -copyFromLocal ${localpath}/01-01-2020.csv ${hdfspath}/${dateval};

	if [ $? -ne 0 ];
	then
	  echo " [$timeAndDate] ERROR ::Failed to copy the file from local TO HDFS"
		job_status="FAILED"

	else

	echo " [$timeAndDate] INFO :: Successfully copied file from Local to HDFS  : ${dateval}"
	job_status="COMPLETED"

	fi
fi

#update  status entry in audit table
currenttime=$(date '+%Y-%m-%d %H:%M:%S')
     mysql -u${username} -p${auth_value} -e "update ${audit_db}.${audit_tb} set run_status='${job_status}',job_end_time='${currenttime}' where job_id=${job_id}"
    if [ $? -ne 0 ]; then
        echo"[$timeAndDate] ERROR :: failed to update records in audit table for job_id:${job_id}"
        exit 1
    fi
    echo "[$timeAndDate] INFO :: Successfully update record into audit table:${audit_db}.${audit_tb} for job_id =${job_id}"
echo "----------------------------------------------"