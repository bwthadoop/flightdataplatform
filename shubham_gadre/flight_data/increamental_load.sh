#######################################################################################
# Script Name  : increamental_load.sh                                                 #
# Author       : Shubham Gadre                                                        #
# Date created : 2021-09-19                                                           #
# Discription  : copy data to hdfs from local                                         #
#######################################################################################

. /home/hadoop/flight_data/job.config
# Date format varible
dateval=$(date +"%F")
// **********************************************************************************************
hadoop fs -mkdir -p ${hadoop_arc_loc}/${dateval}
if [ $? -ne 0 ];
then
  echo "ERROR: Create archive directory failed"
  exit 1
else
  echo "INFO: Create archive directory successful"
fi

##-----------mysql insert running job status----------
echo "Script Name : " $0
job_id=$(date '%H%M%S')
job_name=$0

mysql -u${username} -p${auth_value} -e "insert into ${audit_db}.${audit_tb} (job_id, job_name,run_status) values(${job_id},'${job_name}','RUNNING')"

if [ $? -ne 0 ]; then
    echo" [$timeAndDate] ERROR :: failed to insert records in audit table"
    exit 1
fi
echo"[$timeAndDate] INFO :: Successfully inserted records in audit table for job_id:${job_id}"

// *************************************************************************************************************
hadoop fs -copyFromLocal ${linux_loc}/${input_file} ${hadoop_arc_loc}/${dateval}
if [ $? -ne 0 ];
then
  echo "ERROR: File already exists"
echo "Do you want overwrite the existing file"
read user_input
if [ $user_input == "Yes" -o $user_input == "yes" -o $user_input == "y" ]
then
hadoop fs -copyFromLocal -f ${linux_loc}/${input_file} ${hadoop_arc_loc}/${dateval}
echo "INFO: File Overwritten Successfully"
fi
else
  echo "INFO: Create archive directory successful"
fi
// *********************************************************************************************************************
#update  status entry in audit table

currenttime=$(date '+%Y-%m-%d %H:%M:%S')

mysql -u${username} -p${auth_value} -e "update ${audit_db}.${audit_tb} set run_status='${job_status}',job_end_time='${currenttime}' where job_id=${job_id}"

    if [ $? -ne 0 ]; then
        echo"[$timeAndDate] ERROR :: failed to update records in audit table for job_id:${job_id}"
        exit 1
    fi
    echo "[$timeAndDate] INFO :: Successfully update record into audit table:${audit_db}.${audit_tb} for job_id =${job_id}"