###################################################################
#script_name=flightbash.sh                                        #
#Author=snehal sargar                                             #
#Create_date=17-09-2021                                           #
#Description=Import data into HDFS from linux using copyFromLocal #
###################################################################

#directory with date in hdfs
#file with date
#filedate=$(date +"%y%m%d%H%M%S")
date=$(date +"%Y-%m-%d")

# Take database_name and table_name as arguments
#log_path=/home/hadoop/project2/log_files

log_date=$(date +"%Y-%m-%d %H:%M:%S")

#bash_file=$(basename -- "$0")

bashfile_name=$(basename -- "$0" | cut -f 1 -d '.')

#location of logfile
logfile_location="/home/hadoop/project2/log_files/"${log_date}.txt

exec > >(tee -a "${logfile_location}")
exec 2>&1


#check credential config file
. credential.config
if [ $? -ne 0 ]
then
echo ${log_date_time} ${bashfile_name} "ERROR: failed to import credential.config file" >>"${logfile_location}"
exit 1
fi
echo ${log_date_time} ${bashfile_name}"INFO: successfully import credential.config file" >>"${logfile_location}"

#check job config file
. job.config
if [ $? -ne 0 ]
then
echo  ${log_date_time} ${bashfile_name}"ERROR: failed to import job.config file" >>"${logfile_location}"
exit 1
fi
echo ${log_date_time} ${bashfile_name}"INFO: successfully import job.config file" >>"${logfile_location}"


#audit table data insertion
job_id=$(date '+%y%m%d%H%M%S')
job_name=${bashfile_name}

#insert data in audit table
#mysql -u${user_name} -p${mysql_password} -e "insert into ${audit_dataset_name}.${audit_table_name}(job_id,job_name,run_date,run_status)values(${job_id},'${job_name}',current_date(),'RUNNING')"

mysql -u${user_name} -p${mysql_password} -e "insert into ${audit_dataset_name}.${audit_table_name}(job_id,job_name,status,job_start_time)values(${job_id},'${job_name}','RUNNING',current_timestamp())"

if [ $? -ne 0 ]; then
    echo "ERROR: failed to insert record into an audit table"
    exit 1
fi
echo "INFO: successfully inserted record into an audit table for job_id:${job_id}"


# copy datafile from local to hdfs
hadoop fs -mkdir -p ${mkdir_path}/${date}

hadoop fs -copyFromLocal -f ${local_path} ${mkdir_path}/${date}
if [$? -ne 0 ]; then
 echo "ERROR: failed to copy data from local to hdfs" >>"${logfile_location}"

exit 1
fi
hadoop fs -copyFromLocal -f ${local_path} ${mkdir_path}/${date}
echo "INFO: successfully copied data from local to hdfs" >>"${logfile_location}"


#update record in audit table

mysql -u${user_name} -p${mysql_password} -e "update ${audit_dataset_name}.${audit_table_name} set status='FAILED' where job_id=${job_id} and job_name='${job_name}'"
if [ $? -ne 0 ]; then
  echo "ERROR: failed to update record into an audit table for job_id:${job_id}"
 exit 1
 fi
echo "INFO: successfully updated record into an audit table for job_id:${job_id}"

#echo "INFO: successfully imported data from mysql table :$1.$2"

#update success entry in audit table


mysql -u${user_name} -p${mysql_password} -e "update ${audit_dataset_name}.${audit_table_name} set status='COMPLETED',job_end_time=current_timestamp() where job_id=${job_id} and job_name='${job_name}'"
if [ $? -ne 0 ]; then
    echo "ERROR: failed to update record into an audit table for job_id: ${job_id}"
    exit 1
fi
echo "INFO: successfully update record into audit table: ${audit_dataset_name}.${audit_table_name} for job_id: ${job_id} and job_name='${job_name}'"


