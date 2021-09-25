##########################################################################
#script_name=fullLoad.sh                                                 #
#Author=Komal Dabhade                                                    #
#Create_date=17-09-2021                                                  #
#Description=Import airport data into HDFS from mysql using sqoop import #
##########################################################################

# Take database_name and table_name as arguments
#log_path=/home/hadoop/project2/log_files

log_date=$(date +"%Y-%m-%d %H:%M:%S")
#bash_file=$(basename -- "$0")
bashfile_name=$(basename -- "$0" | cut -f 1 -d '.')

#location of logfile
logfile_location="/home/hadoop/project2/log_files/"${log_date}.txt
#exec 2>>"${logfile_location}"
#exec &> >(tee -a "${logfile_location}")
exec > >(tee -a "${logfile_location}")
exec 2>&1

. credential.config
. job.config

#log data
echo "Airport_data_migration" >>"${logfile_location}"
echo "BashFile_Name: " ${bashfile_name} >>"${logfile_location}"
echo "Database_Name: " $1 >>"${logfile_location}"
echo "Table_Name: " $2 >>"${logfile_location}"
echo "Host_Name: " ${host_name} >>"${logfile_location}"
echo "Port_Name: " ${port_name} >>"${logfile_location}"
echo "Location_of_Logfile: " ${logfile_location} >>"${logfile_location}"

#date time of log file
log_date_time=$(date +"%Y-%m-%d %H:%M:%S")

#file with date
filedate=$(date +"%Y%m%d_%H%M%S")
date=$(date +"%Y-%m-%d")

# validation for arguments
if [ -z $1 ]
then
  echo ${log_date_time} ${bashfile_name} "ERROR :Please Provide Database Name!" >>"${logfile_location}"
exit 1
fi
 echo ${log_date_time} ${bashfile_name}"INFO: Successfully imported Database name" >>"${logfile_location}"
if [ -z $2 ]
 then
  echo ${log_date_time} ${bashfile_name} "ERROR :Please Provide Table Name!" >>"${logfile_location}"
exit 1
fi
echo ${log_date_time} ${bashfile_name}"INFO: Successfully imported Table name" >>"${logfile_location}"
if [ -z $3 ]
 then
#split=""
#else
#split_colum_name=$3

   echo ${log_date_time} ${bashfile_name} "ERROR: failed to import split-column " >>"${logfile_location}"
    exit 1
fi
echo ${log_date_time} ${bashfile_name}"INFO: successfully import split column ${split_column_name}" >>"${logfile_location}"

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
echo ${log_date_time} ${bashfile_name}"INFO: running sqoop import history load command" >>"${logfile_location}"

#audit table data insertion
job_id=$(date '+%y%m%d%H%M%S')
job_name=${job_name}.$2

#insert data in audit table
#mysql -u${user_name} -p${mysql_password} -e "insert into ${audit_dataset_name}.${audit_table_name}(job_id,job_name,run_date,run_status)values(${job_id},'${job_name}',current_date(),'RUNNING')"

mysql -u${user_name} -p${mysql_password} -e "insert into ${audit_dataset_name}.${audit_table_name}(job_id,job_name,status,job_start_time)values(${job_id},'${job_name}','RUNNING',current_timestamp())"

if [ $? -ne 0 ]; then
    echo "ERROR: failed to insert record into an audit table"
    exit 1
fi
echo "INFO: successfully inserted record into an audit table for job_id:${job_id}"

#sqoop import command
sqoop import --connect "jdbc:mysql://${host_name}:${port_name}/$1" --username ${user_name} --password-file ${auth_value} --table $2 --target-dir ${target_base_path}/$2/${date}/$2_${filedate}.csv --delete-target-dir --split-by ${split_column_name}

if [ $? -ne 0 ]
then
echo ${log_date_time} ${bashfile_name}"ERROR: failed to import data from mysql table : $1.$2" >>"${logfile_location}"
exit 1
fi
echo ${log_date_time} ${bashfile_name}"INFO: successfully imported data from mysql table:$1.$2" >>"${logfile_location}"

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

#log file location
echo "Location_of_Logfile: " ${logfile_location} >>"${logfile_location}"
