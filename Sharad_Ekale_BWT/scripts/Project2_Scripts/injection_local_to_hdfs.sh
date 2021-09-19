########################################################
# Script name : injection_local_to_hdfs.sh             #
# Author      : Sharad Ekale                           #
# Create date : 2021-09-18                             #
# Description : Coping data from local directory       #
#               to hdfs directory                      #
########################################################

#Create datetime var for log file and write log into file.
date_time_log=$(date +"%Y/%m/%d %H%M%S")
log_datetime=$(date +%Y%m%d%H%M%S)
bash_name=$(basename  "$0" | cut -f 1 -d '.')
log_location="${target_basepath}"${bash_name}"_"${log_datetime}.log
exec 2>>${log_location}

. credential.config
if [ $? -ne 0 ]; then
  echo ${date_time_log} "ERROR:" "failed to import credential_config.config file" ${bash_name} >>"${log_location}"
  exit 1
  fi
 echo ${date_time_log} "INFO:" "successfully loaded credentials" ${bash_name} >>"${log_location}"

. job.config
if [ $? -ne 0 ]; then
  echo ${date_time_log} "ERROR:" "failed to import job_config.config file" ${bash_name} >>"${log_location}"
  exit 1
  fi
 echo ${date_time_log} "INFO:" "successfully loaded job details" ${bash_name} >>"${log_location}"

# audit table details
job_id=$(date '+%Y%m%d%H%M%S')
  if [ $? -ne 0 ]; then
    echo ${date_time_log} "ERROR": "Failed to create job id" ${bash_name} >>"${log_location}"
    exit 1
  fi
    echo ${date_time_log} "INFO:" "job id successfully created ${job_id}" ${bash_name} >>"${log_location}"
job_name="file_coping_to_hdfs"

#insert in audit table
mysql -u${username} -p${password} -e "insert into ${audit_database_name}.${audit_tablename}(job_id,job_name,job_status,job_start_time) values(${job_id},'${job_name}','RUNNING',CURRENT_TIMESTAMP())"
if [ $? -ne 0 ]; then
    echo ${date_time_log} "ERROR: Failed to insert record into audit table job_id:${job_id}" ${bash_name} >>"${log_location}"
    exit 1
fi
    echo ${date_time_log} "INFO: successfully  inserted record in audit table job_id:${job_id}" ${bash_name} >>"${log_location}"

#checks to see if hadoop output dir exists
hadoop fs -test -d ~/"${target_basepath}/${job_id}"
if [ $? == 0 ]; then
   hadoop fs -rm -r ~/"${target_basepath}"
else
    echo ${date_time_log} "Output file doesn't exist and will be created when hadoop runs" ${bash_name} >>"${log_location}"
fi
#making target directory
hadoop fs -mkdir ${target_basepath}${job_id}

#coping file from local to hdfs
hadoop fs -copyFromLocal ${source_path}* ${target_basepath}/${job_id}/

#update record in audit table
if [ $? -ne 0 ]; then
    echo ${date_time_log} "ERROR:" "Failed to copy data from local ${source_path} to hdfs ${target_basepath}" ${bash_name} >>"${log_location}"
  mysql -u${username} -p${password} -e "update ${audit_database_name}.${audit_tablename} set job_status='FAILED' where job_id=${job_id}"
  mysql -u${username} -p${password} -e "update ${audit_database_name}.${audit_tablename} set job_end_time=CURRENT_TIMESTAMP() WHERE job_id=${job_id}"
    if [ $? -ne 0 ]; then
        echo ${date_time_log} "ERROR: Failed to update run_status(failed) record in audit table job_id:${job_id}" ${bash_name} >>"${log_location}"
        exit 1
    fi
      	echo ${date_time_log} "INFO: successfully updated record in audit table job_id:${job_id}" ${bash_name} >>"${log_location}"
        exit 1
fi
echo ${date_time_log} "INFO: Successfully copy data from local ${source_path} to hdfs ${target_basepath}" ${bash_name} >>"${log_location}"

#update job_status that import job is successful
mysql -u${username} -p${password} -e "update ${audit_database_name}.${audit_tablename} set job_status='COMPLETED' WHERE job_id=${job_id}"
mysql -u${username} -p${password} -e "update ${audit_database_name}.${audit_tablename} set job_end_time=CURRENT_TIMESTAMP() WHERE job_id=${job_id}"
if [ $? -ne 0 ]; then
  echo ${date_time_log} "ERROR: failed to update job_status in table ${audit_tablename}" ${bash_name} >>"${log_location}"
  exit 1
fi

