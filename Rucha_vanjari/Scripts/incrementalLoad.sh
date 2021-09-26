##############################################################################
# Script name : incrementalLoad.sh                                           #
# Author      : Rucha Vanjari                                                #
# Create date : 2021-09-19                                                   #
# Description : Import incremental data from local to hdfs location          #
#                                                                            #
##############################################################################

#Create datetime var for log file and write log into file.

date_time=$(date +"%Y/%m/%d %H%M%S")
log_datetime=$(date +%Y%m%d%H%M%S)
bash_name=$(basename  "$0" | cut -f 1 -d '.')
log_location="${target_basepath}"${bash_name}"_"${log_datetime}.log
exec 2>>${log_location}

. credentials.config
if [ $? -ne 0 ]; then
  echo ${date_time} "ERROR:" "failed to import credential_config.config file" ${bash_name} >>"${log_location}"
  exit 1
  fi
 echo ${date_time} "INFO:" "successfully loaded credentials" ${bash_name} >>"${log_location}"

. job.config
if [ $? -ne 0 ]; then
  echo ${date_time} "ERROR:" "failed to import job_config.config file" ${bash_name} >>"${log_location}"
  exit 1
  fi
 echo ${date_time} "INFO:" "successfully loaded job details" ${bash_name} >>"${log_location}"

# Job ID creation.

job_id=$(date '+%Y%m%d%H%M%S')
  if [ $? -ne 0 ]; then
    echo ${date_time} "ERROR": "Failed to create job id" ${bash_name} >>"${log_location}"
    exit 1
  fi
    echo ${date_time} "INFO:" "job id successfully created ${job_id}" ${bash_name} >>"${log_location}"

job_name="filecopy_local_to_hdfs"

#insert into audit table.

mysql -u${m_username} -p${m_password} -e "insert into ${audit_database_name}.${audit_tablename}(job_id,job_name,job_status,job_start_time) values(${job_id},'${job_name}','RUNNING',CURRENT_TIMESTAMP())"
if [ $? -ne 0 ]; then
    echo ${date_time} "ERROR: Failed to insert record into audit table job_id:${job_id}" ${bash_name} >>"${log_location}"
    exit 1
fi
    echo ${date_time} "INFO: successfully  inserted record in audit table job_id:${job_id}" ${bash_name} >>"${log_location}"

#check if output directory exists or not{HDFS}!

hadoop fs -test -d ~/"${target_basepath}/${job_id}"
if [ $? == 0 ]; then
   hadoop fs -rm -r ~/"${target_basepath}"hadoop fs -test -d ~/
else
    echo ${date_time} "The file doesn't exist!" ${bash_name} >>"${log_location}"
fi


#making target directory
hadoop fs -mkdir ${target_basepath}${job_id}

#copy file from local to hdfs location
hadoop fs -copyFromLocal ${source_path} ${target_basepath}/${job_id}/

#update record in audit table
if [ $? -ne 0 ]; then
    echo ${date_time} "ERROR:" "Failed to copy data from local ${source_path} to hdfs ${target_basepath}" ${bash_name} >>"${log_location}"
  mysql -u${m_username} -p${m_password} -e "update ${audit_database_name}.${audit_tablename} set job_status='FAILED' where job_id=${job_id}"
  mysql -u${m_username} -p${m_password} -e "update ${audit_database_name}.${audit_tablename} set job_end_time=CURRENT_TIMESTAMP() WHERE job_id=${job_id}"
    if [ $? -ne 0 ]; then
        echo ${date_time} "ERROR: Failed to update run_status(failed) record in audit table job_id:${job_id}" ${bash_name} >>"${log_location}"
        exit 1
    fi
      	echo ${date_time} "INFO: successfully updated record in audit table job_id:${job_id}" ${bash_name} >>"${log_location}"
        exit 1
fi
echo ${date_time} "INFO: Successfully copy data from local ${source_path} to hdfs ${target_basepath}" ${bash_name} >>"${log_location}"

#updating job status.
mysql -u${m_username} -p${m_password} -e "update ${audit_database_name}.${audit_tablename} set job_status='COMPLETED' WHERE job_id=${job_id}"
mysql -u${m_username} -p${m_password} -e "update ${audit_database_name}.${audit_tablename} set job_end_time=CURRENT_TIMESTAMP() WHERE job_id=${job_id}"
if [ $? -ne 0 ]; then
  echo ${date_time} "ERROR: failed to update job_status in table ${audit_tablename}" ${bash_name} >>"${log_location}"
  exit 1
fi
