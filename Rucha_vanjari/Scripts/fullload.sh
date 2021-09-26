############################################################################################################
# Script name : FullLoad.sh                                                                                #
# Author      : Rucha Vanjari                                                                              #
# Create date : 2021-09-18                                                                                 #
# Description : Import Full load Data from mysql table                                                     #
#               to the HDFS location                                                                       #
#                                                                                                          #
############################################################################################################

database_name="$1"
table_name="$2"
split_col="$3"

#validations
if [ $# -eq 3 ]; then
    echo "Your Database Name: "$1
    echo "Your Table Name: "$2
    echo "Your Split_by Column Name: "$3
else
    echo "invalid argument please pass three argument(database_name,table_name,split_col) "
    exit 1
fi

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

# JOB ID creation.

job_id=$(date '+%Y%m%d%H%M%S')
  if [ $? -ne 0 ]; then
    echo ${date_time} "ERROR": "Failed to create job id" ${bash_name} >>"${log_location}"
    exit 1
  fi
    echo ${date_time} "INFO:" "job id successfully created ${job_id}" ${bash_name} >>"${log_location}"

job_name="sqoop_import_${table_name}"

#Insert into audit table.

mysql -u${m_username} -p${m_password} -e "insert into ${audit_database_name}.${audit_tablename}(job_id,job_name,job_status,job_start_time) values(${job_id},'${job_name}','RUNNING',CURRENT_TIMESTAMP())"
if [ $? -ne 0 ]; then
    echo ${date_time} "ERROR: Failed to insert record into audit table job_id:${job_id}" ${bash_name} >>"${log_location}"
    exit 1
fi
    echo ${date_time} "INFO: successfully  inserted record in audit table job_id:${job_id}" ${bash_name} >>"${log_location}"

#Running sqoop import job.

echo "INFO: running sqoop import history load command"

sqoop import \
--connect jdbc:mysql://${hostname}:${port}/${database_name} \
--username=${username} \
--password-file ${auth_val} \
--split-by=${split_by} \
--table=${table_name} \
--target-dir=${target_basepath}mysql/$2/$job_id/ \
--delete-target-dir \
>>"${log_location}"

#update record in audit table
if [ $? -ne 0 ]; then
    echo ${date_time} "ERROR:" "Failed to import data from MySQL table" ${bash_name} >>"${log_location}"
  mysql -u${m_username} -p${m_password} -e "update ${audit_database_name}.${audit_tablename} set job_status='FAILED' where job_id=${job_id}"
  mysql -u${m_username} -p${m_password} -e "update ${audit_database_name}.${audit_tablename} set job_end_time=CURRENT_TIMESTAMP() WHERE job_id=${job_id}"
    if [ $? -ne 0 ]; then
        echo ${date_time} "ERROR: Failed to update run_status(failed) record in audit table job_id:${job_id}" ${bash_name} >>"${log_location}"
        exit 1
    fi
      	echo ${date_time} "INFO: successfully updated record in audit table job_id:${job_id}" ${bash_name} >>"${log_location}"
        exit 1
fi
echo ${date_time} "INFO: Successfully imported data from MySQL table" ${bash_name} >>"${log_location}"

#update job_status that import job is successful
mysql -u${m_username} -p${m_password} -e "update ${audit_database_name}.${audit_tablename} set job_status='COMPLETED' WHERE job_id=${job_id}"
mysql -u${m_username} -p${m_password} -e "update ${audit_database_name}.${audit_tablename} set job_end_time=CURRENT_TIMESTAMP() WHERE job_id=${job_id}"
if [ $? -ne 0 ]; then
  echo ${date_time} "ERROR: failed to update job_status in table ${audit_tablename}" ${bash_name} >>"${log_location}"
  exit 1
fi

echo "Import File location:"${target_basepath}mysql/$2/$job_id
echo "Log file location is:"${log_location}