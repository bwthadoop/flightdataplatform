########################################################
# Script name : Fullload.sh                            #
# Author      : Sharad Ekale                           #
# Create date : 2021-09-18                             #
# Description : Import Full load Data from mysql table #
#               through sqoop to hdfs location         #
#          <database_name> <table_name> <split column> #
########################################################

#command line argument validation
: ${1?' You forgot to supply the first argument'}
: ${2?' You forgot to supply the second argument'}
: ${3?' You forgot to supply the third argument'}

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
job_name="sqoop_import_${table_name}"

#insert in audit table
mysql -u${username} -p${password} -e "insert into ${audit_database_name}.${audit_tablename}(job_id,job_name,job_status,job_start_time) values(${job_id},'${job_name}','RUNNING',CURRENT_TIMESTAMP())"
if [ $? -ne 0 ]; then
    echo ${date_time_log} "ERROR: Failed to insert record into audit table job_id:${job_id}" ${bash_name} >>"${log_location}"
    exit 1
fi
    echo ${date_time_log} "INFO: successfully  inserted record in audit table job_id:${job_id}" ${bash_name} >>"${log_location}"

# sqoop import
sqoop import \
--connect jdbc:mysql://${hostname}:${port}/${database_name} \
--username=${username} \
--password=${password} \
--split-by=${split_by} \
--table=${table_name} \
--target-dir=${target_basepath}mysql/$job_id/$2 \
--delete-target-dir | tee
>>"${log_location}"

#update record in audit table
if [ $? -ne 0 ]; then
    echo ${date_time_log} "ERROR:" "Failed to import data from MySQL table" ${bash_name} >>"${log_location}"
  mysql -u${username} -p${password} -e "update ${audit_database_name}.${audit_table_name} set job_status='FAILED' where job_id=${job_id}"
  mysql -u${username} -p${password} -e "update ${audit_database_name}.${audit_table_name} set job_end_time=CURRENT_TIMESTAMP() WHERE job_id=${job_id}"
    if [ $? -ne 0 ]; then
        echo ${date_time_log} "ERROR: Failed to update run_status(failed) record in audit table job_id:${job_id}" ${bash_name} >>"${log_location}"
        exit 1
    fi
      	echo ${date_time_log} "INFO: successfully updated record in audit table job_id:${job_id}" ${bash_name} >>"${log_location}"
        exit 1
fi
echo ${date_time_log} "INFO: Successfully imported data from MySQL table" ${bash_name} >>"${log_location}"

#update job_status that import job is successful
mysql -u${username} -p${password} -e "update ${audit_database_name}.${audit_table_name} set job_status='COMPLETED' WHERE job_id=${job_id}"
mysql -u${username} -p${password} -e "update ${audit_database_name}.${audit_table_name} set job_end_time=CURRENT_TIMESTAMP() WHERE job_id=${job_id}"
if [ $? -ne 0 ]; then
  echo ${date_time_log} "ERROR: failed to update job_status in table ${audit_tablename}" ${bash_name} >>"${log_location}"
  exit 1
fi

echo "Import File location:"${target_basepath}mysql/$job_id/$2
echo "Log file location is:"${log_location}