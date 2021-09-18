########################################################
# Script name : Fullload.sh                            #
# Author      : Sharad Ekale                           #
# Create date : 2021-09-18                             #
# Description : Import Full load Data from mysql table #
#               through sqoop to hdfs location         #
#          <database_name> <table_name> <split column> #
########################################################

#Create datetime var for log file and write log into file.
date_time_log=$(date +"%Y/%m/%d %H%M%S")
log_datetime=$(date +%Y%m%d%H%M%S)
bash_name=$(basename "$0" | cut -f 1 -d '.')
log_location="${target_basepath}"${bash_name}"_"${log_datetime}.log
exec 2>>${log_location}

#Importing Credentials detail
. credential.config
if [ $? -ne 0 ]; then
  echo ${date_time_log} "ERROR:" "failed to import credential_config.config file" ${bash_name} >>"${log_location}"
  exit 1
fi
echo ${date_time_log} "INFO:" "successfully loaded credentials" ${bash_name} >>"${log_location}"

# Job details
target_basepath=/user/cloudera/flightdata/
database_name="$1"
table_name="$2"
split_by="$3"
input_dir=/home/cloudera/bwt_flightdata/airports.csv
output_dir=${target_basepath}${job_id}/

job_id=$(date '+%Y%m%d%H%M%S')
if [ $? -ne 0 ]; then
  echo ${date_time_log} "ERROR": "Failed to create job id" ${bash_name} >>"${log_location}"
  exit 1
fi
echo ${date_time_log} "INFO:" "job id successfully created ${job_id}" ${bash_name} >>"${log_location}"
job_name="${job_name}_$2"

#command line argument validation
: ${1?' You forgot to supply the first argument'}
: ${2?' You forgot to supply the second argument'}
: ${3?' You forgot to supply the third argument'}

#insert in audit table
mysql -u${username} -p${password} -e "insert into ${audit_database_name}.${audit_table_name} (job_id,job_name,run_status,job_start_time,job_end_time) values(${job_id},'${job_name}','RUNNING',CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP())"
if [ $? -ne 0 ]; then
  echo ${date_time_log} "ERROR: Failed to insert record into audit table job_id:${job_id}" ${bash_name} >>"${log_location}"
  exit 1
fi
echo ${date_time_log} "INFO: successfully  inserted record in audit table job_id:${job_id}" ${bash_name} >>"${log_location}"

# sqoop import start
sqoop import \
-Dhadoop.security.credential.provider.path=jceks://hdfs/user/root/mysql.password.jceks \
--connect jdbc:mysql://${hostname}:${port}/$1 \
--username=${username} \
--password-alias mydb.password.alias \
--split-by=$3 \
--table=$2 \
--target-dir=${target_basepath}mysql/$job_id/$2 \
--delete-target-dir | tee
>>"${log_location}"

#update record in audit table
if [ $? -ne 0 ]; then
  echo ${date_time_log} "ERROR:" "Failed to import data from MySQL table" ${bash_name} >>"${log_location}"
  mysql -u${username} -p${password} -e "update ${audit_database_name}.${audit_table_name} set run_status='FAILED' where job_id=${job_id}"
  if [ $? -ne 0 ]; then
    echo ${date_time_log} "ERROR: Failed to update run_status(failed) record in audit table job_id:${job_id}" ${bash_name} >>"${log_location}"
    exit 1
  fi
  echo ${date_time_log} "INFO: successfully updated record in audit table job_id:${job_id}" ${bash_name} >>"${log_location}"
  exit 1
fi
echo ${date_time_log} "INFO: Successfully imported data from MySQL table" ${bash_name} >>"${log_location}"

#update job_status that import job is successful
mysql -u${username} -p${password} -e "update audit.audit_tb set run_status='COMPLETED' WHERE job_id=${job_id}"
if [ $? -ne 0 ]; then
  echo ${date_time_log} "ERROR: failed to update job_status and last value in table ${audit_tablename}" ${bash_name} >>"${log_location}"
  exit 1
fi

echo "Import File location:"${target_basepath}$1/$2
echo "Log file location is:"${log_location}

hadoop fs -test -d ~/"${output_dir}" #checks to see if hadoop output dir exists
if [ $? == 0 ]; then
  hdfs dfs -rm -r ~/"${output_dir}"
else
  echo ${date_time_log} "Output file doesn't exist and will be created when hadoop runs" ${bash_name} >>"${log_location}"
fi

hdfs dfs -test -d ~/"${input_dir}" #checks to see if hadoop input dir exists
if [ $? == 0 ]; then
  hdfs dfs -rm -r ~/"${input_dir}"
  echo ${date_time_log} "Hadoop input dir already exists deleting it now and creating a new one..." ${bash_name} >>"${log_location}"
  hdfs dfs -mkdir ~/"${input_dir}" # makes an input dir for text file to be put in

else
  echo ${date_time_log} "Input file doesn't exist will be created now" ${bash_name} >>"${log_location}"
  hdfs dfs -mkdir ~/"${input_dir}" # makes an input dir for text file to be put in
fi

hadoop fs -copyFromLocal ${input_dir} ${output_dir}
