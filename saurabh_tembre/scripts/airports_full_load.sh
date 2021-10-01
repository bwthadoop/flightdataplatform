###############################################################################
# Project Name: Airport Data Migration                                        #
# Script Name : Load data in HDFS location                                    #
# Author      : Saurabh Tembre                                                #
# Create date : 2021-09-19                                                    #
# Description : Loading Data into MySQL and from MYSQL to HDFS                #
###############################################################################

#airports.sh spark airports

. config/functions.sh

log_function

echo "Script Name: " $0
echo "Database Name: " $1
echo "Table Name: " $2
echo "Split by Column: " $3

#If condition for credential.config file
. config/credentials.config
if [ $? -ne 0 ]
then
  echo ${ERROR} " Failed to Import Credential.config File"
fi
  echo ${INFO} " Credential.config File Imported Successfully"

#If condition for job.config file
. config/jobs.config
if [ $? -ne 0 ]
then
  echo ${ERROR} " Failed to Import Job.config File"
fi
  echo ${INFO} " Job.config File Imported Successfully"

job_id=$(date '+%H%M%S')
job_name="${1}.${2}"

#insert record into audit table
mysql --defaults-extra-file=${mysql_password} -e "insert into ${1}.${airport_audit_table}(job_id,job_name,run_status,run_date) values (${job_id},'${job_name}','RUNNING',current_date)"

if [ $? -ne 0 ]
then
  echo -e ${ERROR} " Failed to Insert Data into Audit Table"
  exit 1
fi
  echo -e ${INFO}" Inserted Record into ${airport_audit_table} for ${job_name}"

#re-run scenario
job_status=$(mysql --defaults-extra-file=${mysql_password} -e "select job_name from ${1}.${airport_audit_table} where run_status = 'Failed'")

#sqoop import job
sqoop import --connect "jdbc:mysql://${host_name}:${port_id}/${1}" --username ${user_name} --password-file ${pass_value} --table ${2} --target-dir ${target_base_path}/${2} --delete-target-dir --split-by ${3}

if [ $? -ne 0 ]
then
echo -e ${ERROR} " Sqoop Import Failed"

#updating table if job fail
mysql --defaults-extra-file=${mysql_password} -e "update ${1}.${airport_audit_table} set run_status='FAILED' where job_id=${job_id} and job_name='${job_name}'"

if [ $? -ne 0 ]
  then
    echo -e ${ERROR} " Failed to Update Audit Table"
    exit 1
  fi
    echo -e ${INFO}" Updated Record into ${airport_audit_table} for ${job_name}"
    exit 1
  fi
    echo -e ${INFO}" Import Job Successful for ${1}.${2}"

#retrive current date
temp_max_date=$(date)

tmp_update_date=$(echo -e $temp_max_date | cut -d' ' -f2)
tmp_update_time=$(echo -e $temp_max_date | cut -d' ' -f3)
update_date=$(echo $tmp_update_date $tmp_update_time)

#update last value
mysql --defaults-extra-file=${mysql_password} -e "update ${1}.${airport_audit_table} set last_job_date='${update_date}',run_status='COMPLETE' where job_id=${job_id}"

if [ $? -ne 0 ]; then
    echo -e ${ERROR} " Failed to Update Record into Audit Table"
    exit 1
fi
  echo -e ${INFO}" Updated Record into ${airport_audit_table} for ${job_id}"

#create hive database
. airport_ddl.sh