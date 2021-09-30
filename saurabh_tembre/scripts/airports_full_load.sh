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
message

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
mysql -u${user_name} -p${mysql_password} -e "insert into ${1}.${audit_table}(job_id,job_name,job_status,run_date) values (${job_id},'${job_name}','RUNNING',current_date)"

if [ $? -ne 0 ]
then
  echo -e ${ERROR} " Failed to Insert Data into Audit Table"
  exit 1
fi
  echo -e ${INFO}" Inserted Record into ${audit_table} for ${job_name}"

#re-run scenario
job_status=$(mysql -u${user_name} -p${pass_value} -e "select job_name from ${1}.${audit_table} where run_status = 'Failed'")

#check condition for number of arguments
if [ "$#" -lt 4 ]
then
  echo " Import Job in Process"
  sqoop import --connect "jdbc:mysql://${host_name}:${port_id}/${1}" --username ${user_name} --password-file ${pass_value} --table ${2} --target-dir ${target_base_path}/${2} --delete-target-dir -m4
fi
  sqoop import --connect "jdbc:mysql://${host_name}:${port_id}/${1}" --username ${user_name} --password-file ${pass_value} --table ${2} --target-dir ${target_base_path}/${2} --delete-target-dir --split-by ${3}

if [ $? -ne 0 ]
then
echo -e ${ERROR} " Sqoop Import Failed"

#updating table if job fail
mysql -u${user_name} -p${mysql_password} -e "update ${1}.${audit_table} set run_status='FAILED' where job_id=${job_id} and job_name='${job_name}'"

if [ $? -ne 0 ]
  then
    echo -e ${ERROR} " Failed to Update Audit Table"
    exit 1
  fi
    echo -e ${INFO}" Updated Record into ${audit_table} for ${job_name}"
    exit 1
  fi
    echo -e ${INFO}" Import Job Successful for ${1}.${2}"

#create hive database
hive_ddl.sh