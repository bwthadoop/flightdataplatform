################################################################################################
# Script name             : DataIngest.sh                                                      #
# Author                  : Ketan Nehete                                                       #
# Create date             : 2021-09-17                                                         #
# Description             : Import Data from sql to hdfs                                       #
# Run command             : bash DataIngest.sh                                                 #
# Run command Syntax      : bash <Script_Name>  <database_name> <table_name> <split-by_col>    #
# Example                 : bash DataIngest.sh  airportdb airporttb airport_id                 #
################################################################################################


#Log creation function
logcreation() {
    LOG_FILE1=/home/hadoop/ProjectLogFile/sparkprojectlogs/$0
    log_date=$(date '+%Y%m%d_%H%M%S')
    exec > >(tee ${LOG_FILE1}_${log_date}.log) 2>&1
}

#Print log path
printlogpath() {
    echo "$(printlog) ERROR:check the log file ${LOG_FILE1}_${log_date}.log"
}

logcreation

#intialize value to jobid and job name
jobid=$(date '+%Y%m%d%H%M%S')
jobname="full_load_hdfs_${2}"

# append following data to log file
echo "Date:" $(date "+%Y-%m-%d %H:%M:%S")
echo "Database:" ${1}
echo "Table:" ${2}
echo "jobid :" ${jobid}
echo "jobname :" ${1}


#importing log_common.sh
. log_common.sh
if [ $? -ne 0 ]; then
    echo "ERROR: Import log_common.sh  failure"
    exit 1
else
    echo "INFO: Import log_common.sh successfully"
fi


#checking parameter
if [[ "$#" = 2 || "$#" = 3 ]]; then
    echo "$(printlog) INFO: number of parameter is correct"
else
    printerror "check the number of parameter"
    printlogpath
    exit 1
fi

#checking split-by status
if [ -z "$3" ]; then
  #splitby="--autoreset-to-one-mapper -m 1"
  splitby="-m 1"
  echo "${splitby}"
else
  splitby="--split-by ${3}"
  echo "${splitby}"
fi

#checking daemon status
checkDameonStatus

#importing credentials.config file
. config/credentials.config
checkstatus "Import credentials.config successfull" "Import credentials.config Failure"

#importing job.config file
. config/job.config
checkstatus "Import job.config successfull" "Import job.config Failure"


#fetching sql password
sql_password=$(echo ${password/file:\/\//})

echo ${sql_password}
database_name=$1


#fetching job status
check_run_status=$(mysql --user=${username} --password=$(cat ${sql_password}) -N -e "select job_status from ${audit_database_name}.${audit_tablename} where job_name='${jobname}' order by job_start_time desc limit 1")
if [ $? -ne 0 ]; then
  printerror "failed to check job status from ${audit_tablename}"
  printlogpath
  exit 1
else
  echo "${printlog}fetched job_status successfully"
  fi

#checking job_status
if [ "$check_run_status" == "RUNNING" ]; then
  printerror "script is already in running state wait for it to complete"
  printlogpath
  exit 1
fi



#insert record into audit_tb
mysql --user=${username} --password=$(cat ${sql_password}) -e "insert into ${audit_database_name}.${audit_tablename}(job_id,job_name,job_status) values(${jobid},'${jobname}','RUNNING')"
checkstatus "Record inserted successfully into ${audit_tablename}" "failed to insert record into ${audit_tablename}"

#creating file name
s=$(date +"%F")
t1=${2}_${s}

#sqoop import command
sqoop import --connect "jdbc:mysql://${hostname}:${port}/${database_name}" --username ${username} --password-file "${password}" --table ${2} --target-dir ${target_basepath}/${t1} --delete-target-dir ${splitby}
if [ $? -ne 0 ];
then
  printerror "Failed to load data from rdbms to hdfs"
  job_failure
  printlogpath
  exit 1
else
  echo "$(printlog) INFO:Successfully imported data from rdbms to hdfs"
  echo "data loaded at location ${target_basepath}/${t1}"
  mysql -u${username} --password=$(cat ${sql_password})  -e "update ${audit_database_name}.${audit_tablename} set job_status='COMPLETE' where job_id=${jobid}"
    checkstatus "INFO:updated record into ${audit_tablename} for ${job_id}" "ERROR: fAILED TO Update RECORD INTO AUDIT TABLE"
  echo "check the log file ${LOG_FILE1}_${log_date}.log"
fi





