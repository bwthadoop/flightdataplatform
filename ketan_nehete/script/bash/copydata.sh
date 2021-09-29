######################################################################################################
# Script name             : copydata.sh                                                              #
# Author                  : Ketan Nehete                                                             #
# Create date             : 2021-09-17                                                               #
# Description             : Import daily load Data from local to hdfs                                #
# Run command             : bash copydata.sh                                                        #
# Run command Syntax      : bash <Script_Name> <local src> <dest src> <overwrite>                    #
# Example                 : bash copydata.sh                                                        #
######################################################################################################


#Log creation function
logcreation() {
    LOG_FILE1=/home/hadoop/ProjectLogFile/sparkprojectlogs/$0
    log_date=$(date '+%Y%m%d_%H%M%S')
    exec > >(tee ${LOG_FILE1}_${log_date}.log) 2>&1
}
logcreation

#Print log path
printlogpath() {
    echo "$(printlog) ERROR:check the log file ${LOG_FILE1}_${log_date}.log"
}

#intialize value to jobid and job name
jobid=$(date '+%Y%m%d%H%M%S')
jobname="copydata_${2}"


# append following data to log file
echo "Date:" $(date "+%Y-%m-%d %H:%M:%S")
echo "localsrc:" ${1}
echo "destsrc:" ${2}


#importing log_common.sh
. log_common.sh
if [ $? -ne 0 ]; then
    echo "ERROR: Import log_common.sh  failure"
    exit 1
else
    echo "INFO: Import log_common.sh successfully"
fi

#checking daemon status
checkDameonStatus

#importing credentials.config file
. config/credentials.config
checkstatus "Import credentials.config successfull" "Import credentials.config Failure"

#importing job.config file
. config/job.config
checkstatus "Import job.config successfull" "Import job.config Failure"


localsrc=$1
destsrc=$2
date=$(date +"%F")
#datetime=$(date '+%Y%m%d%H%M%S')

#checking parameter
if [[ "$#" = 2 || "$#" = 3 ]];
then

  if [[ ${3} = "overwrite" || ${3} = "Overwrite" || ${3} = "" ]];
  then
      echo "$(printlog) INFO:Number of parameter is correct"
  else
     printerror "Check 3rd parameter"
     exit 1

  fi
else
  printerror "Check the number of Parameter"
    exit 1
fi



sql_password=$(echo ${password/file:\/\//})


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



#creating directory in hdfs location
hadoop fs -mkdir -p ${destsrc}/${date}
if [ $? -ne 0 ]; then
    printerror "Directory creation failed"
     job_failure
      printlogpath
    exit 1
else
    echo "INFO: make directory successfully"
fi



#copying data from source
if [ -z ${3} ];
then
  hadoop fs -copyFromLocal ${localsrc}/* ${destsrc}/${date}
  if [ $? -ne 0 ]; then
      printerror  "unable to copy data if file already exits then give overwrite parameter"
       job_failure
        printlogpath
      exit 1
  else
      echo "INFO: copied data successfully"

  fi
else
   hadoop fs -copyFromLocal -f ${localsrc}/* ${destsrc}/${date}
   if [ $? -ne 0 ]; then
         printerror  "unable to copy data"
          job_failure
           printlogpath
         exit 1
     else
         echo "INFO: copied data successfully"
     fi
fi


#creating directory in local location
mkdir -p ${move_dest}/${date}
if [ $? -ne 0 ]; then
    printerror "Directory creation failed"
     job_failure
      printlogpath
    exit 1
else
    echo "INFO: make directory successfully"
fi


#moving file which are copied succesfully
  mv ${localsrc}/* ${move_dest}/${date}
   if [ $? -ne 0 ]; then
         printerror  "unable to move data"
          job_failure
           printlogpath
         exit 1
     else
         echo "INFO: moved data successfully"
     fi


#updating job status complete
mysql -u${username} --password=$(cat ${sql_password})  -e "update ${audit_database_name}.${audit_tablename} set job_status='COMPLETE' where job_id=${jobid}"
          checkstatus "updated record into ${audit_tablename} for ${jobid}" "ERROR: fAILED TO Update RECORD INTO AUDIT TABLE"
        echo "check the log file ${LOG_FILE1}_${log_date}.log"

