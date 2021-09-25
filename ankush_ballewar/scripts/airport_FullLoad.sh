###############################################################################
# script name         : Full_load                                             #
# Author              : Ankush Ballewar                                       #
# create date         : 2021-09-18                                            #
# description         : Import Data Tables from SQL to HDFS                   #
# Run command         : bash history.sh                                       #
# Run command Syntax  : bash <script.sh> <DB name> <table name> <column name> #
# Example             : bash history.sh                                       #
###############################################################################

date_time=$(date '+%Y%m%d_%H%M%S')

# Importing and Executing Log File
#Provided location where it will generate/stored with it's provided name (i.e. $0)
logfile=/home/hadoop/project2logs/$0
# Time Stamp date provided as given format
logdate=$(date '+%Y-%m-%d_%H:%M:%S')

#executing this Log File with syntax like ScriptName(i.e $0) and Executed Date & Time.
exec > >(tee ${logfile}_${logdate}.log) 2>&1

#Created log file function to print logs format
printlog() {
    echo "$(date) $0 "
}
printlogpath() {
    echo "$(printlog) ERROR:check the log file ${logfile}_${log_date}.log"
}


#Checking Parameters
if [ "$#" -ne 3 ]; then
    echo -e "\e[0;31m$(printlog) ERROR: check the number of parameter\e[0m"
        printlogpath
    exit 1
else
    echo -e "\e[0;32m$(printlog) INFO: number of parameter is correct\e[0m"
fi


#Importing common log file for checking status of Daemons
. logcommon.sh
if [ $? -ne 0 ]; then
    echo -e "\e[0;31m$(printlog) ERROR: Error Found.Please check log common file contents\e[0m"
    printlogpath
    exit 1
else
    echo -e "\e[0;32m$(printlog) INFO: log common file file Imported successfully\e[0m"
fi

CheckDaemons


#Importing airport_credentials.config file
. airport_credentials.config
if [ $? -ne 0 ]; then
    echo -e "\e[0;31m$(printlog) ERROR: Error Found.Please check Credential file contents\e[0m"
    printlogpath
    exit 1
else
    echo -e "\e[0;32m$(printlog) INFO: Credential file Imported successfully\e[0m"
fi
#Calling log function to print date,info,warn,error format for credentials file
printlog

#Importing airportjob.config file
. airportjob.config
if [ $? -ne 0 ];
then
  echo -e "\e[0;31mERROR: Import  Airport job config  failure\e[0m"

  exit 1
else
  echo -e "\e[0;32mINFO: Import   Airport job config  successfully\e[0m"
fi

#Created variable and set value as password.txt file's path
# shellcheck disable=SC2154
password_sql=${password/file:\/\//}

#importing table from sql to hdfs location
sqoop import --connect "jdbc:mysql://${hostname}:${port}/${1}" --username ${username} --password-file "${password}" --table ${2} --target-dir ${hdfs_path}/${2}/${date_time} --delete-target-dir --split-by ${3}
# split by used because there no primary key present in the table. So we can provide column name as 3rd argument on terminal.

#Checking sqoop import status
if [ $? -ne 0 ];
then
  echo -e "\e[0;31m$(printlog) ERROR: Please Check Sqoop Command Syntax Properly. Sqoop import job failure\e[0m"
else
  echo -e "\e[0;32m\e[4m$(printlog)INFO: Imported Data Successfully into this HDFS path: \e[0;46m${hdfs_path}\e[0m with file name: \e[1;46m${2}\e[0m \e[0m"

fi