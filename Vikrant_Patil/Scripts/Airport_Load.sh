
########################################################################
# Script Name : Airport_load.sh                                        #
# Author      : Vikrant_Patil                                          #
# date        : 2021-09-20                                             #
# Dataset     : Airport Dataset                                        #
# Description : bash script to import Full load into HDFS Location through sqoop import  #
#Run command Syntax      : bash <Script_Name> <db_name> <table_name> <split_column_name> #
#Run command             : bash Airport_Load.sh flight_data Airport airport_id                        #
########################################################################

#import log_common file first
. log_common.sh
if [ $? -ne 0 ];
then
  echo "$(printlog) ERROR: Import log_common.sh  failure">>$log
  logpath
  exit 1
else
  echo "$(printlog) INFO: Import  log_common.sh  successfully">>$log
fi

#input parameters validation
if [ $# -ne 3 ];
then
  echo "$(printlog) Please check input arguments in bash script" >>$log
  logpath
  exit 1
else
  echo "$(printlog) INFO: all parameters are present">>$log
fi

#Import Airport_job.config first
. Airport_job.config
if [ $? -ne 0 ];
then
  echo " ERROR: Import  Airport job config  failure"

  exit 1
else
  echo " INFO: Import   Airport job config  successfully"
fi



#Import Create SQL table first
. Airport_tb.sh
if [ $? -ne 0 ];
then
  echo " ERROR: Import  Airport_tb.sh  failure"

  exit 1
else
  echo " INFO: Import   Airport_tb.sh  successfully"
fi

#fetching sql password path
sql_password=$(echo ${password/file:\/\//})


#importing table from sql to hdfs location
#exec &> >(tee -a "$log")
sqoop import --connect "jdbc:mysql://${hostname}:${port}/${1}" --username ${username} --password ${password} --table ${table_name} --target-dir ${hdfs_arc_loc}/${2}_${date_time} --delete-target-dir --split-by ${splitby_column}
#Checking sqoop import status
if [ $? -ne 0 ];
then
  echo "$(printlog) ERROR: Sqoop import job failure">>$log
else
  echo "$(printlog) INFO: Imported Data Successfully to HDFS: ${Database_name}.${table_name}"
  echo "$(printlog) INFO: sqoop import successfully run">>$log
fi


