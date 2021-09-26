########################################################################
# Script Name : Airport_load.sh                                        #
# Author      : Pranav_Desai                                          #
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


#Import Airport_job.config first
. Airport_job.config
if [ $? -ne 0 ];
then
  echo "$(printlog) ERROR: Import  Airport job config  failure" >>$log

  exit 1
else
  echo "$(printlog) INFO: Import   Airport job config  successfully" >>$log
fi

job_name=Airport_Load
audit_database_name=Flight_data
audit_table_name=audit_tb
job_id=$(date '+%H%M%S')
job_name="${job_name}_${table_name}"

#Max Date from Table
temp=$(mysql -u${user_name} -p${password} -e "select max(updated_date) from ${audit_database_name}.${table_name}")
if [ $? -ne 0 ]
then
  echo "$(printlog) ERROR: Failed to get Max Update_date Value" >>$log
  exit 1
fi
  echo "$(printlog) INFO: Fetched Updated Date Successfully" >>$log


tmp_update_date=$(echo $tmp | cut -d' ' -f2)
echo ${tmp_update_date}
tmp_update_time=$(echo $tmp | cut -d' ' -f3)
update_date=$(echo $tmp_update_date $tmp_update_time)
echo ${update_date}


#Import Create SQL table first
. Airport_tb.sh
if [ $? -ne 0 ];
then
  echo "$(printlog) ERROR: Import  Airport_tb.sh  failure" >>$log

  exit 1
else
  echo "$(printlog) INFO: Import   Airport_tb.sh  successfully" >>$log
fi

#importing table from sql to hdfs location
#exec &> >(tee -a "$log")
#Insert Record into Audit Table
mysql -uroot -ppassword -e "insert into ${audit_database_name}.${audit_table_name}(job_id,job_name,run_date,run_status)values(${job_id},'${job_name}',current_date(),'RUNNING')";
if [ $? -ne 0 ]
then
  echo "$(printlog) ERROR: Failed to Insert Record into Audit Table" >>$log
  exit 1
fi
  echo "$(printlog) INFO:Inserted Record into ${audit_table_name} for ${job_id}" >>$log

sqoop import --connect "jdbc:mysql://${hostname}:${port}/${1}" --username ${username} --password-file ${password} --table ${table_name} --target-dir ${hdfs_arc_loc}/${2}_${date_time} --delete-target-dir --split-by ${splitby_column}
#Checking sqoop import status
if [ $? -ne 0 ];
then
  echo "$(printlog) ERROR: Sqoop import job failure" >>$log
  #update if job is failed
  mysql -u${user_name} -p${password} -e "update ${audit_database_name}.${audit_table_name} set run_status='FAILED' where job_id=${job_id}"
  if [ $? -ne 0 ]
  then
    echo "$(printlog) ERROR: Failed to Update Record in Audit Table" >>$log
    exit 1
  fi
    echo "$(printlog) INFO:Record Udated into ${audit_table_name} for ${job_id}" >>$log

else
  echo "$(printlog) INFO: Imported Data Successfully to HDFS: ${Database_name}.${table_name}" >>$log
  echo "$(printlog) INFO: sqoop import successfully run">>$log
  #Update record into audit table
  mysql -u${user_name} -p${password} -e "update ${audit_database_name}.${audit_table_name} set last_job_value='${temp}',run_status='COMPLETE' where job_id=${job_id}"
    if [ $? -ne 0 ]
    then
      echo "$(printlog)ERROR: Failed to Update Audit Table" >>$log
      exit 1
    fi
    echo "$(printlog)INFO: Updated Record into ${audit_table_name} for ${job_id}" >>$log
fi


