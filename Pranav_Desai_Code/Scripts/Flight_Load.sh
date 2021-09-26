########################################################################
# Script Name : FlightIncremental_load.sh                                                             #
# Author      : Pranav_Desai                                                            #
# date        : 2021-09-20                                                               #
# Dataset     : Flight Dataset                                                          #
# Description : bash script to import incremental load into HDFS Location  #
#Run command Syntax      : bash <Script_Name> <db_name> <table_name> <user_input> #
#Run command             : bash FlightIncremental_load.sh flight_data Flights yes         #
##########################################################################################
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

#importing job.config file
. Flight_job.config
if [ $? -ne 0 ];
then
  echo "$(printlog)ERROR: Import job.config failure">>$log
  exit 1
else
  echo "$(printlog)INFO: Import job.config successfully">>$log
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

#Create archive directory at HDFS location
hadoop fs -mkdir -p ${hdfs_arc_loc}/flat_files
if [ $? -ne 0 ];
then
  echo "$(printlog)ERROR: Create archive directory failed">>$log
  exit 1
else
  echo "$(printlog)INFO: Create archive directory successful">>$log
fi

#Insert Record into Audit Table
mysql -uroot -ppassword -e "insert into ${audit_database_name}.${audit_table_name}(job_id,job_name,run_date,run_status)values(${job_id},'${job_name}',current_date(),'RUNNING')";
if [ $? -ne 0 ]
then
  echo "$(printlog) ERROR: Failed to Insert Record into Audit Table" >>$log
  exit 1
fi
  echo "$(printlog) INFO:Inserted Record into ${audit_table_name} for ${job_id}" >>$log

#Copy files from local to HDFS archive location
hadoop fs -copyFromLocal ${linux_data_loc}/${input_file} ${hdfs_arc_loc}/flat_files/1_Jan_2020
if [ $? -ne 0 ];
then
  echo "$(printlog) ERROR: ${input_file} already exists">>$log
if [ ${user_input} == "yes" ]
then
hadoop fs -copyFromLocal -f ${linux_data_loc}/1jan.csv ${hdfs_arc_loc}/flat_files/1_Jan_2020
echo "$(printlog) INFO: ${input_file} Overwritten Successfully">>$log
#Update record into audit table
  mysql -u${user_name} -p${password} -e "update ${audit_database_name}.${audit_table_name} set last_job_value='${temp}',run_status='COMPLETE' where job_id=${job_id}"
    if [ $? -ne 0 ]
    then
      echo "$(printlog)ERROR: Failed to Update Audit Table" >>$log
      exit 1
    fi
    echo "$(printlog)INFO: Updated Record into ${audit_table_name} for ${job_id}" >>$log
fi
else
  echo " $(printlog) INFO: Create archive directory successful " >>$log
  #Update record into audit table
    mysql -u${user_name} -p${password} -e "update ${audit_database_name}.${audit_table_name} set last_job_value='${temp}',run_status='COMPLETE' where job_id=${job_id}"
      if [ $? -ne 0 ]
      then
        echo "$(printlog)ERROR: Failed to Update Audit Table" >>$log
        exit 1
      fi
      echo "$(printlog)INFO: Updated Record into ${audit_table_name} for ${job_id}" >>$log
fi


hive -f Flights.hql