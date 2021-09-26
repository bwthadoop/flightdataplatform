#####################################################################################
# Script Name: history_load.sh                                                      #
#Auther      : Bhagyashri Pasalkar                                                  #
#create date : 16-09-2021                                                           #
#Description : Import data into HDFS location from mysql using sqoop import         #
#              [History load job] On-time job                                       #
#####################################################################################
#file with date
Date=$(date -I)
datetime=$(date +"%Y-%m-%d_%H-%M-%S")

directory= $(hadoop fs -mkdir -p /home/hadoop/batch21/spark_project/${mysql_table_name}/RDBMS/${Date}/${mysql_table_name}_${datetime}.csv)

. /home/hadoop/batch21/spark_project/credential_config.config
if [ $? -ne 0 ]; then
    echo "ERROR: failed to import credential_config.config file"
    exit 1
fi
echo "INFO: successfully imported credential_config.config file"

. /home/hadoop/batch21/spark_project/job_config.config
if [ $? -ne 0 ]; then
    echo "ERROR: failed to import job_config.config file"
    exit 1
fi
echo "INFO: successfully imported job_config.config file"

job_id=$(date '+%H%M%S')
job_name=sqoop_import_"$2"

#insert sqoop import job

mysql -u${username} -p${auth_value} -e "insert into ${audit_dataset_name}.${audit_table_name}(job_id,job_name,status,job_start_time) values(${job_id},'${job_name}','RUNNING',now())"

if [ $? -ne 0 ]; then
    echo "ERROR: failed to insert record into an audit table"
    exit 1
fi
echo "INFO: successfully inserted record into an audit table for job_id:${job_id}"

#Running sqoop import job
echo "INFO :running sqoop import history load command"

sqoop import --connect "jdbc:mysql://${host_name}:${port_name}/${mysql_database_name}" --username ${username} --password ${auth_value} --table ${mysql_table_name} --target-dir ${target_base_path} --delete-target-dir -split-by ${t_column}

if [ $? -ne 0 ]; then
    echo "ERROR: failed to import data from mysql table : $1.$2"

    #update record in audit table
    mysql -u${username} -p${auth_value} -e "update ${audit_dataset_name}.${audit_table_name} set status='FAILED' where job_id=${job_id} and job_name='${job_name}'"
    if [ $? -ne 0 ]; then
        echo "ERROR: failed to update record into an audit table for job_id:${job_id}"
        exit 1
    fi
    echo "INFO: successfully updated record into an audit table for job_id:${job_id}"

    exit 1
fi
echo "INFO: successfully imported data from mysql table :$1.$2"

#retrieve max update timestamp value
#temp_val=$(mysql -u${username} -p${auth_value} -e "select job_start_time) from ${mysql_database_name}.${mysql_table_name}")
#if [ $? -ne 0 ]; then
   # echo "ERROR: failed to get job_start_time value from mysql table: ${mysql_database_name}.${mysql_table_name}"
   # exit 1
#fi
#max_update_date=$(echo ${temp_val} | cut -d' ' -f2)
#max_update_time=$(echo ${temp_val} | cut -d' ' -f3)

#if [ $? -ne 0 ]; then
 #   echo "ERROR: failed to retrieve max update datetime value"
  #  exit 1
#fi
#max_update_datetime="${max_update_date} ${max_update_time}"
#echo "INFO: current job max datetime value: ${max_update_datetime}"

#updated success entry in audit table

mysql -u${username} -p${auth_value} -e "update ${audit_dataset_name}.${audit_table_name} set status='COMPLETED',job_end_time=current_timestamp() where job_id=${job_id} and job_name='${job_name}'"
if [ $? -ne 0 ]; then
    echo "ERROR: failed to update record into an audit table for job_id: ${job_id}"
    exit 1
fi
echo "INFO: successfully update record into audit table: ${audit_dataset_name}.${audit_table_name} for job_id: ${job_id}"