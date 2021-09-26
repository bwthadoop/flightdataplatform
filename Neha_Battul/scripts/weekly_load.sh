
###################################################################
#script name: weekly_load.sh                                      #
#Author     : Miss Neha                                           #
#Create date: 2021-09-16                                          #
#description: Import data into hdfs from mysql using sqoop import #
#               [History_load]                                    #
#                                                                 #
###################################################################
#!/bin/bash


path=/project2/bwt_flightdata/RDBMS
DATETIME=$(date '+%y-%m-%d_%H-%M-%S')
DATE=$(date -I)

#log_file creation

log_file=/home/hadoop/project2/Log_File/${0}${DATETIME}.log

exec > >(tee -a $log_file)
exec 2>&1

if [ $? -ne  0 ]; then
echo "Date: ${DATETIME}"

echo "Script name: ${0}"

echo "${DATETIME} Error : log file can not be imported"
exit 1
fi
echo "Date: ${DATETIME}"

echo "Script name: ${0}"

echo "${DATETIME} INFO : log file is imported successfully"


# Import configuration files

. /home/hadoop/project2/config/credential_config.config
if [ $? -ne 0 ]; then
    echo "${DATETIME} ERROR : failed to import credential_config.config"
    exit 1
fi
    echo "${DATETIME} INFO : credential_config.config Successfully imported"

. /home/hadoop/project2/config/job_config.config
if [ $? -ne 0 ]; then
    echo "${DATETIME} ERROR : failed to import job_config.config"
    exit 1
fi
    echo "${DATETIME} INFO : job_config.config Successfully imported"


#running sqoop import job -> importing tables into HDFS

job_id=$(date '+%H%M%S')
job_name="${job_name}_${table_name}"

#insert record into audit table

mysql -u${username} -p${auth_value} -e "insert into ${audit_dataset_name}.${audit_table_name} (job_id,job_name,status,job_start_time) values(${job_id},'${job_name}','RUNNING',now())"

if [ $? -ne 0 ]; then
    echo "${DATETIME} ERROR : failed to insert records in audit table for job_id:${job_id}"
    exit 1
fi
echo "${DATETIME} INFO : Successfully inserted records in audit table for job_id:${job_id}"


echo "INFO : running sqoop import weekly load command"

sqoop import --connect "jdbc:mysql://${host_name}:${port_name}/${mysql_database_name}" --username ${username} --password ${auth_value} --table ${mysql_table_name} --target-dir ${target_base_path} --split-by ${split_by} -m ${num_mapper} --delete-target-dir

if [ $? -ne 0 ]; then
    echo "${DATETIME} ERROR : failed to import data from mysql table:${mysql_database_name}.$mysql_table_name"
    exit 1
    #if sqoop import job fail then update audit table status failed
    #update record in audit_table

mysql -u${username} -p${auth_value} -e "update ${audit_dataset_name}.${audit_table_name} set status='FAILED' where job_id=${job_id} and job_name='${job_name}'"
    if [$? -ne 0]; then
        echo "${DATETIME} ERROR : failed to update records in audit table for job_id:${job_id}"
        exit 1
    fi
    echo "${DATETIME} INFO : Successfully updated records in audit table for job_id:${job_id}"
    exit 1
fi
echo "${DATETIME} INFO : Successfully imported data from mysql table:${mysql_database_name}.${mysql_table_name}"

#update status entry in audit table
     mysql -u${username} -p${auth_value} -e "update ${audit_dataset_name}.${audit_table_name} set status='COMPLETED' where job_id=${job_id} and job_name='${job_name}'"
    if [ $? -ne 0 ]; then
        echo "${DATETIME} ERROR : failed to update records in audit table for job_id:${job_id}"
        exit 1
    fi
    echo "${DATETIME} INFO : Successfully update record into audit table:${audit_dataset_name}.${audit_table_name} for job_id = ${job_id} "




