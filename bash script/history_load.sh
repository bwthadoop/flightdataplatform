
##############################################################################
# script Name : History_load.sh                                             #
# Author      : Prince kr. Mishra                                           #
# Create data : 2021-09-18                                                  #
# Description : Import data into HDFS location from mysql using sqoop import #
#             [History load job]  on-time job                                #
###############################################################################


if [ -z $1 ] ; then
  echo "${datetime} Error :Please Provide Database Name!" && exit 1; >>$log
fi

if [ -z $2 ] ; then
  echo "${datetime} Error :Please Provide Table Name!" && exit 1; >>$log
fi


if [ -z $3 ] ; then
  split=""
else
	split_column=$3
	split="--split-by ${split_column}"
fi
mysql_db=$1
mysql_tb=$2
audit_tb=audit_table

#log Create
#!/bin/bash

script_run_date=$(date '+%d_%m_%Y_%H:%M:%S')

mkdir -p /home/hadoop/script_log/full_load/${mysql_tb}

log=/home/hadoop/script_log/full_load/${mysql_tb}/${script_run_date}.txt
exec > >(tee -a /home/hadoop/script_log/full_load/${mysql_tb}/${script_run_date}.txt) 
exec 2>&1

 echo datetime= "${datetime}" >>$log
 echo "mysql_db_name :-   ${mysql_db}" >>$log
 echo "mysql_tb_name :-   ${mysql_tb}" >>$log
 echo ""
 echo ""

# Script Running vailadetion

result=$(mysql -uroot -ppassword -e " select status from  flight_data.audit where status ='running' limit 1");

final_result=$(echo $result|cut -d" " -f2)
echo $final_result

VAR1=${final_result}
VAR2="running"


if [ "$VAR1" = "$VAR2" ]; then
    echo "bash script is already running" >>$log
   exit 1
fi

 # Create HDFS Location
D=$(date '+%d-%m-%Y')
hadoop fs -rm -r /project2/airport_data/RDBMS/

hadoop fs -mkdir -p /project2/airport_data/RDBMS/${D}/file_name=${mysql_tb}.csv
target_dir_path=/project2/airport_data/RDBMS/${D}/file_name=${mysql_tb}.csv

. /home/hadoop/project2/config/credential_config.config

if [ $? -ne 0 ]
then
echo  "ERROR:failed to import credential_config.config file" >>$log
exit 1
fi

echo "INFO:successfully import credential_config.config file" >>$log

job_id=$(date '+%H%M%S')
job_name="${mysql_tb}"

# insert record into audit table

mysql -u${user_name} -p${auth_value} -e " insert into ${mysql_db}.${audit_tb} (job_id,job_name,status,job_start_time) values(${job_id},'${job_name}','running',now())"

if [ $? -ne 0 ]; then

  echo "${datetime} INFO : [error] : to insert record into audit_table" >>$log
fi
echo  "${datetime} INFO : [successfully] : insert record into audit_table for job_id : ${job_id}" >>log


# Sqoop Import Command 

sqoop import --connect "jdbc:mysql://${host_name}:${port_name}/${mysql_db}" --username ${user_name} --password ${auth_value} --table ${mysql_tb} --target-dir ${target_dir_path} --delete-target-dir --split-by ${split_column} ; 


if [ $? -ne 0 ]; then

  echo "${datetime} INFO : [error] : failed to import data in hdfs location "  >>$log

mysql -u${user_name} -p${auth_value} -e "update ${mysql_db}.${audit_tb}  set status='failed' where job_id=${job_id} and job_name='${job_name}'"

  exit 1

fi
echo  "${datetime} INFO : [successfully] : import record hdfs location : ${job_id} : ${target_dir_path} " >>$log
    
##Maintain Audit Table

mysql -u${user_name} -p${auth_value} -e "update ${mysql_db}.${audit_tb}  set status='success',job_end_time = now() where job_id=${job_id} and job_name='${job_name}'"

if [ $? -ne 0 ]; then

  echo "${datetime} INFO : [error] : to insert record into audit_table" >>$log

fi
echo  "${datetime} INFO : [successfully] : insert record into audit_table for job_id : ${job_id}" >>$log

