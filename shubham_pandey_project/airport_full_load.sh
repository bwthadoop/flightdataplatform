#################################################
# script name: flightfullload.sh		#
#author      : shubhbham pandey			#
#create_date: 2021-09-16			#
#load_type :fullload_airport_data	
#discription: import data rdbms to hdfs(arc) location	#
#################################################

datetime=$(date '+%d-%m-%Y %H:%M:%S')




run_date=$(date '+%d-%m-%Y')
echo $run_date



if [ -z $1 ] ; then
  echo "${datetime} Error :Please Provide Database Name!" && exit 1;
fi

if [ -z $2 ] ; then
  echo "${datetime} Error :Please Provide Table Name!" && exit 1;
fi


if [ -z $3 ] ; then
  split=""
else
	split_column=$3
	split="--split-by ${split_column}"
fi
mysql_db=$1
mysql_tb=$2
audit_tb=audit


######



#!/bin/bash

script_run_date=$(date '+%d_%m_%Y_%H:%M:%S')

mkdir -p /home/hadoop/script_logf/fullload/${mysql_tb}

log=/home/hadoop/script_logf/fullload/${mysql_tb}/${script_run_date}.txt
exec > >(tee -a /home/hadoop/script_logf/fullload/${mysql_tb}/${script_run_date}.txt)
exec 2>&1




 echo datetime= "${datetime}" >>$log
 echo "mysql_db_name :-   ${mysql_db}" >>$log
 echo "mysql_tb_name :-   ${mysql_tb}" >>$log
 echo ""
 echo ""




#######
 result=$(mysql -uroot -ppassword -e " select status from  flight_data.audit where status ='running' limit 1");

final_result=$(echo $result|cut -d" " -f2)
echo $final_result

VAR1=${final_result}
VAR2="running"


if [ "$VAR1" = "$VAR2" ]; then
    echo "bash script is already running" >>$log
   exit 1
fi
######





dat=$(date '+%d-%m-%Y')


hadoop fs -mkdir -p /project1/airport_data/RDBMS/${dat}/FILE_NAME=${mysql_tb}.csv
target_location=/project1/airport_data/RDBMS/${s}/FILE_NAME=${mysql_tb}.csv



. /home/hadoop/project1/shell/credintial.config

## username,password
if [ $? -ne 0 ]
 then
    echo  "${datetime} INFO : [error] : failed to import credential.config.file" >>$log
    exit 1
fi
echo "${datetime} INFO : [successfully] : imported credential_config.config file" >>$log

#. /home/hadoop/project1/shell/job.config
#if [ $? -ne 0 ]; then
#echo " ${datetime} INFO : [error] : failed to import  job_config.file"
 #  exit 1

#fi
#echo " ${datetime} INFO : [successfully] : imported job.config file "

#####

job_id=$(date '+%H%M%S')
job_name="${mysql_tb}"

# insert record into audit table

mysql -u${user_name} -p${auth_value} -e " insert into ${mysql_db}.${audit_tb} (job_id,job_name,status,job_start_time) values(${job_id},'${job_name}','running',now())"

if [ $? -ne 0 ]; then

  echo "${datetime} INFO : [error] : to insert record into audit_table" >>$log
  exit 1

fi
echo  "${datetime} INFO : [successfully] : insert record into audit_table for job_id : ${job_id}" >>$log

##

sqoop import --connect "jdbc:mysql://${host}/${mysql_db}"  --username ${user_name} --password ${auth_value} --table ${mysql_tb} --target-dir ${target_location}  --delete-target-dir  ${split} 

if [ $? -ne 0 ]; then

  echo "${datetime} INFO : [error] : failed to import data in hdfs location " >>$log

mysql -u${user_name} -p${auth_value} -e "update ${mysql_db}.${audit_tb}  set status='failed' where job_id=${job_id} and job_name='${job_name}'"

  exit 1

fi
echo  "${datetime} INFO : [successfully] : import record hdfs location : ${job_id} : ${target_location} " >>$log

mysql -u${user_name} -p${auth_value} -e "update ${mysql_db}.${audit_tb}  set status='success' , job_end_time=now()  where job_id=${job_id} and job_name='${job_name}'"

if [ $? -ne 0 ]; then

  echo "${datetime} INFO : [error] : to insert record into audit_table" >>$log
  exit 1

fi
echo  "${datetime} INFO : [successfully] : insert record into audit_table for job_id : ${job_id}" >>$log






















