###########################################################
# Script name :airport.sh                                    #
# Autor:       Tushar_Gadekar                               #
# Description: data migration using bash                    #
# creator:   Tushar G
# Created date: 17-Sep-2021
#                                                           #
###########################################################

echo -e "\e[34mWelcome to migration Project_2\e[0m"
#!/bin/bash
date=$(date +"%F")
id=$(date +"%F%T")
Script_name="flight.sh"
log_date=$(date +'%Y%m%d_%H%M%S')
LOGFILE="${id}.${Script_name}.log"

##Use $# which is equal to the number of arguments supplied
 if [ "$#" -ne  3 ]
   then
      echo "Usage: Need 3 arguments to run this script"
       exit 1
 else
     echo -e "\e[32m${log_date}:[INFO]:${Script_name}:Argument count correct. Continuing processing..\e[0m"
 fi


# append following data to log file

echo "${LOGFILE}:data Migration Bash Run"

#Bring credentiol_config.config file into Script
. config1/project2_credentiol.config

if [ $? -ne 0 ]; then
   echo -e "\e[1m${log_date}:[Error]:${Script_name}:\e[31mFailed to import project2_credentiol.config file"
    exit 1
fi
echo -e "\e[32m${log_date}:[INFO]:${Script_name}:successfully import project2_credentiol.config file\e[0m"
#Bring project_job.config file into Script
. config1/project2_job.config
if [ $? -ne 0 ]; then
    echo -e "\e[1m${log_date}:[Error]:${Script_name}:\e[31mFailed to import  project2_job.config file"
    exit 1
fi
echo -e "\e[32m${log_date}:[INFO]:${Script_name}:successfully import project2_job.config file\e[0m"


job_id=$(date '+%H%M%S')
# shellcheck disable=SC2154
job_name="${job_name}_${table_name}"

# insert record into audit_tb
# shellcheck disable=SC2154
mysql -u"${username}" -p"${password}" -e "insert into ${audit_database_name}.${audit_table_name}(job_id,job_name
  ,run_date,run_status) values(${job_id},'${job_name}',current_date(),'RUNNING')"
# shellcheck disable=SC2181
## Error Handling
if [ $? -ne 0 ]; then
    echo "${log_date}:Error:${Script_name}:Failed to insert data into audit table"
    exit 1
fi
echo "${log_date}:INFO:${Script_name}:successfully insert data into audit table for job_id=${job_id}"


# Retrive max(update_date) from audit_tb whose run status = Completed
# shellcheck disable=SC2091
# shellcheck disable=SC1007
temp_var= $(mysql -u"${username}" -p"${password}" -e "select run_status from ${audit_database_name}.${audit_table_name} where run_status='COMPLETED' and job_name='${job_name}' order by job_end_time desc limit 1;")

## shellcheck disable=SC2154
#max_job_date=$(echo "${temp_var}" | cut -d' ' -f2)
#echo "$max_job_date"
#max_job_time=$(echo "${temp_var}" | cut -d' ' -f3)
## shellcheck disable=SC2086
#echo $max_job_time
## Error Handling
# shellcheck disable=SC2181
if [ $? -ne 0 ]; then
    echo "${log_date}:Error:${Script_name}:Failed to get last_job_value data into table"
    exit 1
fi
max_job_value="${max_job_date} ${max_job_time}"
echo "INFO:successfuly retrive trim MAX_UPDATE_DATETIME=${max_job_value}"



## sqoop import implemantation
echo "${log_date}:INFO:${Script_name}:****************Runnig sqoop import job for******************"
# sqoop import

        sqoop import --connect jdbc:mysql://${hostname}:${port_name}/$1 \
        --username ${username}  \
        --password-file ${password_file} \
        --table $2 \
        --fields-terminated-by '\001' \
        --delete-target-dir \
        --target-dir ${target_dir_path} \
        --split-by $3 \

# Error Handling
# shellcheck disable=SC2181
if [ $? -ne 0 ]; then
    # shellcheck disable=SC2154

    echo "${log_date}:Error:${Script_name}: while import sqoop job:${database_name}.${table_name}"

    # update record into audit_tb if job failed
    # shellcheck disable=SC2086
    mysql -u${username} -p"${password}" -e "update ${audit_database_name}.${audit_table_name} set run_status='FAILED'
where job_id=${job_id} and job_name='${job_name}'"
    if [ $? -ne 0 ]; then
     echo "${log_date}:Error:${Script_name}:Failed to update data into audit table"
        exit 1
    fi
    echo "${log_date}:INFO:${Script_name}:successfully update data into audit table for job_id=${job_id}"
    exit 1
fi
echo "${log_date}:INFO:${Script_name}:Successfuly data import for ${database_name}.$2 in HDFS location:  ${target_dir_path}"


 hive -e "
        create database IF NOT EXISTS bwt_airportdata_arc;
        use  bwt_airportdata_arc;

       CREATE  TABLE IF NOT EXISTS  bwt_airportdata_arc.airport (
       airport_id	        string,
       city               string,
       state              string,
       name               string)
        row format delimited
        fields terminated by ','
        lines terminated by '\n'
        stored as textfile
        location '${target_dir_path}';

        load data inpath '${target_dir_path}/$2/*' overwrite into table bwt_airportdata_arc.airport;

     "
        echo Hive create table add partition: EntryDate=${day} ok...


# update succes entery in audit_table
# shellcheck disable=SC2086
mysql -u${username} -p"${password}" -e "update ${audit_database_name}.${audit_table_name} set run_status='COMPLETED'
where job_id=${job_id} and job_name='${job_name}'"
# shellcheck disable=SC2181
if [ $? -ne 0 ]; then
    echo "${log_date}:Error:${Script_name}:Failed to update data into audit table"
    exit 1
fi
echo "${log_date}:INFO:${Script_name}:successfully updated record to audit_tb=${audit_database_name}.${audit_table_name} for job_id=${job_id}"

#load data inpath '/user/airport_data/mysql/2021-09-18/*' overwrite into table bwt_airportdata_arc.airport;
