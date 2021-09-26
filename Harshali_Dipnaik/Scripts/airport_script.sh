#########################################################
#script name : airport_script.sh                             #
#Author name: Harshali Dipnaik                          #
#Date:20-09-2021                                        #
#Description : Import airport data from RDBMS                       #
#run_command syntax : bash script_name.sh database_name table_name split_col #
#########################################################

#validation for script arguments
if [ $# -eq 3 ]; then
    echo "Database Name: "$1
    echo "Table Name: "$2
    echo "Split_by Column Name: "$3
else
    echo "invalid argument please pass three argument(database_name,table_name,split_column_name) "
    exit 1
fi

#Arguments (DB,TBl,COL)
database_name=$1
table_name=$2
split_column_name=$3

#create datetime var for log file
date_time=$(date +"%Y-%m-%d %H:%M:%S")
dt_for_log=$(date +%Y%m%d%H%M%S)

#Get a bash_name
bash_name=$(basename -- "$0" | cut -f 1 -d '.')

#create a variable for log location
log_location="/home/hadoop/AirportDBProject/"${bash_name}"/"${dt_for_log}.log
exec 2>>${log_location}

#importing credential.config file
echo "Importing credential.config file....."
. credential.config
if [ $? -ne 0 ]; then
    echo ${date_time} "ERROR" ${bash_name} "Failed to load credential.config" >>"${log_location}"
    exit 1
fi
echo ${date_time} "SUCCESS:" ${bash_name} "successfully imported credential.config" >>"${log_location}"

#importing job.config file
echo "Importing job.config file....."
. job.config
if [ $? -ne 0 ]; then
    echo ${date_time} "ERROR:" ${bash_name} "Failed to load job.config" >>"${log_location}"
    exit 1
fi
echo ${date_time} "SUCCESS:" ${bash_name} "successfully imported job.config" >>"${log_location}"

job_id=$(date '+%H%M%S')
if [ $? -ne 0 ]; then
    echo ${date_time} "ERROR:" ${bash_name} "Failed to create Job ID" >>"${log_location}"
    exit 1
fi
echo "job id successfully created ${job_id}"
job_name="FullLoad_${table_name}"

#entry in audit table
mysql -u${sql_username} -p${sql_password} -e "insert into ${audit_database_name}.${audit_table_name}(job_id,job_name,run_status) values(${job_id},'${job_name}','RUNNING')"
if [ $? -ne 0 ]; then
    echo ${date_time} "ERROR:" ${bash_name} "Failed to insert record in audit table JOB_ID:${job_id}" >>"${log_location}"
    exit 1
fi
echo ${date_time} "ERROR:" ${bash_name} "successfully  inserted record in audit table JOB_ID:${job_id}" >>"${log_location}"



#Import data in HDFS
echo "Importing Data......"
sqoop import --connect jdbc:mysql://${host_name}:${port_number}/${database_name} --username ${username} --password-file ${passwordfile_location} --table ${table_name}  --target-dir ${target_base_path}/${table_name} --delete-target-dir --split-by ${split_column_name}
if [ $? -ne 0 ]; then
    echo ${date_time} "ERROR:" ${bash_name} "Data load Failed" >>"${log_location}"
    #update record in audit table
    mysql -u${username} -p${pass} -e "update ${audit_database_name}.${audit_table_name} set run_status='FAILED' WHERE job_id=${job_id}"
    if [ $? -ne 0 ]; then
        echo ${date_time} "ERROR:" ${bash_name} "Failed to update record in audit table JOB_ID:${job_id}" >>"${log_location}"
        exit 1
    fi

     echo ${date_time} "ERROR:" ${bash_name} "successfully  update record in audit table JOB_ID:${job_id}" >>"${log_location}"
     exit 1
fi
echo ${date_time} "SUCCESS:" ${bash_name} "Successfully data load" >>"${log_location}"


#update success entry in audit table
mysql -u${sql_username} -p${sql_password} -e "update ${audit_database_name}.${audit_table_name} set run_status='COMPLETED' WHERE job_id=${job_id}"
if [ $? -ne 0 ]; then
    echo "MESSAGE:Failed to update record in audit table JOB_ID:${job_id}"
    exit 1
fi

#Message for completion
echo "Bash Executed. Please check" "${log_location}" "for more details."



