#########################################################
#script name : flight_script.sh                            #
#Author name: Harshali Dipnaik                          #
#Date:18-09-2021                                        #
#Description : flight Data Load                        #
#run_command syntax : bash script_name file_source_path overwirite(yes/no) #
#########################################################

if [ $# -eq 2 ]; then
  echo "File Location: "$1
  echo "Overwrite Selection"$2
else
  echo "Please mention file name only"
  exit 1
fi

#create datetime var for log file
date_time=$(date +"%Y-%m-%d %H:%M:%S")
dt_for_log=$(date +%Y%m%d%H%M%S)
current_date=$(date +"%Y_%m_%d")
filedir=$1
overwrite_option=$2

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
job_name="_$1"

#entry in audit table
mysql -u${sql_username} -p${sql_password} -e "insert into ${audit_database_name}.${audit_table_name}(job_id,job_name,run_status) values(${job_id},'${job_name}','RUNNING')"
if [ $? -ne 0 ]; then
  echo ${date_time} "ERROR:" ${bash_name} "Failed to insert record in audit table JOB_ID:${job_id}" >>"${log_location}"
  exit 1
fi
echo ${date_time} "SUCCESS:" ${bash_name} "successfully  inserted record in audit table JOB_ID:${job_id}" >>"${log_location}"

hadoop fs -mkdir -p ${filedir}/${current_date}
if [ $? -ne 0 ]; then
  echo ${date_time} "ERROR:" ${bash_name} "Directory not created" >>"${log_location}"
else
  echo ${date_time} "SUCCESS:" ${bash_name} "${filedir}/${current_date} created successfully" >>"${log_location}"
fi
#hadoop fs -copyFromLocal  ${inputpath} ${filedir}/${current_date}

if [ "$overwrite_option" == "Yes" ]; then
  hadoop fs -copyFromLocal -f ${inputpath} ${filedir}/${current_date}
  if [ $? -ne 0 ]; then
    echo ${date_time} "ERROR:" ${bash_name} "Error:Data not copied" >>"${log_location}"
    #update record in audit table
        mysql -u${username} -p${pass} -e "update ${audit_database_name}.${audit_table_name} set run_status='FAILED' WHERE job_id=${job_id}"
        if [ $? -ne 0 ]; then
            echo ${date_time} "ERROR:" ${bash_name} "Failed to update record in audit table JOB_ID:${job_id}" >>"${log_location}"
            exit 1
        fi
         echo ${date_time} "ERROR:" ${bash_name} "successfully  update record in audit table JOB_ID:${job_id}" >>"${log_location}"
   else
    echo ${date_time} "SUCCESS:" ${bash_name} "Data overwrite successfully" >>"${log_location}"
  fi
else
  hadoop fs -copyFromLocal ${inputpath} ${filedir}/${current_date}
  if [ $? -ne 0 ]; then
    echo ${date_time} "ERROR:" ${bash_name} "Error:Data not copied" >>"${log_location}"
    #update record in audit table
        mysql -u${username} -p${pass} -e "update ${audit_database_name}.${audit_table_name} set run_status='FAILED' WHERE job_id=${job_id}"
        if [ $? -ne 0 ]; then
            echo ${date_time} "ERROR:" ${bash_name} "Failed to update record in audit table JOB_ID:${job_id}" >>"${log_location}"
            exit 1
        fi
         echo ${date_time} "ERROR:" ${bash_name} "successfully  update record in audit table JOB_ID:${job_id}" >>"${log_location}"
    else
    echo ${date_time} "SUCCESS:" ${bash_name} "Data Copied successfully" >>"${log_location}"
  fi
fi

#update success entry in audit table
mysql -u${sql_username} -p${sql_password} -e "update ${audit_database_name}.${audit_table_name} set run_status='COMPLETED' WHERE job_id=${job_id}"
if [ $? -ne 0 ]; then
  echo "MESSAGE:Failed to update record in audit table JOB_ID:${job_id}"
  exit 1
fi

#Message for completion
echo "Bash Executed. Please check" "${log_location}" "for more details."
