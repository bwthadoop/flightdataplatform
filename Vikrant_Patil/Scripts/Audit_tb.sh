job_name=Airport_Load
table_name=Airport
user_name=root
password=password
audit_database_name=Flight_data
audit_table_name=audit_tb
job_id=$(date '+%H%M%S')
job_name="${job_name}_${table_name}"

#Insert Record into Audit Table
mysql -uroot -ppassword -e "insert into ${audit_database_name}.${audit_table_name}(job_id,job_name,run_date,run_status)values(${job_id},'${job_name}',current_date(),'RUNNING')";
if [ $? -ne 0 ]
then
  echo "ERROR: Failed to Insert Record into Audit Table"
  exit 1
fi
  echo "INFO:Inserted Record into ${audit_table_name} for ${job_id}"

#update if job is failed
mysql -u${user_name} -p${password} -e "update ${audit_database_name}.${audit_table_name} set run_status='FAILED' where job_id=${job_id}"
if [ $? -ne 0 ]
then
  echo "ERROR: Failed to Update Record in Audit Table"
  exit 1
fi
  echo "INFO:Record Udated into ${audit_table_name} for ${job_id}"

#Max Date from Table
temp=$(mysql -u${user_name} -p${password} -e "select max(updated_date) from ${audit_database_name}.${table_name}")
if [ $? -ne 0 ]
then
  echo "ERROR: Failed to get Max Update_date Value"
  exit 1
fi
  echo "INFO: Fetched Updated Date Successfully"


tmp_update_date=$(echo $tmp | cut -d' ' -f2)
echo ${tmp_update_date}
tmp_update_time=$(echo $tmp | cut -d' ' -f3)
update_date=$(echo $tmp_update_date $tmp_update_time)
echo ${update_date}

#Update record into audit table
mysql -u${user_name} -p${password} -e "update ${audit_database_name}.${audit_table_name} set last_job_value='${temp}',run_status='COMPLETE' where job_id=${job_id}"
  if [ $? -ne 0 ]
  then
    echo "ERROR: Failed to Update Audit Table"
    exit 1
  fi
  echo "INFO: Updated Record into ${audit_table_name} for ${job_id}"


