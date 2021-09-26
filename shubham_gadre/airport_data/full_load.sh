#######################################################################################
# Script Name  : full_load.sh                                                     #
# Author       : Shubham Gadre                                                        #
# Date created : 2021-09-19                                                           #
# Discription  : Ingestion of data from rdbms to hdfs                                 #
#######################################################################################

datetime=$(date '+%d-%m-%Y %H:%M:%S')
date=$(date '+%d-%b')

if [ -z $1 ] ; then
  echo "${datetime} Error :Please Provide Database Name!" && exit 1;
fi

if [ -z $2 ] ; then
  echo "${datetime} Error :Please Provide Table Name!" && exit 1;
fi

mysql_db=$1
mysql_tb=$2
// *************************************************************************************************************************************

. /home/hadoop/airport_data/airportjob.config

if [ $? -ne 0 ]
 then
    echo  "${datetime}:Failed to import Config file"
    exit 1
fi
echo "${datetime} : Successfully imported config file"

// *******************************************************************************************************************************************************************************

job_id=$(date '+%H%M%S')
job_name="data_ingestion_${mysql_tb}"

mysql -u${user} -p${pass} -e "insert into ${audit_db_name}.${audit_tb_name} ( job_id, job_name,status) values(${job_id},'${job_name}','Running')"

if [ $? -ne 0 ]
 then
   echo "${datetime} : Failed to insert record into audit_table"
   exit 1

fi
echo "${datetime} : Successfully inserted records into audit_table for job_id : ${job_id}"
// ***********************************************************************************************************************************************************************************************************

sqoop import --connect "jdbc:mysql://${host_name}:${port_name}/${mysql_db}" --username ${user} --password-file /passfile/pass.txt --table ${mysql_tb} --target-dir ${target_location}/${date} --delete-target-dir

// *************************************************************************************************************************************************************************************************************

currenttime=$(date '+%Y-%m-%d %H:%M:%S')
mysql -u${user_name} -p${pass} -e "update ${audit_db_name}.${audit_tb_name}  set run_status='completed' , job_end_time='${currenttime}' where job_id=${job_id} and job_name='${job_name}'"

   if [ $? -ne 0 ]
 then
   echo "${datetime} : Failed to updated record audit_table :${job_id}"
   exit 1

fi
echo "${datetime}: Updated record into audit  table : ${audit_db_name}.${audit_tb_name} : for job_id : ${job_id}"