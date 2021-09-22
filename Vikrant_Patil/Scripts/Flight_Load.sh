########################################################################
# Script Name : Flight_load.sh                                        #
# Author      : Vikrant_Patil                                          #
# date        : 2021-09-20                                             #
# Dataset     : Flight Dataset                                        #
# Description : bash script to import partial load into HDFS Location into hive  #
#Run command Syntax      : bash <Script_Name> <db_name> <table_name>  #
#Run command             : bash Flight_Load.sh flight_data flights                         #
########################################################################
#importing job.config file
. Flight_job.config
if [ $? -ne 0 ];
then
  echo "ERROR: Import job.config failure"
  exit 1
else
  echo "INFO: Import job.config successfully"
fi

hadoop fs -mkdir -p ${hdfs_arc_loc}/flat_files
if [ $? -ne 0 ];
then
  echo "ERROR: Create archive directory failed"
  exit 1
else
  echo "INFO: Create archive directory successful"
fi
hadoop fs -copyFromLocal ${linux_data_loc}/${input_data_file} ${hdfs_arc_loc}/flat_files/${input_file}
if [ $? -ne 0 ];
then
  echo "ERROR: ${input_data_file} already exists"
echo "Do you want overwrite the existing ${input_data_file} file"
read user_input
if [ $user_input == "Yes" -o $user_input == "yes" -o $user_input == "y" ]
then
hadoop fs -copyFromLocal -f ${linux_data_loc}/${input_data_file} ${hdfs_arc_loc}/flat_files/${input_file}
echo "INFO: ${input_data_file} Overwritten Successfully"
fi
else
  echo "INFO: Create archive directory successful"
fi


hive -f Flights.hql