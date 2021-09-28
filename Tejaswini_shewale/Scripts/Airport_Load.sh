#importing job.config file
. Flight_job.config
if [ $? -ne 0 ];
then
  echo "ERROR: Import job.config failure"
  exit 1
else
  echo "INFO: Import job.config successfully"
fi

hadoop fs -mkdir -p ${hdfs_arc_loc}/flat_files/${Date}
if [ $? -ne 0 ];
then
  echo "ERROR: Create archive directory failed"
  exit 1
else
  echo "INFO: Create archive directory successful"
fi
hadoop fs -copyFromLocal ${linux_data_loc}/${input_file} ${hdfs_arc_loc}/flat_files/${Date}
if [ $? -ne 0 ];
then
  echo "ERROR: ${input_file} already exists"
echo "Do you want overwrite the existing ${input_file} file"
read user_input
if [ $user_input == "Yes" -o $user_input == "yes" -o $user_input == "y" ]
then
hadoop fs -copyFromLocal -f ${linux_data_loc}/1jan.csv ${hdfs_arc_loc}/flat_files/1_Jan_2020
echo "INFO: ${input_file} Overwritten Successfully"
fi
else
  echo "INFO: Create archive directory successful"
fi