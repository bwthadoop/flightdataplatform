
#Hadoop Archive path validation
hadoop fs -test -d ${hdfs_arc_loc}
if [ $? == 0 ]; then
    echo "Hadoop Archive location is present"
else
    echo "hadoop Archive location does not exists"
    hadoop fs -mkdir -p ${hdfs_arc_loc}
fi

#Linux Dataset path Validation

if [ -d "${linux_data_loc}" ]; then
  echo "Linux path exists"
else
   echo "Error: ${linux_data_loc} not found"
    exit 1
fi

# Linux Data files validation

if [ -f "${linux_data_loc}/${Data_file_name}" ];
then
   echo "File ${Data_file_name} exist on ${linux_data_loc} path."
else
   echo "File ${Data_file_name} does ont exist on ${linux_data_loc} path."
   echo "Terminating execution"
   exit 1
fi

#Password File Validation
#Log file path Validation
if [ -d "${log_loc}" ];
then
   echo "${log_loc} path exists."
else
   echo " path dont exist.Creating path."
   mkdir -p ${log_loc}
fi

#