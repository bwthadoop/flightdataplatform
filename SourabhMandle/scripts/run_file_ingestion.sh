###################################################################################
# Script name    :Flight.sh                                                      #
# Author         :Sourabh Mandle                                                  #
# Description    :Flight data migration bash                                      #
# creator        :Sourabh Mandle                                                  #
# Created date   :20-sept-2021                                                    #
#                                                                                 #
###################################################################################

# initializing the log file

#log_date=$(date '+%Y-%m-%d %H:%M:%S')
#log=/home/hadoop/sourabh1/project2/bash/logfiles$0_${log_date}.log

current_date='date +%Y-%m-%d'

echo "-----Importing Credential file-------" #>>$log

. config_files/credential_config.config

if [ $? -ne 0 ]; then
    echo "Error:failed to import credential_config.config file" #>>$log
    exit 1
fi

echo "INFO:successfully import credential_config.config file" #>>$log

echo "-------Importing Job config file------" #>>$log

. config_files/project_job.config

if [ $? -ne 0 ]; then
    echo "Error:failed to import project_job.config file" #>>$log
    exit 1
fi
echo "INFO:successfully imported project_job.config file" #>>$log

#Generate local path
#input_dir_path= "${local_path}"

#Generate hdfs location
#hdfs_dir_path= "${hdfs_path}"


#if [ $? -ne 0 ]; then
#    echo "Error:failed to create directory at hdfs location: ${hdfs_dir_path}" #>>$log
#    exit 1
#fi
#echo "INFO:successfully create directory at hdfs location: ${hdfs_dir_path}" #>>$log

#hadoop fs -test -e /${hdfs_dir_path}
#if [ $? -eq 0 ]
#then
#	echo " file exist in the hdfs direcotry"
#else
#	echo " file doesn't exist in the hdfs directory"
#fi


# copying data from local to hdfs location
#if #[ -d ${hdfs_dir_path} ];

# creating directory at hdfs location
hadoop fs -mkdir -p ${hdfs_path}
if [ $? -eq 0 ]
then
	echo "INFO:successfully created directory on hdfs location: ${hdfs_path}"
else
	echo "Error:failed to create directory on hdfs location: ${hdfs_path}"
fi

# copying data from local to hdfs location

hadoop fs -copyFromLocal -f ${local_path} ${hdfs_path}
if [ $? -eq 0 ]
then
	echo "INFO:successfully copy the data from local to hdfs location: ${hdfs_path}"
else
	echo "Error:failed to copy the data from local to hdfs location: ${hdfs_path}"
fi



# then
   # echo "Info : input directory exist ${local_path}" #>>$log

   # if [ $? -ne 0 ]; then
     #   echo "Error:failed to copy the data from local to hdfs location: ${hdfs_path}" #>>$log
     #   exit 1
   # fi
   # echo "INFO:successfully copy the data from local to hdfs location: ${hdfs_path}" #>>$log

#else
  # echo "Info : input directory does not exist ${local_path}" #>>$log
   #exit 1





