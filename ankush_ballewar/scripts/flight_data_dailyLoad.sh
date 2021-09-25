##################################################################################
# script name            : Daily_load                                            #
# Author                 : Ankush Ballewar                                       #
# create date            : 2021-09-17                                            #
# description            : Copy Data from Local to HDFS                          #
# Run command            : bash history.sh                                       #
# Run command Syntax     : bash <script.sh> <Source File path > <target path>    #
# command (overwritten)  : bash <script.sh> <Source File path > <target path> <y>#
# Example                : bash history.sh  /home/hadoop/myfile  /user/myfile    #
# Example (overwritten)  : bash history.sh  /home/hadoop/myfile  /user/myfile <y>#
##################################################################################


source_file=${1}
target_file=${2}
arg3=${3}
date=$(date +"%F")
date_time=$(date '+%Y%m%d_%H%M%S')
file_name=$(echo ${1} | cut -d"." -f1 )
file_ext=$(echo ${1} | cut -d"." -f2 )
filename=${file_name}_${date_time}.${file_ext}

#Checking Parameters
if [ "$#" -lt 2 ];
then
    echo -e "\e[0;31mERROR: check the number of parameter\e[0m"
    exit 1
else
    echo -e "\e[0;32mINFO: number of parameter is correct\e[0m"
fi

#Importing flight_job.config file
. flight_job.config
if [ $? -ne 0 ];
then
   echo -e "\e[0;31mERROR: flight_job file not imported successfully\e[0m"
   exit 1

else
echo -e "\e[0;32mINFO: flight_job file imported successfully\e[0m "
fi

#creating directory on HDFS Location
#creating directory on HDFS Location
hadoop fs -mkdir -p ${target_path}/${2}
if [ $? -ne 0 ];
then
   echo -e "\e[0;31mERROR: directory not created Successfully\e[0m"
  exit 1
else
echo -e "\e[0;32mINFO: directory created Successfully\e[0m"
fi

# Copy data from local to HDFS

  hadoop fs -copyFromLocal ${source_path}/${1} ${target_path}/${2}/${filename}
  if [ $? -ne 0 ];
  then
     echo -e "\e[0;31mERROR: data not copied Successfully\e[0m"
     exit 1
  else
   echo -e "\e[0;32mINFO: data copied Successfully\e[0m"
  fi
if [[ "$arg3" == "y" || "$arg3" == "yes" || "$arg3" == "YES" ]];
then
  hadoop fs -copyFromLocal -f ${source_path}/${1} ${target_path}/${2}/${filename}
  if [ $? -ne 0 ];
  then
    echo -e "\e[0;31mERROR: data overwritten failed\e[0m"
     exit 1
  else
    echo -e "\e[0;32mINFO: data overwritten Successfully\e[0m"
  fi
fi