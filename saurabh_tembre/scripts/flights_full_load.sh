###############################################################################
# Project Name: Flight Data Migration                                         #
# Script Name : Load data in HDFS location                                    #
# Author      : Saurabh Tembre                                                #
# Create date : 2021-09-27                                                    #
# Description : Loading Data from Local to into Hive through HDFS             #
###############################################################################

#If condition for credential.config file
. config/credentials.config
if [ $? -ne 0 ]
then
  echo ${ERROR} "Failed to Import Credential.config File"
fi
  echo ${INFO} "Credential.config File Imported Successfully"

#If condition for job.config file
. config/jobs.config
if [ $? -ne 0 ]
then
  echo ${ERROR} "Failed to Import Job.config File"
fi
  echo ${INFO} "Job.config File Imported Successfully"

echo "Copying Flight File to HDFS"
copyFromLocal ${src_path} ${target_base_path}

if [ $? -ne 0 ]
then
  echo -e ${ERROR} "Failed to Load Flight File in HDFS"
fi
  echo ${INFO} "Successfully Loaded Flight File in HDFS"

if [ $? -ne 0 ]
then
  echo -e ${ERROR} " Failed to Create Database in Hive"
fi
  echo -e ${INFO}" Successfully Created Database in Hive"

hive -e "create table flights_temp
         (
         DayofMonth	int,
         DayOfWeek	int,
         Carrier string,
         OriginAirportID int,
         DestAirportID int,
         DepDelay int,
         ArrDelay int,
         EntryDate date
         )
         row format delimited
         fields terminated by ','
         lines terminated by '/n'
         "
hive -e " create external table if not exists ${1}.flights
         (
         airport_id int,
         city string,
         name string
         )
         partitioned by(state string)
         row format delimited
         fields terminated by ','
         lines terminated by '/n'
         location '${target_base_path}/${2}'
         "