 #####################################################
# Script Name : incremental.sh
# Author      : Govind Pal
# Create Date : 16-09-2021
# Description : Import data into HDFS location from linux using copy from local
#####################################################

##!/bin/bash
#hadoop@hadoop:~/flight_project/script$ bash copytohdfs.sh
# hadoop@hadoop:~$ hadoop fs -ls /user/flight_data

#/user/flight_data/airport_tb
 #/user/flight_data/flight_tb

#hadoop fs -cat /user/flight_data/2021-09-18 |head -n 10
#hadoop fs -ls /user/flight_data


# Logs creation

logcreation()
{
LOG_FILE=/home/hadoop/flight_project/log/$0
log_id=$(date +"%F-%T")
exec > >(tee ${LOG_FILE}_${log_id}) 2>&1
}
logcreation
echo "Script Name : " $0
echo "Date : " $(date)

# Date format variable

Date_type=$(date +"%F")

# Create directory for current date

hadoop fs -mkdir -p /flights_data/${Date_type};

if [ $? -ne 0 ]
then
  echo "ERROR : failed to create directory for date : ${Date_type}"
fi
echo "INFO : successfully created directory for date : ${Date_type}"

# Copy data for particular date from linux to HDFS


hadoop fs -copyFromLocal /home/hadoop/flight_project/data/01-01-2020.csv /user/flight_data/flight_tb/${Date_type};

if [ $? -ne 0 ];
then
  echo "Error:Failed to copy the data"

# To resolve the file already exist error, take user input to overwrite the data

  read -p "Do you want to overwrite? :" userinput

  if [ $userinput == 'Y' ];
  then
   hadoop fs -copyFromLocal -f /home/hadoop/flight_project/data/01-01-2020.csv /user/flight_data/flight_tb/{Date_type};

   echo "Info: Successfully overwrite the data"

  fi

  if [ $userinput == 'N' ];
      then
         echo "Info:Failed to copy the data"

  fi

fi

