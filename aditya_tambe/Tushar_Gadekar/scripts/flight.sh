###########################################################
# Script name :flight.sh                                    #
# Autor:       Tushar_Gadekar                               #
# Description: data migration using bash                    #
# creator:   Tushar G
# Created date: 16-Sep-2021
#                                                           #
###########################################################

echo -e "\e[34mWelcome to migration Project_2\e[0m"
##!/bin/bash
date=$(date +"%F")
id=$(date +"%F%T")
Script_name="flight.sh"
localpath="/home/hadoop/Hadoop_21/Project2/split_data_files/flights.csv"
hdfspath="/user/flight_data/local/${date}/"
LOGFILE="${id}.${Script_name}.log"


echo "${LOGFILE}:data Migration Bash Run"
##split file using date
awk -F', ' '
  ($8 ==01/01/2020)
  { print}' /home/hadoop/Hadoop_21/Project2/Fulltabledata/flights.csv > ${localpath}
  #/home/hadoop/Hadoop_21/Project2/split_data_files/flights.csv > /home/hadoop/Hadoop_21/Project2/split_data_files/file.csv

## sqoop import implemantation
echo "${LOGFILE}**************** mkdir ${hdfspath} to hdfs location ******************"


#!/bin/bash
if [ ! -d "$hdfspath" ]; then
  hadoop fs -mkdir -p "$hdfspath"
fi
echo "${LOGFILE}****************copyFromLocal to hdfs location ******************"
hadoop fs -copyFromLocal -f ${localpath} ${hdfspath}
##hadoop fs -copyFromLocal -p "$localpath" "$hdfspath"

## Error Handling
if [ $? -ne 0 ]; then
    echo "${LOGFILE}:Error: while copyFromLocal job:${localpath}.${hdfspath}"
    exit 1
fi
echo "${LOGFILE}:INFO: Successfuly data load in HDFS location for:${Script_name}"








###split file using date
#awk -F', ' '
#  ($8 ==01/01/2020)
#  { print}' /home/hadoop/Hadoop_21/Project2/split_data_files/flights.csv > /home/hadoop/Hadoop_21/Project2/split_data_files/file.csv
#
###split file using line count
#echo -e "\e[34mWelcome to migration Project_2\e[0m"
##!/bin/bash
#FILENAME="/home/hadoop/Hadoop_21/Project2/Fulltabledata/airports.csv"
#HDR=$(head -1 $FILENAME)   # Pick up CSV header line to apply to each file
#split -l 182 $FILENAME xyz  # Split the file into chunks of 182 lines each
#n=1
#for f in xyz*              # Go through all newly created chunks
#do
#   echo $HDR > /home/hadoop/Hadoop_21/Project2/split_data_files/flight${n}.csv    # Write out header to new file called "Part(n)"
#   cat $f >> /home/hadoop/Hadoop_21/Project2/split_data_files/flight${n}.csv      # Add in the 182 lines from the "split" command
#   rm $f                   # Remove temporary file
#   ((n++))                 # Increment name of output part
#done
