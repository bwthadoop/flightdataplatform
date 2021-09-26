###################################################################
#script name: DDL.hql                                                                                                                                                                          #
#Author     : Bhagyashri Pasalakr                                          #
#Create date: 2021-09-18                                        #
#description: Import data into hdfs from mysql using sqoop import #
#               [History_load job]                                #
#                                                                 #
###################################################################

#log_file creation
DATETIME=$(date '+%y-%m-%d_%H-%M-%S')
log=/home/hadoop/spark_project/logfile/"incremental_load"${DATETIME}.log

exec > >(tee -a $log)
exec 2>&1

if [ $? -ne  0 ]; then
echo "Date: " ${DATETIME} >>$log

echo "Script name:  incremental_load.sh" >>$log

echo "[Error] : log file can not be imported" >>$log
exit 1
fi
echo "Date: "${DATETIME} >>$log

echo "Script name: incremental_load.sh" >>$log

echo "[INFO] : log file is imported successfully" >>$log

#data ingesting to hive table from
loc=/spark_project/bwt_flightdata/RDBMS/${DATE}/${mysql_table_name}_${DATETIME}.csv

#creating external hive tables for airports data and flights data

hive -e "create external table if not exists bwt_flightdata_arc.airport_details
(
airport_id string,
city string,
state string,
name string
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile
location '/spark_project/RDBMS/2021-09-18/airport_details_2021-09-1813-14-01.csv" into table airport_details.csv'"


hive -e "create table if not exists bwt_flightdata_arc.flights
(dayofweek string,
carrier string,
originairportid string,
destairportid string,
depdelay string,
arrdelay string,
entrydate string
)
partitioned by (dayofmonth string)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile
location '/spark_project/bwt_flightdata_arc/flights'"


#loading data
loaddata=$(load data inpath '/spark_project/Linux/bwt_flightdata/2021-09-20/flights01-01-2020.csv/flights01-01-2020.csv' into table flights partition (dayofmonth='1'));


if [ $? -ne 0 ]
then
  echo "ERROR : failed to create hive table"
fi
echo "INFO : successfully created hive table"