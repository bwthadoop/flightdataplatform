
###################################################################
#script name: DDL.hql                                             #                                                                                                                             #
#Author     : Miss Neha                                    		  #
#Create date: 2021-09-16         			                      #
#description: Creating tables into HIVE                           #
#               		         				                  #
###################################################################

#log_file creation

DATETIME=$(date '+%y-%m-%d_%H-%M-%S')
log=/home/hadoop/project2/logs/${0}${DATETIME}.log

exec > >(tee -a $log)
exec 2>&1

if [ $? -ne  0 ]; then
echo "Date: " ${DATETIME}

echo "Script name:  ${0}"

echo "[Error] : log file can not be imported"
exit 1
fi
echo "Date: "${DATETIME}

echo "Script name: ${0}"

echo "[INFO] : log file is imported successfully"


#data ingesting to hive table from

loc=/project2/bwt_flightdata/RDBMS/${DATE}/${mysql_table_name}_${DATETIME}.csv


#creating Database
hive -e "create database IF NOT EXISTS project2;
use project2;
#creating external hive tables for airports data and flights data
create external table if not exists project2.airports
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
location '/project2/bwt_flightdata/RDBMS/2021-09-18/airports_21-09-18_21-38-57.csv''"

if [ $? -ne 0 ]
then
  echo "ERROR : failed to create airport table"
fi
echo "INFO : successfully created airport table"
hive -e "create external table if not exists project2.flights
(
entrydate string,
dayofmonth string,
dayofweek string,
carrier string,
originairportid string,
destairportid string,
depdelay string,
arrdelay string
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile
location '/project2/bwt_flightdata/Linux/2021-09-18/flights_2021-09-18_22-46-55.csv'"

if [ $? -ne 0 ]
then
  echo "ERROR : failed to create flight table"
fi
echo "INFO : successfully created flight table"
