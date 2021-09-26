#####################################################################################
# Script Name: flight_daily_load.sh                                                 #
#Auther      : Bhagyashri Pasalkar                                                  #
#create date : 17-09-2021                                                           #
#Description : Import data into HDFS location from linux using copyFromLocal         #
#              [Daily_load job]                                                     #
#####################################################################################

#[creating directory with date in HDFS]
Date=$(date +"%F")

datetime=$(date +"%Y-%m-%d_%H%S%M")
#config

. /home/hadoop/batch21/spark_project/fcredential_config.config
if [ $? -ne 0 ]; then
    echo "ERROR: failed to import credential_config.config file"
    exit 1
fi
echo "INFO: successfully imported credential_config.config file"

. /home/hadoop/batch21/spark_project/fjob_config.config
if [ $? -ne 0 ]; then
    echo "ERROR: failed to import job_config.config file"
    exit 1
fi

hadoop fs -mkdir -p /spark_project/bwt_flightdata/Linux/${Date}/flights.csv

hadoop fs -copyFromLocal -f /home/hadoop/batch21/spark_project/flights.csv /spark_project/bwt_flightdata/Linux/${Date}/flights.csv

echo "INFO: successfully imported job_config.config file"