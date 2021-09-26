#import log_common file first
. log_common.sh
if [ $? -ne 0 ];
then
  echo "$(printlog) ERROR: Import log_common.sh  failure">>$log
  logpath
  exit 1
else
  echo "$(printlog) INFO: Import  log_common.sh  successfully">>$log
fi

#Create Hive Table for Airport Data
date_time=$(date '+%Y%m%d_%H%M%S')
hive -f Flights_Hive_Table.hql
if [ $? -ne 0 ];
then
  echo "$(printlog) ERROR: Create Flight table in hive  failed">>$log

  exit 1
else
  echo "$(printlog) INFO: Create Flight table in hive successful">>$log
fi