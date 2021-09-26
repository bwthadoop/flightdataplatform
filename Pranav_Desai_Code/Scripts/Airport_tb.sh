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

mysql -uroot -ppassword < Airport.sql
if [ $? -ne 0 ];
then
  echo "$(printlog) ERROR: Create Airport table  failure">>$log

  exit 1
else
  echo "$(printlog) INFO: Create Airport Table successful">>$log
fi
mysql -uroot -ppassword < Airport_Data_Load.sql
if [ $? -ne 0 ];
then
  echo "$(printlog) ERROR: Data Load in Airport table  failed">>$log

  exit 1
else
  echo "$(printlog) INFO: Data Load in Airport table successful">>$log
fi
. Airport_job.config
hive -f Airport.hql
if [ $? -ne 0 ];
then
  echo "$(printlog) ERROR: Create Airport table in hive  failed">>$log

  exit 1
else
  echo "$(printlog) INFO: Create Airport table in hive successful">>$log
fi
