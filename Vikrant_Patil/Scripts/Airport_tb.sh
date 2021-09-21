mysql -uroot -ppassword < Airport.sql
if [ $? -ne 0 ];
then
  echo " ERROR: Create Airport table  failure"

  exit 1
else
  echo " INFO: Create Airport Table successful"
fi
mysql -uroot -ppassword < Airport_Data_Load.sql
if [ $? -ne 0 ];
then
  echo " ERROR: Data Load in Airport table  failed"

  exit 1
else
  echo " INFO: Data Load in Airport table successful"
fi
. Airport_job.config
hive -f Airport.hql
if [ $? -ne 0 ];
then
  echo " ERROR: Create Airport table in hive  failed"

  exit 1
else
  echo " INFO: Create Airport table in hive successful"
fi
