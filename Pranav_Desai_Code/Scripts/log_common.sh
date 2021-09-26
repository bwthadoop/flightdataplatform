log_date=$(date '+%Y%m%d_%H%M%S')
log=/home/hadoop/Project1/logs/$0_${log_date}.log


printlog() {
    echo "$(date '+%Y/%m/%d  %H:%M:%S')  $0 "
}

#error function for file path

logpath() {
   echo " $(printlog) Task Failed.Please see log: $log"
}

# append following data to log file
echo "Database Name:" $1 >>$log
echo "Table Name:" $2 >>$log
echo "Split-by Column Name:"$3 >>$log
echo "Date:" $(date "+%Y-%m-%d %H:%M:%S") >> $log

#importing credentials.config file
. credentials.config
if [ $? -ne 0 ];
then
  echo "$(printlog) ERROR: Import log_common.sh  failure">>$log
   logpath
  exit 1
else
  echo "$(printlog) INFO: Import credentials.config successfully">>$log
fi

#Daemons Validation
. daemon.sh
if ps -p $PID1 && ps -p $PID2 && ps -p $PID3 && ps -p $PID4 && ps -p $PID5
then
   echo "$(printlog) INFO: All required daemons are running" >> $log
else
echo "$(printlog) ERROR: All required daemons are not running" >> $log
echo "$(printlog) INFO: Starting required daemons" >>$log
eval start-all.sh
if [ $? -ne 0 ];
then
  echo "$(printlog) ERROR: Please Check Daemons status">>$log
logpath
  exit 1
else
  echo "$(printlog) INFO: All daemons started successfully">>$log
fi
fi