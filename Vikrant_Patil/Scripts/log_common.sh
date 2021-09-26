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

