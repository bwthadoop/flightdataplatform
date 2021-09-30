#Function for creating log
function log_function()
{
log_file_location=/home/hadoop/hadoopsession/spark_project
log_date=$(date +'%d_%b_%Y_%H_%M_%S')
exec > >(tee ${0}_${log_date}) 2>&1
}

#message function
function message()
{
  ERROR='\033[1;91m\e[5m\033[5mERROR\033[0m'
  INFO='\033[0;32m\033[5mINFO\033[0m'
}
