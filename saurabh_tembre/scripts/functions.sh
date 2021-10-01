#Function for creating log
function log_function()
{
log_file_location=/home/hadoop/hadoopsession/spark_project
log_date=$(date +'%d_%b_%Y_%H_%M_%S')
exec > >(tee ${0}_${log_date}) 2>&1
}
