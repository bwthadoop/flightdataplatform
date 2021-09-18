#!/usr/bin/env bash
##############################################################################
# script name : run_file_ingestion.sh
# Author      : bwt
# Created Date: 18/09/2021
# Description : Ingest data from local to HDFS
# Usage       : bash run_file_ingestion.sh
##############################################################################
source ~/environment_profile.sh

# initialising the logger
LOGFILE=${SYS_LOG}/"run_file_ingestion_"`date +%Y%m%d_%H%M%S`.log
. ${SYS_ROOT}/aditya_tambe/utils/lib_log.bash
log_init

#
## checking no of parameters passed
#if [ $# -ne 1 ]
#then
#  log_error "Incorrect no of parameters - only 1 parameter required - file_name_prefix"
#  exit 98
#fi

current_date=`date +%Y_%m_%d`
# generate local path
input_dir_path="${INPUT_DATA}/${current_date}"

# generate hdfs location
hadoop_dir_path=/user/root/flightdata/${current_date}/

# create a directory at hdfs location
hadoop fs -mkdir -p ${hadoop_dir_path}
RC=$?
# shellcheck disable=SC1073
if [ ${RC} -ne 0 ]
then
  log_error " At execution hadoop directory creation - Return code ${RC}"
  log_info "Log file path:${LOGFILE}"
  exit ${RC}
fi

# running copy command
if [ -d ${input_dir_path} ]
then
  log_info "Input directory exist: ${input_dir_path}"
  hadoop fs -copyFromLocal ${input_dir_path}/* ${hadoop_dir_path}
  RC=$?
  # shellcheck disable=SC1073
  if [ ${RC} -ne 0 ]
  then
    log_error " At execution of copy file - Return code ${RC}"
    log_info "Log file path:${LOGFILE}"
    exit ${RC}
  fi
else
  log_error "Input directory not exist : ${input_dir_path}"
  exit 99
fi
