checkDameonStatus()
{
  jpscheck=$(jps | grep -w NodeManager| cut -d " " -f 2)

  if [ "NodeManager" == "${jpscheck}" ]; then
    echo "$(printlog) INFO: NodeManager is active"
  else
    echo "$(printlog) ERROR: NodeManager is not active"
    echo "$(printlog) Starting NodeManager"
    eval start-all.sh
  fi

  jpscheck=$(jps | grep -w SecondaryNameNode| cut -d " " -f 2)

  if [ "SecondaryNameNode" == "${jpscheck}" ]; then
    echo "$(printlog) INFO: SecondaryNameNode is active"
  else
    echo "$(printlog) ERROR: SecondaryNameNode is not active"
    echo "$(printlog) Starting SecondaryNameNode"
    eval start-all.sh
  fi

  jpscheck=$(jps | grep -w NameNode| cut -d " " -f 2)

  if [ "NameNode" == "${jpscheck}" ]; then
    echo "$(printlog) INFO: NameNode is active"
  else
    echo "$(printlog) ERROR: NameNode is not active"
    echo "$(printlog) Starting NameNode"
    eval start-all.sh
  fi

  jpscheck=$(jps | grep -w DataNode| cut -d " " -f 2)

  if [ "DataNode" == "${jpscheck}" ]; then
    echo "$(printlog) INFO: DataNode is active"
  else
    echo "$(printlog) ERROR: DataNode is not active"
    echo "$(printlog) Starting DataNode"
    eval start-all.sh
  fi

  jpscheck=$(jps | grep -w ResourceManager| cut -d " " -f 2)

  if [ "ResourceManager" == "${jpscheck}" ]; then
    echo "$(printlog) INFO: ResourceManager is active"
  else
    echo "$(printlog) ERROR: ResourceManager is not active"
    echo "$(printlog) Starting ResourceManager"
    eval start-all.sh
  fi


}
##############################################
printlog() {
    echo "$(date '+%Y/%m/%d  TIME : %H:%M:%S')  $0 "
}
###############
printerror() {
 echo "$(printlog) $(tput setaf 1) $(tput bold) ERROR: ${1} $(tput sgr 0) "
}
##############



checkstatus() {
  if [ $? -ne 0 ];
  then
      printerror "${2}"
      printlogpath
      exit 1
   else
    echo "$(printlog) INFO:${1}"
  fi
}


########jobstatus
job_failure() {
  #update job_status that job is failed
  . job.config
        mysql --user=${username} --password=$(cat ${sql_password}) -e "update ${audit_database_name}.${audit_tablename} set job_status='FAILED' WHERE job_id=${jobid}"
    if [ $? -ne 0 ]; then
      printerror "Failed to update job_status(failed) into ${audit_tablename} for ${jobid}"
      printlogpath
      exit 1
    fi
    echo "$(printlog) INFO: Job_status updated successfully into ${audit_tablename} for ${jobid}"
      printlogpath
}


