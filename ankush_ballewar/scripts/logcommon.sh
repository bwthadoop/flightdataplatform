CheckDaemons()
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
printlog() {
    echo "$(date '+%Y/%m/%d  TIME : %H:%M:%S')  $0 "
}
