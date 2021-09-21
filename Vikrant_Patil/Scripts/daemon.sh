PID1=$(jcmd | grep DataNode | cut -d " " -f 1)
if ps -p $PID1 > /dev/null
then
   echo "$(printlog) INFO: DataNode is running" >> $log
else
echo "$(printlog) ERROR: DataNode is not running" >> $log
fi
PID2=$(jcmd | grep NameNode | cut -d " " -f 1)
if ps -p $PID2 > /dev/null
then
   echo "$(printlog) INFO: NameNode is running" >> $log
else
echo "$(printlog) ERROR: NameNode is not running" >> $log
fi
PID3=$(jcmd | grep ResourceManager| cut -d " " -f 1)
if ps -p $PID3 > /dev/null
then
   echo "$(printlog) INFO: ResourceManager is running" >> $log
else
echo "$(printlog) ERROR: ResourceManager is not running" >> $log
fi
PID4=$(jcmd | grep NodeManager| cut -d " " -f 1)
if ps -p $PID4 > /dev/null
then
   echo "$(printlog) INFO: Nodemanager is running" >> $log
else
echo "$(printlog) ERROR: Nodemanager is not running" >> $log
fi
PID5=$(jcmd | grep SecondaryNameNode | cut -d " " -f 1)
if ps -p $PID5 > /dev/null
then
   echo "$(printlog) INFO: SecondaryNameNode is running" >> $log
else
echo "$(printlog) ERROR: SecondaryNameNode is not running" >> $log
fi

