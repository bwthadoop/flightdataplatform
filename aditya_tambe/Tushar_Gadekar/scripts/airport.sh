#!/bin/bash
echo -e "\e[34mWelcome to migration Project_2\e[0m"
#!/bin/bash
date=$(date +"%F")
id=$(date +"%F%T")
Script_name="flight.sh"
log_date=$(date +'%Y%m%d_%H%M%S')
LOGFILE="${id}.${Script_name}.log"

##Use $# which is equal to the number of arguments supplied
 if [ "$#" -ne  3 ]
   then
      echo "Usage: Need 3 arguments to run this script"
       exit 1
 else
     echo -e "\e[32m${log_date}:[INFO]:${Script_name}:Argument count correct. Continuing processing..\e[0m"
 fi


# append following data to log file

echo "${LOGFILE}:data Migration Bash Run"

#Bring credentiol_config.config file into Script
. config1/project2_credentiol.config

if [ $? -ne 0 ]; then
   echo -e "\e[1m${log_date}:[Error]:${Script_name}:\e[31mFailed to import project2_credentiol.config file"
    exit 1
fi
echo -e "\e[32m${log_date}:[INFO]:${Script_name}:successfully import project2_credentiol.config file\e[0m"

#Bring project_job.config file into Script
. config1/project2_job.config
if [ $? -ne 0 ]; then
    echo -e "\e[1m${log_date}:[Error]:${Script_name}:\e[31mFailed to import  project2_job.config file"
    exit 1
fi
echo -e "\e[32m${log_date}:[INFO]:${Script_name}:successfully import project2_job.config file\e[0m"

for((i=0;;i++))
do
    day=$(date "+%y%m%d" -d "$4 $i day")
    if [ ${day} -gt $5 ]
    then
       break
    else
        #sql="select pass_id,pass_time,camera_id,camera_code,device_id,server_id,face_img_url,environ_img_url,quality_score,age,gender,attractive,eyeglass,sunglass,smile,mask,race,eyeopen,mouthopen,beard,feature,create_time from brsface.t_person_passinfo_${day} where \$CONDITIONS";

         sqoop import --connect jdbc:mysql://${hostname}:${port_name}/$1 \
                --username ${username}  \
                --password-file ${password_file} \
                --table $2 \
                --fields-terminated-by '\001' \
                --delete-target-dir \
                --target-dir ${target_dir_path}/$2 \
                --split-by $3 \


#        sqoop import --connect jdbc:mysql://${host}:3306/brsface \
#        --username root \
#        --password 123456 \
#        --query "${sql}" \
#        --fields-terminated-by '\001' \
#        --delete-target-dir \
#        --target-dir hdfs://hadoop01:9000/data01/mysql2hdfs/brsface/t_person_passinfo/${day}/ \
#        --split-by pass_id \
#        -m 1
#DayofMonth	DayOfWeek	Carrier	OriginAirportID	DestAirportID	DepDelay	ArrDelay	EntryDateIF NOT EXISTS bwt_flightdata_arc
        echo Sqoop import data:${day} success...

        hive -e "
        create database IF NOT EXISTS bwt_flightdata_arc;
        use bwt_flightdata_arc;


       CREATE  TABLE IF NOT EXISTS bwt_flightdata_arc.temp_flight (
       DayofMonth	            string,
       DayOfWeek              string,
       Carrier                string,
       OriginAirportID        bigint,
       DestAirportID          bigint,
       DepDelay               string,
       ArrDelay               string,
       EntryDate              string)
        row format delimited
        fields terminated by '\001'
        stored as textfile;

        load data inpath "${target_dir_path}/$2/*" overwrite into table bwt_flightdata_arc.temp_flight;

        CREATE  TABLE IF NOT EXISTS bwt_flightdata_arc.flight (
       DayofMonth	            string,
       DayOfWeek              string,
       Carrier                string,
       OriginAirportID        bigint,
       DestAirportID          bigint,
       DepDelay               string,
       ArrDelay               string)
       PARTITIONED BY (EntryDate   string)
        row format delimited
        fields terminated by '\001'
        stored as textfile
        location '${target_dir_path}/$2';

     set hive.exec.dynamic.partition=true;
     set hive.exec.dynamic.partition.mode=nonstrict;

     insert overwrite table flight partition(EnteryDate) select DayofMonth,DayOfWeek,CarrierOrigin,AirportID,DestAirportID,DepDelay,ArrDelay,EntryDate from table temp_flight;
     "
        echo Hive create table add partition: EntryDate=${day} ok...

    fi
done
