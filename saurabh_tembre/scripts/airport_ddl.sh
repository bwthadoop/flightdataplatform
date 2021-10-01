$1=spark
$2=airports

hive -e "create database if not exists ${1}"

#create hive external table
hive -e " create table if not exists ${1}.airports_temp
         (
         airport_id int,
         city string,
         state string,
         name string
         )
         row format delimited
         fields terminated by ','
         lines terminated by '/n'
         "

hive -e " create external table if not exists ${1}.airports
         (
         airport_id int,
         city string,
         name string
         )
         partitioned by(state string)
         row format delimited
         fields terminated by ','
         lines terminated by '/n'
         location '${target_base_path}/${2}'
         "
read -p "Enter column for Partition: " partition
$3=partition

hive -e " set hive.exec.dynamic.partition=true;
          set hive.exec.dynamic.partition.mode=nonstrict;
          set hive.support.quoted.identifiers=none;
          insert overwrite ${1}.${2} partition(${3}) select \`(${3})?+.+\`,${3} from ${1}.${airport_temp};
          "
if [ $? -ne 0 ]
then
  echo -e ${ERROR} " Failed to Insert into Hive Partition Table"
    exit 1
fi
  echo -e ${INFO}" Record Inserted into Hive Partition Table"