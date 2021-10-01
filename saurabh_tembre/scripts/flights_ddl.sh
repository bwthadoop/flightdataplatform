hive -e "create table flights_temp
         (
         DayofMonth	int,
         DayOfWeek	int,
         Carrier string,
         OriginAirportID int,
         DestAirportID int,
         DepDelay int,
         ArrDelay int,
         EntryDate date
         )
         row format delimited
         fields terminated by ','
         lines terminated by '/n'
         "
         load data inpath "${target_base_path}/${2}" into table ${flights_temp};

hive -e " create external table if not exists ${1}.flights
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
read -p "Enter column for Partition: " part_col

hive -e " set hive.exec.dynamic.partition=true;
          set hive.exec.dynamic.partition.mode=nonstrict;
          set hive.support.quoted.identifiers=none;
          insert overwrite ${1}.${2} partition(${part_col}) select \`(${part_col})?+.+\`,${part_col} from ${1}.${flights_temp};
          "
if [ $? -ne 0 ]
then
  echo -e ${ERROR} " Failed to Insert into Hive Partition Table"
    exit 1
fi
  echo -e ${INFO}" Record Inserted into Hive Partition Table"