#!/usr/bin/env bash

alter_command=$1
echo 'alter table command :'${alter_command}

hive_schema=$2
echo 'hive schema:'${hive_schema}

parquet_table_name=$3
echo 'parquet table name:'${parquet_table_name}

year=$4
echo 'year:'${year}

month=$5
echo 'month:'${month}

day=$6
echo 'day:'${day}

hour=$7
echo 'hour:'${hour}

target=$8
echo 'target: '${target}

partition_by=$9
echo 'partition by:'${partition_by}

HIVE_PATH='hive'
echo 'HIVE PATH:'${HIVE_PATH}

if [[ ${partition_by} == 'y' ]]
then
        partition_to_delete="year=$year"
fi

if [[ ${partition_by} == 'm' ]]
then
        partition_to_delete="year=$year, month=$month"
fi

if [[ ${partition_by} == 'd' ]]
then
        partition_to_delete="year=$year, month=$month, day=$day"
fi

if [[ ${partition_by} == 'h' ]]
then
        partition_to_delete="year=$year, month=$month, day=$day, hour=$hour"
fi

echo 'Partition to delete '${partition_to_delete}

if [[ ${alter_command} == 'add' ]]
then
        exec_command=${HIVE_PATH}" -e \"alter table $hive_schema.$parquet_table_name add partition (${partition_to_delete}) location '${target}';\""
fi

if [[ ${alter_command} == 'drop' ]]
then
        exec_command=${HIVE_PATH}" -e \"alter table $hive_schema.$parquet_table_name drop partition (${partition_to_delete}) ;\""
fi

if [[ ${alter_command} == 'drop' ]]
then
        exec_command=${HIVE_PATH}" -e \"alter table $hive_schema.$parquet_table_name drop partition (${partition_to_delete}) ;\""
fi

echo 'execution command: ' ${exec_command}

eval ${exec_command}