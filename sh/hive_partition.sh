#!/usr/bin/env bash
# Copyright 2019 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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

HIVE_PATH='kubectl -n default-tenant exec $(kubectl -n default-tenant get pods --no-headers -o custom-columns=":metadata.name" | grep shell)  -- /bin/bash -c "/hive/bin/hive -hiveconf hive.metastore.uris=thrift://hive:9083 '

echo 'HIVE PATH:'${HIVE_PATH}

if [[ ${partition_by} == 'y' ]]
then
        partition_to_delete="year=$year"
fi

if [[ ${partition_by} == 'M' ]]
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
        exec_command=${HIVE_PATH}" -e 'alter table ${hive_schema}.${parquet_table_name} add partition (${partition_to_delete}) location \\\"${target}\\\";'\""
fi

if [[ ${alter_command} == 'drop' ]]
then
        exec_command=${HIVE_PATH}" -e 'alter table $hive_schema.$parquet_table_name drop partition (${partition_to_delete}) ;'\""
fi

echo 'execution command: ' ${exec_command}

eval ${exec_command}