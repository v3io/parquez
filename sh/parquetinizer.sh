#!/bin/bash

#exec &> >(logger -t /home/iguazio/parquez/parquetinizer.sh -s)

utc_time=$(date +%Y-%m-%d-%H-%M)

log_dir=$PWD"/logs/"

mkdir -p ${log_dir}

log_file=${log_dir}"parquetinizer.log."${utc_time}

parquez_dir='/home/iguazio/parquez'

echo parquez_dir:$parquez_dir 2>&1 | tee -a $log_file

kv_table_name=$1

echo kv_table_name:$kv_table_name 2>&1 | tee -a $log_file

kv_window="${2}"

echo kv_window:${kv_window} 2>&1 | tee -a $log_file

historical_window=$3

echo historical_window:$historical_window 2>&1 | tee -a $log_file

partition_by=$4

echo partition_by:$partition_by 2>&1 | tee -a $log_file

v3io_container=$5

echo v3io_container:$v3io_container 2>&1 | tee -a $log_file

hive_schema=$6

echo hive_schema:$hive_schema 2>&1 | tee -a $log_file

compression_type=$7

echo compression_type:$compression_type 2>&1 | tee -a $log_file

parquet_table_name="${kv_table_name}_${compression_type}"

echo coalesce:$coalesce 2>&1 | tee -a $log_file

coalesce=$8

running_user=`whoami`
echo "user is: $running_user" 2>&1 | tee -a $log_file

now=`date -u`

echo "current UTC time: $now"

prev_window=`date -u -d "$now-$kv_window"`
echo "$kv_window ago: $prev_window"

year="20"`date '+%y' -u -d "$prev_window"`
echo "year is: $year"

month=`date '+%m' -u -d "$prev_window"`
echo "month is: $month"

day=`date '+%d' -u -d "$prev_window"`
echo "day is: $day"

hour=`date '+%H' -u -d "$prev_window"`
echo "hour is: $hour"


year_cur="20"`date '+%y' -u -d "$now"`
echo "current year is: $year_cur"

month_cur=`date '+%m' -u -d "$now"`
echo "current month is: $month_cur"

day_cur=`date '+%d' -u -d "$now"`
echo "current day is: $day_cur"

hour_cur=`date '+%H' -u -d "$now"`
echo "current hour is: $hour_cur"


old_window=`date -u -d "$now-$historical_window"`
echo "hitorical window : $old_window"

old_year="20"`date '+%y' -u -d "$old_window"`
echo "year is: $old_year"

old_month=`date '+%m' -u -d "$old_window"`
echo "month is: $old_month"

old_day=`date '+%d' -u -d "$old_window"`
echo "day is: $old_day"

old_hour=`date '+%H' -u -d "$old_window"`
echo "hour is: $old_hour"

if [ $partition_by == 'y' ]
then
    source="v3io://$v3io_container/$kv_table_name/year=$year"
	target="v3io://$v3io_container/$parquet_table_name/year=$year"
	parquetToDelete="v3io://$v3io_container/$parquet_table_name/year=$old_year"
fi

if [ $partition_by == 'm' ]
then
    source="v3io://$v3io_container/$kv_table_name/year=$year/month=$month"
	target="v3io://$v3io_container/$parquet_table_name/year=$year/month=$month"
	parquetToDelete="v3io://$v3io_container/$parquet_table_name/year=$old_year/month=$old_month"
fi

if [ $partition_by == 'd' ]
then
    source="v3io://$v3io_container/$kv_table_name/year=$year/month=$month/day=$day"
	target="v3io://$v3io_container/$parquet_table_name/year=$year/month=$month/day=$day"
	parquetToDelete="v3io://$v3io_container/$parquet_table_name/year=$old_year/month=$old_month/day=$old_day"
fi

if [ $partition_by == 'h' ]
then
    source="v3io://$v3io_container/$kv_table_name/year=$year/month=$month/day=$day/hour=$hour"
	target="v3io://$v3io_container/$parquet_table_name/year=$year/month=$month/day=$day/hour=$hour"
	parquetToDelete="v3io://$v3io_container/$parquet_table_name/year=$old_year/month=$old_month/day=$old_day/hour=$old_hour"
fi

echo "source is: $source" 2>&1 | tee -a $log_file

echo "target is: $target" 2>&1 | tee -a $log_file

echo "parquetToDelete is: $parquetToDelete" 2>&1 | tee -a $log_file

while [[ 1 ]]
	do
		ps -f | grep parquez-assembly ; ret=$?
		if  [[ $ret ]]
		then
			echo "No parquez-assembly process is running. Continue..." 2>&1 | tee -a $log_file
			break
		fi
		echo "another parquez-assembly is already running ... sleep for 10 secs before retrying" 2>&1 | tee -a $log_file
		sleep 10

	done
##########################################################################
# build KV where clause
clause="where year >= $year"
base_or_clause="where year >= $year"
or_left=""
or_right=""
range_flip=0
if [ $month -eq $month_cur ]
then
        clause="$clause AND month >= $month"
        base_or_clause="$base_or_clause AND month >= $month"
else
        or_left="month >= $month"
        or_right="month <= $month_cur"
        if [ $month_cur -lt $month ]
        then
                range_flip=1
        else
                clause="$clause AND ( month >= $month AND month <= $month_cur )"
        fi
fi

if [ $day -eq $day_cur ]
then
        clause="$clause AND day >= $day"
        base_or_clause="$base_or_clause AND day >= $day"
else
        day_cond_left="day >= $day"
        day_cond_right="day <= $day_cur"
        if [ -z "$or_left" ]
        then
                or_left=$day_cond_left
                or_right=$day_cond_right
        else
                or_left="$or_left AND $day_cond_left"
                or_right="$or_right AND $day_cond_right"
        fi
        if [ $day_cur -lt $day ]
        then
                range_flip=1
        else
                clause="$clause AND ( day >= $day AND day <= $day_cur )"
        fi
fi

if [ $hour -eq $hour_cur ]
then
        clause="$clause AND hour > $hour"
        base_or_clause="$base_or_clause AND hour > $hour"
else
        hour_cond_left="hour > $hour"
        hour_cond_right="hour <= $hour_cur"
        if [ -z "$or_left" ]
        then
                or_left=$hour_cond_left
                or_right=$hour_cond_right
        else
                or_left="$or_left AND $hour_cond_left"
                or_right="$or_right AND $hour_cond_right"
        fi
        if [ $hour_cur -lt $hour ]
        then
                range_flip=1
        else
                clause="$clause AND ( hour > $hour AND hour <= $hour_cur )"
        fi

fi

if [[ -n "$or_left" &&  $range_flip -eq 1 ]]
then
        clause="$base_or_clause AND ( ( $or_left ) OR ( $or_right ) )"
fi

echo "query: $clause" 2>&1 | tee -a $log_file
##################################################################################################

spark_command="/spark/bin/spark-submit --driver-memory 8g --class io.iguaz.v3io.spark2.tools.KVTo${compression_type} /v3io/${v3io_container}/v3io-spark2-tools_2.11.jar ${source} ${target} ${coalesce}"

spark_command | tee -a $log_file

if [ $? -eq 0 ]; then
    echo KV to parquet finished with success 2>&1 | tee -a $log_file
else
    echo KV to parquet finished with failed 2>&1 | tee -a $log_file
    exit 1
fi

popd

alter_view_command="${parquez_dir}/sh/alter_kv_view.sh ${kv_table_name} '${kv_window}'"

echo ${alter_view_command} 2>&1 | tee -a $log_file

eval ${alter_view_command} 2>&1 | tee -a $log_file

${parquez_dir}/sh/hive_partition.sh add $hive_schema $parquet_table_name $year $month $day $hour ${target} $partition_by

kvDeleteCommand="hdfs dfs -rm -R ${source}"

kubectl -n default-tenant exec -it $shell_container -- /bin/bash -c "$kvDeleteCommand"  2>&1 | tee -a $log_file

parquetDeleteCommand="hdfs dfs -rm -R ${parquetToDelete}"

kubectl -n default-tenant exec -it $shell_container -- /bin/bash -c "$parquetDeleteCommand" 2>&1 | tee -a $log_file

${parquez_dir}/sh/hive_partition.sh drop $hive_schema $parquet_table_name $old_year $old_month $old_day $old_hour $target $partition_by  2>&1 | tee -a $log_file

popd




