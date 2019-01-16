#!/usr/bin/bash -x

exec &> >(logger -t /home/iguazio/parquez/parquetinizer.sh -s)

kv_table_name=$1

kv_window=$2

running_user=`whoami`
echo "user is: $running_user"

export JAVA_HOME="/usr/lib/jvm/java-1.7.0-openjdk"
export HADOOP_HOME="/opt/hadoop"
export HADOOP_CONF="/opt/hadoop/etc/hadoop/"
export HADOOP_CONF_DIR="/opt/hadoop/etc/hadoop/"

now=`date -u`
#now=`date -u -d "Mon Jan 1 00:11:25 IST 2018"`
#now=`date -u -d "Thu Feb 26 23:11:25 IST 2018"`
echo "current UTC time: $now"

prev_window=`date -u -d "$now-$kv_window"`
echo "3 hours ago: $prev_window"

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

old_window=`date -u -d "$now-7 days"`
echo "7 days ago: $old_window"

old_year="20"`date '+%y' -u -d "$old_window"`
echo "year is: $old_year"

old_month=`date '+%m' -u -d "$old_window"`
echo "month is: $old_month"

old_day=`date '+%d' -u -d "$old_window"`
echo "day is: $old_day"

old_hour=`date '+%H' -u -d "$old_window"`
echo "hour is: $old_hour"



kv_to_delete=$kv_to_delete"/year=$year/month=$month/day=$day/hour=$hour"

/opt/spark2/bin/spark-submit --master yarn --num-executors 24 --executor-memory 5g --driver-memory 1g --conf 'spark.driver.extraJavaOptions=-Dread.partitions=24 -Dmax-in-flight=64' --class io.iguaz.v3io.spark2.tools.DeleteTable /home/iguazio/igz/bigdata/libs/v3io-spark2-tools_2.11.jar $kv_to_delete


