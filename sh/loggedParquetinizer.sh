#!/usr/bin/env

parquez_dir='/home/iguazio/parquez'

ts=$(date +"%Y%m%d%H%M%S")

echo $ts

${parquez_dir}/sh/parquetinizer.sh ${1} "${2}" "${3}" ${4} ${5} ${6}  2>&1 | tee /var/log/iguazio/${ts}_parquetinizer.log