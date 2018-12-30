#!/usr/bin/bash -x

crontab -l > mycron
#echo new cron into cron file
echo "$1" >> mycron
#install new cron file
crontab mycron
rm mycron
