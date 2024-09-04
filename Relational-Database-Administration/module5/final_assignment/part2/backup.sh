#!/bin/sh

folder_name=$(date +"%Y%m%d")

mkdir /tmp/$folder_name 

cd /tmp/$folder_name

echo "backup started"

sudo mysqldump --all-databases > backup-file.sql

echo "backup finished"



