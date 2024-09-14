#! /bin/bash

MYSQL_PASSWORD='MY_PASSWORD'

mysqldump -u root -h localhost --password=$MYSQL_PASSWORD sales > sales_data.sql