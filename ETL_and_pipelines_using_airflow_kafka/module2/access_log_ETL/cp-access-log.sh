#! /bin/bash

# Extraction Phase

echo "-- Extraction Phase"

# Downloading File

echo "---- Downloading File"

wget "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Bash%20Scripting/ETL%20using%20shell%20scripting/web-server-access-log.txt.gz"

echo "---- Unzipping"

# Unzip the file to extract the .txt file.
gunzip -f web-server-access-log.txt.gz

# Getting required columns

echo "---- Extracting Required Columns"

cut -d"#" -f1-4 web-server-access-log.txt > extracted-data.txt

# Transformation Phase 

echo "-- Transformation Phase"

cat extracted-data.txt | tr "#" "," > transformed-data.csv

# Loading Phase 

echo "Loading Phase"

export PGPASSWORD=1234

echo "\c template1; \COPY access_log FROM './transformed-data.csv' DELIMITERS ',' CSV HEADER;" | psql --username=postgres --host=localhost 
