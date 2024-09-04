# Extract phase

echo "Extracting data"

# Extract the columns 1 (user name), 2 (user id) and 
# 6 (home directory path) from /etc/passwd

cut -d":" -f1,3,6 /etc/passwd > extracted-data.txt

# Transformation Phase
echo "Transforming data"


cat extracted-data.txt | tr ":" "," > transformed-data.csv

# Loading Phase

echo "Loading data"
# Set the PostgreSQL password environment variable.
# Replace <yourpassword> with your actual PostgreSQL password.
# export PGPASSWORD=<yourpassword>;
# Send the instructions to connect to 'template1' and
# copy the file to the table 'users' through command pipeline.
echo "\c template1;\COPY users  FROM './transformed-data.csv' DELIMITERS ',' CSV;" | sudo -u postgres psql

echo "SELECT * FROM users;" | sudo -u postgres psql template1