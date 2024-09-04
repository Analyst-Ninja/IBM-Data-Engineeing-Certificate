# Import libraries required for connecting to mysql
import mysql
from dotenv import load_dotenv
load_dotenv()
from os import getenv

MYSQL__USER=getenv("MYSQL__USER")
MYSQL_PASS=getenv("MYSQL_PASS")
MYSQL__HOSTNAME=getenv("MYSQL__HOSTNAME")
MYSQL__DB=getenv("MYSQL__DB")
MYSQL__PORT=getenv("MYSQL__PORT")
POSTGRES_DB=getenv("POSTGRES_DB")
POSTGRES__USER=getenv("POSTGRES__USER")
POSTGRES__PASS=getenv("POSTGRES__PASS")
POSTGRES__HOSTNAME=getenv("POSTGRES__HOSTNAME")
POSTGRES__PORT=getenv("POSTGRES__PORT")



# Import libraries required for connecting to DB2 or PostgreSql
import mysql.connector
import psycopg2

# Connect to MySQL
connMYSQL = mysql.connector.connect(
	user=MYSQL__USER, 
	password=MYSQL_PASS,
	host=MYSQL__HOSTNAME,
	database=MYSQL__DB
)

mysqlCursor = connMYSQL.cursor()

# Connect to DB2 or PostgreSql
connPostgreSQL = psycopg2.connect(
	database=POSTGRES_DB, 
	user=POSTGRES__USER,
	password=POSTGRES__PASS,
	host=POSTGRES__HOSTNAME, 
	port= POSTGRES__PORT
)

postgresCursor = connPostgreSQL.cursor()

# Find out the last rowid from DB2 data warehouse or PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database or PostgreSql.

def get_last_rowid():

	sql = """SELECT MAX(rowid) FROM sales_data;"""
	
	postgresCursor.execute(sql)
	res = postgresCursor.fetchall()[0][0]
	return res


last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):

	sql = """ SELECT * FROM sales_data WHERE rowid > %s """

	mysqlCursor.execute(sql, (rowid,))
	res = mysqlCursor.fetchall()

	return res

new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database or PostgreSql.

def insert_records(records):
	sql = """
		INSERT INTO sales_data(rowid, product_id, customer_id,quantity) 
		VALUES (%s,%s,%s,%s)
		"""
	for record in records:
		postgresCursor.execute(sql, record)
		connPostgreSQL.commit()
	pass

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
connMYSQL.close()

# disconnect from DB2 or PostgreSql data warehouse 
connPostgreSQL.close()
# End of program