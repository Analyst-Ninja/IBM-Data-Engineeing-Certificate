import psycopg2

conn = psycopg2.connect(database = "mydb2", 
                        user = "postgres", 
                        host= 'localhost',
                        password = "1234",
                        port = 5432)

# Open a cursor to perform database operations
cur = conn.cursor()
# Execute a command: create datacamp_courses table

cur.execute(
    """
    DROP TABLE IF EXISTS datacamp_courses
    """
)


cur.execute("""
            CREATE TABLE datacamp_courses(
            course_id SERIAL PRIMARY KEY,
            course_name VARCHAR (50) UNIQUE NOT NULL,
            course_instructor VARCHAR (100) NOT NULL,
            topic VARCHAR (20) NOT NULL);
            """)
# Make the changes to the database persistent
conn.commit()
# Close cursor and communication with the database

cur.execute("INSERT INTO datacamp_courses(course_name, course_instructor, topic) VALUES('Introduction to SQL','Izzy Weber','Julia')")

cur.execute("INSERT INTO datacamp_courses(course_name, course_instructor, topic) VALUES('Analyzing Survey Data in Python','EbunOluwa Andrew','Python')")

cur.execute("INSERT INTO datacamp_courses(course_name, course_instructor, topic) VALUES('Introduction to ChatGPT','James Chapman','Theory')")
cur.execute("INSERT INTO datacamp_courses(course_name, course_instructor, topic) VALUES('Introduction to Statistics in R','Maggie Matsui','R')")

cur.execute("INSERT INTO datacamp_courses(course_name, course_instructor, topic) VALUES('Hypothesis Testing in Python','James Chapman','Python')")
cur.close()
conn.commit()

cur.execute('SELECT * FROM datacamp_courses;')
rows = cur.fetchall()
conn.commit()
conn.close()
for row in rows:
    print(row)