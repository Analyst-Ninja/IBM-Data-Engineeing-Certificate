from pymongo import MongoClient

connentURL = "mongodb://127.0.0.1:27017"

# user = ''
# password = ''
# host = 'localhost'
# connentURL = "mongodb://{}:{}@{}:27017/?authSource=admin".format(user,password,host)


conn = MongoClient(connentURL)
print(conn)

print('Getting the List of DBs')

dbs = conn.list_database_names()

print('Printing Databases')
for i in range(len(dbs)):
    print(f'{i} --> {dbs[i]}')

print('Closing Connection')

conn.close()

# mydb = conn.myDatabase



# print(mydb)

# print('Getting the List of DBs')

# dbs = conn.list_database_names()

# print('Printing Databases')
# for i in range(len(dbs)):
#     print(f'{i} --> {dbs[i]}')

# print('Closing Connection')

# conn.close()


