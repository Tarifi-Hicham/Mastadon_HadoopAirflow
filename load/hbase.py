# import the libs
from datetime import datetime
import happybase


# made connection with hbase on localhost:9090
connection = happybase.Connection('127.0.0.1',9090)
print("Connection was successfully established with the server.")

# tables names
user_table = 'user_table'
post_table = 'post_table'

# read the content of txt file included on the root.
f = open("output.txt", "r")


# check if the tables exists.
table_list = [t.decode('utf-8') for t in connection.tables()]

# create the table if not exist (user table)
if user_table not in table_list :
    connection.create_table(
        user_table,
        {
            'user_details':dict()
        }
    )
    print(f"Table {user_table} was successfully created.")

# create the table if not exist (post table)
if post_table not in table_list :
    connection.create_table(
        post_table,
        {
            'post_details':dict()
        }
    )
    print(f"Table {user_table} was successfully created.")

# make connection to tables
table_user = connection.table(user_table)
table_post = connection.table(post_table)

# retreive if there is any data 
# for data in table_user.scan():
#     print(data)
# for data in table_post.scan():
#     print(data)
# get the data base on the column family and row key
# ps = table.row(row='fr',columns=['post:language'])
# print(ps)

# insert into the tables
for x in f:
    # get line from the content the text file splited by "
    item = x.split('"')
    # get the column name from the line
    column = item[1].split(':')[0]
    # get the row key from the line
    row_key = item[1].split(':')[1]
    # get the value from the line
    values = item[1].split(':')[0]
    if  values == 'following_count' or values == 'followers_count' or values == 'status_count' or values == 'created_at' :
        if values == 'created_at':
            # convert datetime from millisecondes to "yyyy-MM-dd hh:mm:ss"
            created_at = datetime.fromtimestamp(int(item[2].strip()) / 1000)
            table_user.put(row_key, {"user_details:" + column:str(created_at)})
        else:
            table_user.put(row_key, {"user_details:" + column:item[2].strip()})
    elif values =='username'or values == 'url':
        table_user.put(row_key, {"user_details:" + column:item[3].strip()})
    elif values == 'user_id' or values == 'media' or values == 'favourites_count' or values == 'reblogs_count':
        table_post.put(row_key, {"post_details:" + column:item[2].strip()})
    elif values =='language' or values == 'tag' or values == 'visibility':
        table_post.put(row_key, {"post_details:" + column:item[3].strip()})

print("Data was inserted successfully.")
f.close()
connection.close()