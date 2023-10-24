from datetime import datetime
import happybase

# made connection with hbase on localhost:9090
connection = happybase.Connection('127.0.0.1',9090)
print("Connection was successfully established with the server.")

# tables names
user_table = 'user_table'
post_table = 'post_table'

# make connection to tables
table_user = connection.table(user_table)
table_post = connection.table(post_table)


print("------------------------User with the highest followers------------------------------")


# get data from table user
rows = table_user.scan()

# Initialize a dictionary to store user followers
user_followers = {}

# Iterate over the rows and store user followers
for row_key, data in rows:
    followers_count = int(data[b'user_details:followers_count'])
    username = data[b'user_details:username'].decode('utf-8')
    
    user_followers[username] = followers_count

# Sort users based on follower count
sorted_users = sorted(user_followers.items(), key=lambda x: x[1], reverse=True)

# Specify the number of top users you want to retrieve
num_top_users = 5  
# Print the top users with the highest number of followers
print(f"Top {num_top_users} users with the highest number of followers:")

for i, (username, followers_count) in enumerate(sorted_users[:num_top_users]):
    print(f"{i+1}. User: {username} | Followers: {followers_count}")

print("------------------------User engagement------------------------------")


# Retrieve post data
post_rows = table_post.scan()

# Iterate over post rows
for post_row_key, post_data in post_rows:
    # Extract relevant data from post data
    user_id = post_data[b'post_details:user_id'] #.decode('utf-8')
    reblogs_count = int(post_data[b'post_details:reblogs_count'])
    favourites_count = int(post_data[b'post_details:favourites_count'])
    

    # Retrieve user data from user_table
    user_data = table_user.row(user_id,columns=["user_details:followers_count", "user_details:username"])
    # print(user_data)

    # Check if user data exists for the post
    if user_data:
        followers_count = int(user_data[b'user_details:followers_count'])
        username = user_data[b'user_details:username'].decode('utf-8')
        followers_count = 1 if followers_count == 0 else followers_count
        # Calculate engagement rate
        engagement_rate = (favourites_count + reblogs_count) / followers_count

        # Print the engagement rate for the user
        print(f"Engagement rate for user {username}: {round(engagement_rate*100,2)}%")
    else:
        print(f"No post data found for user {username}")


print("------------------------User inscription------------------------------")

# Retrieve user data
user_rows = table_user.scan()

# Dictionary to store user counts by month
users_by_month = {}

# Iterate over user rows
for user_row_key, user_data in user_rows:
    # Extract the creation date of the user
    created_at = user_data[b'user_details:created_at'].decode('utf-8')

    # Parse the creation date string to datetime object
    created_at_date = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')

    # Extract the month and year from the creation date
    month = created_at_date.strftime('%Y-%m')
    year = created_at_date.strftime('%Y')

    # Update the user count for the specific month
    if month in users_by_month:
        users_by_month[month] += 1
    else:
        users_by_month[month] = 1

# sort values by count
sorted_users_by_month = sorted(users_by_month.items(), key=lambda x: x[1], reverse=True)

# Print the user counts by month
for month, user_count in sorted_users_by_month:
    print(f"Month: {month}, User Count: {user_count}")

# Close the HBase connection
connection.close()
