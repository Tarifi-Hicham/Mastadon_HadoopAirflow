# Analysis of user interaction data on the Mastodon platform.

### Description:
This project aims to analyze social media data to obtain information about user engagement, content popularity, etc..
It utilizes MapReduce for data processing, stores the results in HBase, and orchestrates the workflow using Apache Airflow.

### Data Source { Mastodon API }:
[Mastodon](https://joinmastodon.org/) is a social media platform that operates on an open-source framework [Github doc](https://github.com/felx/mastodon-documentation). It boasts a comprehensive Application Programming Interface (API) that empowers developers to interact with various facets of the platform. Here's a succinct overview of the Mastodon API's key components:

- Secure Authentication: Mastodon's API incorporates robust authentication methods, enabling developers to implement secure user authentication and access control.

- User Account Management: The API facilitates the management of user accounts, encompassing tasks such as handling user profile data, preferences, and settings.

- Toot Management: Developers can utilize the API to create, retrieve, and manage "toots" (equivalent to tweets in Mastodon), encompassing features such as posting, fetching, and deleting toots.

- Notifications: The API offers access to notifications, enabling developers to fetch and manage various notifications, including mentions, likes, and reposts.

- Timelines: Developers can leverage the API to access different timelines, such as the home timeline, local timeline, and federated timeline. This empowers them to retrieve and interact with posts from diverse timelines.

- User Interactions: The API facilitates user interactions, encompassing functionalities like following/unfollowing users, liking toots, and reposting (boosting) content.

- Search Functionality: Mastodon's API supports powerful search capabilities, enabling users to search for specific content, users, or hashtags within the Mastodon network.

- Streaming Capabilities: The API offers streaming capabilities, allowing developers to implement real-time updates for activities such as new toots, notifications, and other interactions.

- Instance and Federation Information: Mastodon's API provides valuable insights into instances and federation, enabling developers to retrieve data about instances, their policies, and the federated network of instances.

### Data Structure:

| Data Type        | Fields/Attributes                                              |
|------------------|---------------------------------------------------------------|
| User Data        | Username, Display Name, Bio, Avatar Image, Header Image, Follower Count, Following Count, Account Creation Date |
| User Preferences | Privacy Settings, Notification Preferences, Account Visibility Options, Content Viewing Preferences |
| Toots (Posts)    | Toot ID, Content Text, Attached Media, Creation Timestamp, Visibility Settings, Content Tags, Reblogs (Boosts) Count, Likes (Favorites) Count, Mentioned Users |
| Notifications    | Notification ID, Notification Type, Related Toot ID, Timestamp, Notifying User |
| Instance Data    | Instance Name, Instance Description, Instance Rules and Policies, Instance Admins and Moderators |
| Federation Data  | Connected Instances, Federation Policies, Interaction Policies with External Instances |
| Metadata         | Hashtag Name, Associated Toots, Media ID, Media Type, Media URL, Language of the Toot |
| Interaction Data | Follower ID, Followed User ID, User ID, Liked Toot ID, User ID, Boosted Toot ID |



### Mission:
As a data engineer, my primary objective is to maintain a robust big data pipeline capable of extracting data using the Mastodon API and storing it in Hadoop HDFS as JSON files. Subsequently, I will develop a MapReduce script to transform the data into key-value pairs. The processed data will be efficiently stored in HBase, enabling valuable insights to be derived from it. To ensure seamless and efficient workflow management, I will integrate Apache Airflow, a powerful platform for workflow orchestration and management. This integration will facilitate automated execution and real-time monitoring of the data analysis process, ensuring its effectiveness.

### Technologies:
Apache hadoop, HBase, Airflow, Python.


