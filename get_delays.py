
import psycopg2
import pandas as pd
import numpy as np

# set up log in information
params_dic = {
    "host"      : "localhost",
    "dbname"    : "flights",
    "user"      : "postgres",
    "password"  : "Silvana1005.",
    "port" : "5432"      # this is the port set up by your default wizard
}
# we can use "SELECT * FROM pg_settings WHERE name = 'port';" in pgAdmin to discover our port number
# learn more about ports here: https://www.cloudflare.com/learning/network-layer/what-is-a-computer-port/

# connect to your database using the dictionary above
# the ** operator unpacks all your settings into their appropriate params
# without the ** operator, we would have to manually set these params via
# psycopg2.connect(host="localhost", dbname="flights", user="postgres", password="password", port="...")
conn = psycopg2.connect(**params_dic)

# create a cursor
# think of a cursor like an object that stores and executes queries
# we prepare it by calling conn.cursor() in Python
# for our purpose, a cursor is an object that allows us to execute queries 
# learn more about cursors here: https://www.geeksforgeeks.org/what-is-cursor-in-sql/
cursor = conn.cursor()

# execute a query using the cursor
# docs of cursor here: https://www.psycopg.org/docs/cursor.html
cursor.execute("SELECT * FROM real_flight where cancelled = '0' and diverted = '0'")

# pull all rows from the query you just executed
rows = cursor.fetchall()
#print(len(rows))

# close your cursor, must be done once you are done interacting w/your cursor
cursor.close()


# save your list of tuples (representing rows) into a dataframe!
df = pd.DataFrame(rows, columns=[desc.name for desc in cursor.description])


df.dropna(subset=['arr_del15', 'dep_del15'], inplace=True)

#df['delayed'] = False

df['delayed'] = np.where((df["arr_del15"]=='1') & (df["dep_del15"]=='1'), True, False)

#print(df.groupby('op_unique_carrier').agg({'delayed': [('mymean', 'mean')]}).sort_values(by=["mymean"]))
# 
# 
# .sort_values(by=["op_unique_carrier"]))
df2 = df.groupby('op_unique_carrier').agg(mean_delayed=('delayed', 'mean')).sort_values(by=["mean_delayed"])
df2.to_csv("delayed_airlines.csv")
#print(grouped_single)
# print out the first 5 rows of your dataframe!
#print(df.head())
df3 = df.groupby('origin_airport_id').agg(mean_delayed=('delayed', 'mean')).sort_values(by=["mean_delayed"])
df3.to_csv("delayed_airports.csv")
