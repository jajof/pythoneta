# %% Take care of any imports

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import sum as Fsum

import datetime
import os

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


spark = SparkSession \
    .builder \
    .appName("Our first Python Spark SQL example") \
    .getOrCreate()

# %% Reading json file.
path = '/home/jorge/Documentos/pythoneta/data engineering/3. Spark and data lakes/3. Spark_Essentials Videos/Codigo/data/sparkify_log_small.json'
abs_path = os.path.abspath(path)
user_log_df = spark.read.json(abs_path)

# %% Data Exploration 

# View 5 records 
print(user_log_df.take(5))

# Print the schema
user_log_df.printSchema()

# Describe the dataframe
user_log_df.describe().show()

# Describe the statistics for the song length column
user_log_df.select("length").describe().show()

# Count the rows in the dataframe
print(user_log_df.count())
   

# Select the page column, drop the duplicates, and sort by page
user_log_df.select("page").dropDuplicates().sort("page").show()


# Select data for all pages where userId is 1046
user_log_df.select(["userId", "firstname", "page", "song"]) \
   .where(user_log_df.userId == "1046") \
   .show()


# %% Calculate Statistics by Hour
get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0).hour)
user_log_df = user_log_df.withColumn("hour", get_hour(user_log_df.ts))
user_log_df.select('hour').dropDuplicates().sort("hour").show()

print(user_log_df.head(1))

# Select just the NextSong page
songs_in_hour_df = user_log_df.where(user_log_df.page == "NextSong") \
                    .groupby(user_log_df.hour) \
                    .count() \
                    .orderBy(user_log_df.hour.cast("float"))
songs_in_hour_df.show()

songs_in_hour_pd = songs_in_hour_df.toPandas()
songs_in_hour_pd['hour'] = songs_in_hour_pd['hour'].astype("float")

plt.scatter(songs_in_hour_pd['hour'], songs_in_hour_pd['count'])
plt.xlim(-1, 24)
plt.ylim(0, 1.2 * max(songs_in_hour_pd['count']))
plt.xlabel("Hour")
plt.ylabel("Songs played")
plt.show()

# %% Drop Rows with Missing Values

user_log_valid_df = user_log_df.dropna(how='any', subset = ['userId', 'sessionId'])

# How many are there now that we dropped rows with null userId or sessionId?
print(user_log_df.count())
print(user_log_valid_df.count())

# select all unique user ids into a dataframe
user_log_df.select("userId") \
   .dropDuplicates() \
   .sort("userId").show()

# Select only data for where the userId column isn't an empty string (different from null)
user_log_valid_df = user_log_df.where(user_log_valid_df["userId"]!= "")
print(user_log_valid_df.count())


# %% # # Users Downgrade Their Accounts
# 
# Find when users downgrade their accounts and then show those log entries. 
user_log_valid_df.filter(user_log_valid_df.page == 'Submit Downgrade').show()

# Create a user defined function to return a 1 if the record contains a downgrade
flag_downgrade_event = udf(lambda x: 1 if x == 'Submit Downgrade' else 0, IntegerType())

# Select data including the user defined function
user_log_valid_df = user_log_valid_df \
  .withColumn("downgraded", flag_downgrade_event("page"))
print(user_log_valid_df.head(5))


