# # Data Wrangling with DataFrames Coding Quiz

# %% Take care of any imports

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, udf, col
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

# TODOS: 
# 1) import any other libraries you might need
# 2) instantiate a Spark session 
# 3) read in the data set located at the path "../../data/sparkify_log_small.json"
# 4) write code to answer the quiz questions 


# %% # # Question 1
# 
# Which page did user id "" (empty string) NOT visit

# TODO: write your code to answer question 1
user_log_df.select("page").where(user_log_df.userId == "") \
   .dropDuplicates().show()


# %%# # Question 2 - Reflect
# 
# What type of user does the empty string user id most likely refer to?
user_log_df.where(user_log_df.userId == '') \
    .groupby('page').count().orderBy(desc('count')).show()

#%% # # Question 3
# How many female users do we have in the data set?
user_log_df.select(['gender', 'userId']) \
   .dropDuplicates() \
   .groupby('gender').count().show()


#%% Question 4
# How many songs were played from the most played artist?
user_log_df.select(['artist', 'song']) \
  .groupby('artist').count().orderBy(desc('count')).show()

#%% Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
from pyspark.sql.window import Window
logs_df = user_log_df


user_window = Window \
    .partitionBy('userID') \
    .orderBy(desc('ts')) \
    .rangeBetween(Window.unboundedPreceding, 0)

ishome = udf(lambda ishome : int(ishome == 'Home'), IntegerType())

# Filter only NextSong and Home pages, add 1 for each time they visit Home
# Adding a column called period which is a specific interval between Home visits
cusum = logs_df.filter((logs_df.page == 'NextSong') | (logs_df.page == 'Home')) \
    .select('userID', 'page', 'ts') \
    .withColumn('homevisit', ishome(col('page'))) \
    .withColumn('period', Fsum('homevisit') \
    .over(user_window)) 
    
# This will only show 'Home' in the first several rows due to default sorting

cusum.show(300)


# See how many songs were listened to on average during each period
cusum.filter((cusum.page == 'NextSong')) \
    .groupBy('userID', 'period') \
    .agg({'period':'count'}) \
    .agg({'count(period)':'avg'}) \
    .show()
