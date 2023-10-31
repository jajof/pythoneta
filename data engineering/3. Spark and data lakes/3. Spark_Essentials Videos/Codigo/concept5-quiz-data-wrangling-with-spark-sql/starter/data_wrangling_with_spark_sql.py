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

# We create a temporary view of the table
user_log_df.createOrReplaceTempView("user_log_table")

#%% Question 1
# 
# Which page did user id ""(empty string) NOT visit?

spark.sql("""
        Select distinct page
        from user_log_table
        where userId = ''
""").show()

#%% Question 2 - Reflect
# 
# Why might you prefer to use SQL over data frames? Why might you prefer data frames over SQL?

#%% Question 3
# How many female users do we have in the data set?

spark.sql("""Select gender, count(distinct userid) 
          from user_log_table
          group by gender
          """).show()


#%%
# # Question 4
# 
# How many songs were played from the most played artist?

# TODO: write your code to answer question 4

# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.

# TODO: write your code to answer question 5

