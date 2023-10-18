# %%
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import os

spark = SparkSession \
    .builder \
    .appName("Our first Python Spark SQL example") \
    .getOrCreate()



# %% Reading json file.
path = '/home/jorge/Documentos/pythoneta/data engineering/3. Spark and data lakes/3. Spark_Essentials Videos/Codigo/data/sparkify_log_small.json'
abs_path = os.path.abspath(path)

user_log_df = spark.read.json(abs_path)

# See how Spark inferred the schema from the JSON file
user_log_df.printSchema()
print(user_log_df.describe())

user_log_df.show(n=1)
print(user_log_df.take(5))

# %% We are changing file formats

out_path = "/home/jorge/Documentos/pythoneta/data engineering/3. Spark and data lakes/3. Spark_Essentials Videos/Codigo/data/sparkify_log_small.csv"
abs_out_path = os.path.abspath(out_path)

user_log_df.write.mode("overwrite").save(abs_out_path, format="csv",header=True)

# %%
# Notice we have created another dataframe here
# We wouldn't usually read the data that we just wrote
# This does show, however, that the read method works with
# Different data types
user_log_2_df = spark.read.csv(abs_out_path, header=True)
user_log_2_df.printSchema()

# Choose two records from the CSV file
print(user_log_2_df.take(2))    

# Show the userID column for the first several rows
user_log_2_df.select("userID").show()

print(user_log_2_df.take(1))