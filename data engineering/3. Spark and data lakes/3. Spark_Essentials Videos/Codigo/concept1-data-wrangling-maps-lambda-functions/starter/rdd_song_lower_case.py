### 
# You might have noticed this code in the screencast.
#
# import findspark
# findspark.init('spark-2.3.2-bin-hadoop2.7')
#
# The findspark Python module makes it easier to install
# Spark in local mode on your computer. This is convenient
# for practicing Spark syntax locally. 
# However, the workspaces already have Spark installed and you do not
# need to use the findspark module
#
###

# %%
import pyspark
sc = pyspark.SparkContext(appName="maps_and_lazy_evaluation_example")


# Starting off with a regular python list
log_of_songs = [
        "Despacito",
        "Nice for what",
        "No tears left to cry",
        "Despacito",
        "Havana",
        "In my feelings",
        "Nice for what",
        "despacito",
        "All the stars"
]

# parallelize the log_of_songs to use with Spark
distributed_song_log_rdd = sc.parallelize(log_of_songs)


# show the original input data is preserved
print('\nShowing the original input data is preserved')
distributed_song_log_rdd.foreach(print)
# %%

# create a python function to convert strings to lowercase
def convert_song_to_lowercase(song):
    return song.lower()


# use the map function to transform the list of songs with the python function that converts strings to lowercase
lower_case_songs = distributed_song_log_rdd.map(convert_song_to_lowercase)
print('\nShowing the lowered case')
lower_case_songs.foreach(print)


# %%
# Show the original input data is still mixed case
print('\nShowing the original input data is still mixed case')
distributed_song_log_rdd.foreach(print)


# Use lambda functions instead of named functions to do the same map operation

# %%
print('\nLower case with lambda functions')
distributed_song_log_rdd.map(lambda song: song.lower()).foreach(print)

# %%
