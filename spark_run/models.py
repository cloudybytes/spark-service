from django.conf import settings
from django.db import models
from pyspark.sql import SparkSession

scSpark = SparkSession \
    .builder \
    .appName("reading csv") \
    .getOrCreate()

dataset_path = settings.DATASET_PATH
movie_file = dataset_path +'movies.csv'
rating_file = dataset_path +'rating.csv'
users_file = dataset_path +'users.csv'
zip_file = dataset_path +'zipcodes.csv'
movieData = scSpark.read.csv(movie_file, sep=",").toDF("movieid" , "title" , "releasedate" , "unknown" , "Action" , "Adventure" , "Animation" , "Children" , "Comedy" , "Crime" , "Documentary" , "Drama" , "Fantasy" , "Film_Noir" , "Horror" , "Musical" , "Mystery" , "Romance" , "Sci_Fi" , "Thriller" , "War" , "Western" ).cache()
usersData = scSpark.read.csv(users_file, sep=",").toDF("userid" , "age" , "gender" , "occupation" , "zipcode").cache()
ratingData = scSpark.read.csv(rating_file, sep=",").toDF("userid" , "movieid" , "rating" , "timestamp").cache()
zipData = scSpark.read.csv(zip_file, sep=",").toDF("zipcode" , "zipcodetype" , "city" , "state").cache()
# print('Total Records = {}'.format(movieData.count()))
# print('Total Records = {}'.format(usersData.count()))
# print('Total Records = {}'.format(ratingData.count()))
# print('Total Records = {}'.format(zipData.count()))
# movieData.show()
# zipData.show()


