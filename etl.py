import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, monotonically_increasing_id
from pyspark.sql.types import IntegerType, StringType, DoubleType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('KEYS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('KEYS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """ This is function instantiate connection to apache """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
     To process song data from s3 bucket and create songs and artists dimensions and save into S3 buckets
    :param spark: spark instance to access the spark API
    :param input_data: S3 buckets containing all sparkify data set
    :param output_data: Destination to save processed data
    :return: None
    """
    # get filepath to song data file
    song_data ="{input_data}song-data/*/*/*/*.json".format(input_data=input_data)

    # read song data file
    df = spark.read.json(song_data).dropDuplicates().cache()

    df.createOrReplaceTempView('song_df_table')

    df = df.withColumn("song_id", df["song_id"].cast(StringType()))
    df = df.withColumn("title", df["title"].cast(StringType()))
    df = df.withColumn("artist_id", df["artist_id"].cast(StringType()))
    df = df.withColumn("year", df["year"].cast(IntegerType()))
    df = df.withColumn("duration", df["duration"].cast(DoubleType()))
    df = df.withColumn("artist_name", df["artist_name"].cast(StringType()))
    df = df.withColumn("artist_location", df["artist_location"].cast(StringType()))
    df = df.withColumn("artist_latitude", df["artist_latitude"].cast(DoubleType()))
    df = df.withColumn("artist_longitude", df["artist_longitude"].cast(DoubleType()))

    # extract columns to create songs table
    songs_table = df.select(
        col('song_id'),
        col('title'),
        col('artist_id'),
        col('year'),
        col('duration')
    ).dropDuplicates(["song_id"])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id') \
        .parquet("{output_data}songs/songs_table.parquet".format(
        output_data=output_data))

    # extract columns to create artists table
    artists_table = df.select(
        col('artist_id'),
        col('artist_name'),
        col('artist_location'),
        col('artist_latitude'),
        col('artist_longitude')
    ).dropDuplicates(["artist_id"])

    # write artists table to parquet files
    artists_table.write.parquet("{output_data}artists/artists_table.parquet".format(
        output_data=output_data))


def process_log_data(spark, input_data, output_data):
    """
     To process log data from S3 Buckets and create songs and artists dimensions
     and save them in another s3 bucket.
    :param spark: spark instance to access the spark API
    :param input_data: S3 buckets containing all sparkify data set
    :param output_data: Destination to save processed data
    :return: None
    """
    # get filepath to log data file
    log_data ="{input_data}log-data/*events.json".format(input_data=input_data)

    # read log data file
    df = spark.read.json(log_data).dropDuplicates().cache()

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong').cache()

    # extract columns for users table
    user_table = df.select(
        col('firstName'),
        col('lastName'),
        col('gender'),
        col('level'),
        col('userId')
    ).distinct(["userId"])

    # write users table to parquet files
    user_table.write.parquet("{output_data}users/users_table.parquet".format(
        output_data=output_data))

    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    # create timestamp column from original timestamp column
    df = df.withColumn('timestamp', get_timestamp(col('ts')))

    # create datetime column from original timestamp column
    df = df.withColumn('start_time', get_timestamp(col('ts')))

    # extract columns to create time table
    df = df.withColumn('hour', hour('timestamp'))
    df = df.withColumn('day', dayofmonth('timestamp'))
    df = df.withColumn('month', month('timestamp'))
    df = df.withColumn('year', year('timestamp'))
    df = df.withColumn('week', weekofyear('timestamp'))
    df = df.withColumn('weekday', dayofweek('timestamp'))

    time_table = df.select(
        col('start_time'),
        col('hour'),
        col('day'),
        col('week'),
        col('month'),
        col('year'),
        col('weekday')
    ).distinct()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
        .parquet("{output_data}time/time_table.parquet".format(
        output_data=output_data))

    # read in song data to use for songplays table
    song_df = spark.sql(
        'SELECT DISTINCT song_id, artist_id, artist_name FROM song_df_table'
    )

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(
        song_df,
        song_df.artist_name == df.artist,
        'inner'
    ) \
        .distinct() \
        .select(
        col('start_time'),
        col('userId'),
        col('level'),
        col('sessionId'),
        col('location'),
        col('userAgent'),
        col('song_id'),
        col('artist_id'),
        col('year'),
        col('month')) \
        .withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month') \
        .parquet("{output_data}songplays/songplays_table.parquet".format(
        output_data=output_data))

def main():
    # instantiate spark session
    spark = create_spark_session()
    # refeference the root s3 bucket
    input_data = "s3a://udacity-dend/"
    # specify S3 bucket to save output
    output_data = ""
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
