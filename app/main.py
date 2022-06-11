import json

from pyspark.sql import SparkSession
import pyspark.sql.types as T
import datetime
from pyspark.sql.functions import udf, col, to_date
from pyspark.sql.types import IntegerType, DateType

DATASET_PATH = './data/USvideos.csv'
RESULTS_PATH = './data/results'


def task1(df):
    schema = T.StructType([
        T.StructField('id', T.StringType(), True),
        T.StructField('title', T.StringType(), True),
        T.StructField('description', T.StringType()),
        T.StructField('latest_views', T.LongType()),
        T.StructField('latest_likes', T.LongType()),
        T.StructField('latest_dislikes', T.LongType()),
        T.StructField('trending_days', T.ArrayType(
            T.StructType([
                T.StructField('date', T.StringType()),
                T.StructField('likes', T.LongType()),
                T.StructField('dislikes', T.LongType())]
            ))),
    ])

    top_trending_ids = [
        row.video_id for row in df.groupby('video_id').count().sort("count", ascending=False) \
            .limit(10).select('video_id').collect()
    ]

    data = []
    for video_id in top_trending_ids:
        video_df = df.filter(df.video_id == video_id).sort('date', ascending=False)

        row = list(video_df.select('video_id', 'title', 'description', 'views', 'likes', 'dislikes').collect()[0])
        row.append([
            list(trending_day_data) for trending_day_data in
            video_df.select('trending_date', 'likes', 'dislikes').collect()
        ])
        data.append(row)

    res_df = spark.createDataFrame(data=data, schema=schema)

    res = dict()
    res['videos'] = [json.loads(_) for _ in res_df.toJSON().collect()]
    with open(f'{RESULTS_PATH}/task_1.json', "w") as outfile:
        json.dump(res, outfile)

    return res_df


def task2(df):
    # TODO: should solve this
    pass


def convert_date(x):
    tmp = x.split('.')
    return udf(str(datetime.datetime(int(tmp[0]), int(tmp[1]), int(tmp[2]))))


if __name__ == '__main__':
    spark = SparkSession.builder.appName("appTest").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.option('header', 'true').csv(DATASET_PATH).na.drop(subset=["description", "trending_date"])
    df = df.withColumn("likes", df["likes"].cast(IntegerType()))
    df = df.withColumn("dislikes", df["dislikes"].cast(IntegerType()))
    df = df.withColumn("views", df["views"].cast(IntegerType()))
    df = df.withColumn('date', to_date(col("trending_date"), "yy.dd.MM").cast("date"))

    tasks = [task1, task2]
    for task in tasks:
        task_df = task(df)
        print('\n[result]: task1: ')
        task_df.printSchema()
        task_df.show(truncate=True)
