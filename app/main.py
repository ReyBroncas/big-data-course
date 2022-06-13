import json

from pyspark.sql import SparkSession
import pyspark.sql.types as T
import datetime
from pyspark.sql.functions import udf, col, to_date, desc
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

DATASET_PATH = './data/USvideos.csv'
CATEGORIES_DATA_PATH = './data/US_category_id.json'
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

    res = spark.createDataFrame(data=data, schema=schema)

    out = dict()
    out['videos'] = [json.loads(_) for _ in res.toJSON().collect()]
    with open(f'{RESULTS_PATH}/task_1.json', "w") as outfile:
        json.dump(out, outfile)

    return res


def task2(df):
    schema = T.StructType([
        T.StructField("start_date", T.StringType()),
        T.StructField("end_date", T.StringType()),
        T.StructField("category_id", T.IntegerType()),
        T.StructField("category_name", T.StringType()),
        T.StructField("number_of_videos", T.LongType()),
        T.StructField("total_views", T.LongType()),
        T.StructField("video_ids", T.ArrayType(T.StringType())),
    ])

    @udf(returnType=T.StringType())
    def get_week_date(item):
        if item is None:
            return None
        return item.strftime("%Y-%V")

    categories = get_categories()
    df = df.withColumn("week_date", get_week_date(col("date")))
    week_dates = [
        row.week_date for row in
        df.groupBy('week_date').agg(col('week_date')).select('week_date').collect()
    ]

    data = []
    for week in week_dates:
        week_videos = df.filter(df.week_date == week) \
            .groupBy('video_id', 'category_id').agg(F.max(col('views')) - F.min(col('views'))) \
            .filter('(max(views) - min(views)) > 0')

        top_category = week_videos.groupBy('category_id').agg(F.sum('(max(views) - min(views))').alias('views')) \
            .sort(desc('views')).limit(1).collect()

        if not len(top_category):
            continue

        videos = [
            _[0] for _ in
            week_videos.select('video_id').where(week_videos.category_id == top_category[0].category_id).collect()
        ]

        end_date = datetime.datetime.strptime(week + ' 0', '%Y-%W %w')
        start_date = end_date - datetime.timedelta(days=6)

        data.append([
            str(start_date), str(end_date), int(top_category[0].category_id),
            categories.get(top_category[0].category_id),
            len(videos), top_category[0].views, videos
        ])

    res = spark.createDataFrame(data=data, schema=schema)

    out = dict()
    out['weeks'] = [json.loads(_) for _ in res.toJSON().collect()]
    with open(f'{RESULTS_PATH}/task_2.json', "w") as outfile:
        json.dump(out, outfile)

    return res


def task4(df):
    schema = T.StructType([
        T.StructField("channel_name", T.StringType()),
        T.StructField("start_date", T.StringType()),
        T.StructField("end_date", T.StringType()),
        T.StructField("total_views", T.LongType()),
        T.StructField("video_views", T.ArrayType(
            T.StructType([
                T.StructField("video_id", T.StringType()),
                T.StructField("views", T.LongType()),
            ])
        )),
    ])

    distinct_videos = df.groupBy('channel_title', 'video_id').agg(F.max(col('views')))
    top_channels = distinct_videos.groupBy('channel_title').agg(F.sum('max(views)')).sort(
        desc('sum(max(views))')).limit(20)

    data = []
    end_time = str(list(df.agg(F.max('date')).collect()[0])[0])
    start_time = str(list(df.agg(F.min('date')).collect()[0])[0])

    for channel in top_channels.collect():
        title, total_views = list(channel)
        data.append([title, start_time, end_time, total_views, [
            list(_) for _ in
            distinct_videos.select('video_id', 'max(views)').where(distinct_videos.channel_title == title).collect()
        ]])

    res = spark.createDataFrame(data=data, schema=schema)

    out = dict()
    out['channels'] = [json.loads(_) for _ in res.toJSON().collect()]
    with open(f'{RESULTS_PATH}/task_4.json', "w") as outfile:
        json.dump(out, outfile)

    return res


def task5(df):
    schema = T.StructType([
        T.StructField("channel_name", T.StringType()),
        T.StructField("total_trending_days", T.StringType()),
        T.StructField("video_days", T.ArrayType(
            T.StructType([
                T.StructField("video_id", T.StringType()),
                T.StructField("video_title", T.StringType()),
                T.StructField("trending_days", T.LongType()),
            ])
        )),
    ])

    data = []
    channel_trending_days = [
        list(_) for _ in df.groupby('channel_title').count() \
            .sort("count", ascending=False).limit(10).collect()
    ]

    for channel, total_trending_days in channel_trending_days:
        trending_videos = df.filter(df.channel_title == channel).groupBy(
            "video_id", "title").agg(F.count(F.col("date").alias('trending_days')))

        data.append([channel, str(total_trending_days), [
            list(_) for _ in trending_videos.collect()
        ]])

    res = spark.createDataFrame(data=data, schema=schema)

    out = dict()
    out['channels'] = [json.loads(_) for _ in res.toJSON().collect()]
    with open(f'{RESULTS_PATH}/task_5.json', "w") as outfile:
        json.dump(out, outfile)

    return res


def task6(df):
    schema = T.StructType([
        T.StructField("category_id", T.StringType()),
        T.StructField("category_name", T.StringType()),
        T.StructField("videos", T.ArrayType(
            T.StructType([
                T.StructField("video_id", T.StringType()),
                T.StructField("video_title", T.StringType()),
                T.StructField("ratio_likes_dislike", T.FloatType()),
                T.StructField("views", T.LongType()),
            ])
        )),
    ])

    data = []
    for category_id, category_title in get_categories().items():
        top_videos = df.filter((df.views >= 100000) & (df.category_id == category_id)) \
            .groupBy('video_id', 'title', 'views').agg(F.max(F.col('likes') / F.col("dislikes")).alias('ratio')) \
            .sort(desc('ratio')).limit(10)

        data.append([category_id, category_title, [
            list(_) for _ in top_videos.select('video_id', 'title', 'ratio', 'views').collect()
        ]])

    res = spark.createDataFrame(data=data, schema=schema)

    out = dict()
    out['categories'] = [json.loads(_) for _ in res.toJSON().collect()]
    with open(f'{RESULTS_PATH}/task_6.json', "w") as outfile:
        json.dump(out, outfile)

    return res


def convert_date(x):
    tmp = x.split('.')
    return udf(str(datetime.datetime(int(tmp[0]), int(tmp[1]), int(tmp[2]))))


def get_categories():
    data = json.load(open(CATEGORIES_DATA_PATH))
    out = dict()
    for i in data['items']:
        out[i['id']] = i['snippet']['title']
    return out


if __name__ == '__main__':
    spark = SparkSession.builder.appName("appTest").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.option('header', 'true').csv(DATASET_PATH).na.drop(subset=["description", "trending_date"])
    df = df.withColumn("likes", df["likes"].cast(IntegerType()))
    df = df.withColumn("dislikes", df["dislikes"].cast(IntegerType()))
    df = df.withColumn("views", df["views"].cast(IntegerType()))
    df = df.withColumn('date', to_date(col("trending_date"), "yy.dd.MM").cast("date"))

    tasks = [task1, task2, task4, task5, task6]
    for task in tasks:
        task_df = task(df)
        print(f'\n[result]: {task.__name__}: ')
        task_df.printSchema()
        task_df.show(truncate=True)
