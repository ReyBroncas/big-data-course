from pyspark.sql import SparkSession

DATASET_PATH = "./data/PS_20174392719_1491204439457_log.csv"

if __name__ == '__main__':
    spark = SparkSession.builder.appName("appTest").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.option('header', 'true').csv(DATASET_PATH)
    print(f'[result]: lines count: {df.count()}')
