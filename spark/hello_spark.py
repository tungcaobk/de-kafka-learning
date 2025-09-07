from pyspark.sql import SparkSession


def main():
    print("Starting...")

    spark = SparkSession.builder \
        .appName("HelloSpark") \
        .master("spark://spark:7077") \
        .getOrCreate()

    print("Connected Spark")
    data_list = [("Phuong", 31),
                 ("Huy", 31),
                 ("Bee", 25)]

    df = spark.createDataFrame(data_list).toDF("Name", "Age")
    df.show()

    print("Stop HelloSpark")
    spark.stop()

if __name__ == "__main__":
    main()