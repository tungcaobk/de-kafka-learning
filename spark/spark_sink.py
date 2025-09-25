from itertools import groupby

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col, to_date, spark_partition_id
from custom_log import setup_logger
from datetime import datetime

logging = setup_logger("spark_sql", "spark_sql.log")


def main():
    print("Starting spark sql...")

    spark = SparkSession.builder \
        .appName("SparkSQL") \
        .master("spark://localhost:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.1") \
        .getOrCreate()

    logging.info("Connected SparkSQL")

    step1 = datetime.now()

    flight_df = spark.read.parquet("/home/tungcaobk97vip/spark-data/flight-time.parquet")

    step2 = datetime.now()
    logging.info(f"Load parquet file in {(step2 - step1).total_seconds()} second")

    logging.info("Flight df schema: ")
    # flight_df.printSchema()
    # flight_df.show()

    # logging.info(f"Num partition before: {flight_df.rdd.getNumPartitions()}")
    # flight_df.groupBy(spark_partition_id()).count().show()

    partitioned_df = flight_df.repartition(5)
    # logging.info(f"Num partition after: {partitioned_df.rdd.getNumPartitions()}")
    # partitioned_df.groupBy(spark_partition_id()).count().show()

    partitioned_df.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "/home/tungcaobk97vip/spark-data/avro/") \
        .save()

    partitioned_df.write \
        .format("json") \
        .mode("overwrite") \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("maxRecordsPerFile", 10000) \
        .option("path", "/home/tungcaobk97vip/spark-data/json/") \
        .save()

    check_avro_df = spark.read.format("avro").load("/home/tungcaobk97vip/spark-data/avro/")
    check_avro_df.show(10, truncate=False)

    logging.info("Stop Spark Join")
    spark.stop()


if __name__ == "__main__":
    main()
