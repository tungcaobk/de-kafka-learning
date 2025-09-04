from pyspark.sql import SparkSession
from custom_log import setup_logger

logging = setup_logger("hellospark", "hellospark.log")


def main():
    logging.info("Starting...")

    local_ip = "103.85.106.65"

    spark = SparkSession.builder \
        .appName("HelloSpark") \
        .master("spark://35.240.246.161:7077") \
        .config("spark.driver.host", local_ip) \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.driver.port", "9999")  \
        .config("spark.driver.blockManager.port", "10005")  \
        .getOrCreate()

    logging.info("Hello Spark")
    data_list = [("Phuong", 31),
                 ("Huy", 31),
                 ("Bee", 25)]

    df = spark.createDataFrame(data_list).toDF("Name", "Age")
    df.show()

    logging.info("Stop HelloSpark")
    spark.stop()

if __name__ == "__main__":
    main()